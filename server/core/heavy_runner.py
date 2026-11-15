# -*- coding: utf-8 -*-
"""把 CPU / IO 密集的后台任务派发到**独立短命进程**（2026-11-15 GIL 根治）。

## 为什么需要
`run.py` 是单进程 uvicorn：HTTP 请求、7 段启动预热、每 540s 保活、盘中
每 30min 全市场扫描、盘后回测，全挤在**同一个解释器的同一个 GIL** 里。
后台线程跑 Python 层重循环时，请求连「进 app 之前」都要排队——11-15 实测
404 空路由 3.06s、自愈探活 8~14s。给调度器换线程池、加串行锁都只是把
排队换个地方，只有**换进程**才是物理隔离。

## 为什么是「短命子进程」而不是常驻 worker 进程
常驻 worker 会踩三个坑：① 看护 `GupiaoBackendWatchdog` 只认 3003 端口与
命令行 `run:app`，重启 HTTP 时按进程树结束只会杀掉 3003 那棵，worker 会
被漏掉 → 孤儿进程带着第二套调度器继续写库（11-09 双调度器事故同款）；② 定时 job 依赖的
内存去重状态在 worker 里，HTTP 侧看不到；③ 要扩看护链，得改仓库外的
SYSTEM 计划任务所用的看护脚本（本机路径属部署资产，不写进代码，见部署文档）。

短命子进程由主进程 spawn、跑完即退，**调度器仍只有一个**（在 HTTP 进程），
所有写后失效信号仍在主进程发出，看护链一行都不用改。父进程消失时，子进程
靠「心跳 + boot 标记」自尽，不会变成孤儿。

## 两种任务
- `kind='job'`：定时任务的副作用（结果本来就落 SQLite 表/推企微），主进程
  不需要返回值，子进程跑完后由 `heavy_tasks.after_job()` 补做缓存失效。
- `kind='warm'`：启动预热（结果要填主进程的内存缓存）。子进程用 pickle 写
  快照，主进程读回后调 `heavy_tasks.apply_warm()` 回填——赋值是毫秒级，
  不占 GIL。pickle 而不是 JSON：缓存值里有 tuple key / set / 嵌套 dict，
  JSON 会静默改类型，本机进程间传递用 pickle 才不丢语义。

## 一键回滚
环境变量 `HEAVY_ISOLATION=0` 或存在 `server/data/heavy_isolation.off`
即全部退回「主进程内同步执行」的旧行为；**无需改代码、无需重启**（调度器
每轮判一次）。两份开关分开是给「临时调试」和「停服维护」各留一条路。

回滚只影响派发，不影响任何业务函数本身——子进程执行的仍是同一个
`scheduler._JOB_REGISTRY` 里的原函数。
"""
from __future__ import annotations

import os
import pickle
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

_SERVER_DIR = Path(__file__).resolve().parent.parent
_DATA_DIR = _SERVER_DIR / 'data'
_SNAPSHOT_DIR = _DATA_DIR / 'heavy_snapshots'
_LOG_DIR = _SERVER_DIR / 'logs' / 'heavy_worker'
_HEARTBEAT = _DATA_DIR / 'heavy_runner.heartbeat'
_OFF_SWITCH = _DATA_DIR / 'heavy_isolation.off'

# 同时最多几个重子进程。设 2 是因为 SQLite WAL 只允许一个写者，重任务多半
# 带长事务；再多只会互相等锁，不会更快。
_MAX_WORKERS = max(1, int(os.getenv('HEAVY_MAX_WORKERS', '2')))
# 队列上限：防止子进程卡死后派发线程无限堆积（每个等待者占一条轻量线程）。
_MAX_QUEUED = 8
_DEFAULT_TIMEOUT = 1800          # 秒；矩阵重跑/全市场同步可跑十几分钟
_HEARTBEAT_INTERVAL = 5
_HEARTBEAT_STALE = 30

_PYTHON = sys.executable
_BOOT = '%d:%.6f' % (os.getpid(), time.time())

_state: Dict[str, Any] = {
    'sem': threading.BoundedSemaphore(_MAX_WORKERS),
    'queued': 0,
    'running': {},
    'lock': threading.Lock(),
    'hb_started': False,
    'stats': {'submitted': 0, 'ok': 0, 'failed': 0, 'skipped': 0},
}


def boot_id() -> str:
    """本 HTTP 进程的唯一标记，子进程据此判断父进程是不是已经换了一代。"""
    return _BOOT


def enabled() -> bool:
    """一键回滚开关。每轮调度判一次，关掉即刻退回旧行为。"""
    if os.getenv('HEAVY_ISOLATION', '1') == '0':
        return False
    try:
        return not _OFF_SWITCH.exists()
    except OSError:
        return True


def _log(msg: str) -> None:
    print(f'[重活隔离 {datetime.now():%H:%M:%S}] {msg}', flush=True)


def _ensure_dirs() -> None:
    _SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    _LOG_DIR.mkdir(parents=True, exist_ok=True)


def _write_heartbeat() -> None:
    try:
        _DATA_DIR.mkdir(parents=True, exist_ok=True)
        tmp = _HEARTBEAT.with_suffix('.tmp')
        tmp.write_text('%s %.6f\n' % (_BOOT, time.time()), encoding='utf-8')
        os.replace(tmp, _HEARTBEAT)
    except OSError:
        pass


def _heartbeat_loop() -> None:
    while True:
        _write_heartbeat()
        time.sleep(_HEARTBEAT_INTERVAL)


def _start_heartbeat_once() -> None:
    with _state['lock']:
        if _state['hb_started']:
            return
        _state['hb_started'] = True
    _write_heartbeat()                      # 必须在 Popen 之前先落一次
    threading.Thread(target=_heartbeat_loop, daemon=True,
                     name='heavy-runner-heartbeat').start()


def stats() -> Dict[str, int]:
    with _state['lock']:
        return dict(_state['stats'], running=len(_state['running']),
                    queued=_state['queued'])


def _bump(key: str) -> None:
    with _state['lock']:
        _state['stats'][key] = _state['stats'].get(key, 0) + 1


def _read_snapshot(path: Path) -> Any:
    """子进程写的 pickle 快照；只在主进程调用，绝不接受外部输入。"""
    return pickle.loads(path.read_bytes())


def _reap(token: str, task_id: str, kind: str, proc: subprocess.Popen,
          out: Path, log_path: Path, timeout: int) -> None:
    started = time.time()
    rc: Optional[int] = None
    try:
        try:
            rc = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            rc = -9
            _log(f'{task_id} 超时 {timeout}s，已终止（{token}）')
        elapsed = time.time() - started

        if kind == 'warm' and rc == 0:
            try:
                payload = _read_snapshot(out)
            except Exception as e:
                _log(f'{task_id} 快照读取失败（缓存不回填，请求侧冷兜底）: '
                     f'{type(e).__name__}: {e}')
            else:
                try:
                    from core import heavy_tasks
                    heavy_tasks.apply_warm(task_id, payload)
                    _log(f'{task_id} 回填完成（{elapsed:.1f}s）')
                except Exception as e:
                    _log(f'{task_id} 回填失败（请求侧冷兜底）: {type(e).__name__}: {e}')

        if kind == 'job':
            # 无论子进程成功与否都补做失效：子进程可能已写库再失败，主进程
            # 缓存若还挂着旧值就会静默显示过期数据。失败也失效是更安全的一侧。
            try:
                from core import heavy_tasks
                heavy_tasks.after_job(task_id, ok=(rc == 0))
            except Exception as e:
                _log(f'{task_id} 写后失效失败（TTL 兜底）: {type(e).__name__}: {e}')

        if rc == 0:
            _bump('ok')
            _log(f'{task_id} 子进程完成 rc=0（{elapsed:.1f}s，{token}）')
        else:
            _bump('failed')
            tail = ''
            try:
                tail = log_path.read_text(encoding='utf-8', errors='replace')[-600:]
            except OSError:
                pass
            _log(f'{task_id} 子进程失败 rc={rc}（{elapsed:.1f}s）'
                 f'｜日志 {log_path.name}｜{tail.strip()[:400]}')
    finally:
        with _state['lock']:
            _state['running'].pop(token, None)
        _state['sem'].release()
        try:
            out.unlink(missing_ok=True)
        except OSError:
            pass
        _prune_logs(rc == 0, log_path)


def _prune_logs(success: bool, path: Path) -> None:
    """子进程日志不无限堆积：成功即删（主日志已有耗时行），失败留 7 天。

    每天几十个定时 job，每个一份日志，不清理几周就是上千个文件。
    """
    try:
        if success:
            path.unlink(missing_ok=True)
            return
        cutoff = time.time() - 7 * 86400
        for p in _LOG_DIR.glob('*.log'):
            try:
                if p.stat().st_mtime < cutoff:
                    p.unlink(missing_ok=True)
            except OSError:
                pass
    except OSError:
        pass


def submit(task_id: str, kind: str, *, timeout: int = _DEFAULT_TIMEOUT,
           extra: Optional[Dict[str, str]] = None) -> bool:
    """派发一个重任务到独立进程；**立即返回**，不阻塞调度循环。

    返回 False 表示「本次没派出去」，调用方应退回主进程内同步执行。
    返回 True 表示已受理（子进程可能仍在排队，不保证已开始跑）。
    """
    if not enabled():
        _bump('skipped')
        return False
    with _state['lock']:
        if _state['queued'] >= _MAX_QUEUED:
            _bump('skipped')
            _log(f'{task_id} 队列已满（{_MAX_QUEUED}），退回主进程执行')
            return False
        _state['queued'] += 1
    _bump('submitted')
    _start_heartbeat_once()
    try:
        _ensure_dirs()
    except OSError as e:
        _log(f'{task_id} 目录准备失败，退回主进程执行: {e}')
        with _state['lock']:
            _state['queued'] -= 1
        return False

    token = '%s-%d' % (task_id, int(time.time() * 1000))
    # 先登记为 pending：submit() 一返回，stats()/wait_idle() 就能看到这个任务，
    # 不会出现「已受理但看起来什么都没在跑」的窗口（也会让测试提前误判完成）。
    with _state['lock']:
        _state['running'][token] = {'pid': 0, 'task': task_id,
                                   'since': time.time(), 'pending': True}
    out = _SNAPSHOT_DIR / (token + '.pkl')
    log_path = _LOG_DIR / (token + '.log')
    cmd = [_PYTHON, '-u', '-m', 'core.heavy_worker',
           '--task', task_id, '--kind', kind,
           '--out', str(out), '--boot', _BOOT]
    for k, v in (extra or {}).items():
        cmd += ['--' + k, str(v)]

    def _launch() -> None:
        proc = None
        try:
            _state['sem'].acquire()
            with _state['lock']:
                _state['queued'] -= 1
            kwargs: Dict[str, Any] = {
                'cwd': str(_SERVER_DIR),
                'stdin': subprocess.DEVNULL,
                'env': {**os.environ, 'HEAVY_WORKER': '1', 'HEAVY_BOOT': _BOOT},
            }
            if os.name == 'nt':
                kwargs['creationflags'] = getattr(subprocess, 'CREATE_NO_WINDOW', 0)
            fh = open(log_path, 'w', encoding='utf-8', errors='replace')
            try:
                proc = subprocess.Popen(cmd, stdout=fh, stderr=subprocess.STDOUT, **kwargs)
            finally:
                fh.close()
            with _state['lock']:
                _state['running'][token] = {'pid': proc.pid, 'task': task_id,
                                           'since': time.time()}
            _log(f'{task_id} 已派发 pid={proc.pid}（{token}）')
            _reap(token, task_id, kind, proc, out, log_path, timeout)
        except Exception as e:
            _log(f'{task_id} 派发失败，退回主进程执行: {type(e).__name__}: {e}')
            if proc is not None and proc.poll() is None:
                try:
                    proc.kill()
                except OSError:
                    pass
            with _state['lock']:
                _state['running'].pop(token, None)
            _state['sem'].release()
            try:
                out.unlink(missing_ok=True)
            except OSError:
                pass

    threading.Thread(target=_launch, daemon=True, name='heavy-launch-' + token).start()
    return True


def wait_idle(timeout: float = 0) -> bool:
    """测试/停机辅助：等到没有在跑或排队的重活。不用于生产路径。"""
    deadline = time.time() + timeout
    while True:
        with _state['lock']:
            if not _state['running'] and _state['queued'] == 0:
                return True
        if time.time() >= deadline:
            return False
        time.sleep(0.05)
