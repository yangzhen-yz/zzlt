# -*- coding: utf-8 -*-
"""重活子进程入口：`python -m core.heavy_worker --task ... --kind ...`。

只由 `heavy_runner.py` 拉起，不接受外部输入。设计约束：

1. **绝不 import `app.main`**——那会建 FastAPI app、装中间件、起 lifespan，
   等于在子进程里再起半个服务。这里只 import 任务所需的最小模块。
2. **父进程消失就自尽**：独立的 watchdog 线程每 2s 读一次
   `server/data/heavy_runner.heartbeat`，比对 boot 标记与时间戳。HTTP 进程
   被看护重启后 boot 会变，旧子进程随即退出，避免孤儿双写/双调度。
3. 结果用 pickle 原子写盘（`tmp` → `os.replace`），主进程只在看到
   `rc == 0` 且文件存在时才读，不会读到半截文件。
4. 所有异常都打完整 traceback 到 stdout（被父进程收进
   `server/logs/heavy_worker/<token>.log`）并以非零码退出，不静默吞掉。
"""
from __future__ import annotations

import argparse
import os
import pickle
import sys
import threading
import time
import traceback
from pathlib import Path

_SERVER_DIR = Path(__file__).resolve().parent.parent
_HEARTBEAT = _SERVER_DIR / 'data' / 'heavy_runner.heartbeat'
_WATCH_INTERVAL = 2
_WATCH_STALE = 30


def _parent_watch(boot: str) -> None:
    """父进程 watchdog：心跳没了 / boot 换了 / 心跳过期 ⇒ 立刻退出。

    Windows 上 `os.getppid()` 在父进程死后可能仍返回旧 PID，不能依赖；
    用文件心跳是本机能做到的最可靠判据。

    2026-11-15：**首轮立即检查，不再先睡 2 秒**。原来若任务在 2 秒内跑完
    （休市日带交易日守卫的 job 秒返回），进程会正常退出而根本没等到检查；
    父进程已换代的子进程本就该启动即自尽，不该白跑一轮重活。
    `heavy_runner._start_heartbeat_once()` 保证 Popen 前心跳已落盘，
    所以首次检查不会误杀；仅对「心跳文件尚未生成」留一轮宽限。
    """
    while True:
        try:
            raw = _HEARTBEAT.read_text(encoding='utf-8').strip().split()
            if len(raw) < 2 or raw[0] != boot:
                os._exit(3)
            if time.time() - float(raw[1]) > _WATCH_STALE:
                os._exit(3)
        except FileNotFoundError:
            pass        # 极端情况：主进程刚起、首笔心跳还没落，等下一轮
        except Exception:
            os._exit(3)
        time.sleep(_WATCH_INTERVAL)


def _write_snapshot(out: Path, payload: object) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix('.tmp')
    with open(tmp, 'wb') as f:
        pickle.dump(payload, f, protocol=pickle.HIGHEST_PROTOCOL)
    os.replace(tmp, out)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--task', required=True)
    ap.add_argument('--kind', required=True, choices=('job', 'warm'))
    ap.add_argument('--out', required=True)
    ap.add_argument('--boot', required=True)
    ap.add_argument('--steps', default='',
                    help='warm_rest 要在子进程里串行跑哪些预热段，逗号分隔')
    args = ap.parse_args()

    threading.Thread(target=_parent_watch, args=(args.boot,), daemon=True,
                     name='heavy-parent-watch').start()

    from core import heavy_tasks
    out = Path(args.out)
    t0 = time.time()
    print(f'[worker] 开始 task={args.task} kind={args.kind} pid={os.getpid()}', flush=True)
    try:
        if args.kind == 'job':
            heavy_tasks.run_job(args.task)
            payload = {'ok': True, 'task': args.task}
        else:
            payload = heavy_tasks.compute_warm(args.task, steps=args.steps)
        _write_snapshot(out, payload)
        print(f'[worker] 完成 task={args.task} 耗时 {time.time() - t0:.1f}s', flush=True)
        return 0
    except SystemExit:
        raise
    except BaseException:
        print(f'[worker] 失败 task={args.task} 耗时 {time.time() - t0:.1f}s', flush=True)
        traceback.print_exc(file=sys.stdout)
        sys.stdout.flush()
        try:
            out.unlink(missing_ok=True)
        except OSError:
            pass
        return 1


if __name__ == '__main__':
    sys.exit(main())
