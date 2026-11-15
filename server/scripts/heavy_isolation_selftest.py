# -*- coding: utf-8 -*-
"""重活进程隔离长期回归自测（2026-11-15）。

零外部请求、不写业务库、不常驻、不依赖线上服务进程。跑法：
    PYTHONPATH=server .venv/Scripts/python.exe server/scripts/heavy_isolation_selftest.py

守的是这几条**不变量**（都是这轮改造里真正会踩的坑）：
1. 开关能一键回滚，关掉后一次子进程都不派。
2. 子进程只重建闭包，绝不起第二套预热/调度线程。
3. 被 `_warp_trading_only` 包装的 job 名字与交易日守卫都不能丢——丢了
   会「盘后重活全漏隔离」或「休市日误写影子表」。
4. 派发成功必须推进 `next_run`，否则同一个 job 每 10 秒重复派发。
5. 快照只带回**本次预热真改动过**的缓存，且不能覆盖主进程的 SWR 失效
   代数；空快照绝不能把已算好的缓存清成空壳。
6. 父进程换了一代（boot 不匹配）时子进程必须自尽，避免孤儿双调度。
"""
from __future__ import annotations

import ast
import os
import pickle
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]          # 仓库根
SERVER = ROOT / 'server'
sys.path.insert(0, str(SERVER))

_PASS = 0
_FAIL = 0


def check(name: str, cond, detail: str = '') -> None:
    global _PASS, _FAIL
    if cond:
        _PASS += 1
        print(f'  OK  {name}')
    else:
        _FAIL += 1
        print(f'  FAIL {name}  {detail}')


def section(title: str) -> None:
    print(f'\n--- {title} ---')


def main() -> int:
    from core import heavy_runner as H
    from core import heavy_tasks as T
    from core import scheduler as S

    # ── A. 一键回滚开关 ────────────────────────────────────────────────
    section('A. 回滚开关')
    _old_env = os.environ.pop('HEAVY_ISOLATION', None)
    with tempfile.TemporaryDirectory() as td:
        _orig_off = H._OFF_SWITCH
        H._OFF_SWITCH = Path(td) / 'heavy_isolation.off'
        try:
            check('默认开启', H.enabled() is True)
            os.environ['HEAVY_ISOLATION'] = '0'
            check('环境变量 HEAVY_ISOLATION=0 关闭', H.enabled() is False)
            check('关闭时 submit 返回 False', H.submit('warm_core', 'warm') is False)
            check('关闭时 should_isolate 为 False', T.should_isolate('job_scan_trend') is False)
            os.environ.pop('HEAVY_ISOLATION', None)
            H._OFF_SWITCH.write_text('off', encoding='utf-8')
            check('标志文件存在时关闭', H.enabled() is False)
            check('标志文件存在时 submit 返回 False', H.submit('warm_core', 'warm') is False)
        finally:
            H._OFF_SWITCH = _orig_off
    if _old_env is not None:
        os.environ['HEAVY_ISOLATION'] = _old_env
    else:
        os.environ.pop('HEAVY_ISOLATION', None)
    check('开关复原后重新开启', H.enabled() is True)

    # ── B. 子进程注册表 ────────────────────────────────────────────────
    section('B. 子进程注册表 / 交易日守卫')
    import threading
    import schedule
    before_threads = {t.name for t in threading.enumerate()}
    S.run_scheduler(register_only=True)
    new_threads = {t.name for t in threading.enumerate()} - before_threads
    check('register_only 不起任何线程', not new_threads, str(new_threads))
    check('job 注册表非空（>=40）', len(S._JOB_REGISTRY) >= 40, str(len(S._JOB_REGISTRY)))
    check('预热注册表非空（>=7）', len(S._WARM_REGISTRY) >= 7, str(len(S._WARM_REGISTRY)))
    check('注册了 job_scan_trend', 'job_scan_trend' in S._JOB_REGISTRY)
    check('注册了 _prewarm_misc', '_prewarm_misc' in S._WARM_REGISTRY)

    wrapped = S._JOB_REGISTRY.get('job_rec_sim_batch_daily')
    inner = getattr(wrapped, 'func', wrapped)
    check('包装 job 的名字仍是原 job 名',
          getattr(wrapped, '__name__', '') == 'job_rec_sim_batch_daily',
          repr(getattr(wrapped, '__name__', None)))
    check('包装 job 仍带交易日守卫（_today_is_trading）',
          '_today_is_trading' in getattr(inner, '__code__', type('X', (), {'co_names': ()})).co_names,
          str(getattr(getattr(inner, '__code__', None), 'co_names', None)))
    check('包装 job 应被隔离', T.should_isolate('job_rec_sim_batch_daily') is True)

    for n in T.LOCAL_JOBS:
        check(f'白名单 {n} 不隔离', T.should_isolate(n) is False)
    for n in ('job_scan_trend', 'job_sync_history', 'job_generate_review',
              'job_rec_sim_daily', 'job_auction_snapshot'):
        check(f'重 job {n} 应隔离', T.should_isolate(n) is True)
    for n in ('', '_wrapped', 'warm_core', 'not_a_job'):
        check(f'非 job 名 {n!r} 不隔离', T.should_isolate(n) is False)

    # schedule 的 job_func 是 functools.partial：名字提取必须有 `.func` 兜底
    names = []
    for j in schedule.get_jobs():
        f = getattr(j, 'job_func', None)
        names.append(getattr(f, '__name__', None)
                     or getattr(getattr(f, 'func', None), '__name__', None))
    check('schedule 中可回收全部 job 名',
          all(n and n.startswith('job_') for n in names),
          str([n for n in names if not n or not n.startswith('job_')][:3]))

    # ── C. 调度接线：next_run 必须推进 ─────────────────────────────────
    section('C. 调度接线')
    src = (SERVER / 'core' / 'scheduler.py').read_text(encoding='utf-8')
    check('派发成功分支调用了 _schedule_next_run',
          'job._schedule_next_run()' in src and 'continue' in src)
    check('常驻保活线程不在一次性预热表',
          'prewarm-universe' not in S._PREWARM_TASK_BY_NAME
          and len(S._PREWARM_TASK_BY_NAME) == 6)
    check('submit_warm_rest 带 steps 参数',
          "extra={'steps': steps}" in src)

    # 真跑一轮 _run_pending_guarded：mock 掉派发与 job 执行，验证 next_run
    schedule.clear()
    calls = {'job': 0, 'submit': 0}

    def _fake_job():
        calls['job'] += 1

    j = schedule.every(60).seconds.do(_fake_job)
    j.next_run = __import__('datetime').datetime.now() - __import__('datetime').timedelta(seconds=1)
    _orig_should, _orig_submit = T.should_isolate, H.submit
    try:
        T.should_isolate = lambda n: True
        H.submit = lambda *a, **k: (calls.__setitem__('submit', calls['submit'] + 1) or True)
        S._run_pending_guarded()
        check('派发成功时 job 原函数未在本进程执行', calls['job'] == 0, str(calls))
        check('派发成功时 submit 被调用 1 次', calls['submit'] == 1, str(calls))
        check('派发成功时 next_run 已推进到未来',
              j.next_run > __import__('datetime').datetime.now(), str(j.next_run))
        # 回滚路径：不派发 → 必须真的跑原函数
        T.should_isolate = lambda n: False
        j.next_run = __import__('datetime').datetime.now() - __import__('datetime').timedelta(seconds=1)
        S._run_pending_guarded()
        check('开关关闭时 job 在本进程执行', calls['job'] == 1, str(calls))
    finally:
        T.should_isolate, H.submit = _orig_should, _orig_submit
        schedule.clear()

    # ── D. 快照差异判据与回填安全 ──────────────────────────────────────
    section('D. 缓存快照')
    from app.api import routes_state as R
    before = T._snapshot_state()
    check('空改动不产出任何快照', len(T._collect_snapshots(before)['stores']) == 0)
    probe = ('__selftest_probe__',)
    R._MISC_SWR_CACHE[probe] = {'ts': 1.0, 'data': 'x'}
    snap = T._collect_snapshots(before)
    check('只改动 1 份就只带 1 份', len(snap['stores']) == 1, str(list(snap['stores'])))
    check('带回的是被改动的那份',
          'app.api.routes_state:_MISC_SWR_CACHE' in snap['stores'])
    R._MISC_SWR_CACHE.pop(probe, None)

    # __swr_gen__ 是主进程自己的失效代数，不能被子进程初始值覆盖
    R._MISC_SWR_CACHE['__swr_gen__'] = {probe: 7}
    R._MISC_SWR_CACHE['keep'] = {'v': 1}
    n = T._apply_snapshots({'stores': {
        'app.api.routes_state:_MISC_SWR_CACHE': {probe: {'ts': 2.0, 'data': 'y'}}}})
    check('回填成功 1 份', n == 1, str(n))
    check('新数据已换入', R._MISC_SWR_CACHE[probe]['data'] == 'y')
    check('主进程 SWR 失效代数保留', R._MISC_SWR_CACHE['__swr_gen__'] == {probe: 7},
          str(R._MISC_SWR_CACHE.get('__swr_gen__')))
    check('旧 key 已清空', 'keep' not in R._MISC_SWR_CACHE)
    R._MISC_SWR_CACHE.clear()
    check('空快照不清空主进程缓存', T._apply_snapshots({'stores': {}}) == 0)

    # 三个 apply_snapshot：空值绝不能把有效缓存覆盖掉
    from services.data import liquidity, universe
    from services.support import settings_preview as SP
    universe._SCOPE_CACHE.update(ts=time.time(), value=['600001'])
    universe.apply_snapshot({'scope': [], 'db_symbols': []})
    check('universe 空快照不覆盖已有范围', universe._SCOPE_CACHE['value'] == ['600001'])
    liquidity._STATS, liquidity._STATS_TS = {'600001': {'a': 1.0}}, time.time()
    liquidity.apply_snapshot({'stats': None})
    check('liquidity 空快照不覆盖已有统计', liquidity._STATS == {'600001': {'a': 1.0}})
    SP._SUMMARY.update(ts=time.time(), data={'v': 1})
    SP.apply_snapshot({'summary': None})
    check('settings_preview 空快照不覆盖已有摘要', SP._SUMMARY['data'] == {'v': 1})

    # warm_core 认领 fp 必须用主进程的 cache_epoch
    S._KEEPWARM_STATE['fp'] = ''
    T.apply_warm('warm_core', {
        'scope': ['600001'], 'selectable': ['600001'],
        'db_symbols': ['600001'], 'db_symbols_fp': 'fp1',
        'excluded': set(), 'stats': {}, 'summary': {'v': 2},
        'kline_fp': 'KLINE_FP',
    })
    check('warm_core 回填后认领 fp',
          S._KEEPWARM_STATE['fp'] == 'KLINE_FP|%d' % universe.cache_epoch(),
          S._KEEPWARM_STATE['fp'])
    check('warm_core 回填了摘要', SP._SUMMARY['data'] == {'v': 2})
    check('warm_core 的 compute 不依赖额外参数',
          'kline_fingerprint' in (SERVER / 'core' / 'heavy_tasks.py').read_text(encoding='utf-8'))

    # ── E. 子进程安全 ──────────────────────────────────────────────────
    section('E. 子进程自尽 / 入口约束')
    worker_src = (SERVER / 'core' / 'heavy_worker.py').read_text(encoding='utf-8')
    check('子进程入口有父进程 watchdog', '_parent_watch' in worker_src)
    check('子进程入口不 import app.main', 'import app.main' not in worker_src)
    check('快照用原子替换', 'os.replace' in worker_src)
    runner_src = (SERVER / 'core' / 'heavy_runner.py').read_text(encoding='utf-8')
    check('派发器有并发上限', '_MAX_WORKERS' in runner_src)
    check('派发器有超时终止', 'proc.kill()' in runner_src)
    # 用 AST 查**真实调用**：注释/docstring 里出现这个词不算数
    # （2026-11-13 重启按钮自测踩过同款误判，别再按字符串搜）。
    _tree = ast.parse(runner_src)
    _bad = [n.func.id for n in ast.walk(_tree)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
            and n.func.id in ('taskkill', 'system', 'popen', 'exec', 'eval')]
    check('派发器不调用 taskkill / system（AST 真实调用）', not _bad, str(_bad))
    check('失败也补做写后失效',
          'heavy_tasks.after_job(task_id, ok=' in runner_src)

    # 真起一个子进程，故意给不匹配的 boot → 必须在 ~2s 内自尽
    env = dict(os.environ, PYTHONPATH=str(SERVER))
    out = Path(tempfile.gettempdir()) / 'heavy_watchdog_probe.pkl'
    try:
        p = subprocess.Popen(
            [sys.executable, '-u', '-m', 'core.heavy_worker',
             '--task', 'job_scan_ma10', '--kind', 'job',
             '--out', str(out), '--boot', 'FAKE_BOOT_SELFTEST'],
            cwd=str(SERVER), env=env, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True)
        try:
            p.communicate(timeout=15)
            rc = p.returncode
        except subprocess.TimeoutExpired:
            p.kill()
            p.communicate()
            rc = 'TIMEOUT'
        check('boot 不匹配时子进程自尽 rc=3', rc == 3, str(rc))
    except Exception as e:
        check('boot 不匹配时子进程自尽 rc=3', False, str(e))
    finally:
        out.unlink(missing_ok=True)

    # ── F. 接线落盘（防多会话覆盖） ───────────────────────────────────
    section('F. 关键接线落盘')
    for path, needle in (
        ('server/app/main.py', "heavy_runner.submit('job_scan_trend'"),
        ('server/app/main.py', "heavy_runner.submit('job_generate_review'"),
        ('server/services/strategy/trend.py', "heavy_runner.submit('job_scan_trend'"),
        ('server/services/support/settings_preview.py', "heavy_runner.submit('warm_core'"),
        ('server/core/scheduler.py', 'submit_warm_core'),
        ('server/core/scheduler.py', '_build_job_registry(locals())'),
    ):
        check(f'{path} 含 {needle[:34]}',
              needle in (ROOT / path).read_text(encoding='utf-8'))

    # main.py 的 create_task 不得缩进到 async 子函数体内造成自递归
    tree = ast.parse((ROOT / 'server/app/main.py').read_text(encoding='utf-8'))
    bad = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for sub in ast.walk(node):
                if (isinstance(sub, ast.Call) and isinstance(sub.func, ast.Attribute)
                        and sub.func.attr == 'create_task' and sub.args
                        and isinstance(sub.args[0], ast.Call)
                        and isinstance(sub.args[0].func, ast.Name)
                        and sub.args[0].func.id == node.name):
                    bad.append(node.name)
    check('main.py 无 create_task 自递归', not bad, str(bad))

    print(f'\n结果：{_PASS}/{_PASS + _FAIL} 通过，{_FAIL} 失败')
    return 1 if _FAIL else 0


if __name__ == '__main__':
    sys.exit(main())
