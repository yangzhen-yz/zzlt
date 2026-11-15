# -*- coding: utf-8 -*-
"""重活任务注册表：哪些任务外包、子进程跑什么、主进程回填什么。

这个文件被**两个进程** import：HTTP 主进程只调 `should_isolate()` /
`after_job()` / `apply_warm()`；子进程只调 `run_job()` / `compute_warm()`。
两边都不得在模块顶层 import 任何重模块，否则冷启动会把隔离的收益吃掉。

## 隔离范围的选择
定时任务 (`job_*`) 的结果本来就落 SQLite 表或推企微，**返回值全部被
`_run_pending_guarded()` 丢弃**，所以外包后主进程唯一要补的是「写后失效」——
子进程里 invalidate 自己的内存缓存毫无意义，主进程那几份还会挂着旧值。
`after_job()` 就是补这一刀。

少数 job 必须留在主进程，见 `LOCAL_JOBS` 注释。

## 回滚
`heavy_runner.enabled()` 关掉后，`should_isolate()` 恒为 False，调度器
自然走原来的 `job.run()`，业务函数一字未动。
"""
from __future__ import annotations

import time
from typing import Any, Dict, Iterable

# 必须留在 HTTP 主进程的定时任务：
# · api_usage_flush：接口用量是主进程 `api_usage` 的内存计数器 + 全局
#   monkeypatch，子进程读不到；搬走会丢计数、且两处 flush 互相覆盖。
# · db_maintenance / db_backup：VACUUM 与在线备份要求库独占，且本来就是
#   独立脚本/子进程，主进程只是拉一下，没有重活。
# · sync_industry_map / matrix_rerun / eval_lab_weekly：内部已用
#   subprocess.Popen 在独立进程干活，主进程这里只有一次 spawn。
LOCAL_JOBS = frozenset({
    'job_api_usage_flush',
    'job_db_maintenance',
    'job_db_backup',
    'job_sync_industry_map',
    'job_matrix_rerun',
    'job_eval_lab_weekly',
})

# 各 job 完成后主进程该失效哪些缓存。宁可多失效一次（下次请求冷算，SWR 有
# stale 兜底），也不能让主进程继续显示子进程已改掉的旧数据。
_REVIEW_JOBS = frozenset({'job_generate_review'})
_RECOMMEND_JOBS = frozenset({
    'job_generate_recommendations', 'job_pre_close_snapshot', 'job_backtest',
    'job_rec_sim_daily', 'job_rec_sim_batch_daily', 'job_recommend_review',
    'job_trend_backtest', 'job_auto_optimize', 'job_shadow_eval_daily',
})
_SCAN_JOBS = frozenset({
    'job_scan_trend', 'job_build_candidate_pool', 'job_scan_ma10',
})

WARM_TASKS = frozenset({'warm_core', 'warm_rest'})

# 子进程跑完一次性预热后要整体搬回主进程的缓存。**显式白名单 + 逐个试
# pickle**：宁可漏一份让请求侧冷算，也不能把不可序列化对象塞进快照把整个
# 回填炸掉；名字写错也只是一条日志，不会让预热失败。
SNAPSHOT_STORES = (
    'app.api.routes_state:_MISC_SWR_CACHE',
    'app.api.routes_state:_DAILY_REVIEWS_CACHE',
    'app.api.routes_state:_HOT_SCAN_CACHE',
    'app.api.routes_state:_DAILY_RECS_CACHE',
    'app.api.routes_state:_REC_REVIEW_CACHE',
    'app.api.routes_state:_REC_SIM_CACHE',
    'app.api.routes_state:_EXCESS_CACHE',
    'services.data.kline_history:_VOL_MAP_CACHE',
    'services.data.kline_history:_AVAILABLE_CACHE',
    'services.ops.action_desk:_DESK_CACHE',
    'services.ops.theme_index:_CACHE',
    'services.ops.cls_pool:_CACHE',
    'services.data.fundflow:_CACHE',
    'services.data.fundflow:_SINA_SNAPSHOT_CACHE',
    'services.data.fundflow:_SINA_RANK_CACHE',
    'services.data.fundflow:_SINA_IND_CACHE',
)


def _snapshot_state() -> Dict[str, bytes]:
    """子进程侧：预热前先给每份缓存留一个字节快照，用作「本次是否真填过」的判据。"""
    import importlib
    import pickle
    state: Dict[str, bytes] = {}
    for spec in SNAPSHOT_STORES:
        try:
            mod, attr = spec.split(':', 1)
            obj = getattr(importlib.import_module(mod), attr)
            if isinstance(obj, dict):
                state[spec] = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
        except Exception:
            pass
    return state


def _collect_snapshots(before: Dict[str, bytes]) -> Dict[str, Any]:
    """子进程侧：只把**本次预热实际改动过**的缓存收进快照。

    不能「只要非空就收」：不少 store 在模块导入时就带初始值（例如
    `_DAILY_RECS_CACHE` 的 `{'ts':0,'payload':None}`、`_AVAILABLE_CACHE`），
    若某轮预热各段全部失败，把这些初始值回填到主进程会把它已经算好的
    缓存覆盖成空壳 —— 页面反而从「有数据」变成「空白」。逐字节比对预热
    前后，没变的一律不带回。
    """
    import importlib
    import pickle
    stores: Dict[str, Any] = {}
    for spec in SNAPSHOT_STORES:
        try:
            mod, attr = spec.split(':', 1)
            obj = getattr(importlib.import_module(mod), attr)
            if not isinstance(obj, dict) or not obj:
                continue
            blob = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
            if before.get(spec) == blob:
                continue
            stores[spec] = obj
        except Exception as e:
            print(f'[worker] 快照跳过 {spec}: {type(e).__name__}: {e}')
    return {'stores': stores}


def _apply_snapshots(payload: Dict[str, Any]) -> int:
    """主进程侧：把快照原子换入本进程的缓存对象。

    `__swr_gen__` 是 SWR 的写后失效代数，属于**主进程自己的状态**，不能被
    子进程的初始值覆盖（否则正在跑的后台刷新会拿着旧代数写回脏数据，
    即 11-04 那个 ABA 坑）。所以先摘出来、换完再放回去。
    """
    import importlib
    n = 0
    for spec, value in (payload.get('stores') or {}).items():
        try:
            mod, attr = spec.split(':', 1)
            store = getattr(importlib.import_module(mod), attr)
            if not isinstance(store, dict) or not isinstance(value, dict) or not value:
                continue
            gen = store.get('__swr_gen__')
            store.clear()
            store.update(value)
            if gen is not None:
                store['__swr_gen__'] = gen
            n += 1
        except Exception as e:
            print(f'[重活隔离] 快照回填 {spec} 失败（该项由请求侧冷兜底）: {e}')
    return n


def should_isolate(task_id: str) -> bool:
    """这个定时 job 该不该派到子进程。每轮调度判一次，支持即时回滚。"""
    if not task_id.startswith('job_') or task_id in LOCAL_JOBS:
        return False
    from core import heavy_runner
    return heavy_runner.enabled()


# ─────────────────── 子进程侧 ───────────────────


def run_job(task_id: str) -> None:
    """在子进程里执行原定时任务函数（**同一个函数对象，不是复制实现**）。

    `run_scheduler(register_only=True)` 只重建 job 闭包并填 `_JOB_REGISTRY`，
    不起预热线程、不进主循环、不起观察线程；否则会在子进程里再跑一整套调度。
    """
    from core import scheduler
    scheduler.run_scheduler(register_only=True)
    fn = scheduler._JOB_REGISTRY.get(task_id)
    if fn is None:
        raise KeyError(f'定时任务 {task_id} 未在 _JOB_REGISTRY 注册')
    fn()


def compute_warm(task_id: str, steps: str = '') -> Dict[str, Any]:
    """在子进程里把三层缓存的值算出来，返回可 pickle 的纯数据。

    子进程里的缓存赋值本身没有意义（进程一退就没了），**取出来**的对象
    才是要交给主进程的东西。这也是为什么这里调的是原来的公共函数而不是
    手抄一遍算法：口径永远跟线上一致。
    """
    if task_id == 'warm_core':
        from app.config import KLINE_HISTORY_DB_PATH
        from services.data import liquidity, universe
        from services.support import data_version, settings_preview

        universe.selectable_count()
        liquidity.stats_map(force=True)
        settings_preview.compute_summary()
        return {
            'scope': list(universe._SCOPE_CACHE['value']),
            'selectable': list(universe._SELECTABLE_CACHE['value']),
            'db_symbols': list(universe._DB_SYMBOLS_CACHE['value']),
            'db_symbols_fp': universe._DB_SYMBOLS_CACHE['fp'],
            'excluded': set(universe._EXCLUDED_CACHE['value']),
            'stats': liquidity._STATS,
            'summary': settings_preview._SUMMARY['data'],
            'kline_fp': data_version.kline_fingerprint(KLINE_HISTORY_DB_PATH),
        }
    if task_id == 'warm_rest':
        # 一次性启动预热（vol / quote-deps / misc / hot-scan / excess / heavy）。
        # 主进程把本轮该跑的段名用逗号传进来；子进程按原顺序**串行**跑，
        # 继承改造前 `_PREWARM_GATE` 的串行语义（不并发抢 SQLite / 东财）。
        # 用 `fn.__wrapped__` 绕过 `_serial_prewarm` 装饰器的 15s 启动延迟与
        # 2s 项间空窗——那是主进程为避免挤占请求才加的，子进程里纯属白等。
        from core import scheduler
        scheduler.run_scheduler(register_only=True)
        before = _snapshot_state()
        done = []
        for name in [s for s in steps.split(',') if s]:
            fn = scheduler._WARM_REGISTRY.get(name)
            if fn is None:
                print(f'[worker] 预热段 {name} 未注册，跳过')
                continue
            raw = getattr(fn, '__wrapped__', fn)
            t0 = time.time()
            try:
                raw()
                done.append('%s:%.1fs' % (name, time.time() - t0))
            except Exception as e:
                # 单段失败不影响其它段，也不让整个快照作废（各段失败时原本
                # 就只打日志、留请求侧冷兜底）。
                done.append('%s:FAILED(%s)' % (name, type(e).__name__))
                print(f'[worker] 预热段 {name} 失败: {type(e).__name__}: {e}')
        payload = _collect_snapshots(before)
        payload['done'] = done
        return payload
    raise KeyError(f'未知预热任务 {task_id}')


# ─────────────────── 主进程侧 ───────────────────


def after_job(task_id: str, ok: bool) -> None:
    """子进程写库完成后，在主进程补做内存缓存失效。

    `ok=False` 也照样失效：子进程可能已经写了一半库才失败，此时让主进程
    继续挂旧值比多冷算一次更危险。失效本身是纯内存的微秒级操作。
    """
    from app.api import routes_state as R
    from services.support import swr

    if task_id in _REVIEW_JOBS:
        R._invalidate_daily_reviews_cache()
    if task_id in _RECOMMEND_JOBS:
        R._invalidate_daily_recs_cache()
        for store in (R._REC_SIM_CACHE, R._REC_REVIEW_CACHE, R._EXCESS_CACHE):
            swr.invalidate(store)
    if task_id in _SCAN_JOBS:
        R._invalidate_hot_scan_cache()
    if task_id == 'job_scan_trend':
        # 定时扫描与「页面 stale 触发的后台重扫」都派到子进程。子进程只写
        # `trend_scan_results` 表，主进程的 `_last_scan_result` 还是旧快照，
        # 不回读会让 /api/trend/scan 一直显示上一轮结果（页面会以为没扫）。
        # 顺带复位 SWR 单飞标志——原来由 `_run()` 的 finally 负责，现在
        # 那一段跑在子进程里，复位必须留在主进程，否则后续重扫再也起不来。
        try:
            from db.database import load_trend_scan_results
            from services.strategy import trend as T
            import time as _time
            cached = load_trend_scan_results()
            if cached and cached.get('stocks'):
                T._last_scan_result = cached
                T._last_scan_date = cached.get('date', '')
                T._last_scan_ts = _time.time()
            T._swr_refresh_inflight = False
        except Exception as e:
            print(f'[重活隔离] job_scan_trend 结果回读失败（页面下次请求会读库兜底）: {e}')


def apply_warm(task_id: str, payload: Dict[str, Any]) -> None:
    """把子进程算好的缓存值原子换入主进程，并认领这一版数据指纹。

    不认领指纹的话，540s 后的保活轮会以为「从没算过」再白重算一次，
    等于把刚省下的降级窗口又请回来（11-15 首轮保活 46s 的同款坑）。
    """
    if task_id == 'warm_rest':
        n = _apply_snapshots(payload)
        print(f'[重活隔离] 启动预热快照回填 {n} 份缓存｜'
              f'子进程分段: {", ".join(payload.get("done", []))}')
        return
    if task_id != 'warm_core':
        raise KeyError(f'未知预热任务 {task_id}')
    from services.data import liquidity, universe
    from services.support import settings_preview
    from core import scheduler

    universe.apply_snapshot(payload)
    liquidity.apply_snapshot(payload)
    settings_preview.apply_snapshot(payload)
    # epoch 必须用**主进程**的：子进程里永远是 0，配置改动代数会丢。
    scheduler._KEEPWARM_STATE['fp'] = '%s|%d' % (
        payload['kline_fp'], universe.cache_epoch())


def isolated_job_names() -> Iterable[str]:
    """自测用：列出会被隔离的 job（不触发任何 import）。"""
    return sorted(n for n in LOCAL_JOBS)
