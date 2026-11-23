# -*- coding: utf-8 -*-
"""SWR 失效代数表契约的长期回归自测（2026-11-18）。

起因（线上实拍）：重活子进程跑完推荐类 job 后，主进程 `heavy_tasks.after_job()`
对 `_REC_REVIEW_CACHE` 调 `swr.invalidate(store)`（整体失效）；`invalidate()`
把代数表 `__swr_gen__` **写进同一个 dict**。随后 `/api/recommend-review?days=30`
冷算完写缓存时，手写的 `_remember_rec_review` 遍历整张表取 `v['ts']` →
`KeyError: 'ts'` → 接口 500，且下一笔请求必现（当日实测 14 次，只能靠重启恢复）。

跑法（零外部请求、不写业务库、不常驻）：
    PYTHONPATH=server python server/scripts/swr_store_contract_selftest.py

守的不变量：
1. `swr.invalidate(store)` 之后，手写缓存助手写缓存不得抛异常；
2. **反证**：把 `_remember_rec_review` 换回老写法，同一场景必须抛 `KeyError('ts')`
   —— 证明这套用例真的能抓到这类回归，而不是恰好全绿；
3. 淘汰仍按 `used`、数据条数不超过上限、代数表必须原样保留；
4. 接口级：注入代数表后 `/api/recommend-review` 的处理函数必须成功返回；
5. 接线落盘：两处手写助手都真的经过 `_swr.data_items`（防多会话把修法覆盖回去）。
"""
from __future__ import annotations

import asyncio
import sys
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
    from services.support import swr
    from app.api import routes_state as R

    # ── A. data_items 自身口径 ─────────────────────────────────────────
    section('A. data_items 只认数据条目')
    store = {1: {'ts': 1.0, 'data': 'a'}, swr._GEN_KEY: {1: 3}, 'junk': 'str'}
    items = swr.data_items(store)
    check('排除代数表', all(k != swr._GEN_KEY for k, _ in items), str(items))
    check('排除非 dict 值', all(isinstance(v, dict) for _, v in items), str(items))
    check('真实数据条目留下', [k for k, _ in items] == [1], str(items))

    # ── B. 线上场景复原：invalidate 之后再写缓存 ───────────────────────
    section('B. invalidate 后写缓存不得抛（复盘汇总）')
    backup = dict(R._REC_REVIEW_CACHE)
    try:
        R._REC_REVIEW_CACHE.clear()
        R._remember_rec_review(30, {'available': True}, 1000.0)
        swr.invalidate(R._REC_REVIEW_CACHE)              # 与 after_job 同一调用
        check('代数表确实进了 store', isinstance(R._REC_REVIEW_CACHE.get(swr._GEN_KEY), dict),
              str(list(R._REC_REVIEW_CACHE)))
        try:
            R._remember_rec_review(30, {'available': True, 'n': 2}, 2000.0)
            check('invalidate 后 _remember_rec_review 不抛', True)
        except Exception as e:
            check('invalidate 后 _remember_rec_review 不抛', False, f'{type(e).__name__}: {e}')
        check('新值已写入', (R._REC_REVIEW_CACHE.get(30) or {}).get('data') == {'available': True, 'n': 2},
              str(R._REC_REVIEW_CACHE.get(30)))
        check('代数表还在（try_refresh 仍能作废旧刷新）',
              isinstance(R._REC_REVIEW_CACHE.get(swr._GEN_KEY), dict))
    finally:
        R._REC_REVIEW_CACHE.clear()
        R._REC_REVIEW_CACHE.update(backup)

    # ── B-反证：老写法必须炸 ──────────────────────────────────────────
    section('B-反证. 老写法在同一场景必须抛 KeyError')
    backup = dict(R._REC_REVIEW_CACHE)
    orig_remember = R._remember_rec_review

    def _old_remember(key, data, ts):                     # 11-18 修复前原文
        for k in [k for k, v in R._REC_REVIEW_CACHE.items() if ts - v['ts'] >= 86400]:
            R._REC_REVIEW_CACHE.pop(k, None)
        while len(R._REC_REVIEW_CACHE) >= 8:
            least = min(R._REC_REVIEW_CACHE,
                        key=lambda k: R._REC_REVIEW_CACHE[k].get('used', R._REC_REVIEW_CACHE[k]['ts']))
            R._REC_REVIEW_CACHE.pop(least, None)
        R._REC_REVIEW_CACHE[key] = {'ts': ts, 'used': ts, 'data': data}

    try:
        R._REC_REVIEW_CACHE.clear()
        swr.invalidate(R._REC_REVIEW_CACHE)
        try:
            _old_remember(30, {'available': True}, 2000.0)
            check('老写法抛 KeyError', False, '没有抛')
        except KeyError as e:
            check('老写法抛 KeyError', True, str(e))
        except Exception as e:
            check('老写法抛 KeyError', False, f'抛的是 {type(e).__name__}: {e}')
    finally:
        R._REC_REVIEW_CACHE.clear()
        R._REC_REVIEW_CACHE.update(backup)

    # ── C. 淘汰口径：按 used、有上限、代数表保留 ───────────────────────
    section('C. 淘汰口径不变')
    backup = dict(R._REC_REVIEW_CACHE)
    try:
        R._REC_REVIEW_CACHE.clear()
        swr.invalidate(R._REC_REVIEW_CACHE)
        now = time.time()
        for i in range(R._REC_REVIEW_MAX_KEYS):
            R._REC_REVIEW_CACHE[f'k{i}'] = {'ts': now, 'used': now + i, 'data': i}
        R._REC_REVIEW_CACHE['k0']['used'] = now - 999      # k0 最久没被读过
        R._remember_rec_review('knew', {'x': 1}, now + 10)
        data_keys = [k for k, _ in swr.data_items(R._REC_REVIEW_CACHE)]
        check('条数不超过上限', len(data_keys) <= R._REC_REVIEW_MAX_KEYS, str(len(data_keys)))
        check('淘汰的是最久未读的', 'k0' not in data_keys, str(data_keys))
        check('新键在里面', 'knew' in data_keys)
        check('代数表未被淘汰', isinstance(R._REC_REVIEW_CACHE.get(swr._GEN_KEY), dict))
    finally:
        R._REC_REVIEW_CACHE.clear()
        R._REC_REVIEW_CACHE.update(backup)

    # ── D. 同源写法：批量扫描缓存 ─────────────────────────────────────
    section('D. _remember_hot_scan 同规')
    backup = dict(R._HOT_SCAN_CACHE)
    try:
        R._HOT_SCAN_CACHE.clear()
        swr.invalidate(R._HOT_SCAN_CACHE)
        try:
            R._remember_hot_scan('abc#1', {'stocks': []}, time.time())
            check('invalidate 后 _remember_hot_scan 不抛', True)
        except Exception as e:
            check('invalidate 后 _remember_hot_scan 不抛', False, f'{type(e).__name__}: {e}')
        check('写入成功', (R._HOT_SCAN_CACHE.get('abc#1') or {}).get('data') == {'stocks': []})
    finally:
        R._HOT_SCAN_CACHE.clear()
        R._HOT_SCAN_CACHE.update(backup)

    # ── E. 接口级：注入代数表后处理函数必须成功 ───────────────────────
    section('E. 接口级回归')
    import services.pick.recommend_review as RR
    from app.api.routers.recommend import get_recommend_review_api

    backup = dict(R._REC_REVIEW_CACHE)
    orig_summary = RR.get_review_summary
    try:
        RR.get_review_summary = lambda days: {'available': True, 'total': 0, 'fake': days}
        R._REC_REVIEW_CACHE.clear()
        swr.invalidate(R._REC_REVIEW_CACHE)
        try:
            resp = asyncio.run(get_recommend_review_api(30))
            ok = bool(resp and resp.get('success'))
            cached_at = (resp.get('data') or {}).get('cachedAt')
        except Exception as e:
            ok, cached_at = False, f'{type(e).__name__}: {e}'
        check('有代数表时接口返回 success', ok, str(cached_at))
        check('返回带数据时刻 cachedAt', isinstance(cached_at, str) and ':' in cached_at, str(cached_at))
    finally:
        RR.get_review_summary = orig_summary
        R._REC_REVIEW_CACHE.clear()
        R._REC_REVIEW_CACHE.update(backup)

    # ── F. 接线落盘（防多会话覆盖） ───────────────────────────────────
    section('F. 接线落盘')
    rs = (SERVER / 'app/api/routes_state.py').read_text(encoding='utf-8')
    check('复盘助手经 data_items', '_swr.data_items(_REC_REVIEW_CACHE)' in rs)
    check('扫描助手经 data_items', '_swr.data_items(_HOT_SCAN_CACHE)' in rs)
    sw = (SERVER / 'services/support/swr.py').read_text(encoding='utf-8')
    check('swr 暴露 data_items', 'def data_items(' in sw)

    print(f'\n结果：{_PASS}/{_PASS + _FAIL} 通过，{_FAIL} 失败')
    return 1 if _FAIL else 0


if __name__ == '__main__':
    sys.exit(main())
