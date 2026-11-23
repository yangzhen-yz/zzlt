# -*- coding: utf-8 -*-
"""收评「涨停/跌停家数」备源回归自测（2026-11-19）。

起因（线上实拍）：11-19 东财全域闸门（IP 级封禁）在 15:12 收评生成时仍开着，
涨停池/跌停池被 `eastmoney_guard.skip('pools')` 跳过、clist 阈值降级也过不了闸门
→ 收评落库 **涨停0/跌停0**，页面涨跌停比显示 "--"。同期涨跌家数已走新浪备源
（696:4360），只有涨跌停家数没有备源。

跑法（零外部请求、不写业务库）：
    PYTHONPATH=server python server/scripts/review_limit_fallback_selftest.py

守的不变量：
1. 东财拿不到时，先走 `sentiment_alt.limit_counts()`（剔 ST），再退当天
   `limit_pool_daily` 落库快照；两级都失败才返回 None（调用方如实显示 0，不编数）。
2. **反证**：关掉 `SENTIMENT_ALT_FALLBACK` 且落库也为空时，必须拿不到任何家数
   —— 证明用例覆盖的正是"老路径全空"这个现场。
3. 备源返回的池子键名（c/n/p/zdp）能正确映射成收评字段，毫元→元换算正确。
4. 接线落盘：收评组装体带 `limitCountSource`、降级打印写明来源。
"""
from __future__ import annotations

import sys
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
    import app.config as C
    from services.data import sentiment_alt as A
    from services.data import limit_pool as LP
    from services.ops import review as Rv

    _orig_alt = A.limit_counts
    _orig_load = LP.load_day
    _orig_switch = C.SENTIMENT_ALT_FALLBACK

    alt_payload = {
        'source': 'sina_rank', 'excludeSt': True, 'limitUp': 24, 'limitDown': 78,
        'ztList': [{'c': '000656', 'n': '金科股份', 'p': 1020, 'zdf': 9.68}],
        'dtList': [{'c': '600001', 'n': '示例跌停', 'p': 9000, 'zdf': -10.0}],
        'limitUpInclSt': 28, 'limitDownInclSt': 83,
    }

    try:
        # ── A. 新浪+腾讯备源 ─────────────────────────────────────────
        section('A. sentiment_alt 备源优先')
        C.SENTIMENT_ALT_FALLBACK = True
        A.limit_counts = lambda: alt_payload
        LP.load_day = lambda day, kind=None: []          # 落库为空，必须走新浪
        fb = Rv._fallback_limit_lists('2026-11-19')
        check('返回了备源', fb is not None, str(fb))
        check('来源标注 sina_rank_alt', (fb or {}).get('source') == 'sina_rank_alt', str(fb))
        check('涨停名单 1 只', len((fb or {}).get('zt') or []) == 1, str(fb))
        check('跌停名单 1 只', len((fb or {}).get('dt') or []) == 1, str(fb))

        # ── B. 映射：毫元 → 元 ──────────────────────────────────────
        section('B. 池子键名 → 收评字段')
        m = Rv._pool_item_to_review(alt_payload['ztList'][0])
        check('name 映射', m['name'] == '金科股份', str(m))
        check('code 映射', m['code'] == '000656', str(m))
        check('涨跌幅映射', m['changePercent'] == 9.68, str(m))
        check('价格 1020 毫元 → 1.02 元', m['price'] == 1.02, str(m))
        check('缺失的 fund/hs 不编数', m['mainNetInflow'] is None and m['turnoverRate'] is None, str(m))

        # ── C. 新浪挂了退落库快照 ───────────────────────────────────
        section('C. 备源失败 → 当天落库快照')
        def _boom():
            raise RuntimeError('sina down')
        A.limit_counts = _boom
        LP.load_day = lambda day, kind=None: [
            {'pool': 'zt', 'symbol': '000656', 'name': '金科股份',
             'change_pct': 9.67, 'price': 1.02, 'lianban': 1},
            {'pool': 'dt', 'symbol': '600001', 'name': '示例跌停',
             'change_pct': -10.0, 'price': 9.0, 'lianban': 1},
            {'pool': 'zb', 'symbol': '000002', 'name': '炸板票',
             'change_pct': 5.0, 'price': 8.0, 'lianban': 1},
        ]
        fb = Rv._fallback_limit_lists('2026-11-19')
        check('来源标注 limit_pool_daily', (fb or {}).get('source') == 'limit_pool_daily', str(fb))
        check('zt 1 只、dt 1 只（zb 不算）',
              len((fb or {}).get('zt') or []) == 1 and len((fb or {}).get('dt') or []) == 1, str(fb))
        check('落库价格元 → 毫元 1020', ((fb or {}).get('zt') or [{}])[0].get('p') == 1020.0, str(fb))

        # ── D-反证：两级都空必须拿不到（老路径现场） ────────────────
        section('D-反证. 开关关闭 + 落库空 = 无备源')
        C.SENTIMENT_ALT_FALLBACK = False
        LP.load_day = lambda day, kind=None: []
        called = {'n': 0}

        def _count_call():
            called['n'] += 1
            return alt_payload
        A.limit_counts = _count_call
        fb = Rv._fallback_limit_lists('2026-11-19')
        check('返回 None（调用方如实显示 0）', fb is None, str(fb))
        check('开关关闭时一次都不请求新浪', called['n'] == 0, str(called))

        # ── E. 接线落盘 ─────────────────────────────────────────────
        section('E. 接线落盘')
        src = (SERVER / 'services/ops/review.py').read_text(encoding='utf-8')
        check('池子 API 空后调用备源', '_fallback_limit_lists(datetime.now()' in src)
        check('返回体标注来源', "'limitCountSource': limit_source" in src)
        check('摘要写明备源口径', '家数来源' in src)
    finally:
        C.SENTIMENT_ALT_FALLBACK = _orig_switch
        A.limit_counts = _orig_alt
        LP.load_day = _orig_load

    print(f'\n结果：{_PASS}/{_PASS + _FAIL} 通过，{_FAIL} 失败')
    return 1 if _FAIL else 0


if __name__ == '__main__':
    sys.exit(main())