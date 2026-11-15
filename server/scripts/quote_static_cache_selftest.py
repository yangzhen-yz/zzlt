"""休市行情静态缓存 · 长期回归自测（2026-11-15）

跑法：PYTHONPATH=server .venv/Scripts/python.exe server/scripts/quote_static_cache_selftest.py

守护的不变量：
1. 静态窗口边界：周末/节假日/盘前/午休/盘后 = 静态；集合竞价与盘中 = 实时；
   交易日历未覆盖的年份保守走实时（绝不长期缓存旧价）。
2. TTL 是「距下一次盘中开始」，9:15 一过缓存必须自动失效。
3. 只有静态窗口才读写缓存；force 能绕过；回滚开关即时生效。
4. 落盘快照能跨进程重载（服务重启后首开也是热的），坏文件不炸。
5. 路由接线：只在完整路径（_timed_out 为空）写缓存、force 参数透传、
   响应头可排障；前端手动刷新带 force=1。
6. 全部用临时目录，不写线上 server/data，零外部网络请求。
"""
from __future__ import annotations

import ast
import datetime as dt
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.support import quote_static_cache as Q  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
PASS = 0
FAIL = 0


def check(name: str, cond, extra: str = '') -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
    else:
        FAIL += 1
        print(f'  FAIL: {name} {extra}')


def main() -> int:
    tmp = tempfile.mkdtemp(prefix='quote_static_selftest_')
    snap = Path(tmp) / 'snapshot.json'
    off = Path(tmp) / 'disabled.off'
    real_snapshot, real_off, real_entries, real_loaded = (
        Q._SNAPSHOT, Q._OFF_FILE, Q._ENTRIES, Q._LOADED)

    try:
        Q._SNAPSHOT = snap
        Q._OFF_FILE = off
        Q._ENTRIES = {}
        Q._LOADED = False

        # ---------- 1. 静态窗口边界 ----------
        print('[1] 静态窗口边界')
        cases = [
            ('周日 13:29', dt.datetime(2026, 11, 15, 13, 29), True),
            ('周六 10:00', dt.datetime(2026, 11, 14, 10, 0), True),
            ('工作日盘中 10:00', dt.datetime(2026, 11, 13, 10, 0), False),
            ('集合竞价 9:20', dt.datetime(2026, 11, 13, 9, 20), False),
            ('午后盘中 14:00', dt.datetime(2026, 11, 13, 14, 0), False),
            ('午间休市 12:00', dt.datetime(2026, 11, 13, 12, 0), True),
            ('盘前 9:00', dt.datetime(2026, 11, 13, 9, 0), True),
            ('盘后 15:30', dt.datetime(2026, 11, 13, 15, 30), True),
            ('收盘定格边界 15:05', dt.datetime(2026, 11, 13, 15, 5), True),
            ('国庆休市 10/2', dt.datetime(2026, 10, 2, 10, 0), True),
            ('日历未覆盖年份 2027', dt.datetime(2027, 1, 2, 13, 0), False),
        ]
        for name, now, expect in cases:
            got = Q.is_static_window(now)
            check(name, got == expect, f'got={got} expect={expect}')
            ttl = Q.static_ttl(now)
            check(name + ' TTL 方向', (ttl > 0) == expect, f'ttl={ttl}')

        # 盘前 9:00 → 9:15 只有 900s，不能一路缓存到收盘
        t0900 = Q.static_ttl(dt.datetime(2026, 11, 13, 9, 0))
        check('盘前 TTL 精确到 9:15（900s）', abs(t0900 - 900) < 2, f'{t0900}')
        check('TTL 封顶 6 小时', Q.static_ttl(dt.datetime(2026, 11, 15, 13, 29))
              <= Q.MAX_TTL_SEC)
        check('盘中 TTL=0', Q.static_ttl(dt.datetime(2026, 11, 13, 10, 0)) == 0.0)

        # ---------- 2. 基本命中 / force / 完整响应校验 ----------
        print('[2] 命中与绕过')
        payload = {'success': True, 'data': {'symbol': 'sz002384', 'raw': 'v_sz002384="1~x";',
                                             'score': 72}}
        check('空缓存未命中', Q.get('sz002384') is None)
        Q.put('sz002384', payload)
        hit = Q.get('sz002384')
        check('put 后命中且内容一致', hit == payload, repr(hit))
        check('force 绕过缓存', Q.get('sz002384', force=True) is None)
        Q.put('sh600000', {'success': False})
        check('不完整响应不写缓存', Q.get('sh600000') is None)
        check('落盘文件已生成', snap.exists())
        check('完整结果标记为非降级', Q._ENTRIES['sz002384']['degraded'] is False)
        st = Q.stats()
        check('stats 口径正确', st['entries'] == 1 and st['staticWindow'] is True
              and st['ttlSec'] > 0, str(st))

        # ---------- 2b. 降级结果也写，但只给 5 分钟 ----------
        print('[2b] 降级短 TTL')
        Q.invalidate()
        Q.put('sz002384', payload, degrade=True)
        e = Q._ENTRIES.get('sz002384')
        check('降级结果确实写入（不能把缓存架空）', e is not None)
        check('降级 TTL 封顶 300s', e and 0 < e['valid_until'] - time.time() <= Q.DEGRADED_TTL_SEC,
              str(e and e['valid_until'] - time.time()))
        check('降级可读但标记 degraded', e and e['degraded'] is True
              and Q.get('sz002384') == payload)
        Q.invalidate()
        Q.put('sz002384', payload)

        # ---------- 3. 盘中绝不读写（mock 掉窗口判定，模拟交易日 10:00） ----------
        print('[3] 盘中旁路')
        with mock.patch.object(Q, 'is_static_window', return_value=False):
            check('盘中不读缓存', Q.get('sz002384') is None)
            Q.put('sh600183', {'success': True, 'data': {'x': 1}})
            check('盘中不写缓存', 'sh600183' not in Q._ENTRIES)
        check('盘中 mock 结束后缓存未被污染', 'sh600183' not in Q._ENTRIES)
        check('回到静态窗口仍命中', Q.get('sz002384') == payload)

        # ---------- 4. 过期失效（模拟 valid_until 已过） ----------
        print('[4] 过期失效')
        Q._ENTRIES['sz002384']['valid_until'] = time.time() - 1
        check('过有效期后不命中', Q.get('sz002384') is None)
        check('过期条目被回收', 'sz002384' not in Q._ENTRIES)
        Q.put('sz002384', payload)
        Q.invalidate()
        check('全量失效生效', Q.get('sz002384') is None and not Q._ENTRIES)
        Q.put('sz002384', payload)
        Q.invalidate('sz002384')
        check('按票失效生效', Q.get('sz002384') is None)

        # ---------- 5. 回滚开关 ----------
        print('[5] 一键回滚')
        Q.put('sz002384', payload)
        off.write_text('off', encoding='utf-8')
        check('.off 文件即时关闭读', Q.get('sz002384') is None)
        Q.put('sh600183', payload)
        check('.off 文件即时关闭写', 'sh600183' not in Q._ENTRIES)
        off.unlink()
        with mock.patch.dict(os.environ, {'QUOTE_STATIC_CACHE': '0'}):
            check('环境变量即时关闭', not Q.enabled() and Q.get('sz002384') is None)
        check('开关撤销后恢复', Q.enabled() and Q.get('sz002384') == payload)

        # ---------- 6. 落盘跨进程重载 + 坏文件容错 ----------
        print('[6] 落盘重载')
        Q._ENTRIES = {}
        Q._LOADED = False
        reloaded = Q.get('sz002384')
        check('模拟重启后磁盘仍是热的', reloaded == payload, repr(reloaded))
        snap.write_text('{ this is not json', encoding='utf-8')
        Q._ENTRIES = {}
        Q._LOADED = False
        check('坏快照不抛异常、按空缓存继续', Q.get('sz002384') is None)
        check('坏快照不残留脏条目', Q.stats()['entries'] == 0)
        Q.put('sz002384', payload)
        snap.write_text('{"version": 999, "entries": {}}', encoding='utf-8')
        Q._ENTRIES = {}
        Q._LOADED = False
        check('版本不匹配的快照被忽略', Q.get('sz002384') is None)

        # ---------- 7. 容量上限 ----------
        print('[7] 容量上限')
        Q._ENTRIES = {}
        Q._LOADED = True
        for i in range(Q.MAX_ENTRIES + 25):
            Q._ENTRIES['sh%06d' % i] = {
                'ts': float(i), 'valid_until': time.time() + 99999,
                'payload': {'success': True, 'data': {}}}
        Q.put('sz000001', payload)
        check('超上限按最旧淘汰', len(Q._ENTRIES) <= Q.MAX_ENTRIES,
              str(len(Q._ENTRIES)))
        check('新写入的票保留', Q.get('sz000001') == payload)

        # ---------- 8. 路由 / 前端接线（AST 与文本，不 import app） ----------
        print('[8] 接线')
        stock_py = (ROOT / 'server/app/api/routers/stock.py').read_text(encoding='utf-8')
        tree = ast.parse(stock_py)
        fn = next((n for n in ast.walk(tree)
                   if isinstance(n, ast.AsyncFunctionDef) and n.name == 'get_stock_full'), None)
        check('get_stock_full 存在', fn is not None)
        if fn:
            args = {a.arg for a in fn.args.args}
            check('force 参数已加到路由', 'force' in args, str(args))
            check('response 参数已加到路由（用于排障响应头）', 'response' in args)
        check('路由读静态缓存', '_qsc.get(sym, force=force)' in stock_py)
        check('路由写静态缓存（降级也写）', '_qsc.put(sym, resp, degrade=bool(_timed_out))' in stock_py)
        check('命中响应头可排障', "X-Quote-Cache" in stock_py)
        check('静态窗口才写缓存', '_static = _qsc.is_static_window()' in stock_py
              and 'if _static:' in stock_py)
        check('休市放宽增强超时、盘中保持 1.2s',
              '_ENRICH_TIMEOUT = 2.5 if _static else 1.2' in stock_py)
        # put 必须被 `if _static:` 包住，不能无条件写（盘中会污染实时性）
        guarded = False
        if fn:
            for node in ast.walk(fn):
                if isinstance(node, ast.If):
                    src = ast.get_source_segment(stock_py, node) or ''
                    if '_qsc.put' in src and 'if _static:' in src:
                        guarded = True
        check('写缓存受静态窗口守卫', guarded)
        check('_timed_out 在函数开头初始化（缓存命中路径不会 NameError）',
              '_timed_out: list = []' in stock_py
              and stock_py.index('_timed_out: list = []') < stock_py.index('_ENRICH_TIMEOUT ='))

        vue = (ROOT / 'client-vue/src/components/tabs/TabQuote.vue').read_text(encoding='utf-8')
        check('前端 fetchStockQuote 支持 force', 'silent = false, force = false' in vue)
        check('前端 force 拼进 URL', "?force=1" in vue)
        check('手动刷新绕过缓存',
              'fetchStockQuote(currentSymbol.value, false, true)' in vue)
        check('静默轮询不带 force（休市日直接吃缓存）',
              'fetchStockQuote(currentSymbol.value, true)' in vue)

        # ---------- 9. 纪律 ----------
        print('[9] 纪律')
        qsrc = (ROOT / 'server/services/support/quote_static_cache.py').read_text(encoding='utf-8')
        qtree = ast.parse(qsrc)
        calls = [n.func.attr if isinstance(n.func, ast.Attribute) else
                 getattr(n.func, 'id', '') for n in ast.walk(qtree) if isinstance(n, ast.Call)]
        check('缓存模块不调用 taskkill / subprocess',
              not any(c in ('taskkill', 'Popen', 'check_output', 'system') for c in calls),
              str(set(calls)))
        check('缓存模块不改交易日历', 'CLOSED_DATES' not in qsrc)
        check('快照路径落在 gitignored 的 server/data', "data' / 'quote_static_snapshot.json'" in qsrc
              or 'data/quote_static_snapshot.json' in qsrc)

    finally:
        Q._SNAPSHOT, Q._OFF_FILE, Q._ENTRIES, Q._LOADED = (
            real_snapshot, real_off, real_entries, real_loaded)
        shutil.rmtree(tmp, ignore_errors=True)

    print(f'\n结果: {PASS}/{PASS + FAIL} 通过' if not FAIL else f'\n结果: {PASS}/{PASS + FAIL}，失败 {FAIL} 项')
    return 1 if FAIL else 0


if __name__ == '__main__':
    raise SystemExit(main())
