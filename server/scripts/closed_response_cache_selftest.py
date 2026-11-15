"""全站休市 GET 响应缓存 · 长期回归自测（2026-11-15）

跑法：PYTHONPATH=server .venv/Scripts/python.exe server/scripts/closed_response_cache_selftest.py

守护的不变量：
1. 窗口口径：盘中不缓存；交易日盘后（15:05～20:00）只给 60s 短 TTL；
   20:00 后 / 非交易日给长 TTL；日历未覆盖年份保守走实时。
2. 显式白名单：用户态接口、触发式 GET、健康探针一律不缓存；
   /api/stock/{symbol}/full 由单票专用缓存接管，通用层必须排除。
3. force / cache-buster / Cache-Control: no-cache 必须绕过；写操作成功后清缓存。
4. 整包回放：status/headers/body 一致、gzip body 不重复压缩、ETag 仍能回 304。
5. 只缓存成功响应：200 包着的 success:false / error 不缓存，set-cookie / no-store 不缓存。
6. 落盘跨进程重载、坏文件容错、容量/体积上限、一键回滚开关。
7. ASGI 中间件端到端：命中不回源、盘中不读缓存、POST 清缓存。
8. 接线：main.py 挂载位置、system.py 排障端点、文档与回滚开关。
全部用临时目录，不写线上 server/data，零外部网络请求。
"""
from __future__ import annotations

import ast
import asyncio
import base64
import datetime as dt
import gzip
import json
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.support import closed_window as W  # noqa: E402
from services.support import closed_response_cache as C  # noqa: E402
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


def _scope(path: str, query: bytes = b'', method: str = 'GET',
           extra_headers=()):
    headers = [(b'accept-encoding', b'gzip')]
    headers.extend(extra_headers)
    return {
        'type': 'http', 'method': method, 'path': path, 'raw_path': path.encode(),
        'query_string': query, 'headers': headers, 'scheme': 'http',
        'server': ('testserver', 80), 'client': ('127.0.0.1', 12345),
        'root_path': '', 'http_version': '1.1',
    }


async def _receive():
    return {'type': 'http.request', 'body': b'', 'more_body': False}


async def _run_middleware(inner, scope):
    """跑一遍中间件，返回 (发出的 ASGI 消息列表)。"""
    sent = []

    async def send(message):
        sent.append(message)

    await C.ClosedWindowCacheMiddleware(inner)(scope, _receive, send)
    return sent


def _body_of(sent):
    return b''.join(m.get('body', b'') for m in sent
                    if m['type'] == 'http.response.body')


def _h(sent, name: str, default: str = '') -> str:
    for m in sent:
        if m['type'] == 'http.response.start':
            return C._header_get(m.get('headers', []), name, default)
    return default


async def main() -> int:
    tmp = tempfile.mkdtemp(prefix='closed_cache_selftest_')
    snap = Path(tmp) / 'closed.json'
    off = Path(tmp) / 'disabled.off'
    real = (C._SNAPSHOT, C._OFF_FILE, C._ENTRIES, C._LOADED, dict(C._STATS))
    payload = {'success': True, 'data': {'symbol': 'sh600000', 'price': 12.34}}
    raw = json.dumps(payload, ensure_ascii=False).encode('utf-8')

    try:
        C._SNAPSHOT = snap
        C._OFF_FILE = off
        C._ENTRIES = {}
        C._LOADED = False

        # ---------- 1. 统一窗口口径 ----------
        print('[1] 窗口口径')
        cases = [
            ('周日 14:52', dt.datetime(2026, 11, 15, 14, 52), True),
            ('周六 10:00', dt.datetime(2026, 11, 14, 10, 0), True),
            ('工作日盘中 10:00', dt.datetime(2026, 11, 13, 10, 0), False),
            ('集合竞价 9:20', dt.datetime(2026, 11, 13, 9, 20), False),
            ('午后盘中 14:00', dt.datetime(2026, 11, 13, 14, 0), False),
            ('午间休市 12:00', dt.datetime(2026, 11, 13, 12, 0), True),
            ('盘后 15:30', dt.datetime(2026, 11, 13, 15, 30), True),
            ('收盘边界 15:05', dt.datetime(2026, 11, 13, 15, 5), True),
            ('国庆休市 10/2', dt.datetime(2026, 10, 2, 10, 0), True),
            ('日历未覆盖 2027', dt.datetime(2027, 1, 2, 13, 0), False),
        ]
        for name, now, expect in cases:
            check(name, W.is_static_window(now) == expect,
                  f'got={W.is_static_window(now)}')
            check(name + ' TTL 方向', (W.cache_ttl(now) > 0) == expect)
        check('盘前 TTL 精确到 9:15（900s）',
              abs(W.cache_ttl(dt.datetime(2026, 11, 13, 9, 0)) - 900) < 2)
        check('盘后 15:30 只给 60s（数据仍在落库）',
              W.cache_ttl(dt.datetime(2026, 11, 13, 15, 30)) == 60.0,
              str(W.cache_ttl(dt.datetime(2026, 11, 13, 15, 30))))
        check('盘后 20:00 起恢复长 TTL',
              W.cache_ttl(dt.datetime(2026, 11, 13, 20, 0)) > 60,
              str(W.cache_ttl(dt.datetime(2026, 11, 13, 20, 0))))
        check('周日给长 TTL',
              W.cache_ttl(dt.datetime(2026, 11, 15, 14, 52)) > 60)
        check('TTL 封顶 6 小时', W.cache_ttl(dt.datetime(2026, 11, 15, 14, 52))
              <= W.MAX_TTL_SEC)
        check('单票 static_ttl 不受盘后短 TTL 影响（收盘价已定格）',
              Q.static_ttl(dt.datetime(2026, 11, 13, 15, 30)) > 60)
        check('窗口判定共用同一份实现',
              Q.is_static_window is W.is_static_window)

        # ---------- 2. 白名单 / deny ----------
        print('[2] 白名单')
        allow = [
            '/api/stock/sh600000', '/api/stock/sh600000/analysis',
            '/api/stocks', '/api/stock-search', '/api/stock-map',
            '/api/stock-concepts/sh600000', '/api/stock-diagnose/sh600000',
            '/api/hot-stocks', '/api/hot-stocks-scan', '/api/hot_search_ranking',
            '/api/market-index', '/api/market-alert', '/api/market-predict',
            '/api/auction/latest', '/api/fund-flow', '/api/rps',
            '/api/dragon-tiger', '/api/northbound', '/api/valuation',
            '/api/kline/sh600000', '/api/kline/sh600000/minute',
            '/api/kline-history/stats', '/api/cls-plate-stocks',
            '/api/cls-ladder', '/api/theme-index', '/api/daily-reviews',
            '/api/daily_recommendations', '/api/recommend-review',
            '/api/recommend-sim', '/api/recommend-sim-batch',
            '/api/recommend-excess', '/api/recommend-batches', '/api/shadow-eval',
            '/api/eval-lab/latest', '/api/trend-stocks',
            '/api/trend/history-counts', '/api/trend/backtest-latest',
            '/api/trend/backtest-history', '/api/trend/backtest-records',
            '/api/trend/backtest-summary', '/api/strategy/weights',
            '/api/strategy/backtest-history', '/api/strategy/predictions',
            '/api/optimization/history', '/api/optimization/trend',
            '/eastmoney/api/qt/clist/get',
        ]
        deny = [
            '/api/stock/sh600000/full', '/api/stock/sh600000/unknown',
            '/api/auth/me', '/api/positions', '/api/push-channels',
            '/api/sim/state', '/api/settings', '/api/settings/preview',
            '/api/tab-scan-config', '/api/custom-scan-pool',
            '/api/custom-scan-pool/symbols', '/api/stock-tags/sh600000',
            '/api/action-desk', '/api/optimization/status', '/api/api-usage',
            '/api/health', '/api/data-sources/health', '/api/system/restart-status',
            '/api/system/closed-cache', '/api/internal/ops-notify-limit',
            '/api/update-hot_search_ranking', '/api/scan-trend-stocks',
            '/api/generate-recommendations', '/api/stock-tags',
            '/index.html', '/assets/index-abc.js',
        ]
        for p in allow:
            check('放行 ' + p, C._is_cacheable_path(p))
        for p in deny:
            check('排除 ' + p, not C._is_cacheable_path(p))

        # ---------- 3. key / cache-buster ----------
        print('[3] 缓存键')
        k1 = C.make_key('/api/fund-flow', b'b=2&a=1', True)
        k2 = C.make_key('/api/fund-flow', b'a=1&b=2', True)
        check('参数顺序无关', k1 == k2, f'{k1} vs {k2}')
        check('gzip 支持度进键', k1 != C.make_key('/api/fund-flow', b'a=1&b=2', False))
        check('不同路径不同键', k1 != C.make_key('/api/rps', b'a=1&b=2', True))
        for q in (b'force=1', b'force=', b'_t=1730000000', b'cacheBust=9',
                  b'refresh=true', b'_=x'):
            check(f'绕过 {q.decode()}', C.make_key('/api/rps', q, True) is None)

        # ---------- 4. 读写 / 过期 / 失效 / 开关 ----------
        print('[4] 读写与开关')
        with mock.patch.object(C, 'is_static_window', return_value=True), \
             mock.patch.object(C, 'cache_ttl', return_value=600.0):
            hdrs = [('content-type', 'application/json'), ('etag', 'W/"abc"')]
            check('空缓存未命中', C.get(k1) is None)
            C.put(k1, 200, hdrs, raw)
            e = C.get(k1)
            check('put 后命中', e is not None)
            check('body 原样回放', e and base64.b64decode(e['body']) == raw)
            check('headers 原样回放', e and ('etag', 'W/"abc"') in e['headers'])
            check('逐跳头已剔除', e and all(h[0].lower() not in C._DROP_HEADERS
                                         for h in e['headers']))
            check('落盘已生成', snap.exists())
            C._ENTRIES[k1]['valid_until'] = time.time() - 1
            check('过期不命中', C.get(k1) is None)
            check('过期条目被回收', k1 not in C._ENTRIES)

            off.write_text('off', encoding='utf-8')
            C.put(k1, 200, hdrs, raw)
            check('.off 即时关闭写', k1 not in C._ENTRIES)
            off.unlink()
            C.put(k1, 200, hdrs, raw)
            with mock.patch.dict(os.environ, {'CLOSED_CACHE': '0'}):
                check('环境变量即时关闭读', C.get(k1) is None)
            check('开关撤销后恢复', C.get(k1) is not None)
            C.invalidate()
            check('全量失效', C.get(k1) is None and not C._ENTRIES)
            C.put(k1, 200, hdrs, raw)
            C.invalidate('/api/fund-flow')
            check('按前缀失效', C.get(k1) is None)
            C.put(k1, 200, hdrs, raw)
            st = C.stats()
            check('stats 口径', st['staticWindow'] is True and st['entries'] == 1
                  and st['stores'] > 0, str(st))

        # ---------- 5. 响应合法性 ----------
        print('[5] 响应校验')
        good = [('content-type', 'application/json')]
        check('正常响应可缓存', C._response_is_good(200, good, raw))
        check('非 200 不缓存', not C._response_is_good(503, good, raw))
        check('空 body 不缓存', not C._response_is_good(200, good, b''))
        check('超大 body 不缓存',
              not C._response_is_good(200, good, b'x' * (C.MAX_BODY_BYTES + 1)))
        check('set-cookie 不缓存', not C._response_is_good(
            200, good + [('set-cookie', 'sid=1')], raw))
        check('no-store 不缓存', not C._response_is_good(
            200, good + [('cache-control', 'no-store')], raw))
        check('响应 no-cache 不缓存（尊重 /api/daily-reviews 的重验证声明）',
              not C._response_is_good(
                  200, good + [('cache-control', 'private, no-cache')], raw))
        check('非 JSON 类型不缓存', not C._response_is_good(
            200, [('content-type', 'image/png')], raw))
        check('200 包 success:false 不缓存', not C._response_is_good(
            200, good, b'{"success": false, "error": "down"}'))
        check('200 包 error 不缓存', not C._response_is_good(
            200, good, b'{"error": "eastmoney blocked"}'))
        check('error:null 不算错误', C._response_is_good(
            200, good, b'{"success": true, "error": null}'))
        zipped = gzip.compress(raw)
        check('gzip 体解压后能检查业务错误', not C._response_is_good(
            200, good + [('content-encoding', 'gzip')],
            gzip.compress(b'{"success": false}')))
        check('gzip 正常体可缓存', C._response_is_good(
            200, good + [('content-encoding', 'gzip')], zipped))
        check('非 JSON 文本放行（东财 jsonp）', C._response_is_good(
            200, good, b'jQuery123({"data":null});'))
        check('坏 gzip 只按 status 放行', C._response_is_good(
            200, good + [('content-encoding', 'gzip')], b'not-a-gzip'))

        # ---------- 6. 落盘重载 / 坏文件 / 容量 ----------
        print('[6] 落盘')
        with mock.patch.object(C, 'is_static_window', return_value=True), \
             mock.patch.object(C, 'cache_ttl', return_value=600.0):
            C._ENTRIES = {}
            C._LOADED = False
            reloaded = C.get(k1)
            check('模拟重启后磁盘仍是热的',
                  reloaded is not None and base64.b64decode(reloaded['body']) == raw)
            snap.write_text('{ not json', encoding='utf-8')
            C._ENTRIES = {}
            C._LOADED = False
            check('坏快照不抛异常', C.get(k1) is None)
            snap.write_text('{"version": 999, "entries": {}}', encoding='utf-8')
            C._ENTRIES = {}
            C._LOADED = False
            check('版本不匹配被忽略', C.get(k1) is None)
            # 旧快照里曾缓存过显式 no-cache 的响应，重载时必须剔掉
            stale = {'version': 1, 'entries': {
                k1: {'ts': time.time(), 'valid_until': time.time() + 999,
                     'status': 200,
                     'headers': [('cache-control', 'private, no-cache')],
                     'body': base64.b64encode(raw).decode(), 'size': len(raw)}}}
            snap.write_text(json.dumps(stale), encoding='utf-8')
            C._ENTRIES = {}
            C._LOADED = False
            check('旧快照中 no-cache 条目被跳过', C.get(k1) is None)
            C._ENTRIES = {}
            C._LOADED = True
            for i in range(C.MAX_ENTRIES + 30):
                C._ENTRIES['k%04d' % i] = {
                    'ts': float(i), 'valid_until': time.time() + 99999,
                    'status': 200, 'headers': [],
                    'body': base64.b64encode(raw).decode(), 'size': len(raw)}
            C.put(k1, 200, good, raw)
            check('超条目上限淘汰', len(C._ENTRIES) <= C.MAX_ENTRIES,
                  str(len(C._ENTRIES)))
            check('新写入保留', C.get(k1) is not None)

        # ---------- 7. ASGI 中间件端到端 ----------
        print('[7] 中间件端到端')
        calls = []

        async def inner(scope, receive, send):
            calls.append((scope['method'], scope['path']))
            hdrs = [(b'content-type', b'application/json'),
                    (b'etag', b'W/"abc"'), (b'server', b'uvicorn'),
                    (b'date', b'Sun, 15 Nov 2026 06:00:00 GMT')]
            await send({'type': 'http.response.start', 'status': 200,
                        'headers': hdrs})
            await send({'type': 'http.response.body', 'body': raw,
                        'more_body': False})

        async def post_inner(scope, receive, send):
            calls.append(('POST', scope['path']))
            await send({'type': 'http.response.start', 'status': 200,
                        'headers': [(b'content-type', b'application/json')]})
            await send({'type': 'http.response.body',
                        'body': b'{"success": true}', 'more_body': False})

        with mock.patch.object(C, 'is_static_window', return_value=True), \
             mock.patch.object(C, 'cache_ttl', return_value=600.0):
            # 本组从干净内存起步，且不读上一组留下的磁盘快照
            if snap.exists():
                snap.unlink()
            C._ENTRIES = {}
            C._LOADED = True
            calls.clear()
            s = _scope('/api/market-index')
            first = await _run_middleware(inner, s)
            second = await _run_middleware(inner, s)
            check('首次未命中回源', _body_of(first) == raw and len(calls) >= 1)
            check('第二次命中不再回源', len(calls) == 1, str(calls))
            check('命中响应体一致', _body_of(second) == raw)
            check('命中带 X-Closed-Cache: hit', _h(second, 'x-closed-cache') == 'hit')
            check('命中带 age', _h(second, 'x-closed-cache-age') != '')
            check('命中不带旧 Server/Date 头',
                  _h(second, 'server') == '' and _h(second, 'date') == '')
            check('命中 content-length 与 body 一致',
                  _h(second, 'content-length') == str(len(raw)))

            # ETag 协商仍然生效
            s304 = _scope('/api/market-index',
                          extra_headers=[(b'if-none-match', b'W/"abc"')])
            r304 = await _run_middleware(inner, s304)
            check('ETag 命中回 304', r304[0]['status'] == 304,
                  str(r304[0]['status']))
            check('304 空 body', _body_of(r304) == b'')
            check('304 不带 content-encoding', _h(r304, 'content-encoding') == '')
            check('304 不回源', len(calls) == 1)

            # force / no-cache / 白名单外 一律回源
            calls.clear()
            await _run_middleware(inner, _scope('/api/market-index', b'force=1'))
            check('force 参数绕过缓存', len(calls) == 1)
            await _run_middleware(inner, _scope(
                '/api/market-index', extra_headers=[(b'cache-control', b'no-cache')]))
            check('请求 no-cache 绕过缓存', len(calls) == 2)
            await _run_middleware(inner, _scope('/api/health'))
            await _run_middleware(inner, _scope('/api/health'))
            check('白名单外每次回源', len(calls) == 4)
            await _run_middleware(inner, _scope('/api/stock/sh600000/full'))
            await _run_middleware(inner, _scope('/api/stock/sh600000/full'))
            check('/full 交给单票专用缓存（本层不缓存）', len(calls) == 6)

            # 盘中完全旁路
            with mock.patch.object(C, 'is_static_window', return_value=False):
                calls.clear()
                await _run_middleware(inner, s)
                await _run_middleware(inner, s)
                check('盘中两次都回源', len(calls) == 2, str(calls))
                check('盘中不带命中头',
                      _h(await _run_middleware(inner, s), 'x-closed-cache') == '')

            # gzip body 整包回放（不重复压缩）
            async def gzip_inner(scope, receive, send):
                calls.append(('GZIP', scope['path']))
                await send({'type': 'http.response.start', 'status': 200,
                            'headers': [(b'content-type', b'application/json'),
                                        (b'content-encoding', b'gzip')]})
                await send({'type': 'http.response.body', 'body': zipped,
                            'more_body': False})

            calls.clear()
            g1 = await _run_middleware(gzip_inner, _scope('/api/daily-reviews'))
            g2 = await _run_middleware(gzip_inner, _scope('/api/daily-reviews'))
            check('gzip 首次原样返回', _body_of(g1) == zipped)
            check('gzip 命中回放同一份压缩 bytes', _body_of(g2) == zipped)
            check('gzip 命中不回源', len(calls) == 1)
            check('gzip 命中保留 content-encoding',
                  _h(g2, 'content-encoding') == 'gzip')

            # 错误响应不缓存
            async def err_inner(scope, receive, send):
                calls.append(('ERR', scope['path']))
                await send({'type': 'http.response.start', 'status': 200,
                            'headers': [(b'content-type', b'application/json')]})
                await send({'type': 'http.response.body',
                            'body': b'{"success": false}', 'more_body': False})

            calls.clear()
            await _run_middleware(err_inner, _scope('/api/northbound'))
            await _run_middleware(err_inner, _scope('/api/northbound'))
            check('业务错误响应不缓存', len(calls) == 2, str(calls))

            # 写操作成功后清缓存
            check('写前缓存非空', C.stats()['entries'] > 0)
            await _run_middleware(post_inner, _scope('/api/daily-review/generate',
                                                    method='POST'))
            check('POST 2xx 清空缓存', C.stats()['entries'] == 0,
                  str(C.stats()))

        # ---------- 8. 接线 ----------
        print('[8] 接线')
        main_py = (ROOT / 'server/app/main.py').read_text(encoding='utf-8')
        check('main.py 导入中间件',
              'from services.support.closed_response_cache import ClosedWindowCacheMiddleware' in main_py)
        idx_gzip = main_py.index('app.add_middleware(GZipMiddleware')
        idx_cache = main_py.index('app.add_middleware(ClosedWindowCacheMiddleware)')
        idx_cors = main_py.index('app.add_middleware(CORSMiddleware')
        check('挂载顺序 GZip → Cache → CORS（缓存在 GZip 外层）',
              idx_gzip < idx_cache < idx_cors)
        check('CORS 暴露排障头', 'X-Closed-Cache-Age' in main_py)
        sys_py = (ROOT / 'server/app/api/routers/system.py').read_text(encoding='utf-8')
        check('运维排障端点存在', '/api/system/closed-cache"' in sys_py)
        check('手动失效端点存在', '/api/system/closed-cache/invalidate"' in sys_py)
        check('排障端点不在缓存白名单内',
              '/api/system/' in C.DENY_PREFIXES)
        stock_py = (ROOT / 'server/app/api/routers/stock.py').read_text(encoding='utf-8')
        check('单票 /full 仍走专用缓存', '_qsc.get(sym, force=force)' in stock_py)
        check('单票专用缓存仍在写', '_qsc.put(sym, resp, degrade=bool(_timed_out))' in stock_py)
        drv = (ROOT / 'scripts/daily_reviews_cache_selftest.py').read_text(encoding='utf-8')
        check('收评分页自测显式绕过全站缓存（否则 mock 被磁盘快照顶掉）',
              "os.environ['CLOSED_CACHE'] = '0'" in drv)

        # ---------- 9. 纪律 ----------
        print('[9] 纪律')
        for name, src in (('closed_response_cache', (ROOT / 'server/services/support/closed_response_cache.py').read_text(encoding='utf-8')),
                          ('closed_window', (ROOT / 'server/services/support/closed_window.py').read_text(encoding='utf-8'))):
            tree = ast.parse(src)
            calls_ast = [n.func.attr if isinstance(n.func, ast.Attribute) else
                         getattr(n.func, 'id', '')
                         for n in ast.walk(tree) if isinstance(n, ast.Call)]
            check(f'{name} 不调用 taskkill / subprocess',
                  not any(c in ('taskkill', 'Popen', 'check_output', 'system')
                          for c in calls_ast), str(set(calls_ast)))
            check(f'{name} 不改交易日历', 'CLOSED_DATES' not in src)
        check('缓存快照落在 gitignored 的 server/data',
              "data' / 'closed_response_snapshot.json'" in
              (ROOT / 'server/services/support/closed_response_cache.py').read_text(encoding='utf-8'))

    finally:
        (C._SNAPSHOT, C._OFF_FILE, C._ENTRIES, C._LOADED) = real[:4]
        C._STATS.clear()
        C._STATS.update(real[4])
        shutil.rmtree(tmp, ignore_errors=True)

    print(f'\n结果: {PASS}/{PASS + FAIL} 通过' if not FAIL
          else f'\n结果: {PASS}/{PASS + FAIL}，失败 {FAIL} 项')
    return 1 if FAIL else 0


if __name__ == '__main__':
    raise SystemExit(asyncio.run(main()))
