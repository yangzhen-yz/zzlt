"""全站 GET 响应的「休市静态窗口整包缓存」ASGI 中间件（2026-11-15）。

背景（用户要求：「其他接口也排查下，非交易日非交易时间能走缓存就走缓存，
没必要每次都实时获取数据」）：

排查发现除了已治理的 ``/api/stock/{symbol}/full``，还有一大批接口在非交易日
仍然每次现拉外网或全表重算：

- 零缓存裸奔：``/api/kline/{symbol}/minute`` 直连腾讯分时、``/eastmoney/*``
  代理直转东财 clist、``/api/stock-concepts/{symbol}`` 直连东财/同花顺。
- 只有 5 秒缓存：``fetch_with_retry`` 系（``/api/stock/{symbol}``、
  ``/api/stocks``、``/api/hot-stocks``、``/api/stock-search``、``/api/market-index``）。
- 只有固定短 TTL：``/api/market-index`` / ``theme-index`` / ``cls-ladder`` /
  ``kline-history/stats`` 统一 60s SWR，休市切 Tab 仍不断后台刷新。
- 日频产物但每次现算：``/api/trend/history-counts``、``/api/recommend-review``、
  ``/api/recommend-sim*``、``/api/recommend-excess``、``/api/daily-reviews`` 等。

逐个接口打补丁会变成十几份口径各异的缓存，所以这里做成 **一层 ASGI 中间件**：
在静态窗口内对白名单 GET 接口缓存「压缩后的整包响应」（status + headers + body），
命中直接回放，不进路由、不触发 SWR 后台刷新、不占 GIL。盘中完全旁路，
原有 5s / 60s 实时缓存与 2 秒轮询行为一字不变。

为什么缓存压缩后的 body：本中间件挂在 GZip 外层，命中时直接回放 gzip bytes，
避免每请求重新压缩大响应体（``/api/daily-reviews`` 111KB 的教训见知识库 §4）；
``accept-encoding`` 是否支持 gzip 已进入缓存键。

与盘中/盘后的边界（口径全部来自 ``closed_window``，不另写日历）：
- 盘中 9:15-11:30 / 13:00-15:05：完全不缓存。
- 交易日 15:05-20:00：收评/回测/复盘/收盘快照陆续落库，只给 60s 短 TTL，
  避免用户看一整晚旧收评。
- 20:00 之后、盘前、午休、周末/节假日：长 TTL（到下一次开盘，封顶 6h）。
- 任何非 GET/HEAD 的 2xx 写操作都会清空缓存，保证「手动生成收评/推荐」后
  立刻看到新数据。
- 交易日历未覆盖的年份保守走实时。

白名单是**显式 allowlist**：只列与登录用户无关、休市日内容不变的只读接口。
持仓 / 模拟盘 / 行动台 / 自定义池 / 标签 / 配置 / 健康检查 / 优化任务状态等
一律不缓存。``/api/stock/{symbol}/full`` 由 ``quote_static_cache`` 专管
（它有增强项降级短 TTL 的额外语义），这里显式排除避免双份快照。

一键回滚：环境变量 ``CLOSED_CACHE=0`` 或存在
``server/data/closed_response_cache.off``，每请求判定，改文件即时生效。
"""
from __future__ import annotations

import base64
import gzip
import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl

from services.support.closed_window import cache_ttl, is_static_window

# ── 配置 ──────────────────────────────────────────────────────────────────
MAX_ENTRIES = 300
MAX_BODY_BYTES = 2 * 1024 * 1024      # 单条响应上限；超过不缓存（也不截断回放）
MAX_DISK_BYTES = 12 * 1024 * 1024    # 落盘总体积上限（base64 后），超过淘汰最旧

_SERVER_DIR = Path(__file__).resolve().parents[2]
_SNAPSHOT = _SERVER_DIR / 'data' / 'closed_response_snapshot.json'
_OFF_FILE = _SERVER_DIR / 'data' / 'closed_response_cache.off'

# 与登录用户无关、休市日内容不变的只读接口。前缀匹配，按最长语义排序无所谓。
CACHEABLE_PREFIXES: Tuple[str, ...] = (
    '/api/stock/',                 # 单票行情 / analysis（full 下面单独排除）
    '/api/stocks',
    '/api/stock-search',
    '/api/stock-map',
    '/api/stock-concepts/',
    '/api/stock-diagnose/',
    '/api/hot-stocks',            # 含 /api/hot-stocks-scan
    '/api/hot_search_ranking',
    '/api/market-index',
    '/api/market-alert',
    '/api/market-predict',
    '/api/auction/latest',
    '/api/fund-flow',
    '/api/rps',
    '/api/dragon-tiger',
    '/api/northbound',
    '/api/valuation',
    '/api/kline/',                # 日K + 分时（休市定格）
    '/api/kline-history/stats',
    '/api/cls-plate-stocks',
    '/api/cls-ladder',
    '/api/theme-index',
    '/api/daily-reviews',
    '/api/daily_recommendations',
    '/api/recommend-review',
    '/api/recommend-sim',
    '/api/recommend-sim-batch',
    '/api/recommend-excess',
    '/api/recommend-batches',
    '/api/shadow-eval',
    '/api/eval-lab/latest',
    '/api/trend-stocks',
    '/api/trend/history-counts',
    '/api/trend/backtest-latest',
    '/api/trend/backtest-history',
    '/api/trend/backtest-records',
    '/api/trend/backtest-summary',
    '/api/strategy/weights',
    '/api/strategy/backtest-history',
    '/api/strategy/predictions',
    '/api/optimization/history',
    '/api/optimization/trend',
    '/eastmoney/',                # 前端复盘页直连东财的代理通道
)

# 显式排除（优先于白名单）：用户态数据、触发式 GET、健康/运维探针、可变配置。
DENY_PREFIXES: Tuple[str, ...] = (
    '/api/auth/', '/api/positions', '/api/push-channels', '/api/sim/',
    '/api/account/', '/api/settings', '/api/tab-scan-config',
    '/api/custom-scan-pool', '/api/stock-tags',
    '/api/action-desk', '/api/optimization/status',
    '/api/api-usage', '/api/health', '/api/data-sources/', '/api/system/',
    '/api/internal/', '/api/update-', '/api/scan-', '/api/generate-',
    '/api/kline-history/sync', '/api/kline-history/backfill',
    '/api/eval-lab/run',
)
_FULL_SUFFIX = '/full'                           # /full 由 quote_static_cache 专管

# query 中出现这些参数说明调用方明确要新鲜数据 / 是 cache-buster → 不缓存
_BYPASS_QUERY_KEYS = frozenset({
    'force', 'refresh', 'nocache', 'nostore', 'cachebust', 'random', 'rnd',
    '_', '_t', 'timestamp', 't', 'ts', 'nonce',
})

# 回放时必须丢掉的逐跳头 / 由外层重新生成的头（date 回放旧值会扰乱浏览器缓存判断）
_DROP_HEADERS = frozenset({
    'connection', 'keep-alive', 'transfer-encoding', 'date', 'server',
    'x-process-time', 'set-cookie', 'content-length',
})
# 命中后附加的排障头
_HIT_HEADER = 'x-closed-cache'

_LOCK = threading.Lock()
_ENTRIES: Dict[str, Dict[str, Any]] = {}
_LOADED = False
_STATS: Dict[str, int] = {
    'hits': 0, 'misses': 0, 'stores': 0, 'bypasses': 0, 'invalidations': 0,
}


# ── 开关与窗口 ────────────────────────────────────────────────────────────
def enabled() -> bool:
    """一键回滚开关（每请求判一次，改文件/改环境变量即时生效）。"""
    if os.environ.get('CLOSED_CACHE', '1') == '0':
        return False
    return not _OFF_FILE.exists()


def _bump(name: str) -> None:
    with _LOCK:
        _STATS[name] = _STATS.get(name, 0) + 1


def _header_get(headers, name: str, default: str = '') -> str:
    """headers 可以是 ASGI 的 bytes 对，也可以是缓存里的 str 对。"""
    want = name.lower().encode('latin-1')
    for k, v in headers or ():
        kb = k if isinstance(k, bytes) else str(k).encode('latin-1')
        if kb.lower() == want:
            return (v.decode('latin-1') if isinstance(v, bytes) else str(v))
    return default


def _is_cacheable_path(path: str) -> bool:
    """显式 allowlist + deny 优先。返回 True 表示这个 GET 允许进缓存。"""
    if not path.startswith('/api/') and not path.startswith('/eastmoney/'):
        return False
    # 单票路径单独判定：/full 归 quote_static_cache 专管，只放行基础行情与 analysis
    if path.startswith('/api/stock/'):
        if path.endswith(_FULL_SUFFIX):
            return False
        return path.endswith('/analysis') or path.count('/') == 3
    for d in DENY_PREFIXES:
        if path.startswith(d):
            return False
    return any(path.startswith(p) for p in CACHEABLE_PREFIXES)


def make_key(path: str, raw_query: bytes, gzip_ok: bool) -> Optional[str]:
    """构造缓存键；返回 None 表示这次请求必须绕过（cache-buster / force）。"""
    qs = (raw_query or b'').decode('latin-1')
    pairs = parse_qsl(qs, keep_blank_values=True) if qs else []
    for k, _v in pairs:
        if k.lower() in _BYPASS_QUERY_KEYS:
            return None
    norm = '&'.join(f'{k}={v}' for k, v in sorted(pairs))
    return f'GET|{path}|{norm}|gz={1 if gzip_ok else 0}'


# ── 内存 + 落盘 ───────────────────────────────────────────────────────────
def _ensure_loaded() -> None:
    """懒加载磁盘快照（进程启动后第一次访问才读，失败只当空缓存）。"""
    global _LOADED
    if _LOADED:
        return
    _LOADED = True
    try:
        if not _SNAPSHOT.exists():
            return
        doc = json.loads(_SNAPSHOT.read_text(encoding='utf-8'))
        if doc.get('version') != 1:
            return
        for key, entry in (doc.get('entries') or {}).items():
            if (isinstance(entry, dict) and isinstance(entry.get('body'), str)
                    and isinstance(entry.get('valid_until'), (int, float))
                    and isinstance(entry.get('headers'), list)):
                # 兼容旧快照：曾经把显式声明 no-cache 的响应也缓存过，加载时剔掉
                cc = _header_get(entry['headers'], 'cache-control').lower()
                if 'no-cache' in cc or 'no-store' in cc:
                    continue
                _ENTRIES[key] = entry
    except Exception as e:
        print(f'[休市响应缓存] 快照读取失败（按空缓存继续）: {type(e).__name__}: {e}')


def get(key: str) -> Optional[Dict[str, Any]]:
    """命中返回缓存条目（含 status/headers/body），未命中/过期返回 None。"""
    if not enabled() or not is_static_window():
        return None
    with _LOCK:
        _ensure_loaded()
        entry = _ENTRIES.get(key)
        if not entry:
            _STATS['misses'] += 1
            return None
        if time.time() >= entry['valid_until']:
            _ENTRIES.pop(key, None)
            _STATS['misses'] += 1
            return None
        _STATS['hits'] += 1
        return entry


def put(key: str, status: int, headers: List[Tuple[str, str]], body: bytes) -> None:
    """写入整包响应并原子落盘。只在静态窗口、且响应通过校验后调用。"""
    if not enabled() or not is_static_window():
        return
    ttl = cache_ttl()
    if ttl <= 0 or not body or len(body) > MAX_BODY_BYTES:
        return
    stamp = time.time()
    entry: Dict[str, Any] = {
        'ts': stamp,
        'valid_until': stamp + ttl,
        'status': status,
        'headers': headers,
        'body': base64.b64encode(body).decode('ascii'),
        'size': len(body),
    }
    with _LOCK:
        _ensure_loaded()
        _ENTRIES[key] = entry
        _STATS['stores'] += 1
        _evict_locked()
        _flush_locked()


def _evict_locked() -> None:
    """调用方持锁：条目数 / 总体积双上限，按写入时间淘汰最旧。"""
    items = sorted(_ENTRIES.items(), key=lambda kv: kv[1].get('ts', 0))
    total = sum(e.get('size', 0) for _k, e in items)
    # base64 膨胀约 4/3，按膨胀后估算磁盘占用
    disk = total * 4 // 3
    for k, e in items:
        if len(_ENTRIES) <= MAX_ENTRIES and disk <= MAX_DISK_BYTES:
            break
        _ENTRIES.pop(k, None)
        total -= e.get('size', 0)
        disk = total * 4 // 3


def _flush_locked() -> None:
    """调用方必须持有 _LOCK；原子替换，写坏了也不影响内存里的热缓存。"""
    try:
        _SNAPSHOT.parent.mkdir(parents=True, exist_ok=True)
        tmp = _SNAPSHOT.with_suffix('.json.tmp')
        tmp.write_text(json.dumps({'version': 1, 'entries': _ENTRIES},
                                  ensure_ascii=False), encoding='utf-8')
        os.replace(tmp, _SNAPSHOT)
    except Exception as e:
        print(f'[休市响应缓存] 落盘失败（内存缓存仍有效）: {type(e).__name__}: {e}')


def invalidate(prefix: Optional[str] = None) -> None:
    """写后失效：任何写操作成功后调用；None 表示全清。"""
    with _LOCK:
        _ensure_loaded()
        if prefix is None:
            _ENTRIES.clear()
        else:
            for k in [k for k in _ENTRIES if prefix in k]:
                _ENTRIES.pop(k, None)
        _STATS['invalidations'] += 1
        _flush_locked()


def stats() -> Dict[str, Any]:
    """排障用：窗口、TTL、条目数、体积、命中计数。"""
    with _LOCK:
        _ensure_loaded()
        return {
            'enabled': enabled(),
            'staticWindow': is_static_window(),
            'ttlSec': round(cache_ttl(), 1),
            'entries': len(_ENTRIES),
            'bytes': sum(e.get('size', 0) for e in _ENTRIES.values()),
            **dict(_STATS),
        }


# ── 响应合法性校验 ────────────────────────────────────────────────────────
def _response_is_good(status: int, headers, body: bytes) -> bool:
    """只缓存成功、非私有、非错误的响应，避免把 200 包着的错误固化一整晚。"""
    if status != 200 or not body or len(body) > MAX_BODY_BYTES:
        return False
    ctype = _header_get(headers, 'content-type').lower()
    if not (ctype.startswith('application/json') or ctype.startswith('text/')
            or 'json' in ctype):
        return False
    if _header_get(headers, 'set-cookie'):
        return False
    # 尊重服务端声明：/api/daily-reviews 明确发 `private, no-cache`（允许客户端
    # 存储但每次必须重验证），中间层不能整包缓存后跳过重验证；它本来就有
    # ETag/304 + 预压缩 bundle，不需要再叠一层。
    cc = _header_get(headers, 'cache-control').lower()
    if 'no-store' in cc or 'no-cache' in cc:
        return False
    # 明文检查业务错误字段：缓存的是 gzip 后的 bytes，这里只在**写入时**解压一次，
    # 命中路径零解压开销。非 JSON（jsonp / 东财代理文本）解析失败即放行。
    raw = body
    if 'gzip' in _header_get(headers, 'content-encoding').lower():
        try:
            raw = gzip.decompress(body)
        except Exception:
            return True          # 解不开就只按 status 判，不误杀
    try:
        doc = json.loads(raw.decode('utf-8', 'replace'))
    except Exception:
        return True
    if isinstance(doc, dict):
        if doc.get('success') is False or doc.get('ok') is False:
            return False
        if doc.get('error') or doc.get('detail'):
            return False
    return True


def _clean_headers(headers) -> List[Tuple[str, str]]:
    """ASGI bytes 头 → 可序列化 str 头，丢掉逐跳头与由外层重新生成的头。"""
    out: List[Tuple[str, str]] = []
    for k, v in headers or ():
        ks = k.decode('latin-1') if isinstance(k, bytes) else str(k)
        vs = v.decode('latin-1') if isinstance(v, bytes) else str(v)
        if ks.lower() in _DROP_HEADERS or ks.lower() == _HIT_HEADER:
            continue
        out.append((ks, vs))
    return out


def _as_bytes_headers(items: List[Tuple[str, str]]):
    return [(k.encode('latin-1'), v.encode('latin-1')) for k, v in items]


# ── ASGI 中间件 ───────────────────────────────────────────────────────────
class ClosedWindowCacheMiddleware:
    """纯 ASGI 中间件（不用 BaseHTTPMiddleware，避免二次 buffering 与 context 丢失）。

    挂载位置：``config_reload`` → CORS → **本中间件** → GZip → router。
    这样缓存的是 GZip 已压缩的 bytes，命中直接回放；CORS 头由外层每次补，
    ``X-Process-Time`` 由外层每次重算（命中时显示的是真实命中耗时）。
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope.get('type') != 'http':
            await self.app(scope, receive, send)
            return

        method = scope.get('method', 'GET').upper()
        path = scope.get('path', '') or ''
        headers = scope.get('headers', [])

        # 写操作：成功响应后清空缓存，保证手动生成/修改后立刻可见
        if method not in ('GET', 'HEAD'):
            status_box = {'status': 0}

            async def write_send(message):
                if message['type'] == 'http.response.start':
                    status_box['status'] = message.get('status', 0)
                await send(message)
                if (message['type'] == 'http.response.body'
                        and not message.get('more_body')
                        and 200 <= status_box['status'] < 300):
                    invalidate()

            await self.app(scope, receive, write_send)
            return

        # 盘中 / 开关关闭时零额外开销直接透传（连计数锁都不拿）
        if method != 'GET' or not enabled() or not is_static_window():
            await self.app(scope, receive, send)
            return
        if (not _is_cacheable_path(path)
                or 'no-cache' in _header_get(headers, 'cache-control').lower()
                or 'no-cache' in _header_get(headers, 'pragma').lower()):
            _bump('bypasses')
            await self.app(scope, receive, send)
            return

        gzip_ok = 'gzip' in _header_get(headers, 'accept-encoding').lower()
        key = make_key(path, scope.get('query_string', b''), gzip_ok)
        if key is None:
            _bump('bypasses')
            await self.app(scope, receive, send)
            return

        cached = get(key)
        if cached is not None:
            body = base64.b64decode(cached['body'])
            hit_headers = _clean_headers(cached['headers'])
            age = max(0, int(time.time() - cached.get('ts', time.time())))
            hit_headers.append((_HIT_HEADER, 'hit'))
            hit_headers.append(('x-closed-cache-age', str(age)))

            # 保留 ETag 协商：命中时若客户端带的 If-None-Match 一致，回 304 空体
            etag = _header_get(hit_headers, 'etag')
            inm = _header_get(headers, 'if-none-match')
            if etag and inm and (inm.strip() in ('*', etag.strip())
                                 or etag.strip() in inm):
                keep = [(k, v) for k, v in hit_headers
                        if k.lower() in ('etag', 'cache-control', 'vary',
                                         _HIT_HEADER, 'x-closed-cache-age')]
                await send({'type': 'http.response.start', 'status': 304,
                            'headers': _as_bytes_headers(keep)})
                await send({'type': 'http.response.body', 'body': b'',
                            'more_body': False})
                return

            hit_headers.append(('content-length', str(len(body))))
            await send({'type': 'http.response.start',
                        'status': cached.get('status', 200),
                        'headers': _as_bytes_headers(hit_headers)})
            await send({'type': 'http.response.body', 'body': body,
                        'more_body': False})
            return

        # 未命中：原样透传，同时收集响应；只在最后一个 body 之后写缓存
        start_headers: List[Tuple[str, str]] = []
        status = {'code': 0}
        chunks: List[bytes] = []
        aborted = {'flag': False}

        async def capture_send(message):
            if message['type'] == 'http.response.start':
                status['code'] = message.get('status', 0)
                start_headers.extend(_clean_headers(message.get('headers', [])))
                await send(message)
                return
            if message['type'] == 'http.response.body':
                chunk = message.get('body', b'') or b''
                if not aborted['flag']:
                    chunks.append(chunk)
                    if sum(len(c) for c in chunks) > MAX_BODY_BYTES:
                        aborted['flag'] = True
                        chunks.clear()
                await send(message)
                if not message.get('more_body'):
                    if not aborted['flag'] and _response_is_good(
                            status['code'], start_headers, b''.join(chunks)):
                        put(key, status['code'], start_headers, b''.join(chunks))
                return
            await send(message)

        await self.app(scope, receive, capture_send)
