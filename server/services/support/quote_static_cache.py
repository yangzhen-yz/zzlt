"""休市 / 非交易时段的单票行情「整包缓存」（2026-11-15）。

背景（用户实测提问：「今天不是交易日，查看股票价格属于历史数据，为什么加载这么慢」）：
``/api/stock/{symbol}/full`` 每次请求都 ``fetch_with_retry`` 去外网拉腾讯行情
（2 次重试、(1.5, 3) 超时、失败再退新浪），再并行跑 5 项增强数据 + 趋势 + 评分。
``_analysis_cache`` TTL 只有 3 秒（为盘中 2 秒轮询设计），休市日等于每次点票
都全量重算。线上响应头 ``x-process-time: 2973``，公网隧道 1.8～4.3s。

但休市日的价格是**上一交易日收盘定格值**，不会变；RPS / 基本面 / 龙虎榜 /
估值 / 股东数这些增强数据也都是日频，一天之内不会变。把它们当实时数据反复
从外网重拉，是纯浪费。

方案：**静态窗口内缓存整包响应**，内存 + 落盘（服务重启后依旧是热的，解决
「重启后冷缓存首开慢」）。窗口判定用交易日历（含节假日/调休），盘中完全旁路，
保持 2 秒轮询的实时性。手动刷新用 ``force=1`` 绕过。

2026-11-15 下午：窗口判定收拢到 ``support/closed_window.py``，与全站 GET 响应
缓存 ``closed_response_cache`` 共用一份口径，避免两套日历判断各自漂移。本模块
只保留单票特有的「增强项降级 → 5 分钟短 TTL」逻辑。

安全边界：
- 交易日历只硬编码到 2026 年，未知年份 **保守当作非静态窗口**（走实时），
  绝不因为日历过期而把旧价一直缓存下去。
- 缓存有效期不是固定 TTL，而是「到下一次盘中开始」的秒数（封顶 6 小时）：
  9:15 集合竞价一开就自动失效，不会出现开盘了还拿上周五收盘价。
- 降级结果（某一项增强数据超时）也写，但有效期封顶 5 分钟：既不固化残缺
  评分一整天，也不让「增强项偶发超时」把缓存判据整个架空 —— 线上实测第一版
  坚持「只写完整路径」，5 只票只有 2 只进得了快照，等于没缓存。
- 一键回滚：环境变量 ``QUOTE_STATIC_CACHE=0`` 或存在
  ``server/data/quote_static_cache.off``，即时生效、不用改代码不用重启。
"""
from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

# 窗口判定与 TTL 口径的唯一来源；re-export 让既有调用方/自测继续按 Q.* 使用。
from services.support.closed_window import (  # noqa: F401
    DEGRADED_TTL_SEC,
    MAX_TTL_SEC,
    is_static_window,
    static_ttl,
)

MAX_ENTRIES = 200                # 单票整包的内存/落盘容量上限

_SERVER_DIR = Path(__file__).resolve().parents[2]
_SNAPSHOT = _SERVER_DIR / 'data' / 'quote_static_snapshot.json'
_OFF_FILE = _SERVER_DIR / 'data' / 'quote_static_cache.off'

_LOCK = threading.Lock()
_ENTRIES: Dict[str, Dict[str, Any]] = {}
_LOADED = False


def enabled() -> bool:
    """一键回滚开关（每请求判一次，改文件/改环境变量即时生效）。"""
    if os.environ.get('QUOTE_STATIC_CACHE', '1') == '0':
        return False
    return not _OFF_FILE.exists()


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
        for sym, entry in (doc.get('entries') or {}).items():
            if (isinstance(entry, dict) and isinstance(entry.get('payload'), dict)
                    and entry['payload'].get('success')
                    and isinstance(entry.get('valid_until'), (int, float))):
                _ENTRIES[sym] = entry
    except Exception as e:
        print(f'[行情静态缓存] 快照读取失败（按空缓存继续）: {type(e).__name__}: {e}')


def get(symbol: str, *, force: bool = False,
        now: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
    """命中返回整包响应（原样可交给 FastAPI 序列化），未命中返回 None。"""
    if force or not enabled() or not is_static_window(now):
        return None
    with _LOCK:
        _ensure_loaded()
        entry = _ENTRIES.get(symbol)
        if not entry:
            return None
        if time.time() >= entry['valid_until']:
            _ENTRIES.pop(symbol, None)
            return None
        return entry['payload']


def put(symbol: str, payload: Dict[str, Any], *, degrade: bool = False,
        now: Optional[datetime] = None) -> None:
    """写入整包响应并原子落盘。只在静态窗口调用。

    ``degrade=True``（某一项增强数据超时降级）时也写，但有效期封顶
    ``DEGRADED_TTL_SEC``：既不固化残缺评分一整天，也不让「增强项偶发超时」
    把缓存判据整个架空。
    """
    if not enabled() or not is_static_window(now):
        return
    if not isinstance(payload, dict) or not payload.get('success'):
        return
    ttl = static_ttl(now)
    if degrade:
        ttl = min(ttl, DEGRADED_TTL_SEC)
    stamp = time.time()
    entry = {'ts': stamp, 'valid_until': stamp + ttl,
             'degraded': bool(degrade), 'payload': payload}
    with _LOCK:
        _ensure_loaded()
        _ENTRIES[symbol] = entry
        # 超上限按写入时间淘汰最旧的一批
        if len(_ENTRIES) > MAX_ENTRIES:
            oldest = sorted(_ENTRIES.items(), key=lambda kv: kv[1]['ts'])
            for k, _v in oldest[:len(_ENTRIES) - MAX_ENTRIES]:
                _ENTRIES.pop(k, None)
        _flush_locked()


def _flush_locked() -> None:
    """调用方必须持有 _LOCK；原子替换，写坏了也不影响内存里的热缓存。"""
    try:
        _SNAPSHOT.parent.mkdir(parents=True, exist_ok=True)
        tmp = _SNAPSHOT.with_suffix('.json.tmp')
        tmp.write_text(json.dumps({'version': 1, 'entries': _ENTRIES},
                                  ensure_ascii=False), encoding='utf-8')
        os.replace(tmp, _SNAPSHOT)
    except Exception as e:
        print(f'[行情静态缓存] 落盘失败（内存缓存仍有效）: {type(e).__name__}: {e}')


def invalidate(symbol: Optional[str] = None) -> None:
    """写后失效：日K同步 / 配置改动 / 手动刷新后可调用；None 表示全清。"""
    with _LOCK:
        _ensure_loaded()
        if symbol is None:
            _ENTRIES.clear()
        else:
            _ENTRIES.pop(symbol, None)
        _flush_locked()


def stats() -> Dict[str, Any]:
    """排障用：当前缓存条目数、静态窗口、TTL。"""
    with _LOCK:
        _ensure_loaded()
        return {'enabled': enabled(), 'staticWindow': is_static_window(),
                'ttlSec': round(static_ttl(), 1), 'entries': len(_ENTRIES),
                'newestTs': max((e['ts'] for e in _ENTRIES.values()), default=0)}
