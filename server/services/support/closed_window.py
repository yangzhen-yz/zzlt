"""统一的「非交易日 / 非交易时段」判定与缓存 TTL 口径（2026-11-15）。

背景：先给 ``/api/stock/{symbol}/full`` 做了单票整包静态缓存，随后用户要求
「其他接口也排查下，非交易日非交易时间能走缓存就走缓存」。若每个模块各写一份
交易日历判断，迟早出现「单票缓存认为休市、复盘缓存认为盘中」的口径分裂，
所以把窗口判定收拢到这里，``quote_static_cache``（单票）与
``closed_response_cache``（全站 GET 响应）共用同一份。

三个时间概念，别混：
- ``is_static_window``：价格/行情**不会变**的窗口（非交易日全天、交易日的
  盘前 / 午休 / 盘后）。盘中（9:15-11:30、13:00-15:05）为 False，缓存完全旁路。
- ``static_ttl``：单票行情缓存的有效秒数 = 距下一次盘中开始（2026-11-21 去掉
  6 小时封顶）。9:15 集合竞价一开自动失效，绝不拿上周五收盘价冒充现价。
  原 6h 封顶会让周末/夜间缓存固定过期一次，穿透到路由后撞上新浪兜底 10s
  超时（实测 8.6s）；而静态窗口内行情本就不变，缓存覆盖整段才对。
- ``cache_ttl``：全站响应缓存的有效秒数。比 ``static_ttl`` 多一层
  「盘后数据仍在落库」守卫：交易日 15:05 之后收评(15:10)、回测(15:20)、
  复盘(16:10)、双源校验(16:30)、收盘快照(18:15) 陆续写库，此时若给长 TTL，
  用户会看到一整晚的旧收评。所以交易日 9:15～20:00 内只给 60 秒短 TTL；
  20:00 之后以及整个非交易日才给长 TTL。

安全边界：交易日历只维护到 2026 年，未知年份一律保守走实时，
绝不因为日历过期而把旧数据长期缓存。
"""
from __future__ import annotations

from datetime import datetime, time as dtime, timedelta
from typing import Optional

from services.data.trading_calendar import is_calendar_covered, is_trading_day

# 盘中时段（当日分钟数，左闭右开）。9:15 起集合竞价价格就会动，15:05 收盘定格。
# 比前端 helpers.js 的 9:30 / 15:00 更保守：宁可少缓存 15 分钟，也不拿旧价冒充现价。
LIVE_WINDOWS = ((9 * 60 + 15, 11 * 60 + 30), (13 * 60, 15 * 60 + 5))
# 2026-11-21：原为 6*3600，作为 static_ttl 的封顶。周末/夜间行情本就不变，
# 6h 封顶却强制缓存过期一次 → 夜间访问穿透到新浪兜底（连接失败跑满 10s 超时，
# 实测 8.6s）。现改为「到下次开盘」精确给 TTL；这里只留一个更大的安全上限，
# 防日历异常导致 _next_session_start 算出离谱的天数，正常窗口都不会碰到它。
MAX_TTL_SEC = 72 * 3600      # 3 天安全上限（正常静态窗口 <= 一个周末+节假日）
DEGRADED_TTL_SEC = 300       # 增强数据降级的结果最多缓存 5 分钟

# 盘后定时任务窗口（当日分钟数）。20:00 之后当日数据全部定版，才允许长 TTL。
VOLATILE_FROM = 9 * 60 + 15
VOLATILE_UNTIL = 20 * 60
VOLATILE_TTL_SEC = 60.0


def is_static_window(now: Optional[datetime] = None) -> bool:
    """当前是否处于「行情不会变」的窗口（非交易日 或 非盘中时段）。

    允许传 now 是为了自测能覆盖周末/盘前/盘中/盘后/节假日边界，不需要 freeze
    全局时间。
    """
    now = now or datetime.now()
    day = now.date()
    if not is_calendar_covered(day):
        return False
    if not is_trading_day(day):
        return True
    mins = now.hour * 60 + now.minute
    return not any(a <= mins < b for a, b in LIVE_WINDOWS)


def _next_session_start(now: datetime) -> datetime:
    """下一次盘中开始的时刻；找不到（日历异常）就退化为 6 小时后。"""
    for delta in range(0, 10):
        day = (now + timedelta(days=delta)).date()
        if not is_calendar_covered(day) or not is_trading_day(day):
            continue
        for start_min in (9 * 60 + 15, 13 * 60):
            cand = datetime.combine(day, dtime(0)) + timedelta(minutes=start_min)
            if cand > now:
                return cand
    return now + timedelta(seconds=MAX_TTL_SEC)


def static_ttl(now: Optional[datetime] = None) -> float:
    """静态窗口内缓存的有效秒数 = 距下一次盘中开始，封顶 6 小时；盘中为 0。"""
    now = now or datetime.now()
    if not is_static_window(now):
        return 0.0
    return max(60.0, min((_next_session_start(now) - now).total_seconds(),
                         float(MAX_TTL_SEC)))


def is_volatile_window(now: Optional[datetime] = None) -> bool:
    """交易日 9:15～20:00：行情或盘后落库数据仍在变，缓存只能短 TTL。

    盘中已被 ``static_ttl`` 归零，这里真正起作用的是 11:30～13:00 午休与
    15:05～20:00 盘后两段。
    """
    now = now or datetime.now()
    day = now.date()
    if not is_calendar_covered(day) or not is_trading_day(day):
        return False
    mins = now.hour * 60 + now.minute
    return VOLATILE_FROM <= mins < VOLATILE_UNTIL


def cache_ttl(now: Optional[datetime] = None) -> float:
    """全站 GET 响应缓存的有效秒数：盘中 0、盘后短、休市长。"""
    now = now or datetime.now()
    ttl = static_ttl(now)
    if ttl <= 0:
        return 0.0
    if is_volatile_window(now):
        return max(5.0, min(ttl, VOLATILE_TTL_SEC))
    return ttl
