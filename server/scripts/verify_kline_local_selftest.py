"""verify_kline_local_selftest.py - 补验优先本地日K、外部熔断不再整批失败（2026-11-23）

守的不变量：
  ① 本地有完整日K → 取"严格晚于预测日的第一根"算次日涨跌，结果正确；
  ② 预测日是本地最新 bar（今天）→ 返回 None，绝不拿当日 unfinished bar 冒充次日；
  ③ 本地缺票 → 回退外部 get_kline_data（打桩），行为与改造前一致；
  ④ 本地拿到的日期段不含预测日 → 回退外部（不能拿最近一根硬凑）；
  ⑤ 外部也失败 → 返回 None，不编数、不抛异常。

全部打桩，不碰生产库、不发真实网络请求。
"""
import os
import sys
import asyncio

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services.strategy import strategy as S  # noqa: E402

FAILS = []
ORIG_GET_KLINE = S.get_kline_data
import services.data.kline_history as kh
_ORIG_GET_ENGINE = kh.get_engine       # 收尾还原，避免污染同进程其它自测
CALLS = {'external': 0}


def ck(cond, msg):
    print(('  ✓ ' if cond else '  ✗ ') + msg)
    if not cond:
        FAILS.append(msg)


# 本地日K（升序，腾讯同构 [date, open, close, high, low, volume]）
LOCAL = [
    ['2026-11-10', 10.0, 10.5, 10.6, 9.9, 1000],
    ['2026-11-11', 10.5, 11.0, 11.1, 10.4, 1100],
    ['2026-11-12', 11.0, 11.55, 11.6, 10.9, 1200],
    ['2026-11-13', 11.55, 11.0, 11.7, 10.8, 1300],
]


class FakeEngine:
    def __init__(self, data):
        self.data = data

    def get_kline_tencent_format(self, symbol, limit=640):
        return [list(r) for r in self.data.get(symbol.lower(), [])]


def patch_engine(data):
    # strategy 模块里是函数内 `from services.data.kline_history import get_engine`，
    # 所以必须打在该模块的命名空间上。
    import services.data.kline_history as kh
    kh.get_engine = lambda: FakeEngine(data)


def patch_external(fn):
    async def wrapped(symbol):
        CALLS['external'] += 1
        return fn(symbol)
    S.get_kline_data = wrapped


print('[1] 本地有完整日K：按"严格晚于预测日的第一根"算次日涨跌')
patch_engine({'sh600001': LOCAL})
patch_external(lambda s: ck(False, f'不该走外部（{s}）') or [])
r = asyncio.run(S._get_next_day_change_from_kline('sh600001', '2026-11-11'))
# 11-11 close=11.0 → 11-12 close=11.55 → +5.0%
ck(r == 5.0, f'次日涨跌=+5.0%（实得 {r}）')
ck(CALLS['external'] == 0, f'外部零调用（实得 {CALLS["external"]}）')

print('[2] 反证：预测日是本地最新 bar → 返回 None，不拿当日冒充次日')
r2 = asyncio.run(S._get_next_day_change_from_kline('sh600001', '2026-11-13'))
ck(r2 is None, f'最新日无次日 → None（实得 {r2}）')

print('[3] 反证：预测日早于本地最早一根 → 回退外部拿更长历史')
CALLS['external'] = 0
LONGER = [['2026-11-01', 9.0, 10.0, 10.1, 8.9, 900]] + LOCAL  # 外部含预测日
patch_external(lambda s: LONGER)
r3 = asyncio.run(S._get_next_day_change_from_kline('sh600001', '2026-11-01'))
ck(CALLS['external'] == 1, f'回退了外部一次（实得 {CALLS["external"]}）')
# 11-01 close=10.0 → 11-10 close=10.5 → +5%
ck(r3 == 5.0, f'外部历史生效（实得 {r3}）')

print('[4] 本地缺票 → 回退外部')
patch_engine({})
CALLS['external'] = 0
r4 = asyncio.run(S._get_next_day_change_from_kline('sz999999', '2026-11-10'))
ck(CALLS['external'] >= 1, f'缺票时调了外部（实得 {CALLS["external"]}）')

print('[5] 外部也失败 → None，不编数')
async def boom(symbol):
    raise RuntimeError('模拟东财熔断')
S.get_kline_data = boom
r5 = asyncio.run(S._get_next_day_change_from_kline('sz999998', '2026-11-10'))
ck(r5 is None, f'双源都失败 → None（实得 {r5}）')

# 还原
S.get_kline_data = ORIG_GET_KLINE
kh.get_engine = _ORIG_GET_ENGINE

print(f'\n结果：{"全部通过" if not FAILS else str(len(FAILS)) + " 条失败"}')
for f in FAILS:
    print('  ✗ ' + f)
sys.exit(1 if FAILS else 0)
