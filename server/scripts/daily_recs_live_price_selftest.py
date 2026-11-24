"""daily_recs_live_price_selftest.py - 今日推荐卡片「现价回填但不算浮盈」自测（2026-11-23）

守的不变量：
  ① 今日票（isToday）拿到腾讯实时现价 currentPrice + 当日涨跌幅 currentChangePercent；
  ② **今日票绝不写 profitRatio**——买入参考价不是真实成交价，写进去就成了
     "按挂单价买入浮盈 X%"的误导（这是本次修复的红线，反证必须存在）；
  ③ 历史票行为逐字节不变：仍写 currentPrice + profitRatio；
  ④ 行情拉不到（空返回 / 脏数据）时不编价、不写假涨跌幅；
  ⑤ 打平场景：无 change_map 字段时 currentChangePercent 不出现（不编 0）。

全部用打桩，不碰生产库、不发真实网络请求。
"""
import os
import sys
import asyncio

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.api import routes_state as st  # noqa: E402

FAILS = []
TODAY = st.date.today().isoformat()
YDAY = (st.date.today() - st.timedelta(days=1)).isoformat() if hasattr(st.date, 'timedelta') else '2026-11-20'


def ck(cond, msg):
    print(('  ✓ ' if cond else '  ✗ ') + msg)
    if not cond:
        FAILS.append(msg)


# 腾讯返回行格式：v_sz000001="51~名字~代码~现价~昨收~...~涨跌~涨跌幅~..."
def _line(sym, price, pct, name='自测票'):
    code = sym[2:]
    parts = [''] * 34
    parts[1], parts[2], parts[3] = name, code, str(price)
    parts[32] = str(pct)
    return f'v_{sym}="{ "~".join(parts) }";'


def _run(fake_recs, quote_text):
    """打桩并跑一次构建，返回 (payload, 桩调用次数)"""
    orig_load, orig_get = st.load_daily_recommendations, st._sync_get
    calls = []

    def fake_load():
        # 深拷贝，避免多次 _run 之间互相污染
        import copy
        return copy.deepcopy(fake_recs)

    def fake_get(url):
        calls.append(url)
        class R:
            text = quote_text
        return R()

    st.load_daily_recommendations = fake_load
    st._sync_get = fake_get
    try:
        payload = asyncio.run(st._build_daily_recs_payload())
    finally:
        st.load_daily_recommendations, st._sync_get = orig_load, orig_get
    return payload, calls


print('[1] 今日票：回填现价＋涨跌幅，但不写 profitRatio')
recs = [{
    'date': TODAY,
    'daily_recommendations': [{
        'symbol': 'sz002242', 'name': '今日票', 'price': '10.00', 'changePercent': 1.0,
        'buySellPoints': {'buy': '10.20'},   # 买入参考比现价高，若算了 profitRatio 会是负数 → 必被反证抓到
    }],
}]
q = _line('sz002242', 11.50, 3.33)
payload, calls = _run(recs, q)
s = payload['data'][0]['daily_recommendations'][0]
ck(payload['data'][0]['isToday'] is True, '今日记录被标记 isToday')
ck(s.get('currentPrice') == 11.5, f'currentPrice 回填现价 11.5（实得 {s.get("currentPrice")}）')
ck(s.get('currentChangePercent') == 3.33,
   f'currentChangePercent 回填 3.33（实得 {s.get("currentChangePercent")}）')
ck('profitRatio' not in s,
   f'今日票**没有** profitRatio（反证：实得 {s.get("profitRatio")}）')
ck(s['price'] == '10.00' and s['buySellPoints']['buy'] == '10.20',
   '原始 price / 买入参考价未被改写（前端"推荐时价"仍可展示）')
ck(len(calls) == 1 and 'sz002242' in calls[0], f'今日票进入了行情批量拉取（{calls}）')

print('[2] 历史票：行为不变，仍写 currentPrice + profitRatio')
hrec = {
    'date': YDAY,
    'daily_recommendations': [{
        'symbol': 'sh600000', 'name': '历史票', 'price': '9.00', 'changePercent': 0.5,
        'buySellPoints': {'buy': '10.00'},
    }],
}
payload, _ = _run([hrec], _line('sh600000', 11.00, 2.0))
h = payload['data'][0]['daily_recommendations'][0]
ck(payload['data'][0]['isToday'] is False, '历史记录 isToday=False')
ck(h.get('currentPrice') == 11.0, f'历史票现价 11.0（实得 {h.get("currentPrice")}）')
ck(h.get('profitRatio') == 10.0, f'历史票 profitRatio=(11-10)/10=10%（实得 {h.get("profitRatio")}）')

print('[3] 混合日：今日票不写盈亏、历史票照写，互不串味')
payload, calls = _run([recs[0], hrec], _line('sz002242', 11.5, 3.33) + _line('sh600000', 11.0, 2.0))
a, b = payload['data'][0]['daily_recommendations'][0], payload['data'][1]['daily_recommendations'][0]
ck('profitRatio' not in a and b.get('profitRatio') == 10.0,
   f'今日无盈亏(={a.get("profitRatio")})、历史有(={b.get("profitRatio")})')
ck('sz002242' in calls[0] and 'sh600000' in calls[0], '两天的票合并成一次批量拉取')

print('[4] 反证：行情拉取失败/脏数据 → 不编价、不写假字段')
for label, qt in [('空返回', ''), ('脏数据', 'garbage;xxx=~~'), ('零现价', _line('sz002242', 0, 3.33))]:
    payload, _ = _run(recs, qt)
    x = payload['data'][0]['daily_recommendations'][0]
    ck('currentPrice' not in x and 'currentChangePercent' not in x and 'profitRatio' not in x,
       f'{label}时三个字段都不出现（实得 {[k for k in ("currentPrice","currentChangePercent","profitRatio") if k in x]}）')

print('[5] 反证：涨跌幅字段缺失 → 不写 currentChangePercent（不编 0）')
raw = _line('sz002242', 11.5, 3.33)
payload, _ = _run(recs, raw.split(';')[0].rsplit('~', 3)[0] + '~";')  # 砍掉尾部字段
x = payload['data'][0]['daily_recommendations'][0]
ck('currentChangePercent' not in x, f'字段缺失时不写（实得 {x.get("currentChangePercent")}）')

print(f'\n结果：{"全部通过" if not FAILS else str(len(FAILS)) + " 条失败"}')
for f in FAILS:
    print('  ✗ ' + f)
sys.exit(1 if FAILS else 0)
