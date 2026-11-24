# -*- coding: utf-8 -*-
"""real_fill_only_selftest.py - 守卫"生产回测只算真实能成交的买卖"（2026-11-24）。

背景：用户要求"去掉所有假设，只保留确实能买进和卖出的场景"。此前生产回测里
有一条 auction（假设封板票次日竞价也买到了）和一条 filled（假设排板 100% 成交），
页面默认还高亮它们，导致 -38% 的反事实曲线被读成策略实绩。这两条已从生产引擎、
页面、每日监控、卡片出口文案中删除。本自测长期守住这个边界。

运行：python server/scripts/real_fill_only_selftest.py
只用源码断言 + 纯函数撮合，不碰真库、不发外部请求。
"""
import os
import sys

SERVER = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT = os.path.dirname(SERVER)
sys.path.insert(0, SERVER)

from services.pick import rec_sim as S  # noqa: E402

PASS = 0
FAIL = 0


def ck(cond, msg):
    global PASS, FAIL
    if cond:
        PASS += 1
        print('  [OK] ' + msg)
    else:
        FAIL += 1
        print('  [NG] ' + msg)


def src(rel):
    with open(os.path.join(ROOT, rel), encoding='utf-8') as fh:
        return fh.read()


print('[1] 生产引擎只暴露真实成交入口')
ck(set(S.ENTRIES) == {'tailclose', 'paiban'},
   f'ENTRIES 只剩 tailclose 与离线 paiban（实得 {sorted(S.ENTRIES)}）')
ck(S.DEFAULT_CURVES == ('tailclose_open',),
   f'默认曲线只有 tailclose_open（实得 {S.DEFAULT_CURVES}）')

print('[2] simulate() 只返回两条真实曲线')
from unittest import mock  # noqa: E402

fake_recs = [{'rec_date': '2026-11-20', 'symbol': 'sh600000', 'name': '测试票',
              'price': 10.0, 'score': 60.0, 'change_percent': 9.9, 'buy_timing': ''}]
# 普通日：开高低收都在 10 附近，前收 10，买价取记录价，次日开盘卖。
fake_bars = {'600000': [('2026-11-19', 9.5, 10.0, 9.4, 10.0),
                        ('2026-11-20', 10.0, 10.5, 9.9, 10.4),
                        ('2026-11-21', 10.4, 10.6, 10.3, 10.5)]}
with mock.patch.object(S, 'load', return_value=(fake_recs, fake_bars)):
    out = S.simulate(since='2026-11-20')
keys = [c['key'] for c in out['curves']]
ck(keys == ['tailclose_open', 'tailclose_close'],
   f'两条曲线键精确匹配（实得 {keys}）')
ck(all(c['entry'] == 'tailclose' for c in out['curves']), '两条入口都是 tailclose')
ck(not any(k.startswith('auction_') or k.startswith('filled_') for k in keys),
   'auction_/filled_ 假设曲线零残留')

print('[3] 一字涨停买不到：反证必须被剔除')
# 2026-11-20 全天钉在涨停价 11.0（前收 10.0，10% 涨停），没有可买价。
hard_bars = {'600000': [('2026-11-19', 9.5, 10.0, 9.4, 10.0),
                        ('2026-11-20', 11.0, 11.0, 11.0, 11.0),
                        ('2026-11-21', 11.0, 11.2, 10.8, 11.1)]}
orders, skipped = S.plan(fake_recs, hard_bars, 'open', entry='tailclose')
ck(not orders, f'一字板零成交订单（实得 {len(orders)}）')
ck(any('买不到' in why for _, why in skipped),
   f'跳过原因明确写买不到（实得 {[w for _, w in skipped]}）')

print('[4] 页面、批次、每日监控、卡片文案都不再引用假设曲线')
vue = src('client-vue/src/components/tabs/TabBacktest.vue')
ck("byKey('auction_open')" not in vue and "byKey('filled_open')" not in vue,
   '前端没有 auction/filled 的 byKey 引用')
ck('PRIOR_SHOWN = [\'tailclose_open\', \'tailclose_close\']' in vue,
   '首屏只画两条真实曲线')
ck('真实成交口径' in vue, '页面明确写真实成交口径')
batch = src('server/services/pick/rec_sim_batch.py')
ck("_curve_stats('tailclose'" in batch and "_curve_stats('auction'" not in batch,
   '拆批次回测改走 tailclose')
api = src('server/services/pick/recommend.py')
ck("key IN ('tailclose_open','tailclose_close')" in api,
   '卡片出口文案读 tailclose 两行，不再读 filled')
ck("key IN ('filled_open','filled_close')" not in api, '卡片不读 filled 假设数')

print('[5] 源码守卫：生产文件不得再引入假设入口调用')
for rel in ('server/services/pick/rec_sim.py', 'server/services/pick/rec_sim_batch.py',
            'server/app/api/routers/recommend.py'):
    text = src(rel)
    calls = [ln for ln in text.splitlines()
             if ('_curve_stats(\'auction\'' in ln or '_curve_stats(\'filled\'' in ln
                 or 'entry == \'auction\'' in ln or 'entry=\'auction\'' in ln)]
    ck(not calls, f'{rel} 无 auction/filled 生产调用')

print(f'结果：{PASS} 通过 / {FAIL} 失败')
raise SystemExit(1 if FAIL else 0)
