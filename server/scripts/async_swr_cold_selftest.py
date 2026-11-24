"""async_swr_cold_selftest.py - 回测冷算不压事件循环自测（2026-11-23）

守的几条不变量（后续计划 §1 第 12 条，11-07 实拍换来的）：
  ① cold 分支的 compute 在线程池跑，不在事件循环线程（反证：老写法直接
     await 一个同步 compute 会在主线程跑，耗时期间全站不响应）；
  ② 冷算期间事件循环仍能响应别的任务（用 asyncio.sleep(0) + 心跳任务实测）；
  ③ cacheable=False（参数白名单外）既不读也不写缓存，且冷算同样在线程池；
  ④ fresh 命中不调 compute；stale 命中立即返旧值、后台刷新照常写回；
  ⑤ 三个回测路由源码里不再有同步 swr_get 调用（防回退）。

不打网络、不碰生产库、不改口径。
"""
import os
import sys
import asyncio
import threading
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services.support import swr  # noqa: E402

FAILS = []
MAIN_TID = threading.get_ident()


def ck(cond, msg):
    print(('  ✓ ' if cond else '  ✗ ') + msg)
    if not cond:
        FAILS.append(msg)


print('[1] cold：compute 在子线程执行，事件循环线程不被占用')
store = {}
tids = []
calls = []


def slow_compute():
    tids.append(threading.get_ident())
    time.sleep(0.05)          # 模拟 7.4s 冷算的缩小版
    return {'v': len(calls)}


async def case1():
    # 心跳任务：如果没有 to_thread，同步 sleep 会堵住 loop，心跳计数为 0
    beats = []

    async def heartbeat():
        for _ in range(30):
            await asyncio.sleep(0.002)
            beats.append(1)

    hb = asyncio.create_task(heartbeat())
    data, state = await swr.async_swr_get(
        store, 'k', lambda: asyncio.to_thread(slow_compute),
        ttl=60, stale_max=3600, label='selftest', gap=120)
    await hb
    return data, state, beats


data, state, beats = asyncio.run(case1())
ck(state == 'cold' and data == {'v': 0}, f'冷算返回正常（state={state}, data={data}）')
ck(tids and tids[0] != MAIN_TID, f'compute 在子线程（thread={tids[0] if tids else None}）')
ck(len(beats) >= 20, f'冷算期间事件循环仍在转（心跳 {len(beats)} 次，反证：堵住则 <20）')

print('[2] cacheable=False：不读不写缓存，冷算同样在线程池')
store2 = {'k': {'ts': time.time(), 'data': {'v': 999}}}
tids2 = []
n2 = [0]


def compute2():
    tids2.append(threading.get_ident())
    n2[0] += 1
    return {'v': n2[0]}


async def case2():
    return await swr.async_swr_get(
        store2, 'k', lambda: asyncio.to_thread(compute2),
        ttl=60, stale_max=3600, label='selftest2', gap=120, cacheable=False)


d2, s2 = asyncio.run(case2())
ck(s2 == 'cold' and d2 == {'v': 1}, f'白名单外每次现算（state={s2}, data={d2}）')
ck(store2['k']['data'] == {'v': 999}, '缓存里那份没被覆盖')
ck(tids2 and tids2[0] != MAIN_TID, '白名单外的冷算也在线程池')

print('[3] fresh 命中：compute 一次都不调')
store3 = {}
n3 = [0]


def compute3():
    n3[0] += 1
    return {'v': n3[0]}


async def case3():
    fac = lambda: asyncio.to_thread(compute3)
    a = await swr.async_swr_get(store3, 'k', fac, ttl=60, stale_max=3600, label='s3')
    b = await swr.async_swr_get(store3, 'k', fac, ttl=60, stale_max=3600, label='s3')
    return a, b


(a, sa), (b, sb) = asyncio.run(case3())
ck(sa == 'cold' and sb == 'fresh' and n3[0] == 1,
   f'首算一次、二次命中（states={sa}/{sb}, calls={n3[0]}）')

print('[4] stale：立即返旧值，后台刷新仍写回')
store4 = {}
n4 = [0]


def compute4():
    n4[0] += 1
    return {'v': n4[0]}


async def case4():
    fac = lambda: asyncio.to_thread(compute4)
    await swr.async_swr_get(store4, 'k', fac, ttl=0.01, stale_max=3600, label='s4')
    await asyncio.sleep(0.03)   # 让首份过期
    d, s = await swr.async_swr_get(store4, 'k', fac, ttl=0.01, stale_max=3600, label='s4')
    await asyncio.sleep(0.05)   # 等后台刷新
    return d, s, store4['k']['data']


d4, s4, cached4 = asyncio.run(case4())
ck(s4 == 'stale' and d4 == {'v': 1}, f'过期立即返旧值（state={s4}, data={d4}）')
ck(cached4 == {'v': 2}, f'后台刷新写回新值（实得 {cached4}）')

print('[5] 源码守卫：三个回测路由不再同步调 swr_get')
root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
src = open(os.path.join(root, 'app', 'api', 'routers', 'recommend.py'), encoding='utf-8').read()
ck('_swr.swr_get(' not in src, 'recommend.py 里零同步 swr_get 调用')
ck(src.count('await _swr.async_swr_get(') == 3,
   f'三处回测接口已换 async 版（实得 {src.count("await _swr.async_swr_get(")}）')
swrsrc = open(os.path.join(root, 'services', 'support', 'swr.py'), encoding='utf-8').read()
ck('cacheable: bool = True' in swrsrc, 'async_swr_get 带 cacheable 守卫')

print(f'\n结果：{"全部通过" if not FAILS else str(len(FAILS)) + " 条失败"}')
for f in FAILS:
    print('  ✗ ' + f)
sys.exit(1 if FAILS else 0)
