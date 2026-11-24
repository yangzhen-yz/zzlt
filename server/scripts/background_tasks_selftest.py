"""background_tasks_selftest.py - 手动刷新后台任务托管自测（2026-11-23）

守的几条不变量：
  ① 同 key 重复触发被拒（反证：老写法 create_task 会叠跑两份重活）；
  ② 不同 key 可并行；
  ③ 任务持有强引用、状态从 running→done/failed，finished 时间落上；
  ④ 失败被记录而非静默吞掉；
  ⑤ 状态接口只回纯数据（不泄露 task 对象、不可 JSON 化内容）；
  ⑥ 源码守卫：5 处手动刷新 API 已无裸 create_task(_bg())。

纯内存打桩，不触发真实 RPS/K线回填。
"""
import os
import sys
import asyncio
import json

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.api import routes_state as st  # noqa: E402

FAILS = []


def ck(cond, msg):
    print(('  ✓ ' if cond else '  ✗ ') + msg)
    if not cond:
        FAILS.append(msg)


async def case():
    print('[1] 同 key 重复触发被拒（防叠跑）')
    gate = asyncio.Event()
    ran = []

    async def slow():
        ran.append(1)
        await gate.wait()

    ok1 = st.spawn_tracked('selftest_a', slow)
    await asyncio.sleep(0)              # 让 runner 起来
    ok2 = st.spawn_tracked('selftest_a', slow)
    ck(ok1 is True and ok2 is False, f'首次 True、重复 False（实得 {ok1}/{ok2}）')
    ck(len(ran) == 1, f'只跑了一份（实得 {len(ran)}）')

    print('[2] 不同 key 可并行')
    ok3 = st.spawn_tracked('selftest_b', slow)
    await asyncio.sleep(0)
    ck(ok3 is True and len(ran) == 2, f'不同 key 另起一份（实得 ran={len(ran)}）')

    s = st.background_tasks_status()
    d = {x['key']: x for x in s['data']}
    ck(d['selftest_a']['status'] == 'running' and d['selftest_b']['status'] == 'running',
       f'两任务都 running（实得 {[(k, v["status"]) for k, v in d.items()]}）')

    print('[3] 完成后状态落 done/finished')
    gate.set()
    await asyncio.sleep(0.02)
    s2 = {x['key']: x for x in st.background_tasks_status()['data']}
    for k in ('selftest_a', 'selftest_b'):
        ck(s2[k]['status'] == 'done' and s2[k]['finished'],
           f'{k} done 且有 finished（实得 {s2[k]["status"]}/{s2[k]["finished"]}）')

    print('[4] 失败被记录而非静默')
    async def boom():
        raise RuntimeError('模拟重活失败')

    st.spawn_tracked('selftest_fail', boom)
    await asyncio.sleep(0.02)
    f = {x['key']: x for x in st.background_tasks_status()['data']}['selftest_fail']
    ck(f['status'] == 'failed' and 'RuntimeError' in f['error'],
       f'失败状态与原因落上（实得 {f["status"]}/{f["error"]}）')

    print('[5] 状态接口可 JSON 序列化（不泄露 task）')
    payload = st.background_tasks_status()
    try:
        txt = json.dumps(payload, ensure_ascii=False)
        ck('task' not in txt and 'selftest_a' in txt, '状态纯数据、可序列化')
    except Exception as e:
        ck(False, f'JSON 序列化失败: {e}')


asyncio.run(case())

print('[6] 源码守卫：5 处手动刷新无裸 create_task')
root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for f in ('data.py', 'kline.py'):
    src = open(os.path.join(root, 'app', 'api', 'routers', f), encoding='utf-8').read()
    ck('create_task(_bg())' not in src, f'{f} 零裸 create_task')
    ck('spawn_tracked(' in src, f'{f} 使用托管入口')
ck(sum(open(os.path.join(root, 'app', 'api', 'routers', f), encoding='utf-8').read()
       .count('spawn_tracked(') for f in ('data.py', 'kline.py')) == 5,
   '恰好 5 处改走托管')

print(f'\n结果：{"全部通过" if not FAILS else str(len(FAILS)) + " 条失败"}')
for f in FAILS:
    print('  ✗ ' + f)
sys.exit(1 if FAILS else 0)
