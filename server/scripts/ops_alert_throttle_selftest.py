# -*- coding: utf-8 -*-
"""Regression checks for the shared daily cap on non-stock service alerts."""
import json
import os
import subprocess
import shutil
import sys
import tempfile
import threading
import time
import importlib
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services.ops import alert_throttle as T

PASS, FAIL = [], []
_TMP = tempfile.mkdtemp(prefix='ops_alert_throttle_')
T.STATE_PATH = os.path.join(_TMP, 'state.json')
T._daily_limit = lambda: 2


def ck(cond, msg):
    (PASS if cond else FAIL).append(msg)
    print(f"  [{'OK' if cond else 'XX'}] {msg}")


def count():
    with open(T.STATE_PATH, encoding='utf-8') as f:
        return json.load(f)['count']


print('[1] 跨来源共享额度，达到每日上限后不再发送')
sent = []
ck(T.push_non_stock_alert('a', 'sched', lambda text: sent.append(text) or True),
   '第一条运维告警发送')
ck(T.push_non_stock_alert('b', 'db', lambda text: sent.append(text) or True),
   '第二个来源共用额度并发送')
ck(not T.push_non_stock_alert('c', 'http', lambda text: sent.append(text) or True),
   '第三个来源被每日总上限拦截')
ck(len(sent) == 2 and count() == 2, f'实际发送 2 次、计数 2（sent={len(sent)}）')

print('[2] 失败不占额度，进程重启后状态仍保留')
T.STATE_PATH = os.path.join(_TMP, 'retry.json')
ck(not T.push_non_stock_alert('fail', 'sched', lambda _: False), '发送失败返回 False')
ck(count() == 0, '失败发送已回退额度')
ck(T.push_non_stock_alert('ok', 'http', lambda _: True), '后续发送成功')
ck(count() == 1, '成功计数持久化到状态文件')
T = importlib.reload(T)
T.STATE_PATH = os.path.join(_TMP, 'retry.json')
T._daily_limit = lambda: 2
ck(T.push_non_stock_alert('ok2', 'db', lambda _: True), '重载模块状态后仍可使用剩余额度')
ck(not T.push_non_stock_alert('blocked', 'sched', lambda _: True), '重启后仍遵守每日上限')

print('[3] 自然日切换后旧日额度不阻塞新一天')
class _NextDay:
    @staticmethod
    def now():
        return datetime(2030, 1, 2)


T.datetime = _NextDay
T.STATE_PATH = os.path.join(_TMP, 'retry.json')
ck(T.push_non_stock_alert('next-day', 'sched', lambda _: True), '换日后允许新日首条告警')
ck(count() == 1, '换日后状态计数从 0 开始')

print('[4] 配置 0 禁止推送，非法状态 fail-closed')
T.datetime = datetime
T.STATE_PATH = os.path.join(_TMP, 'disabled.json')
T._daily_limit = lambda: 0
ck(not T.push_non_stock_alert('disabled', 'sched', lambda _: True), '上限 0 时不发送')
ck(not os.path.exists(T.STATE_PATH), '关闭时不创建额度状态')
T._daily_limit = lambda: 2
T.STATE_PATH = os.path.join(_TMP, 'corrupt.json')
with open(T.STATE_PATH, 'w', encoding='utf-8') as f:
    f.write('{broken')
ck(not T.push_non_stock_alert('corrupt', 'sched', lambda _: True), '损坏状态不绕过每日上限')

print('[5] 并发告警不可超过配置上限')
T.STATE_PATH = os.path.join(_TMP, 'concurrent.json')
sent_count = []
sent_lock = threading.Lock()


def sender(_):
    time.sleep(0.03)
    with sent_lock:
        sent_count.append(1)
    return True


threads = [threading.Thread(target=lambda: T.push_non_stock_alert('parallel', 'test', sender))
           for _ in range(8)]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()
ck(len(sent_count) == 2 and count() == 2,
   f'8 路并发只发送 2 次（sent={len(sent_count)}）')

print('[6] 多进程共用同一限额')
T.STATE_PATH = os.path.join(_TMP, 'multiprocess.json')
_child_code = (
    "import os; from services.ops import alert_throttle as t; "
    "t.STATE_PATH=os.environ['OPS_ALERT_STATE']; t._daily_limit=lambda:2; "
    "raise SystemExit(0 if t.push_non_stock_alert('x','child',lambda _:True) else 1)"
)
_child_env = os.environ.copy()
_server_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
_child_env['PYTHONPATH'] = _server_root + os.pathsep + _child_env.get('PYTHONPATH', '')
_child_env['OPS_ALERT_STATE'] = T.STATE_PATH
_children = [subprocess.Popen(
    [sys.executable, '-c', _child_code], env=_child_env,
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
) for _ in range(8)]
_exit_codes = [child.wait() for child in _children]
ck(_exit_codes.count(0) == 2 and count() == 2,
   f'8 个独立进程共发送 2 次（成功进程={_exit_codes.count(0)}）')

print(f'\n结果：{len(PASS)} 通过 / {len(FAIL)} 失败')
shutil.rmtree(_TMP)
if FAIL:
    for msg in FAIL:
        print('  失败：', msg)
    sys.exit(1)
