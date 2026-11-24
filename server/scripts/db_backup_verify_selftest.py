"""db_backup_verify_selftest.py - 备份副本完整性校验与告警自测（2026-11-23）

背景：db_backup.py 从 11-05 起就每天 03:30 备份三库（manifest 实证连续 8 天），
文档"备份自动化未做"是过期结论。真正缺的是**验证备份副本没写坏 + 失败告警**：
原脚本只 print 到日志，scheduler 拉起子进程后从不看退出码，备份失败无人知晓。

本测守的不变量：
  ① 正常备份：manifest.ok=True、每库 check='ok'、副本可读且行数一致；
  ② 副本损坏 → verify_backup 抛异常，manifest.ok=False + 推告警（打桩捕获）；
  ③ 主库缺字段/库文件缺失不误伤其它库（失败按库记录，不整批静默）；
  ④ dry-run 不落盘、不校验、不告警；
  ⑤ 源码守卫：scheduler 的备份 job 有退出码回调（防回退成"拉起不管"）。

全部在临时目录造小库，不碰 server/data 与真实 analysis_out，不打真实企微。
"""
import os
import sys
import json
import shutil
import sqlite3
import tempfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts import db_backup as B  # noqa: E402

FAILS = []
ALERTS = []


def ck(cond, msg):
    print(('  [OK] ' if cond else '  [NG] ') + msg)
    if not cond:
        FAILS.append(msg)


def _mkdb(path, rows=20):
    conn = sqlite3.connect(path)
    conn.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)')
    conn.executemany('INSERT INTO t(v) VALUES (?)', [(f'row{i}',) for i in range(rows)])
    conn.commit()
    conn.close()


tmp = tempfile.mkdtemp(prefix='db_backup_verify_')
data = os.path.join(tmp, 'data')
os.makedirs(data)
# 三库小副本（主库+日K+分钟）；discover_dbs 硬读全局 config 的真实主库路径，
# 会把生产库混进临时目录、跨盘符 relpath 报错，所以下面打桩 discover——
# 本测对象是「副本校验 + 告警」，不是 discover（生产同盘，已连续 8 天实证）。
_mkdb(os.path.join(tmp, 'gupiao.db'), 15)
_mkdb(os.path.join(data, 'history_kline.db'), 40)
_mkdb(os.path.join(data, 'history_minute.db'), 10)
B.discover_dbs = lambda server_dir=None: [
    os.path.join(tmp, 'gupiao.db'),
    os.path.join(data, 'history_kline.db'),
    os.path.join(data, 'history_minute.db'),
]

# 告警打桩：只在本模块命名空间打，避免影响别处
B._alert = lambda text: ALERTS.append(text)

print('[1] 正常备份：manifest.ok=True、每库 check=ok、副本可读')
m = B.run_backup('incremental', server_dir=tmp, keep_days=7)
ck(m['ok'] is True, f"manifest.ok=True（实得 {m['ok']}）")
ck(len(m['files']) == 3, f'备份 3 库（实得 {len(m['files'])}）')
ck(all(f['check'] == 'ok' for f in m['files']), f"每库 check=ok（实得 {[f['check'] for f in m['files']]}）")
ck(all(os.path.exists(f['dst']) for f in m['files']), '副本文件都在')
# 副本行数一致
for f in m['files']:
    c = sqlite3.connect(f'file:{f["dst"]}?mode=ro', uri=True)
    n = c.execute('SELECT COUNT(*) FROM t').fetchone()[0]
    c.close()
    src_n = (15 if 'gupiao.db' in f['src'] else
             40 if 'history_kline' in f['src'] else 10)
    ck(n == src_n, f'{os.path.basename(f["src"])} 副本行数一致（{n}）')
man_path = os.path.join(os.path.dirname(m['files'][0]['dst']), 'manifest.json')
ck(os.path.exists(man_path), 'manifest.json 落盘')
ck(not ALERTS, f'成功不告警（实得 {len(ALERTS)} 条）')

print('[2] 副本损坏：verify_backup 抛异常（反证：真能抓到坏备份）')
bad = os.path.join(data, 'bad.db')
_mkdb(bad, 5)
with open(bad, 'r+b') as fh:
    fh.seek(4096)
    fh.write(b'\xff' * 64)          # 破坏页内容
try:
    B.verify_backup(bad)
    caught = False
except Exception as e:
    caught = True
    print(f'    捕获: {type(e).__name__}')
ck(caught, '损坏副本被 verify_backup 抓到')

print('[3] 备份中途损坏 → manifest.ok=False + 告警（打桩）')
# 打桩 backup_one：对 history_minute 故意写一个坏库
orig_backup = B.backup_one


def bad_backup(src, dst):
    n = orig_backup(src, dst)
    if 'history_minute' in src:
        with open(dst, 'r+b') as fh:
            fh.seek(4096)
            fh.write(b'\xff' * 64)
    return n


B.backup_one = bad_backup
try:
    m2 = B.run_backup('incremental', server_dir=tmp, keep_days=7)
    raised = None
except RuntimeError as e:
    raised = e
    m2 = None
B.backup_one = orig_backup
ck(raised is not None, f'失败时非零退出（抛 RuntimeError，实得 {type(raised).__name__ if raised else None}）')
man2 = None
for d in sorted(os.listdir(os.path.join(tmp, 'analysis_out'))):
    if d.startswith('db_backup_'):
        mp = os.path.join(tmp, 'analysis_out', d, 'manifest.json')
        if os.path.exists(mp):
            with open(mp, encoding='utf-8') as fh:
                cand = json.load(fh)
            if cand.get('ok') is False:
                man2 = cand
ck(man2 is not None, '失败那次的 manifest.ok=False 已落盘')
if man2:
    marks = [f for f in man2['files'] if f.get('check') == 'failed']
    ck(len(marks) == 1 and 'history_minute' in marks[0]['src'],
       f'只标记坏的那个库（实得 {[f["src"].split(os.sep)[-1] for f in marks]}）')
ck(len(ALERTS) >= 1 and '数据库备份失败' in ALERTS[-1], f'推了告警（实得 {len(ALERTS)} 条）')

print('[4] dry-run：不落盘、不校验、不告警')
before = set(os.listdir(os.path.join(tmp, 'analysis_out')))
alert_before = len(ALERTS)
m3 = B.run_backup('incremental', server_dir=tmp, keep_days=7, dry_run=True)
after = set(os.listdir(os.path.join(tmp, 'analysis_out')))
ck(before == after, 'dry-run 没新建备份目录')
ck(len(ALERTS) == alert_before, 'dry-run 不告警')

print('[5] 源码守卫：scheduler 备份 job 有退出码回调')
root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
src = open(os.path.join(root, 'core', 'scheduler.py'), encoding='utf-8').read()
ck('_on_exit' in src and 'db-backup-exit-watch' in src,
   '拉起后有退出码监听线程（防回退成"拉起不管"）')
ck('push_non_stock_alert' in src, '退出码异常时会推非股票告警')

print('[6] 源码守卫：备份脚本输出必须能被 GBK 编码（真实事故 11-24 03:30）')
src_b = open(B.__file__, encoding='utf-8').read()
try:
    src_b.encode('gbk')
    gbk_safe = True
except UnicodeEncodeError as e:
    gbk_safe = False
    print(f'    不可编码字符: {e}')
ck(gbk_safe, 'db_backup.py 无 GBK 不可编码字符（stdout 重定向到 GBK 日志，✓/✗ 会把成功备份误判失败）')

shutil.rmtree(tmp, ignore_errors=True)
print(f'\n结果：{"全部通过" if not FAILS else str(len(FAILS)) + " 条失败"}')
for f in FAILS:
    print('  ✗ ' + f)
sys.exit(1 if FAILS else 0)
