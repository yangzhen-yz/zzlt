"""预热/保活「数据版本门控」长期回归自测（2026-11-15）

守住两个不变量，防止以后有人「优化」回去把全站卡死：

  1. 本地日K库内容 / 范围配置**变了** → 必须重算（宁可多算一次也不能给旧数据）；
  2. **没变** → 绝不周期性重算，只续期三层缓存 TTL（否则每 9 分钟一轮 169s
     重活抢 GIL，单进程 uvicorn 的所有请求连 404 都排队 1~14s）。

为什么「只跳过不续期」是错的（反证必须留着）：universe/liquidity 缓存 TTL 600s、
settings_preview 的 `/api/settings` 在 age>540s 会**另起后台线程**重算摘要。
跳过重算却不管时间戳，600s 后每个请求都要付冷路径，还会拉起一堆重算线程。

零外部请求、不写真实库：日K库指到临时文件，测完恢复。
"""
from __future__ import annotations

import ast
import os
import shutil
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_SERVER))

from core import scheduler as S                       # noqa: E402
from services.data import universe as U, liquidity as L  # noqa: E402
from services.support import (data_version as DV,        # noqa: E402
                              settings_preview as SP)

PASS, FAIL = 0, 0


def check(name: str, ok: bool, extra: str = '') -> None:
    global PASS, FAIL
    if ok:
        PASS += 1
    else:
        FAIL += 1
        print(f'  FAIL  {name}  {extra}')


def section(title: str) -> None:
    print(f'\n--- {title} ---')


# ───────────────────────────── 1. 指纹本身 ─────────────────────────────
tmpdir = tempfile.mkdtemp(prefix='prewarm_fp_')
try:
    db = os.path.join(tmpdir, 'history_kline.db')
    Path(db).write_bytes(b'x')
    section('1. kline_fingerprint')
    fp0 = DV.kline_fingerprint(db)
    check('缺失的 -wal/-shm 记为 0（与「存在但为空」同值）',
          fp0.split('|')[1:] == ['0', '0'], fp0)
    check('同一状态指纹稳定', DV.kline_fingerprint(db) == fp0)

    os.utime(db, ns=(1_700_000_000_000_000_000, 1_700_000_000_000_000_000))
    fp1 = DV.kline_fingerprint(db)
    check('主库 mtime 变化 → 指纹变化', fp1 != fp0, f'{fp0} -> {fp1}')

    wal = db + '-wal'
    Path(wal).write_bytes(b'W')
    fp2 = DV.kline_fingerprint(db)
    check('WAL 出现（size 0→1）→ 指纹变化', fp2 != fp1, f'{fp1} -> {fp2}')

    # 2026-11-15 线上踩坑的反证：SQLite 连接开关/checkpoint 只刷 WAL/SHM 的
    # mtime、内容一个字节没变。第一版把 mtime 拼进指纹，重启后首轮保活因此
    # 误判「数据变了」白重算 46.0s。现在必须不触发。
    os.utime(wal, ns=(1_700_000_001_000_000_000, 1_700_000_001_000_000_000))
    check('反证：只刷 WAL 的 mtime、size 不变 → 指纹不变（不白重算）',
          DV.kline_fingerprint(db) == fp2, DV.kline_fingerprint(db))

    # SHM 首次出现是 size 0→32768，按设计算变化；之后只刷 mtime 不该算
    shm = db + '-shm'
    Path(shm).write_bytes(b'\0' * 32768)
    fp3 = DV.kline_fingerprint(db)
    check('SHM 出现（size 0→32768）→ 指纹变化', fp3 != fp2)
    os.utime(shm, ns=(1_700_000_001_000_000_000, 1_700_000_001_000_000_000))
    check('反证：只刷 SHM 的 mtime、size 不变 → 指纹不变（不白重算）',
          DV.kline_fingerprint(db) == fp3, DV.kline_fingerprint(db))

    Path(wal).write_bytes(b'WW')
    fp4 = DV.kline_fingerprint(db)
    check('WAL 真的堆了新写（size 1→2）→ 指纹变化', fp4 != fp3)

    # 文件被删了又重建（WAL 模式下最后一条连接关闭时的常态）不该算数据变化
    os.remove(wal)
    check('反证：WAL 文件删除（2→0）→ 指纹变化（有未合并写时该重算）',
          DV.kline_fingerprint(db) != fp4)
    Path(wal).write_bytes(b'')
    fp_empty = DV.kline_fingerprint(db)
    os.remove(wal)
    check('反证：WAL 从「空文件」变「不存在」→ 同值 0，指纹稳定',
          DV.kline_fingerprint(db) == fp_empty,
          f'{fp_empty} -> {DV.kline_fingerprint(db)}')

    missing = DV.kline_fingerprint(os.path.join(tmpdir, 'not_exist.db'))
    check('库完全不存在也返回可用指纹（不抛异常）', missing == '-|0|0', missing)

    # ─────────────────── 2. 纯函数判据（含反证） ───────────────────
    section('2. keepwarm_should_recompute / heavy_prewarm_due')
    check('从没记过指纹 → 重算', S.keepwarm_should_recompute('', 'a'))
    check('指纹没变 → 不重算', not S.keepwarm_should_recompute('a', 'a'))
    check('指纹变了 → 重算', S.keepwarm_should_recompute('a', 'b'))
    check('反证：last 为空但 now 为空也判重算', S.keepwarm_should_recompute('', ''))
    check('交易日 → 跑重预热', S.heavy_prewarm_due(True))
    check('休市日 → 跳过重预热', not S.heavy_prewarm_due(False))
    check('反证/回滚：allow_holiday=True 恢复老行为', S.heavy_prewarm_due(False, True))
    check('反证：交易日不受 allow_holiday 影响', S.heavy_prewarm_due(True, False))

    # ─────────────────── 3. _db_symbols 按指纹缓存 ───────────────────
    section('3. universe._db_symbols 按库指纹缓存')
    real_path = U.KLINE_HISTORY_DB_PATH
    kdb = os.path.join(tmpdir, 'kline.db')
    conn = sqlite3.connect(kdb)
    conn.execute('CREATE TABLE stock_daily (symbol TEXT, date TEXT)')
    conn.executemany('INSERT INTO stock_daily VALUES (?, ?)',
                     [('600001', '2026-11-13'), ('000001', '2026-11-13')])
    conn.commit()
    conn.close()

    U.KLINE_HISTORY_DB_PATH = kdb
    U._DB_SYMBOLS_CACHE.update(fp='', value=[])
    real_connect = sqlite3.connect
    calls = []

    def _counting(path, *a, **kw):
        calls.append(str(path))
        return real_connect(path, *a, **kw)

    saved_connect = U.sqlite3.connect
    U.sqlite3.connect = _counting
    try:
        r1 = U._db_symbols()
        check('首次读到 2 只票', sorted(r1) == ['000001', '600001'], str(r1))
        check('首次查库 1 次', len(calls) == 1, str(calls))
        r2 = U._db_symbols()
        check('指纹未变：第二次不查库（命中缓存）', len(calls) == 1, str(calls))
        check('两次结果一致', r1 == r2)
        check('返回的是副本，改它不污染缓存',
              (r2.append('999999') or U._db_symbols() == r1))

        # 反证：真实写库（等价于 18:15 日K同步）后指纹必然变，必须重扫、看到新票
        U._DB_SYMBOLS_CACHE['fp'] = 'stale-fingerprint'
        c2 = real_connect(kdb)
        c2.execute("INSERT INTO stock_daily VALUES ('600002', '2026-11-14')")
        c2.commit()
        c2.close()
        r4 = U._db_symbols()
        check('库变了 → 重新查库', len(calls) == 2, str(calls))
        check('库变了 → 看到新增的票（绝不吃旧缓存）', '600002' in r4, str(r4))
        check('重扫后缓存指纹已刷新', U._DB_SYMBOLS_CACHE['fp'] != 'stale-fingerprint')
        r5 = U._db_symbols()
        check('重扫后再问一次命中新缓存', len(calls) == 2 and '600002' in r5, str(calls))
    finally:
        U.sqlite3.connect = saved_connect
        U.KLINE_HISTORY_DB_PATH = real_path
        U._DB_SYMBOLS_CACHE.update(fp='', value=[])

    # ─────────────────── 4. 配置代数进指纹 ───────────────────
    section('4. 配置改动必须让保活轮重算')
    fp_before = S.current_data_fp()
    epoch_before = U.cache_epoch()
    U.invalidate_cache()
    check('invalidate_cache 递增代数', U.cache_epoch() == epoch_before + 1)
    check('配置改动后指纹变化', S.current_data_fp() != fp_before)
    check('代数进指纹后触发重算', S.keepwarm_should_recompute(fp_before, S.current_data_fp()))

    # ─────────────────── 5. 三层 touch 只续期不重算 ───────────────────
    section('5. touch_* 只续期已有值')
    U._SCOPE_CACHE.update(ts=0.0, value=['600001'])
    U._SELECTABLE_CACHE.update(ts=0.0, value=['600001'])
    U.touch_caches()
    check('universe 范围缓存续期', U._SCOPE_CACHE['ts'] > 0)
    check('universe 选票缓存续期', U._SELECTABLE_CACHE['ts'] > 0)
    U._SCOPE_CACHE.update(ts=0.0, value=[])
    U.touch_caches()
    check('反证：空缓存不续期（留请求侧现算）', U._SCOPE_CACHE['ts'] == 0.0)

    L._STATS, L._STATS_TS = None, 0.0
    L.touch_stats()
    check('流动性从没算过时 touch 不动', L._STATS_TS == 0.0)
    L._STATS, L._STATS_TS = {'600001': {}}, 0.0
    L.touch_stats()
    check('流动性已有值时续期', L._STATS_TS > 0)
    check('续期后 stats_map 命中缓存不再查库', L.stats_map() is L._STATS)

    SP._SUMMARY.update(ts=0.0, data=None)
    SP.touch_summary()
    check('摘要没有快照时 touch 不动', SP._SUMMARY['ts'] == 0.0)
    SP._SUMMARY.update(ts=0.0, data={'dataScope': 2462})
    SP.touch_summary()
    check('摘要有快照时续期', SP._SUMMARY['ts'] > 0)
    check('续期后 summary_age 回到接近 0（不会触发后台重算线程）',
          0 <= SP.summary_age() < 2, f'{SP.summary_age():.2f}')
    check('快照内容未被改写', SP.summary_snapshot() == {'dataScope': 2462})

    # ─────────────────── 6. 接线静态检查（防「加了函数没接上」） ───────────────────
    section('6. 接线静态检查')
    sched_src = (_SERVER / 'core' / 'scheduler.py').read_text(encoding='utf-8')
    sched_tree = ast.parse(sched_src)
    keepwarm_fn = None
    for node in ast.walk(sched_tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == '_keepwarm_round':
            keepwarm_fn = node
            break
    check('找到 _keepwarm_round 定义', keepwarm_fn is not None)
    if keepwarm_fn:
        called = {n.func.attr for n in ast.walk(keepwarm_fn)
                  if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
        for need in ('touch_caches', 'touch_stats', 'touch_summary',
                     'refresh_caches', 'compute_summary', 'should_keepwarm_refresh'):
            check(f'保活轮接了 {need}', need in called, str(sorted(called)))
        check('保活轮接了指纹判据',
              any(isinstance(n, ast.Call) and getattr(n.func, 'id', '') == 'keepwarm_should_recompute'
                  for n in ast.walk(keepwarm_fn)))
        check('只在整轮成功后才认领指纹',
              any(isinstance(n, ast.Assign) and '_KEEPWARM_STATE' in ast.unparse(n.targets[0])
                  for n in ast.walk(keepwarm_fn)))

    thread_src = sched_src
    check('三段重预热都受 heavy_prewarm_due 结果门控',
          thread_src.count('if _heavy_due:') >= 3,
          f'if _heavy_due 出现 {thread_src.count("if _heavy_due:")} 次')

    main_src = (_SERVER / 'app' / 'main.py').read_text(encoding='utf-8')
    main_tree = ast.parse(main_src)
    gated = False
    for node in ast.walk(main_tree):
        if isinstance(node, ast.If) and any(
                isinstance(n, ast.Call) and getattr(n.func, 'id', '') == 'is_trading_day'
                for n in ast.walk(node.test)):
            if any(isinstance(n, ast.Call) and getattr(n.func, 'attr', '') == 'create_task'
                   for n in ast.walk(node)):
                gated = True
    check('启动钩子的 force 全量扫描已加交易日门控', gated)
    check('启动钩子保留「复用上一交易日结果」分支', '直接复用落库结果' in main_src)

finally:
    shutil.rmtree(tmpdir, ignore_errors=True)

print(f'\n===== 通过 {PASS} / {PASS + FAIL} =====')
if FAIL:
    print(f'失败 {FAIL} 项')
    sys.exit(1)
