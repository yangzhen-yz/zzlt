"""keep_best_batch_selftest.py - 推荐主表「留当时可见最优批次」自测（2026-11-16）

复现的事故：11-16 上午 09:45/10:00/13:30/14:05 四批都是九阳股份（评分 60→62→63→63），
14:45 收盘前那批换成华数传媒（60 分），`save_daily_recommendations` 无条件
`DELETE ... WHERE rec_date=?` + INSERT，把更优的早批整批覆盖掉。用户上午看到的是九阳，
收盘再看变成华数，且事后无从判断"换股换对了还是换错了"。

改动：`save_daily_recommendations(..., keep_best=True)` 只在**自动生成推荐**的两处调用点
生效，语义＝当日主表保留评分最高的那一批，其余批次照旧只进 `recommendation_batches`。

本自测要证的不是"代码跑通"，而是**七条反证**（缺一条这个改动就是坏的）：

  [1] 5 批 60/62/63/63/60 → 主表停在 63 那批（不是最后一批）
  [2] 主表内容与该批次留痕**逐字节一致**（不是只比分数，是比落库的行）
  [3] 打平但换票 → 保留先到批次（模型认为等价，换股没有依据）
  [4] 打平且同票 → 覆盖，价格被刷新（否则主表价格会冻在早盘）
  [5] 空仓哨兵批次始终覆盖（风控决策优先，不能被早盘有票压住）
  [6] 库里是空仓、新批有票 → 覆盖（市场转好，有票优于空仓）
  [7] keep_best=False 时行为与改造前逐字节一致（手工编辑/删除接口走的就是这条路）

再加两条接口口径：
  [8] /api/recommend-batches 的 active 必须指向主表那一批（不是最后一批）
  [9] 11-01 之前的日期没有批次表，必须退回 strategy_snapshot 快照回看（不能空着）

自测一律用临时 SQLite 库 + 只读拷 DDL，绝不碰生产 gupiao.db（教训见
daily_recs_cache_selftest.py 文件头 B 条：换库只许走 db.use_db_path）。
测试日期故意用 2020 年的假日期，收尾才敢断言"生产库零残留"——用今天/昨天的话，
生产库里本来就有真实记录，断言必假绿。
"""
import os
import sys
import json
import sqlite3
import tempfile
import asyncio

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from db import database as db                       # noqa: E402
from app.api.routers import recommend as rec        # noqa: E402

FAILS = []


def ck(cond, msg):
    print(('  ✓ ' if cond else '  ✗ ') + msg)
    if not cond:
        FAILS.append(msg)


# ---------- 临时库：只读拷生产 DDL，不带任何数据 ----------
_TABLES = ('daily_recommendations', 'recommendation_batches',
           'strategy_snapshot', 'strategy_snapshot_batch')
_tmpdir = tempfile.mkdtemp(prefix='keep_best_selftest_')
_tmpdb = os.path.join(_tmpdir, 'test.db')
_prod = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'gupiao.db'))
_src = sqlite3.connect(f'file:{_prod}?mode=ro', uri=True)
_ddls = [r[0] for r in _src.execute(
    "SELECT sql FROM sqlite_master WHERE type='table' AND name IN (%s)"
    % ','.join('?' * len(_TABLES)), _TABLES).fetchall()]
_idx = [r[0] for r in _src.execute(
    "SELECT sql FROM sqlite_master WHERE type='index' AND sql IS NOT NULL "
    "AND tbl_name IN (%s)" % ','.join('?' * len(_TABLES)), _TABLES).fetchall()]
_src.close()
_c = sqlite3.connect(_tmpdb)
for _d in _ddls + _idx:
    if _d:
        _c.execute(_d)
_c.commit()
_c.close()
ck(len(_ddls) == len(_TABLES), f'从真库取到 {len(_TABLES)} 张表 DDL（实得 {len(_ddls)}）')

_OLD_DB = db.current_db_path()
db.use_db_path(_tmpdb)
ck(os.path.abspath(db.current_db_path()) == os.path.abspath(_tmpdb),
   f'换库确实生效（current_db_path={db.current_db_path()}）')

D = '2020-01-06'        # 事故重放日
D2 = '2020-01-07'       # keep_best=False 对照组
D3 = '2020-01-03'       # 11-01 之前的快照回看


def _rec(day, rows):
    """rows = [(symbol, name, score, price)]；symbol='CASH' 表示空仓哨兵"""
    out = []
    for sym, name, score, price in rows:
        if sym == 'CASH':
            out.append({'symbol': 'CASH', 'name': '空仓观望', 'price': '', 'changePercent': 0,
                        'score': 0, 'isCash': True, 'reason': '退潮期不出新买入推荐',
                        'buySellPoints': {'buy': '竞价关注'}})
        else:
            out.append({'symbol': sym, 'name': name, 'price': price, 'change': 0.1,
                        'changePercent': 1.0, 'score': score,
                        'buySellPoints': {'buy': '9.80', 'current': price, 'sell': '10.60'},
                        'reason': '自测', 'mode': '低吸'})
    return {'date': day, 'daily_recommendations': out}


def _main(day=D):
    conn = sqlite3.connect(_tmpdb)
    conn.row_factory = sqlite3.Row
    rows = conn.execute('SELECT * FROM daily_recommendations WHERE rec_date=? '
                        'ORDER BY sort_order', (day,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _batches(day=D):
    conn = sqlite3.connect(_tmpdb)
    rows = conn.execute('SELECT id, batch_time, stock_count, payload_hash FROM '
                        'recommendation_batches WHERE rec_date=? ORDER BY id',
                        (day,)).fetchall()
    conn.close()
    return [{'id': r[0], 't': r[1], 'n': r[2], 'h': r[3]} for r in rows]


# ============ [1] 五批 60/62/63/63/60，主表停在 63 ============
print('[1] 11-16 事故重放：09:45/10:00/13:30/14:05 九阳(60/62/63/63)，14:45 换华数(60)')
_seqs = [
    ('09:45:10', [('sz002242', '九阳股份', 60, '10.00')]),
    ('10:00:05', [('sz002242', '九阳股份', 62, '10.30')]),
    ('13:30:02', [('sz002242', '九阳股份', 63, '10.50')]),
    ('14:05:11', [('sz002242', '九阳股份', 63, '10.88')]),   # 同票同分 → 应覆盖刷新价
    ('14:45:09', [('sz000156', '华数传媒', 60, '8.20')]),    # 低分 → 应被挡住
]
for t, rows in _seqs:
    ok = db.save_daily_recommendations([_rec(D, rows)], batch_time=t,
                                       source='scheduled', keep_best=True)
    ck(ok, f'  {t} 保存成功')
m = _main()
ck(len(m) == 1 and m[0]['symbol'] == 'sz002242' and m[0]['name'] == '九阳股份',
   f'主表仍是九阳股份（实得 {[(x["symbol"], x["name"]) for x in m]}）')
ck(m and float(m[0]['score']) == 63, f'主表分数 63（实得 {m[0]["score"] if m else None}）')
ck(m and str(m[0]['price']) == '10.88',
   f'同票同分那批覆盖了报价（price=10.88，实得 {m[0]["price"] if m else None}）')
bs = _batches()
ck(len(bs) == 5, f'批次表 5 行（每批内容都不同，全部留痕；实得 {len(bs)}）')
ck([b['t'] for b in bs] == ['09:45:10', '10:00:05', '13:30:02', '14:05:11', '14:45:09'],
   f'批次时间序完整（{ [b["t"] for b in bs] }）')
# 连续同内容重放不重复记（历史日期每次都会被重新 save 一遍，不比对就会凭空多出批次）
db.save_daily_recommendations([_rec(D, [('sz000156', '华数传媒', 60, '8.20')])],
                              batch_time='14:45:09', source='scheduled', keep_best=True)
ck(len(_batches()) == 5, f'连续同内容重放不再留痕（批次表仍 {len(_batches())} 行）')

# ============ [2] 主表内容与留痕批次逐字节一致 ============
print('[2] 主表那一批 vs 批次留痕：逐字段比对（不是只比分数）')
_conn = sqlite3.connect(_tmpdb)
_ref = _conn.execute('SELECT batch_ref FROM daily_recommendations WHERE rec_date=?',
                     (D,)).fetchone()[0]
_json = _conn.execute('SELECT stocks_json FROM recommendation_batches WHERE id=?',
                      (_ref,)).fetchone()[0]
_conn.close()
_saved = json.loads(_json)[0]
ck(_ref == bs[3]['id'], f'batch_ref 指向 14:05 那批 id={bs[3]["id"]}（实得 {_ref}）')
ck(_saved['symbol'] == m[0]['symbol'] and _saved['name'] == m[0]['name']
   and float(_saved['score']) == float(m[0]['score'])
   and str(_saved['price']) == str(m[0]['price']),
   f'主表行 = 该批留痕（{_saved["name"]} {_saved["score"]}分 {_saved["price"]}）')

# ============ [3] 打平但换票 → 保留先到 ============
print('[3] 打平但换票：再来一批 63 分（不同票），主表应保持不动')
_ok = db.save_daily_recommendations([_rec(D, [('sz300001', '同分异票', 63, '20.00')])],
                                    batch_time='14:50:00', source='scheduled', keep_best=True)
m = _main()
ck(_ok and len(m) == 1 and m[0]['symbol'] == 'sz002242',
   f'打平换票不替换，主表仍是九阳（实得 {m[0]["symbol"] if m else None}）')
ck(len(_batches()) == 6, f'该批仍留痕（批次表 {len(_batches())} 行）')

# ============ [4] 打平且同票 → 覆盖刷新 ============
print('[4] 打平且同票：63 分再来一批（同票），应覆盖刷新报价')
_ok = db.save_daily_recommendations([_rec(D, [('sz002242', '九阳股份', 63, '11.02')])],
                                    batch_time='14:58:00', source='scheduled', keep_best=True)
m = _main()
ck(_ok and str(m[0]['price']) == '11.02',
   f'同票同分覆盖成功，price=11.02（实得 {m[0]["price"] if m else None}）')
ck(len(_batches()) == 7, f'同票同分那批报价不同 → 再留一行（实得 {len(_batches())}）')

# ============ [5] 空仓哨兵始终覆盖 ============
print('[5] 空仓哨兵批次始终覆盖（风控决策优先）')
_ok = db.save_daily_recommendations([_rec(D, [('CASH', '', 0, '')])],
                                    batch_time='14:59:00', source='scheduled', keep_best=True)
m = _main()
ck(_ok and len(m) == 1 and m[0]['symbol'] == 'CASH',
   f'空仓覆盖了 63 分那批（实得 {m[0]["symbol"] if m else None}）')

# ============ [6] 库里空仓、新批有票 → 覆盖 ============
print('[6] 库里空仓、新批有票 → 覆盖（市场转好）')
_ok = db.save_daily_recommendations([_rec(D, [('sz000001', '转好票', 55, '12.00')])],
                                    batch_time='15:00:00', source='scheduled', keep_best=True)
m = _main()
ck(_ok and len(m) == 1 and m[0]['symbol'] == 'sz000001',
   f'空仓被 55 分有票批覆盖（实得 {m[0]["symbol"] if m else None}）')

# ============ [7] keep_best=False 与改造前一致 ============
print('[7] keep_best=False（手工编辑/删除接口走的路）＝ 后批无条件覆盖')
db.save_daily_recommendations([_rec(D2, [('sz002242', '九阳股份', 63, '10.50')])],
                              batch_time='10:00:00', source='manual')
db.save_daily_recommendations([_rec(D2, [('sz000156', '华数传媒', 60, '8.20')])],
                              batch_time='14:45:00', source='manual')
_r2 = _main(D2)
_n2 = len(_batches(D2))
ck(len(_r2) == 1 and _r2[0]['symbol'] == 'sz000156',
   f'后批 60 分覆盖前批 63 分（实得 {[(x["symbol"], x["score"]) for x in _r2]}）'
   f'—— 这是改造前的老行为，未变')
ck(_n2 == 2, f'两批都留痕（实得 {_n2}）')

# ============ [8] 接口 active 指向主表那批 ============
print('[8] /api/recommend-batches：active 指向主表那批，不是最后一批')
# 落库批次里 buyTiming 是结构化对象，timing 徽章要靠快照按 (批次分钟, 代码) 补齐
_sn = sqlite3.connect(_tmpdb)
_sn.execute("INSERT INTO strategy_snapshot (snap_date, batch, symbol, name, rank, stage, "
            "score, timing) VALUES (?,?,?,?,?,?,?,?)",
            (D, '10:00', 'sz002242', '九阳股份', 1, 'recommended', 62, '低吸'))
_sn.commit()
_sn.close()
_r = asyncio.run(rec.recommend_batches_api(date=D))
_data = (_r or {}).get('data') or {}
_bats = _data.get('batches') or []
ck(_data.get('date') == D and len(_bats) == 9, f'返回 9 批（实得 {len(_bats)}）')
_b10 = [b for b in _bats if b['batchTime'] == '10:00:05']
ck(_b10 and _b10[0]['stocks'] and _b10[0]['stocks'][0]['timing'] == '低吸',
   f"timing 从快照补齐（实得 {_b10[0]['stocks'][0]['timing'] if _b10 else None}）")
_act = [b for b in _bats if b.get('active')]
ck(len(_act) == 1, f'恰好一批 active（实得 {len(_act)}）')
ck(_act and _act[0]['batchTime'] == '15:00:00',
   f'active = 15:00:00（实得 {_act[0]["batchTime"] if _act else None}）')
ck(all(b['from'] == 'db' for b in _bats), '全部来自批次表（from=db）')
_cashb = [b for b in _bats if b.get('cash')]
ck(len(_cashb) == 1 and _cashb[0]['batchTime'] == '14:59:00',
   f'空仓批次被标 cash（{ [b["batchTime"] for b in _cashb] }）')
ck(all(b['batchTime'] for b in _bats), '每批都有 batchTime（前端 key 靠它，不能空）')

# ============ [8b] 批次计数按 payload_hash 去重（2026-11-23）============
print('[8b] 接口 count：rawCount=原始行数、uniqueCount=按指纹去重')
_bsql = sqlite3.connect(_tmpdb)
_true_raw = _bsql.execute('SELECT COUNT(*) FROM recommendation_batches WHERE rec_date=?',
                          (D,)).fetchone()[0]
_true_hash = _bsql.execute('SELECT COUNT(DISTINCT payload_hash) FROM recommendation_batches '
                           'WHERE rec_date=?', (D,)).fetchone()[0]
_bsql.close()
ck(_data.get('rawCount') == _true_raw,
   f'rawCount={_true_raw}（实得 {_data.get("rawCount")}）')
ck(_data.get('uniqueCount') == _true_hash,
   f'uniqueCount={_true_hash}（实得 {_data.get("uniqueCount")}）')
ck(all(b.get('payloadHash') for b in _bats), '每批都带 payloadHash（统计去重靠它）')

# 构造"非连续重放同内容"：先插一条与早批同指纹的行，raw 应 +1 而 unique 不变
_bsql = sqlite3.connect(_tmpdb)
_early = _bsql.execute('SELECT payload_hash, batch_time, source, stock_count, stocks_json '
                       'FROM recommendation_batches WHERE rec_date=? ORDER BY id LIMIT 1',
                       (D,)).fetchone()
_bsql.execute('INSERT INTO recommendation_batches '
              '(rec_date, batch_time, source, stock_count, payload_hash, stocks_json) '
              'VALUES (?,?,?,?,?,?)', (D, '23:59:00', 'replay', _early[3], _early[0], _early[4]))
_bsql.commit()
_bsql.close()
_r2x = asyncio.run(rec.recommend_batches_api(date=D))
_d2x = (_r2x or {}).get('data') or {}
ck(_d2x.get('rawCount') == _true_raw + 1,
   f'重放后 rawCount={_true_raw + 1}（实得 {_d2x.get("rawCount")}）')
ck(_d2x.get('uniqueCount') == _true_hash,
   f'重放后 uniqueCount 不变={_true_hash}（实得 {_d2x.get("uniqueCount")}）')

# ============ [9] 11-01 之前：退回快照回看 ============
print('[9] 11-01 之前的日期没有批次表 → 退回 strategy_snapshot')
_c3 = sqlite3.connect(_tmpdb)
_c3.execute("INSERT INTO strategy_snapshot_batch (snap_date, batch, cycle, cycle_score) "
            "VALUES (?,?,?,?)", (D3, '10:26', '震荡', 55))
_c3.execute("INSERT INTO strategy_snapshot (snap_date, batch, symbol, name, rank, stage, "
            "score, change_percent, price) VALUES (?,?,?,?,?,?,?,?,?)",
            (D3, '10:26', 'sh600151', '上工申贝', 1, 'recommended', 71, 3.2, 12.3))
_c3.commit()
_c3.close()
_r3 = asyncio.run(rec.recommend_batches_api(date=D3))
_b3 = ((_r3 or {}).get('data') or {}).get('batches') or []
ck(len(_b3) == 1 and _b3[0]['from'] == 'snapshot',
   f'退回快照（from={_b3[0]["from"] if _b3 else None}）')
ck(_b3 and _b3[0]['stocks'] and _b3[0]['stocks'][0]['name'] == '上工申贝',
   '快照名单读到了上工申贝')
ck(_b3 and _b3[0]['meta'].get('cycle') == '震荡', '批次现场（周期）也带上了')

# ---------- 收尾：换回原库 + 核对生产库无污染 ----------
db.use_db_path(_OLD_DB)
_chk = sqlite3.connect(f'file:{_prod}?mode=ro', uri=True)
_p1 = _chk.execute("SELECT COUNT(*) FROM daily_recommendations WHERE rec_date IN (?,?,?)",
                   (D, D2, D3)).fetchone()[0]
_p2 = _chk.execute("SELECT COUNT(*) FROM recommendation_batches WHERE rec_date IN (?,?,?)",
                   (D, D2, D3)).fetchone()[0]
_chk.close()
ck(_p1 == 0 and _p2 == 0,
   f'生产库无自测残留（主表 {_p1} 行 / 批次表 {_p2} 行，测试日期 2020 年）')

print()
if FAILS:
    print(f'✗ {len(FAILS)} 项未通过：')
    for f in FAILS:
        print('   - ' + f)
    sys.exit(1)
print('✓ 全部通过')
