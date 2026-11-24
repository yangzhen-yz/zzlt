"""DAL domain: recommend."""

from ..dal_state import *

def _row_to_recommendation(row) -> Dict:
    """daily_recommendations 单行 → 前端推荐 dict（load_daily_recommendations 与行动台共用）"""
    # buy_timing 列兼容：旧库尚未执行 ALTER 时 keys 里没有该列，降级为 None
    has_buy_timing_col = 'buy_timing' in row.keys()
    return {
        'name': row['name'],
        'symbol': row['symbol'],
        'price': row['price'],
        'change': float(row['change_val']) if row['change_val'] else 0,
        'changePercent': float(row['change_percent']) if row['change_percent'] else 0,
        'score': row['score'] or 0,
        'buySellPoints': {
            'buy': row['buy_price'] or '',
            'current': row['current_price'] or '',
            'sell': row['sell_price'] or '',
            # 买入时机（2026-10-12）：旧记录无此列/无此值时为 None，前端自动降级隐藏
            'buyTiming': (json.loads(row['buy_timing'])
                          if has_buy_timing_col and row['buy_timing'] else None),
        } if row['buy_price'] else None,
        'reason': row['reason'] or '',
        # 模式标签（低吸/打板/抄底/接力/其他）：老库没这一列时返回空串，前端不显示徽章
        'mode': (row['mode'] or '') if 'mode' in row.keys() else '',
    }


def load_daily_recommendations() -> List[Dict]:
    try:
        conn = get_db_conn()
        cursor = conn.cursor()
        # QUERY_DAILY_RECOMMENDATIONS_LIMIT 限制的是「天数」而非「行数」
        # daily_recommendations 表每只股票一行（每天 3 只 = 3 行），
        # 若直接 LIMIT 30 行只能读出 10 天，与 save 的 [:30]（按天数）语义不一致。
        # 改为：先取最新 N 天的日期，再查这些天的所有股票行。
        cursor.execute(
            'SELECT DISTINCT rec_date FROM daily_recommendations ORDER BY rec_date DESC LIMIT ?',
            (QUERY_DAILY_RECOMMENDATIONS_LIMIT,)
        )
        recent_dates = [row['rec_date'] for row in cursor.fetchall()]
        if not recent_dates:
            conn.close()
            return []
        placeholders = ','.join('?' * len(recent_dates))
        cursor.execute(
            f'SELECT * FROM daily_recommendations WHERE rec_date IN ({placeholders}) ORDER BY rec_date DESC, sort_order ASC',
            recent_dates
        )
        rows = cursor.fetchall()
        conn.close()

        if not rows:
            return []

        # 按日期分组
        result = []
        current_date = None
        current_group = None
        for row in rows:
            if row['rec_date'] != current_date:
                if current_group:
                    result.append(current_group)
                current_date = row['rec_date']
                current_group = {
                    'date': current_date,
                    'daily_recommendations': []
                }
            current_group['daily_recommendations'].append(_row_to_recommendation(row))
        if current_group:
            result.append(current_group)
        return result
    except Exception as error:
        print(f'加载推荐记录失败: {str(error)}')
        return []


def load_recommendation_group(rec_date: str = None) -> Dict:
    """只取某一天的推荐（缺省=库内最新一天），行动台专用。

    为什么不用 load_daily_recommendations：那会读最近 N 天（约 30 天 × 3 行）并逐行
    解析 JSON；行动台每次轮询都要跑，只需要一组。纯 SQL，不触发任何行情重算
    （/api/daily_recommendations 的 GET 会重算历史价格，绝不可用于热路径）。

    Returns:
        {'date': 'YYYY-MM-DD' | '', 'stocks': [...]}
    """
    try:
        conn = get_db_conn()
        cursor = conn.cursor()
        if not rec_date:
            cursor.execute('SELECT MAX(rec_date) AS d FROM daily_recommendations')
            row0 = cursor.fetchone()
            rec_date = row0['d'] if row0 and row0['d'] else ''
        if not rec_date:
            conn.close()
            return {'date': '', 'stocks': []}
        cursor.execute(
            'SELECT * FROM daily_recommendations WHERE rec_date = ? ORDER BY sort_order ASC',
            (rec_date,)
        )
        rows = cursor.fetchall()
        conn.close()
        return {'date': rec_date, 'stocks': [_row_to_recommendation(r) for r in rows]}
    except Exception as error:
        print(f'加载单日推荐失败: {str(error)}')
        return {'date': '', 'stocks': []}


def save_daily_recommendations(daily_recommendations: List[Dict],
                               batch_time: str = '', source: str = '',
                               keep_best: bool = False) -> bool:
    """保存推荐（当日行覆盖）**并把这一批另存进 recommendation_batches**。

    2026-11-01：原来每批都是 DELETE+INSERT 覆盖当天，09:45/10:00/13:30/14:05 四批
    在库里留不下任何痕迹（10-31 实测 241 行的 created_at 全是 14:45:10）。批次表只追加，
    内容指纹与"该日最后一批"相同的就不再重复记（历史日期每次都会被重新 save 一遍，
    不比对的话每天会凭空多出 30 份重复批次）。

    `keep_best`（2026-11-16，默认 False＝保持原"后批覆盖"行为）：
    仅**自动生成推荐**的两处调用点传 True，语义是「当日主表保留评分最高的那一批，
    而不是无条件保留最后生成的批次」。起因：11-16 上午四批都是九阳股份（评分 60→63），
    14:45 收盘前批换成华数传媒（60）把更优的早批覆盖了，用户只看到最后一批。
    比较口径＝批次内**最高 score**（空仓哨兵 isCash 不参与）；边界四条：
      · 新批是空仓哨兵 → 始终覆盖（风控决策优先，不能被早盘有票压住）；
      · 库里是空仓、新批有票 → 覆盖（市场转好，有票优于空仓）；
      · 新批分数更低 → 保留已有，本批只留痕；
      · 分数打平且**票池不同** → 保留已有（先到先得）。评分模型认为两者等价时，
        "换股"没有任何依据，只会制造"上午看的是 A、收盘变 B"的困惑——正是要修的毛病。
        打平且票池相同（同一批票被重新打分/刷新报价）→ 照常覆盖，纯粹为刷新价格，
        不刷新的话主表价格会冻在早盘那一批上。
    被保护跳过的那一批**仍然写进 recommendation_batches**，前端可全批次回看。
    手工编辑（POST /api/daily_recommendations）与删除接口不传此参，行为不变。
    """
    try:
        if len(daily_recommendations) > QUERY_DAILY_RECOMMENDATIONS_LIMIT:
            daily_recommendations = daily_recommendations[:QUERY_DAILY_RECOMMENDATIONS_LIMIT]
        import datetime as _dt
        stamp = batch_time or _dt.datetime.now().strftime('%H:%M:%S')
        conn = get_db_conn(wait_for_lock=True)
        cursor = conn.cursor()
        for idx, rec_group in enumerate(daily_recommendations):
            rec_date = rec_group.get('date', '')
            recs = rec_group.get('daily_recommendations', [])
            # ── 批次留痕：与当日最后一批内容相同则不重复记 ──
            _h = _payload_hash(recs)
            batch_ref = None
            try:
                last = cursor.execute(
                    'SELECT id, payload_hash FROM recommendation_batches '
                    'WHERE rec_date=? ORDER BY id DESC LIMIT 1', (rec_date,)).fetchone()
                if not last or (last[1] or '') != _h:
                    cursor.execute(
                        'INSERT INTO recommendation_batches '
                        '(rec_date, batch_time, source, stock_count, payload_hash, stocks_json) '
                        'VALUES (?,?,?,?,?,?)',
                        (rec_date, stamp, source or '', len(recs), _h,
                         json.dumps(recs, ensure_ascii=False)))
                    batch_ref = cursor.lastrowid
                else:
                    batch_ref = last[0]
            except Exception as _e:
                # 批次表写失败不能挡住当日推荐本身（它是复盘用的旁证，不是主数据）
                print(f'[推荐批次] 留痕失败（不影响保存）: {type(_e).__name__}: {_e}')
                batch_ref = None
            # ── 最优批次保护（2026-11-16，仅 keep_best=True 的自动生成路径启用）──
            # 批次留痕已写入（上面那步），这里只决定主表要不要被本批替换。
            if keep_best:
                try:
                    _new_scores = []
                    _new_syms = set()
                    for _r in recs:
                        if _r.get('isCash'):
                            continue          # 空仓哨兵不参与评分比较
                        _new_syms.add(str(_r.get('symbol') or ''))
                        try:
                            _new_scores.append(float(_r.get('score') or 0))
                        except (TypeError, ValueError):
                            pass
                    _new_max = max(_new_scores) if _new_scores else 0.0
                    _new_all_cash = bool(recs) and all(_r.get('isCash') for _r in recs)
                    _prev_rows = cursor.execute(
                        'SELECT symbol, score FROM daily_recommendations '
                        'WHERE rec_date=? ORDER BY sort_order', (rec_date,)).fetchall()
                    _prev_syms = {str(r[0]) for r in _prev_rows
                                  if r[0] and str(r[0]).upper() != 'CASH'}
                    _prev_scores = []
                    for _r in _prev_rows:
                        if not _r[0] or str(_r[0]).upper() == 'CASH':
                            continue
                        try:
                            _prev_scores.append(float(_r[1] or 0))
                        except (TypeError, ValueError):
                            pass
                    _prev_max = max(_prev_scores) if _prev_scores else None
                    _prev_has_cash = any(str(r[0] or '').upper() == 'CASH'
                                         for r in _prev_rows)
                    if _prev_max is not None:
                        if _new_all_cash:
                            pass              # 空仓批次＝风控决策，始终覆盖
                        elif _prev_has_cash:
                            pass              # 库里空仓、新批有票 → 覆盖（市场转好）
                        elif _new_max < _prev_max:
                            # 已有更高分批次：主表保留那一批，本批只留痕，不覆盖
                            print(f'[推荐批次] {rec_date} 本批最高 {_new_max:.0f} 分 < '
                                  f'已有 {_prev_max:.0f} 分，主表保留更优批次（本批仅留痕）')
                            continue
                        elif _new_max == _prev_max and _new_syms != _prev_syms:
                            # 打平但换票：模型认为等价，换股没有依据，先到先得
                            print(f'[推荐批次] {rec_date} 本批 {_new_max:.0f} 分与已有打平但'
                                  f'票池不同（{sorted(_new_syms)} ≠ {sorted(_prev_syms)}），'
                                  f'主表保留先到批次（本批仅留痕）')
                            continue
                        # 打平且票池相同 → 落到下面正常覆盖，只为刷新报价/买入时机
                except Exception as _e:
                    # 判断失败不能让推荐保存失败，回退为原来的"直接覆盖"
                    print(f'[推荐批次] 最优保护判断失败（回退为直接覆盖）: '
                          f'{type(_e).__name__}: {_e}')
            # 删除该日期旧数据
            cursor.execute('DELETE FROM daily_recommendations WHERE rec_date = ?', (rec_date,))
            for sort_order, rec in enumerate(recs):
                bsp = rec.get('buySellPoints') or {}
                # 买入时机（2026-10-12）：JSON 持久化，保证刷新后徽章不丢
                buy_timing = bsp.get('buyTiming')
                buy_timing_json = json.dumps(buy_timing, ensure_ascii=False) if buy_timing else ''
                cursor.execute('''
                    INSERT INTO daily_recommendations (rec_date, symbol, name, price, change_val, change_percent, score, buy_price, current_price, sell_price, reason, buy_timing, sort_order, mode, batch_ref)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    rec_date,
                    rec.get('symbol', ''),
                    rec.get('name', ''),
                    str(rec.get('price', '')),
                    rec.get('change', 0),
                    rec.get('changePercent', 0),
                    rec.get('score', 0),
                    bsp.get('buy', ''),
                    bsp.get('current', ''),
                    bsp.get('sell', ''),
                    rec.get('reason', ''),
                    buy_timing_json,
                    sort_order,
                    # 模式标签（2026-10-18）：由 buy_rules.classify 的 verdict 机械映射而来，
                    # 不人工判断 —— 标签打错，按模式的复盘结论就是歪的。
                    rec.get('mode', '') or '',
                    batch_ref
                ))
        conn.commit()
        conn.close()
        _bump_daily_recs_write_version()   # 2026-11-05：让读缓存立即知道"库被写过了"
        return True
    except Exception as error:
        print(f'保存推荐记录失败: {str(error)}')
        return False


def load_recommendation_batches(rec_date: str = '') -> List[Dict]:
    """按日读批次留痕（复盘用）：[{id, rec_date, batch_time, source, stock_count, stocks}]"""
    try:
        conn = get_db_conn()
        cursor = conn.cursor()
        if rec_date:
            rows = cursor.execute(
                'SELECT id, rec_date, batch_time, source, stock_count, payload_hash, stocks_json '
                'FROM recommendation_batches WHERE rec_date=? ORDER BY id', (rec_date,)).fetchall()
        else:
            rows = cursor.execute(
                'SELECT id, rec_date, batch_time, source, stock_count, payload_hash, stocks_json '
                'FROM recommendation_batches ORDER BY id').fetchall()
        conn.close()
        out = []
        for r in rows:
            try:
                stocks = json.loads(r[6] or '[]')
            except Exception:
                stocks = []
            # payload_hash（2026-11-23）：写入侧只跟当日最后一批比，非连续重放同内容
            # 会多留一行。读取侧带出指纹，统计"当天到底推了几次"时按它去重才不虚高。
            out.append({'id': r[0], 'date': r[1], 'batch_time': r[2], 'source': r[3],
                        'stock_count': r[4], 'payload_hash': r[5] or '', 'stocks': stocks})
        return out
    except Exception as error:
        print(f'读取推荐批次失败: {str(error)}')
        return []


def delete_daily_recommendations_by_date(rec_date: str) -> bool:
    """按日期物理删除推荐记录（2026-10-12 新增）

    背景：save_daily_recommendations 只"替换"列表内出现的日期（循环内 DELETE+INSERT），
    调用方把某日期从列表中过滤掉后再 save，该日期的旧行永远不会被删——
    DELETE /api/daily_recommendations/{date} 因此返回"删除成功"但记录仍在（既有 bug）。
    删除类操作必须显式 DELETE，不能依赖 save 的替换语义。
    """
    try:
        conn = get_db_conn(wait_for_lock=True)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM daily_recommendations WHERE rec_date = ?', (rec_date,))
        deleted = cursor.rowcount
        conn.commit()
        conn.close()
        _bump_daily_recs_write_version()   # 2026-11-05：删除同样是写，缓存不能继续用旧档
        if deleted:
            print(f'删除推荐记录: {rec_date}（{deleted} 行）')
        return True
    except Exception as error:
        print(f'删除推荐记录失败: {str(error)}')
        return False



__all__ = [n for n in globals() if not n.startswith("__")]
