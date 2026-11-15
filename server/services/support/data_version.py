"""data_version.py - 本地日K库的「数据版本」指纹（2026-11-15）

背景：启动预热链和常驻保活轮的输入全部来自 ``server/data/history_kline.db``
（``stock_daily`` 346.5 万行）。这份库一天只在 18:15 日K同步、市值/名录刷新时
变一次，但 ``_keepwarm_round`` 每 540s 无条件 ``SELECT DISTINCT symbol FROM
stock_daily`` + 两层范围重算 + 配置摘要重算；11-15 线上实测一轮 169.2s，占空比
约 31%。单进程 uvicorn 的主事件循环与这些后台线程共享同一个 GIL，重活期间所有
请求（连 404 空路由）都在「进 app 之前」排队 1~14s。

指纹用途：数据没变就「只续期缓存、不重算」，变了才跑原来那一整套重活。
用 ``os.stat`` 而不是查库：三次 stat 是微秒级，且不会在读锁/写锁竞争时把
调用方一并拖住。WAL 模式下写会落在 ``-wal``，所以主库与 WAL/SHM 都要看。

**为什么 WAL/SHM 只看 size、不看 mtime（2026-11-15 线上踩坑）**：第一版把三个
文件的 ``(mtime_ns, size)`` 全拼进指纹，重启后 ``_prewarm_universe_first`` 在
10:56:23 记下指纹，随后其它预热项打开/关闭 SQLite 连接，``-wal/-shm`` 的
mtime 在 10:56:25 被刷新（内容一个字节没变、size 恒为 0/32768），11:05:40
首轮保活因此误判「数据变了」→ 白重算 46.0s。WAL 模式下连接开关与 checkpoint
改 mtime 是常态，把数据版本挂在这上面等于没做门控。真正该触发重算的两种变化
都还在判据里：① 主库被写入/checkpoint（mtime＋size）；② WAL 里堆了尚未合并
的写（size 从 0 变大）。

``path`` 由调用方传入（不在本模块 import config），这样自测能指到临时库，
也避免与 ``universe.py`` 里「导入时绑定」的 ``KLINE_HISTORY_DB_PATH`` 口径分叉。

回滚：删掉调用方的指纹判断即恢复「每轮无条件重算」的老行为。
"""
from __future__ import annotations

import os

_SUFFIXES = ('-wal', '-shm')


def kline_fingerprint(path: str) -> str:
    """主库 ``(mtime_ns, size)`` + ``-wal`` / ``-shm`` 的 size；文件不存在记 ``0``。

    WAL/SHM 刻意**只记 size**：文件被 SQLite 删了又重建（都为空）不应算数据变化，
    所以「不存在」与「存在但 size=0」用同一个 ``0`` 表示。宁可多重算一次，
    也绝不因为漏判而让范围清单/摘要停在旧数据上。
    """
    try:
        st = os.stat(path)
        parts = [f'{st.st_mtime_ns}:{st.st_size}']
    except OSError:
        parts = ['-']
    for suffix in _SUFFIXES:
        try:
            parts.append(str(os.stat(path + suffix).st_size))
        except OSError:
            parts.append('0')
    return '|'.join(parts)
