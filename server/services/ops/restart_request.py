"""页面「重启服务」按钮的服务端落点（2026-11-14）。

链路：**只写一次性标志，不碰进程**。

    POST /api/system/restart → 写 server/data/backend_restart_requested
      → SYSTEM 计划任务 GupiaoBackendWatchdog（每 5 分钟）读到标志
      → 杀 3003 进程树 + 重新拉起（reload=False）+ 删标志 + 企微通知
      → 前端按 bootId 变化判定「新进程已起来」

为什么不直接 taskkill / os.kill：后端由看护以 SYSTEM 账户拉起，工具会话与
Web 进程都没权限杀它（11-13 实测 Stop-Process / taskkill 均「拒绝访问」）；
而且绕过看护直接杀会让「重启后拉起、删标志、清僵尸、发通知」这一串动作全丢。
11-13 18:50 已用同一链路成功重启过一次（PID 136184 → 175600）。

标志协议（三方共用同一个文件，必须互不干扰）：
  · backend_watchdog.py：只看 os.path.exists，不读内容；盘中 09:10～15:10 推迟。
  · http_liveness.py：连续 3 次 HTTP 探活失败也会写标志，首行是它自己的认领
    token（`http_liveness pid=... ts=...`），恢复时按 token 撤销。
  · 本模块：首行用 `manual_restart ...` 前缀，http_liveness 认不出就不会误删；
    标志已存在时一律不覆盖（幂等，也不篡改别人的重启意图）。

自测见 scripts/system_restart_selftest.py（临时目录、零外部请求、不写库、不真重启）。
"""

import os
import threading
import time
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

# 与 http_liveness / 仓库外看护指向同一个 server 根与同一个相对路径。
# 做成模块级变量 + 函数是为了自测能整体替换到临时目录，绝不碰线上标志。
from services.ops.http_liveness import FLAG_REL

SERVER_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 进程启动标识：每次后端重启都会变，前端据此判断「新进程已经起来了」。
BOOT_ID = uuid.uuid4().hex[:12]

_FLAG_TOKEN_PREFIX = 'manual_restart'
WATCHDOG_INTERVAL_SEC = 300          # SYSTEM 看护每 5 分钟一轮
STALE_FLAG_SEC = 900                 # 标志滞留超过 15 分钟 → 看护可能没在跑
_LOCK = threading.Lock()

__all__ = ['flag_path', 'status', 'request', 'in_trading_window', 'BOOT_ID']


def flag_path() -> str:
    return os.path.join(SERVER_DIR, FLAG_REL)


def in_trading_window(now: Optional[datetime] = None) -> bool:
    """与 backend_watchdog.py::in_trading_window 同口径：工作日 09:10～15:10。

    盘中看护会保留标志、推迟重启（避免打断 09:45/10:00/13:30/14:05/14:45 批次）。
    允许传 now 是为了自测能覆盖边界，不需要 freeze 时间。
    """
    lt = (now or datetime.now()).timetuple()
    return lt.tm_wday < 5 and 910 <= lt.tm_hour * 100 + lt.tm_min <= 1510


def _read_flag() -> Optional[Dict[str, Any]]:
    """读标志的展示信息；读不出来的部分留空，不让状态接口 500。"""
    p = flag_path()
    try:
        if not os.path.exists(p):
            return None
        with open(p, 'r', encoding='utf-8', errors='replace') as f:
            head = f.readline().strip()
        mtime = os.path.getmtime(p)
    except OSError:
        return None
    out: Dict[str, Any] = {
        'exists': True,
        'requestedAt': datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S'),
        'ageSec': max(0, int(time.time() - mtime)),
        'byUs': head.startswith(_FLAG_TOKEN_PREFIX),
        'origin': '页面按钮' if head.startswith(_FLAG_TOKEN_PREFIX)
                  else ('HTTP 自愈' if head.startswith('http_liveness') else '其它写入方'),
    }
    out['stale'] = out['ageSec'] > STALE_FLAG_SEC
    return out


def status() -> Dict[str, Any]:
    """只读状态。前端提交前后轮询它，按 bootId 变化判断重启完成。"""
    req = _read_flag()
    deferred = in_trading_window()
    out: Dict[str, Any] = {
        'bootId': BOOT_ID,
        'pending': req is not None,
        'request': req,
        'deferred': deferred,
        'watchdogIntervalSec': WATCHDOG_INTERVAL_SEC,
        'staleFlagSec': STALE_FLAG_SEC,
        'serverTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }
    if deferred and req:
        out['note'] = '当前在盘中窗口（工作日 09:10～15:10），看护会保留标志并推迟到 15:10 之后重启。'
    elif req and req.get('stale'):
        out['note'] = ('标志已滞留超过 %d 分钟，看护计划任务可能没在运行；'
                       '请检查 GupiaoBackendWatchdog 或人工处理。' % (STALE_FLAG_SEC // 60))
    elif req:
        out['note'] = '已提交，SYSTEM 看护每 5 分钟一轮，读到标志就会重启后端。'
    else:
        out['note'] = '服务运行正常，没有待处理的重启请求。'
    return out


def request(actor: str, reason: str = '') -> Dict[str, Any]:
    """提交一次重启请求（幂等）。返回 status() 加上 queued / already 标记。

    绝不覆盖已有标志：那可能是 http_liveness 自愈写的，或另一个管理员刚提交的；
    覆盖会破坏它的 token 认领，也可能把「同一轮两次重启」叠起来。
    """
    p = flag_path()
    with _LOCK:
        overrode_selfheal = False
        if os.path.exists(p):
            try:
                with open(p, 'r', encoding='utf-8', errors='replace') as f:
                    head = f.readline().strip()
            except OSError:
                head = ''
            if not head.startswith('http_liveness'):
                out = status()
                out.update({'queued': False, 'already': True})
                return out
            # 自愈写的标志：它探活恢复时会按自己的 token 把整个文件撤销，
            # 那样人工点了「重启」却什么都不会发生。人工明确要求优先级更高，
            # 这里升级成人工意图；自愈撤销时首行认不出，已有逻辑会「保留对方意图」。
            # 11-14 21:20 实测过这个竞态（人工标志 2 秒后被自愈覆盖）。
            overrode_selfheal = True
        try:
            parent = os.path.dirname(p)
            if parent:
                os.makedirs(parent, exist_ok=True)
            token = '%s actor=%s pid=%d ts=%.3f' % (
                _FLAG_TOKEN_PREFIX, actor, os.getpid(), time.time())
            lines = [
                token,
                '原因：页面「重启服务」按钮（%s）' % (reason or '用户主动触发，未填原因'),
                '操作账号：%s' % actor,
                '写入时间：%s' % datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                '由 services/ops/restart_request.py 写入；等待 SYSTEM 看护 GupiaoBackendWatchdog 消费。',
                '盘中 09:10～15:10 会推迟重启，标志保留到 15:10 之后。',
                '注意：首行是认领凭据，http_liveness 恢复时只会撤销自己写的那份，不会删本文件。',
            ]
            if overrode_selfheal:
                lines.append('本请求覆盖了 http_liveness 自愈写入的标志（人工重启优先）。')
            with open(p, 'w', encoding='utf-8', newline='\n') as f:
                f.write('\n'.join(lines) + '\n')
        except OSError as e:
            return {'queued': False, 'already': False,
                    'error': '写重启标志失败：%s: %s' % (type(e).__name__, e),
                    **status()}
        out = status()
        out.update({'queued': True, 'already': False,
                    'overrodeSelfheal': overrode_selfheal})
        return out
