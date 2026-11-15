"""页面「重启服务」按钮的回归自测（2026-11-14）。

零外部请求、不写库、不碰线上标志、不真重启：把 restart_request.SERVER_DIR
整体换到临时目录，所有文件读写都落在那里。换库检查器（R1）认 mkdtemp。

覆盖：标志协议（首行 token 与 http_liveness 互不干扰）、幂等、盘中窗口边界、
滞留诊断、管理员鉴权、以及「绝不直接杀进程」这条安全底线。
"""

import asyncio
import os
import shutil
import sys
import tempfile
from datetime import datetime
from pathlib import Path

SERVER_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVER_DIR))

PASS = 0
FAIL = 0


def ck(cond, msg):
    global PASS, FAIL
    if cond:
        PASS += 1
        print('  ok   ' + msg)
    else:
        FAIL += 1
        print('  FAIL ' + msg)


def main():
    from services.ops import restart_request as R

    # 线上那份标志：自测全程不许碰它（存在则记录内容，结束再比对）
    online_flag = R.flag_path()
    online_before = None
    if os.path.exists(online_flag):
        with open(online_flag, 'r', encoding='utf-8', errors='replace') as f:
            online_before = f.read()

    tmp = tempfile.mkdtemp(prefix='restart_selftest_')  # 检查器 R1 认这个
    real_dir = R.SERVER_DIR
    try:
        R.SERVER_DIR = tmp
        print('[1] 初始状态与路径隔离')
        ck(R.flag_path().startswith(tmp), '标志路径已切到临时目录')
        ck(R.flag_path() != online_flag, '与线上标志不是同一个文件')
        ck(len(R.BOOT_ID) == 12 and all(c in '0123456789abcdef' for c in R.BOOT_ID),
           'bootId 是 12 位十六进制（重启后必变）')
        s0 = R.status()
        ck(s0['pending'] is False and s0['request'] is None, '初始无待处理请求')
        ck(s0['bootId'] == R.BOOT_ID and s0['watchdogIntervalSec'] == 300,
           '状态带 bootId 与看护轮询间隔 300s')
        ck('正常' in s0['note'], '无请求时提示服务正常')

        print('[2] 提交一次重启请求（只写标志）')
        r1 = R.request(actor='tester', reason='自测')
        ck(r1.get('queued') is True and r1.get('already') is False, '首次提交 queued=True')
        ck(r1.get('error') is None, '无写入错误')
        p = R.flag_path()
        ck(os.path.exists(p), '标志文件已生成')
        with open(p, encoding='utf-8') as f:
            content = f.read()
        lines = content.splitlines()
        ck(lines[0].startswith('manual_restart'), '首行是 manual_restart 认领凭据')
        ck(not lines[0].startswith('http_liveness'),
           '首行不是 http_liveness 的 token（它恢复时不会误删本文件）')
        ck('tester' in lines[0], '首行带操作账号')
        ck(any('页面「重启服务」按钮' in l for l in lines), '正文写明来源')
        ck(any('GupiaoBackendWatchdog' in l for l in lines), '正文写明由 SYSTEM 看护消费')
        ck(any('盘中 09:10' in l for l in lines), '正文写明盘中推迟约定')

        print('[3] 幂等：已有标志一律不覆盖')
        mtime_before = os.path.getmtime(p)
        r2 = R.request(actor='another', reason='第二次')
        ck(r2.get('queued') is False and r2.get('already') is True, '重复提交返回 already=True')
        with open(p, encoding='utf-8') as f:
            content2 = f.read()
        ck(content2 == content and os.path.getmtime(p) == mtime_before,
           '文件内容和 mtime 都没被覆盖（不篡改别人的重启意图）')
        s1 = R.status()
        ck(s1['pending'] is True and s1['request']['byUs'] is True, '状态识别为本模块写入')
        ck(s1['request']['origin'] == '页面按钮', '来源标注为页面按钮')
        ck(s1['request']['ageSec'] >= 0 and s1['request']['stale'] is False, '刚写入不算滞留')

        print('[3b] 与 http_liveness 的竞态（11-14 21:20 实测：人工标志 2 秒后被覆盖）')
        with open(p, 'w', encoding='utf-8') as f:
            f.write('http_liveness pid=999 ts=1.0\n原因：自愈探活失败\n')
        r3 = R.request(actor='tester', reason='人工优先')
        ck(r3.get('queued') is True and r3.get('overrodeSelfheal') is True,
           '自愈标志被人工请求升级覆盖（人工重启优先）')
        with open(p, encoding='utf-8') as f:
            head3 = f.readline().strip()
            body3 = f.read()
        ck(head3.startswith('manual_restart'), '覆盖后首行换回 manual_restart（自愈撤销时认不出，不会删）')
        ck('覆盖了 http_liveness' in body3, '正文留痕说明覆盖了谁')
        with open(p, 'w', encoding='utf-8') as f:
            f.write('人工排查：等待 15:10 后重启\n')  # 其它写入方（旧版人工说明）
        r4 = R.request(actor='tester', reason='再点一次')
        ck(r4.get('already') is True, '遇到其它写入方的标志仍不覆盖')
        with open(p, encoding='utf-8') as f:
            ck(f.readline().startswith('人工排查'), '其它写入方的意图原样保留')
        # 恢复成人工标志，供后续段落使用
        with open(p, 'w', encoding='utf-8') as f:
            f.write('manual_restart actor=tester pid=1 ts=1.0\n')

        from services.ops import http_liveness as H
        real_h_dir = H.SERVER_DIR
        try:
            H.SERVER_DIR = tmp
            with H._LOCK:
                H._STATE['flag_by_us'] = False
            wrote = H._write_flag({'probe': {'state': 'timeout', 'elapsed': 8.0}, 'streak': 3})
            ck(wrote is False, '自愈写入前二次检查发现已有标志 → 本轮不写')
            with open(p, 'r', encoding='utf-8') as f:
                ck(f.readline().startswith('manual_restart'), '自愈没有覆盖已有人工标志')
        finally:
            H.SERVER_DIR = real_h_dir
            with H._LOCK:
                H._STATE['flag_by_us'] = False
        hlsrc = (SERVER_DIR / 'services' / 'ops' / 'http_liveness.py').read_text(encoding='utf-8')
        wseg = hlsrc[hlsrc.find('def _write_flag'):hlsrc.find('def _cancel_own_flag')]
        ck('os.path.exists(flag)' in wseg and 'flag_by_us' in wseg,
           '_write_flag 里确有写入前二次检查（不是只靠判定层）')

        print('[4] 盘中窗口边界（与 backend_watchdog 同口径）')
        cases = [
            (datetime(2026, 11, 14, 10, 0), False, '周六 10:00 不算盘中'),
            (datetime(2026, 11, 16, 9, 9), False, '周一 09:09 未进窗口'),
            (datetime(2026, 11, 16, 9, 10), True, '周一 09:10 进入窗口'),
            (datetime(2026, 11, 16, 14, 5), True, '周一 14:05 在窗口内（推荐批次）'),
            (datetime(2026, 11, 16, 15, 10), True, '周一 15:10 仍在窗口内'),
            (datetime(2026, 11, 16, 15, 11), False, '周一 15:11 出窗口'),
            (datetime(2026, 11, 16, 20, 0), False, '周一 20:00 不算盘中'),
        ]
        for dt, want, name in cases:
            ck(R.in_trading_window(dt) is want, name)

        print('[5] 标志滞留诊断（看护可能没在跑）')
        old = R.STALE_FLAG_SEC + 600
        os.utime(p, (os.path.getmtime(p) - old, os.path.getmtime(p) - old))
        s2 = R.status()
        ck(s2['request']['stale'] is True, '超过阈值判定为滞留')
        ck('滞留' in s2['note'] and 'GupiaoBackendWatchdog' in s2['note'],
           '提示里点名检查看护任务')

        print('[6] 安全底线：绝不直接杀进程')
        # 用 AST 查真实 import / 调用，不搜全文字符串——模块注释里本来就要解释
        # 「为什么不用 taskkill / Stop-Process」，全文搜会把注释误判成调用。
        import ast
        src = Path(R.__file__).read_text(encoding='utf-8')
        tree = ast.parse(src)
        banned_calls = {'kill', 'system', 'popen', 'Popen', 'taskkill', 'terminate',
                        'TerminateProcess'}
        banned_imports = {'subprocess', 'psutil', 'signal'}
        violations = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                fn = node.func
                name = fn.attr if isinstance(fn, ast.Attribute) else getattr(fn, 'id', '')
                if name in banned_calls:
                    violations.append('call ' + name)
            elif isinstance(node, ast.Import):
                violations += ['import ' + a.name for a in node.names
                               if a.name.split('.')[0] in banned_imports]
            elif isinstance(node, ast.ImportFrom):
                if (node.module or '').split('.')[0] in banned_imports:
                    violations.append('from ' + (node.module or ''))
        ck(not violations, '模块 AST 里没有杀进程/起子进程的 import 或调用（%s）'
           % (', '.join(violations) or '无'))
        route_src = (SERVER_DIR / 'app' / 'api' / 'routers' / 'system.py').read_text(encoding='utf-8')
        seg = route_src[route_src.find('@router.get("/api/system/restart-status"'):]
        ck('Depends(current_user)' in seg, 'GET 状态接口带登录鉴权')
        ck(seg.count('Depends(current_user)') >= 2, 'POST 重启接口同样带登录鉴权')
        ck('A.can_write(user)' in seg and 'status_code=403' in seg,
           'POST 另有管理员校验，非管理员 403')

        print('[7] 直接调路由函数（不启服务、不写库）')
        try:
            from fastapi import HTTPException
            from services.support import app_settings as A
            from app.api.routers import system as S

            admin = {'id': 1, 'username': 'tester'}
            guest = {'id': 999, 'username': 'guest'}
            orig_can_write = A.can_write
            try:
                A.can_write = lambda u: u.get('id') == 1
                got = asyncio.run(S.restart_status_api(user=admin))
                ck(got['success'] is True and got['data']['canRestart'] is True,
                   '管理员读状态 canRestart=True')
                got2 = asyncio.run(S.restart_status_api(user=guest))
                ck(got2['data']['canRestart'] is False, '普通账号 canRestart=False')
                try:
                    asyncio.run(S.restart_request_api(reason='自测', user=guest))
                    ck(False, '普通账号 POST 应被 403 拦下')
                except HTTPException as e:
                    ck(e.status_code == 403, '普通账号 POST 被 403 拦下')
                os.remove(p)
                got3 = asyncio.run(S.restart_request_api(reason='自测', user=admin))
                ck(got3['success'] is True and got3['data']['queued'] is True,
                   '管理员 POST 成功且写标志')
                got4 = asyncio.run(S.restart_request_api(reason='再点一次', user=admin))
                ck(got4['data']['already'] is True, '管理员重复点不会重复写')
            finally:
                A.can_write = orig_can_write
        except ImportError as e:
            ck(False, '路由函数导入失败：%s' % e)

        print('[8] 线上标志未被触碰')
        after = None
        if os.path.exists(online_flag):
            with open(online_flag, 'r', encoding='utf-8', errors='replace') as f:
                after = f.read()
        ck(after == online_before, '线上标志全程未被创建/修改/删除')
    finally:
        R.SERVER_DIR = real_dir
        shutil.rmtree(tmp, ignore_errors=True)

    print('\n结果：%d 通过 / %d 失败' % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
