"""Domain router: system."""

from fastapi import APIRouter
from ..routes_state import *

router = APIRouter()

@router.get("/api/health", tags=["健康检查"])
async def health_check():
    return {"status": "ok", "time": datetime.now().isoformat()}


@router.get("/api/data-sources/health", tags=["健康检查"])
async def data_sources_health():
    """数据源健康台账（2026-10-19，只读，不发网络请求）。

    行情：腾讯主源 / 新浪备源的连续失败计数与当前路由（是否处于腾讯冷却期）。
    东财：按**主机家族**的熔断状态（2026-11-06 起）。每个 scope 一行 + families 汇总，
    三种"停"分得清：人工暂停（manual）、本家族当日停调（familyStoppedToday）、
    IP 级全域闸门（domainCooling）。页面"数据为什么旧/为什么标了新浪兜底"能直接答。
    """
    from services.data import source_health, eastmoney_guard
    from app.config import (EASTMONEY_DAY_STOP_ENABLED, EASTMONEY_DAY_STOP_PROBE_SEC,
                            EASTMONEY_FAMILY_GATE_ENABLED, EASTMONEY_IP_GATE_FAMILIES)
    # 2026-11-06 修正：原来这里写死的 'datacenter' 是幽灵 scope（当时没有任何调用方
    # 用它计数，永远返回空状态），而真实存在的 holder/pools/appdata 反而读不到。
    scopes = ('clist', 'ulist', 'pools', 'datacenter', 'holder', 'appdata', 'ann', 'push2')
    return {
        "success": True,
        "data": {
            "quotes": {
                "tencent": source_health.status('quote_tencent'),
                "sina": source_health.status('quote_sina'),
                "route": quote_route_status(),
            },
            "eastmoney": {
                "policy": {"familyGate": EASTMONEY_FAMILY_GATE_ENABLED,
                           "dayStop": EASTMONEY_DAY_STOP_ENABLED,
                           "dayStopProbeSec": EASTMONEY_DAY_STOP_PROBE_SEC,
                           "ipGateFamilies": EASTMONEY_IP_GATE_FAMILIES},
                "scopes": {s: eastmoney_guard.status(s) for s in scopes},
                "families": eastmoney_guard.families_status(),
            },
        },
    }


@router.post("/api/data-sources/eastmoney-pause", tags=["运维"])
async def eastmoney_pause_api(payload: Dict, user: Dict = Depends(current_user)):
    """人工暂停东财取数（2026-11-06，管理员）。

    `{"hours": 48, "target": "all", "reason": "东财不让用，先用备源"}`
    target 可以是 'all'（默认）、家族名（main/pools/datacenter/appdata/notice/push2）
    或具体 scope（clist/ulist/…）。写进 `data/eastmoney_guard_state.json`，
    **看护重启后仍然是停的**；期间各消费方走已有的备源/旧值分支。
    hours<=0 等于不暂停。人工暂停优先级最高：一次请求成功也不会把它清掉。
    """
    from services.support import app_settings as A
    if not A.can_write(user):
        raise HTTPException(status_code=403, detail='只有管理员能暂停数据源')
    from services.data import eastmoney_guard
    try:
        hours = float(payload.get('hours') or 0)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail='hours 必须是数字（小时）')
    if not (-1000 <= hours <= 1000):
        raise HTTPException(status_code=400, detail='hours 只接受 -1000~1000（0=不暂停）')
    target = str(payload.get('target') or 'all').strip() or 'all'
    reason = str(payload.get('reason') or '')[:120]
    res = eastmoney_guard.pause(hours=hours, target=target, reason=reason,
                                 actor=user.get('username', ''))
    print(f'[东财熔断] {user.get("username", "?")} 人工暂停 target={target} '
          f'hours={hours}（{reason}）')
    return {"success": True, "data": res}


@router.post("/api/data-sources/eastmoney-resume", tags=["运维"])
async def eastmoney_resume_api(payload: Dict, user: Dict = Depends(current_user)):
    """解除人工暂停（只解人工；自动冷却仍要自己到期或等一次成功）。"""
    from services.support import app_settings as A
    if not A.can_write(user):
        raise HTTPException(status_code=403, detail='只有管理员能恢复数据源')
    from services.data import eastmoney_guard
    target = str(payload.get('target') or 'all').strip() or 'all'
    res = eastmoney_guard.resume(target=target)
    print(f'[东财熔断] {user.get("username", "?")} 解除人工暂停 target={target}（原先{'在停' if res['wasPaused'] else '本就没停'}）')
    return {"success": True, "data": res}


@router.get("/api/api-usage", tags=["运维"])
async def api_usage_api(user: Dict = Depends(current_user)):
    """第三方接口每日调用计数与上限预警（只读）。管理员可改上限。

    用途：改代码前先看"哪个接口快撞上限"，避免把东财/腾讯/新浪等平台的日调用量推过封禁阈值。
    """
    from services.data import api_usage
    from services.support import app_settings as A
    items = api_usage.get_stats()
    history = api_usage.get_history(14)
    return {
        "success": True,
        "data": {
            "items": items,
            "history": history,
            "canWrite": A.can_write(user),
            "writeIds": list(A.write_ids()),
            "alertThreshold": api_usage.ALERT_THRESHOLD * 100,
        },
    }


@router.post("/api/api-usage/limits", tags=["运维"])
async def api_usage_set_limit_api(body: Dict, user: Dict = Depends(current_user)):
    """修改某接口的每日上限（管理员）。key 形如 'eastmoney:clist'。"""
    from services.data import api_usage
    from services.support import app_settings as A
    if not A.can_write(user):
        raise HTTPException(status_code=403, detail='当前账号无权修改接口调用上限（默认仅最早注册的账号可改）')
    key = (body or {}).get('key')
    limit = (body or {}).get('limit')
    if not key or ':' not in key:
        raise HTTPException(status_code=400, detail='key 格式应为 provider:interface，例如 eastmoney:clist')
    try:
        limit = int(limit)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail='limit 必须是正整数')
    if limit <= 0:
        raise HTTPException(status_code=400, detail='limit 必须是正整数')
    api_usage.set_limit(key, limit, updated_by=str(user.get('username') or user.get('id')))
    print(f"[接口用量] {user.get('username') or user.get('id')} 改了 {key} 上限 = {limit}")
    return {"success": True, "data": {"key": key, "limit": limit}}


@router.get("/api/system/restart-status", tags=["运维"])
async def restart_status_api(user: Dict = Depends(current_user)):
    """页面「重启服务」按钮的只读状态：有没有待处理请求、会不会盘中推迟、进程 bootId。

    bootId 每次后端进程启动都会变，前端提交重启前记下旧值，之后轮询本接口，
    值一变就说明看护已经把新进程拉起来了（不要靠「请求成功」判断，重启中间会断连）。
    """
    from services.ops import restart_request as R
    from services.support import app_settings as A
    data = R.status()
    data['canRestart'] = A.can_write(user)
    return {"success": True, "data": data}


@router.post("/api/system/restart", tags=["运维"])
async def restart_request_api(reason: str = '', user: Dict = Depends(current_user)):
    """提交一次后端重启请求（管理员，幂等）。**只写标志，不杀进程**。

    后端是 SYSTEM 账户由 GupiaoBackendWatchdog 计划任务拉起的，Web 进程没权限杀它；
    而且绕过看护直接杀会丢掉「拉起→删标志→清僵尸→企微通知」这一串动作。
    这里写 server/data/backend_restart_requested，看护每 5 分钟一轮读到就重启；
    工作日 09:10～15:10 会保留标志、推迟到 15:10 之后（避免打断推荐批次）。
    标志已存在（别人提交的 / HTTP 自愈写的）时不覆盖，直接返回已有状态。

    注意：若服务已经卡到 HTTP 完全不应答，这个接口同样发不进来——那种情况由
    http_liveness 连续 3 次探活失败自动写标志 + 看护接管，前端会如实提示。
    """
    from services.ops import restart_request as R
    from services.support import app_settings as A
    if not A.can_write(user):
        raise HTTPException(status_code=403, detail='只有管理员账号能重启服务（默认仅最早注册的账号）')
    actor = str(user.get('username') or user.get('id') or 'unknown')
    res = R.request(actor=actor, reason=reason[:120])
    if res.get('error'):
        raise HTTPException(status_code=500, detail=res['error'])
    print(f"[人工重启] {actor} 提交重启请求 queued={res.get('queued')} "
          f"already={res.get('already')} deferred={res.get('deferred')} reason={reason[:60]}")
    return {"success": True, "data": res}


@router.get("/api/system/closed-cache", tags=["运维"])
async def closed_cache_stats_api(user: Dict = Depends(current_user)):
    """休市静态窗口整包缓存的排障视图：窗口、TTL、条目数、命中/未命中/写入计数。

    排障顺序：先看 staticWindow（盘中应为 False）、再看 hits/stores 是否增长、
    最后看响应头 X-Closed-Cache: hit。缓存不热时务必区分「盘中旁路」与
    「白名单没覆盖」，别急着改 TTL。
    """
    from services.support import closed_response_cache as C
    return {"success": True, "data": C.stats()}


@router.post("/api/system/closed-cache/invalidate", tags=["运维"])
async def closed_cache_invalidate_api(user: Dict = Depends(current_user)):
    """手动清空休市响应缓存（改了口径/落库后想立刻看新数据时用）。

    清空只影响「静态窗口内的缓存」，盘中本来就不读缓存；不碰数据库、不重启进程。
    """
    from services.support import closed_response_cache as C
    C.invalidate()
    return {"success": True, "data": C.stats()}

