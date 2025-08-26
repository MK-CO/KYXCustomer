"""
安全管理相关API路由
"""
from fastapi import APIRouter, HTTPException, Depends, Request, Query
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
from app.core.security import rate_limiter, get_client_ip, log_security_event
from app.core.auth import get_current_user
import time
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/security", tags=["安全管理"])


class SecurityStatusResponse(BaseModel):
    success: bool
    data: dict


class IPInfo(BaseModel):
    ip: str
    is_whitelisted: bool
    is_locked: bool
    login_failure_count: int
    lockout_until: Optional[float] = None
    requires_captcha: bool


class SecurityStatsResponse(BaseModel):
    success: bool
    data: dict


class UnlockIPRequest(BaseModel):
    ip: str
    reason: Optional[str] = None


@router.get("/status", summary="获取安全状态", response_model=SecurityStatusResponse)
async def get_security_status(request: Request):
    """
    获取当前IP的安全状态
    """
    client_ip = get_client_ip(request)
    security_info = rate_limiter.get_security_info(client_ip)
    
    return SecurityStatusResponse(
        success=True,
        data=security_info
    )


@router.get("/stats", summary="获取安全统计信息", response_model=SecurityStatsResponse)
async def get_security_stats(
    current_user: dict = Depends(get_current_user)
):
    """
    获取安全统计信息（需要管理员权限）
    """
    try:
        # 检查权限
        if "admin" not in current_user.get("permissions", []):
            raise HTTPException(
                status_code=403,
                detail="需要管理员权限"
            )
        
        current_time = time.time()
        
        # 统计锁定的IP
        locked_ips = []
        for ip, lockout_until in rate_limiter._locked_ips.items():
            if lockout_until > current_time:
                remaining_minutes = int((lockout_until - current_time) / 60)
                locked_ips.append({
                    "ip": ip,
                    "lockout_until": lockout_until,
                    "remaining_minutes": remaining_minutes
                })
        
        # 统计登录失败的IP
        failed_ips = []
        for ip, failures in rate_limiter._login_failures.items():
            one_hour_ago = current_time - 3600
            recent_failures = [
                (ts, user) for ts, user in failures
                if ts >= one_hour_ago
            ]
            if recent_failures:
                failed_ips.append({
                    "ip": ip,
                    "failure_count": len(recent_failures),
                    "latest_failure": max(ts for ts, _ in recent_failures),
                    "usernames": list(set(user for _, user in recent_failures))
                })
        
        # 统计请求频率
        active_ips = set()
        for key in rate_limiter._requests.keys():
            if key.startswith("rate_limit:"):
                ip = key.split(":")[1]
                active_ips.add(ip)
        
        stats = {
            "locked_ips_count": len(locked_ips),
            "locked_ips": locked_ips,
            "failed_login_ips_count": len(failed_ips),
            "failed_login_ips": failed_ips,
            "active_ips_count": len(active_ips),
            "total_captcha_tokens": len(rate_limiter._captcha_cache),
            "whitelist": list(rate_limiter._ip_whitelist),
            "security_settings": {
                "max_login_attempts": rate_limiter._login_failures,
                "lockout_duration_minutes": 30,  # 从配置获取
                "captcha_threshold": 3,  # 从配置获取
                "rate_limit_enabled": True
            }
        }
        
        return SecurityStatsResponse(
            success=True,
            data=stats
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取安全统计信息失败: {e}")
        raise HTTPException(status_code=500, detail="获取安全统计信息失败")


@router.get("/ip-info/{ip}", summary="获取指定IP信息")
async def get_ip_info(
    ip: str,
    current_user: dict = Depends(get_current_user)
):
    """
    获取指定IP的详细信息（需要管理员权限）
    """
    try:
        # 检查权限
        if "admin" not in current_user.get("permissions", []):
            raise HTTPException(
                status_code=403,
                detail="需要管理员权限"
            )
        
        security_info = rate_limiter.get_security_info(ip)
        
        # 获取详细的失败记录
        current_time = time.time()
        one_hour_ago = current_time - 3600
        
        recent_failures = []
        if ip in rate_limiter._login_failures:
            for ts, username in rate_limiter._login_failures[ip]:
                if ts >= one_hour_ago:
                    recent_failures.append({
                        "timestamp": ts,
                        "username": username,
                        "datetime": datetime.fromtimestamp(ts).isoformat()
                    })
        
        # 获取请求记录
        request_stats = {}
        for key, requests in rate_limiter._requests.items():
            if key.startswith(f"rate_limit:{ip}"):
                period = key.split(":")[-1]
                total_requests = sum(count for _, count in requests)
                request_stats[period] = total_requests
        
        detailed_info = {
            **security_info,
            "recent_failures": recent_failures,
            "request_stats": request_stats
        }
        
        return {
            "success": True,
            "data": detailed_info
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取IP信息失败: {e}")
        raise HTTPException(status_code=500, detail="获取IP信息失败")


@router.post("/reload-config", summary="重新加载安全配置")
async def reload_security_config(
    current_user: dict = Depends(get_current_user)
):
    """重新加载安全配置"""
    try:
        from config.properties_loader import config
        
        # 重新加载配置
        config.reload_properties()
        
        # 获取新的配置值
        from config.settings import settings
        new_config = {
            "rate_limit_enabled": settings.security_rate_limit_enabled,
            "requests_per_minute": settings.security_rate_limit_requests_per_minute,
            "requests_per_hour": settings.security_rate_limit_requests_per_hour,
            "requests_per_day": settings.security_rate_limit_requests_per_day,
            "login_max_attempts": settings.security_login_max_attempts,
            "lockout_duration_minutes": settings.security_login_lockout_duration_minutes
        }
        
        return {
            "success": True,
            "message": "安全配置已重新加载",
            "config": new_config
        }
        
    except Exception as e:
        logger.error(f"重新加载安全配置失败: {e}")
        raise HTTPException(status_code=500, detail="重新加载安全配置失败")


@router.post("/unlock-ip", summary="解锁IP")
async def unlock_ip(
    request: UnlockIPRequest,
    http_request: Request,
    current_user: dict = Depends(get_current_user)
):
    """
    解锁指定IP（需要管理员权限）
    """
    try:
        # 检查权限
        if "admin" not in current_user.get("permissions", []):
            raise HTTPException(
                status_code=403,
                detail="需要管理员权限"
            )
        
        client_ip = get_client_ip(http_request)
        
        # 解锁IP
        rate_limiter.reset_login_failures(request.ip)
        
        # 记录安全事件
        log_security_event("ip_unlocked", client_ip, {
            "unlocked_ip": request.ip,
            "admin_user": current_user.get("username"),
            "reason": request.reason or "管理员手动解锁"
        })
        
        logger.info(f"管理员 {current_user.get('username')} 解锁了IP: {request.ip}")
        
        return {
            "success": True,
            "message": f"IP {request.ip} 已成功解锁"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"解锁IP失败: {e}")
        raise HTTPException(status_code=500, detail="解锁IP失败")


@router.post("/lock-ip", summary="锁定IP")
async def lock_ip(
    ip: str = Query(..., description="要锁定的IP地址"),
    duration_minutes: int = Query(30, description="锁定时长（分钟）"),
    reason: str = Query(None, description="锁定原因"),
    http_request: Request = None,
    current_user: dict = Depends(get_current_user)
):
    """
    手动锁定指定IP（需要管理员权限）
    """
    try:
        # 检查权限
        if "admin" not in current_user.get("permissions", []):
            raise HTTPException(
                status_code=403,
                detail="需要管理员权限"
            )
        
        client_ip = get_client_ip(http_request)
        
        # 锁定IP
        rate_limiter.lock_ip(ip, duration_minutes)
        
        # 记录安全事件
        log_security_event("ip_locked_manually", client_ip, {
            "locked_ip": ip,
            "admin_user": current_user.get("username"),
            "duration_minutes": duration_minutes,
            "reason": reason or "管理员手动锁定"
        })
        
        logger.warning(f"管理员 {current_user.get('username')} 锁定了IP: {ip} ({duration_minutes}分钟)")
        
        return {
            "success": True,
            "message": f"IP {ip} 已被锁定 {duration_minutes} 分钟"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"锁定IP失败: {e}")
        raise HTTPException(status_code=500, detail="锁定IP失败")


@router.delete("/clear-cache", summary="清理安全缓存")
async def clear_security_cache(
    cache_type: str = Query("all", description="缓存类型: all, failures, requests, captcha"),
    current_user: dict = Depends(get_current_user)
):
    """
    清理安全相关缓存（需要管理员权限）
    """
    try:
        # 检查权限
        if "admin" not in current_user.get("permissions", []):
            raise HTTPException(
                status_code=403,
                detail="需要管理员权限"
            )
        
        if cache_type == "all" or cache_type == "failures":
            rate_limiter._login_failures.clear()
            rate_limiter._locked_ips.clear()
        
        if cache_type == "all" or cache_type == "requests":
            rate_limiter._requests.clear()
        
        if cache_type == "all" or cache_type == "captcha":
            rate_limiter._captcha_cache.clear()
        
        logger.info(f"管理员 {current_user.get('username')} 清理了安全缓存: {cache_type}")
        
        return {
            "success": True,
            "message": f"已清理 {cache_type} 缓存"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"清理安全缓存失败: {e}")
        raise HTTPException(status_code=500, detail="清理安全缓存失败")
