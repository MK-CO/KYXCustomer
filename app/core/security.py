"""
安全防护模块 - 基于内存的限流和防护机制
"""
import time
import hashlib
import secrets
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from fastapi import Request, HTTPException, status
from config.settings import settings
import logging

logger = logging.getLogger(__name__)


class MemoryRateLimiter:
    """内存限流器"""
    
    def __init__(self):
        # 存储格式: {key: [(timestamp, count), ...]}
        self._requests: Dict[str, List[Tuple[float, int]]] = defaultdict(list)
        # 锁定的IP: {ip: lockout_until_timestamp}
        self._locked_ips: Dict[str, float] = {}
        # 登录失败记录: {ip: [(timestamp, username), ...]}
        self._login_failures: Dict[str, List[Tuple[float, str]]] = defaultdict(list)
        # 验证码: {token: (captcha_text, expire_time)}
        self._captcha_cache: Dict[str, Tuple[str, float]] = {}
        # IP白名单
        self._ip_whitelist = set()
        
        # 解析白名单
        if settings.security_login_ip_whitelist:
            self._ip_whitelist = set(
                ip.strip() for ip in settings.security_login_ip_whitelist.split(",")
                if ip.strip()
            )
        
        # 清理间隔（秒）
        self._last_cleanup = time.time()
        self._cleanup_interval = 300  # 5分钟清理一次
    
    def _cleanup_old_records(self) -> None:
        """清理过期记录"""
        current_time = time.time()
        
        # 每5分钟清理一次
        if current_time - self._last_cleanup < self._cleanup_interval:
            return
        
        cutoff_time = current_time - 86400  # 24小时前的记录
        
        # 清理请求记录
        for key in list(self._requests.keys()):
            self._requests[key] = [
                (ts, count) for ts, count in self._requests[key]
                if ts > cutoff_time
            ]
            if not self._requests[key]:
                del self._requests[key]
        
        # 清理锁定IP
        self._locked_ips = {
            ip: until for ip, until in self._locked_ips.items()
            if until > current_time
        }
        
        # 清理登录失败记录
        for ip in list(self._login_failures.keys()):
            self._login_failures[ip] = [
                (ts, username) for ts, username in self._login_failures[ip]
                if ts > cutoff_time
            ]
            if not self._login_failures[ip]:
                del self._login_failures[ip]
        
        # 清理过期验证码
        self._captcha_cache = {
            token: (text, expire) for token, (text, expire) in self._captcha_cache.items()
            if expire > current_time
        }
        
        self._last_cleanup = current_time
        logger.info("完成安全记录清理")
    
    def is_ip_whitelisted(self, ip: str) -> bool:
        """检查IP是否在白名单中"""
        return ip in self._ip_whitelist or ip == "127.0.0.1" or ip == "localhost"
    
    def is_ip_locked(self, ip: str) -> bool:
        """检查IP是否被锁定"""
        if ip in self._locked_ips:
            if time.time() < self._locked_ips[ip]:
                return True
            else:
                # 锁定时间过期，移除锁定
                del self._locked_ips[ip]
        return False
    
    def lock_ip(self, ip: str, duration_minutes: int = None) -> None:
        """锁定IP"""
        if duration_minutes is None:
            duration_minutes = settings.security_login_lockout_duration_minutes
        
        lockout_until = time.time() + (duration_minutes * 60)
        self._locked_ips[ip] = lockout_until
        
        logger.warning(f"IP {ip} 已被锁定 {duration_minutes} 分钟")
    
    def check_rate_limit(self, key: str, limit: int, window_seconds: int) -> bool:
        """检查速率限制"""
        self._cleanup_old_records()
        
        current_time = time.time()
        window_start = current_time - window_seconds
        
        # 过滤时间窗口内的请求
        valid_requests = [
            (ts, count) for ts, count in self._requests[key]
            if ts >= window_start
        ]
        
        # 计算总请求数
        total_requests = sum(count for _, count in valid_requests)
        
        if total_requests >= limit:
            return False
        
        # 记录新请求
        self._requests[key].append((current_time, 1))
        
        return True
    
    def record_login_failure(self, ip: str, username: str) -> int:
        """记录登录失败，返回失败次数"""
        current_time = time.time()
        
        # 记录失败
        self._login_failures[ip].append((current_time, username))
        
        # 计算最近1小时内的失败次数
        one_hour_ago = current_time - 3600
        recent_failures = [
            (ts, user) for ts, user in self._login_failures[ip]
            if ts >= one_hour_ago
        ]
        
        failure_count = len(recent_failures)
        
        # 检查是否需要锁定
        if failure_count >= settings.security_login_max_attempts:
            self.lock_ip(ip)
        
        return failure_count
    
    def get_login_failure_count(self, ip: str) -> int:
        """获取登录失败次数"""
        current_time = time.time()
        one_hour_ago = current_time - 3600
        
        recent_failures = [
            (ts, user) for ts, user in self._login_failures[ip]
            if ts >= one_hour_ago
        ]
        
        return len(recent_failures)
    
    def reset_login_failures(self, ip: str) -> None:
        """重置登录失败记录"""
        if ip in self._login_failures:
            del self._login_failures[ip]
        if ip in self._locked_ips:
            del self._locked_ips[ip]
    
    def generate_captcha_token(self, captcha_text: str, expire_minutes: int = 10) -> str:
        """生成验证码token"""
        token = secrets.token_urlsafe(32)
        expire_time = time.time() + (expire_minutes * 60)
        self._captcha_cache[token] = (captcha_text.lower(), expire_time)
        return token
    
    def verify_captcha(self, token: str, user_input: str) -> bool:
        """验证验证码"""
        if token not in self._captcha_cache:
            return False
        
        captcha_text, expire_time = self._captcha_cache[token]
        
        # 检查是否过期
        if time.time() > expire_time:
            del self._captcha_cache[token]
            return False
        
        # 验证码使用后删除
        del self._captcha_cache[token]
        
        return user_input.lower() == captcha_text
    
    def get_security_info(self, ip: str) -> dict:
        """获取IP的安全信息"""
        return {
            "ip": ip,
            "is_whitelisted": self.is_ip_whitelisted(ip),
            "is_locked": self.is_ip_locked(ip),
            "login_failure_count": self.get_login_failure_count(ip),
            "lockout_until": self._locked_ips.get(ip),
            "requires_captcha": self.get_login_failure_count(ip) >= settings.security_login_captcha_threshold
        }


# 全局限流器实例
rate_limiter = MemoryRateLimiter()


def get_client_ip(request: Request) -> str:
    """获取客户端真实IP地址"""
    # 检查常见的代理头
    headers_to_check = [
        "X-Forwarded-For",
        "X-Real-IP", 
        "CF-Connecting-IP",  # Cloudflare
        "X-Forwarded-Host"
    ]
    
    for header in headers_to_check:
        if header in request.headers:
            ip = request.headers[header].split(",")[0].strip()
            if ip and ip != "unknown":
                return ip
    
    # 回退到直接连接IP
    return request.client.host if request.client else "unknown"


async def security_middleware(request: Request) -> None:
    """安全防护中间件"""
    # 动态读取配置
    if not settings.security_rate_limit_enabled:
        return
    
    client_ip = get_client_ip(request)
    path = request.url.path
    
    # 跳过静态文件和健康检查
    if any(path.startswith(prefix) for prefix in ["/static", "/health", "/docs", "/openapi.json"]):
        return
    
    # 白名单IP跳过检查
    if rate_limiter.is_ip_whitelisted(client_ip):
        return
    
    # 检查IP是否被锁定
    if rate_limiter.is_ip_locked(client_ip):
        lockout_until = rate_limiter._locked_ips.get(client_ip, 0)
        remaining_minutes = max(0, int((lockout_until - time.time()) / 60))
        
        logger.warning(f"拒绝被锁定IP的请求: {client_ip}, 路径: {path}")
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail={
                "success": False,
                "error_type": "IP_LOCKED",
                "message": f"IP地址已被锁定，请在 {remaining_minutes} 分钟后重试",
                "remaining_minutes": remaining_minutes,
                "locked_ip": client_ip,
                "retry_after": lockout_until
            }
        )
    
    # 动态获取当前限流配置
    minute_limit = settings.security_rate_limit_requests_per_minute
    hour_limit = settings.security_rate_limit_requests_per_hour  
    day_limit = settings.security_rate_limit_requests_per_day
    
    # 检查一般请求频率限制
    rate_key = f"rate_limit:{client_ip}"
    
    # 每分钟限制
    if not rate_limiter.check_rate_limit(f"{rate_key}:minute", minute_limit, 60):
        logger.warning(f"IP {client_ip} 触发每分钟请求限制 (限制: {minute_limit}/分钟)")
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail={
                "success": False,
                "error_type": "RATE_LIMIT_MINUTE",
                "message": f"请求过于频繁，每分钟最多 {minute_limit} 次请求",
                "limit_type": "minute",
                "limit_value": minute_limit,
                "current_ip": client_ip,
                "retry_after_seconds": 60,
                "suggestion": "请等待1分钟后重试"
            }
        )
    
    # 每小时限制
    if not rate_limiter.check_rate_limit(f"{rate_key}:hour", hour_limit, 3600):
        logger.warning(f"IP {client_ip} 触发每小时请求限制 (限制: {hour_limit}/小时)")
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail={
                "success": False,
                "error_type": "RATE_LIMIT_HOUR",
                "message": f"每小时请求次数已达上限 ({hour_limit} 次)",
                "limit_type": "hour",
                "limit_value": hour_limit,
                "current_ip": client_ip,
                "retry_after_seconds": 3600,
                "suggestion": "请等待1小时后重试"
            }
        )
    
    # 每日限制
    if not rate_limiter.check_rate_limit(f"{rate_key}:day", day_limit, 86400):
        logger.warning(f"IP {client_ip} 触发每日请求限制 (限制: {day_limit}/天)")
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail={
                "success": False,
                "error_type": "RATE_LIMIT_DAY",
                "message": f"每日请求次数已达上限 ({day_limit} 次)",
                "limit_type": "day", 
                "limit_value": day_limit,
                "current_ip": client_ip,
                "retry_after_seconds": 86400,
                "suggestion": "请明天再试"
            }
        )


def log_security_event(event_type: str, ip: str, details: dict = None) -> None:
    """记录安全事件"""
    event_data = {
        "timestamp": datetime.now().isoformat(),
        "event_type": event_type,
        "ip": ip,
        "details": details or {}
    }
    
    # 根据事件类型设置日志级别
    if event_type in ["login_failed", "ip_locked", "rate_limit_exceeded"]:
        logger.warning(f"安全事件: {event_type} - IP: {ip} - 详情: {details}")
    elif event_type in ["login_success"]:
        logger.info(f"安全事件: {event_type} - IP: {ip}")
    else:
        logger.info(f"安全事件: {event_type} - IP: {ip} - 详情: {details}")
