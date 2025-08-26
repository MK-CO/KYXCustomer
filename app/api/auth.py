"""
认证相关API路由
"""
from fastapi import APIRouter, HTTPException, Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel
from typing import Optional
from app.core.auth import authenticate_user, create_access_token, verify_token
from app.core.security import rate_limiter, get_client_ip, log_security_event
from app.services.captcha_service import captcha_service
from config.settings import settings
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["认证"])
security = HTTPBearer()


class LoginRequest(BaseModel):
    username: str
    password: str
    captcha_token: Optional[str] = None
    captcha_input: Optional[str] = None


class LoginResponse(BaseModel):
    success: bool
    message: str
    token: str = None
    username: str = None
    requires_captcha: bool = False
    captcha_image: str = None
    captcha_token: str = None
    lockout_minutes: int = None


class CaptchaResponse(BaseModel):
    success: bool
    captcha_image: str
    captcha_token: str


@router.post("/login", summary="用户登录", response_model=LoginResponse)
async def login(login_request: LoginRequest, request: Request):
    """
    用户登录
    
    - **username**: 用户名
    - **password**: 密码
    - **captcha_token**: 验证码令牌（如果需要验证码）
    - **captcha_input**: 验证码输入（如果需要验证码）
    """
    client_ip = get_client_ip(request)
    
    try:
        # 检查IP是否被锁定
        if rate_limiter.is_ip_locked(client_ip):
            lockout_until = rate_limiter._locked_ips.get(client_ip, 0)
            remaining_minutes = max(0, int((lockout_until - __import__('time').time()) / 60))
            
            log_security_event("login_attempt_blocked", client_ip, {
                "username": login_request.username,
                "reason": "ip_locked"
            })
            
            return LoginResponse(
                success=False,
                message=f"IP地址已被锁定，请在 {remaining_minutes} 分钟后重试",
                lockout_minutes=remaining_minutes
            )
        
        # 获取失败次数
        failure_count = rate_limiter.get_login_failure_count(client_ip)
        requires_captcha = failure_count >= settings.security_login_captcha_threshold
        
        # 如果需要验证码，检查验证码
        if requires_captcha and settings.security_login_enable_captcha:
            if not login_request.captcha_token or not login_request.captcha_input:
                # 生成新验证码
                captcha_text, captcha_image = captcha_service.generate_captcha()
                captcha_token = rate_limiter.generate_captcha_token(captcha_text)
                
                return LoginResponse(
                    success=False,
                    message="请输入验证码",
                    requires_captcha=True,
                    captcha_image=captcha_image,
                    captcha_token=captcha_token
                )
            
            # 验证验证码
            if not rate_limiter.verify_captcha(login_request.captcha_token, login_request.captcha_input):
                log_security_event("captcha_failed", client_ip, {
                    "username": login_request.username
                })
                
                # 生成新验证码
                captcha_text, captcha_image = captcha_service.generate_captcha()
                captcha_token = rate_limiter.generate_captcha_token(captcha_text)
                
                return LoginResponse(
                    success=False,
                    message="验证码错误，请重新输入",
                    requires_captcha=True,
                    captcha_image=captcha_image,
                    captcha_token=captcha_token
                )
        
        # 验证用户凭据
        user = authenticate_user(login_request.username, login_request.password)
        if not user:
            # 记录登录失败
            new_failure_count = rate_limiter.record_login_failure(client_ip, login_request.username)
            
            log_security_event("login_failed", client_ip, {
                "username": login_request.username,
                "failure_count": new_failure_count,
                "reason": "invalid_credentials"
            })
            
            logger.warning(f"登录失败：用户名或密码错误 - {login_request.username} from {client_ip}")
            
            # 检查是否需要验证码
            if new_failure_count >= settings.security_login_captcha_threshold and settings.security_login_enable_captcha:
                captcha_text, captcha_image = captcha_service.generate_captcha()
                captcha_token = rate_limiter.generate_captcha_token(captcha_text)
                
                return LoginResponse(
                    success=False,
                    message="用户名或密码错误，请输入验证码",
                    requires_captcha=True,
                    captcha_image=captcha_image,
                    captcha_token=captcha_token
                )
            
            # 检查是否被锁定
            if rate_limiter.is_ip_locked(client_ip):
                return LoginResponse(
                    success=False,
                    message=f"登录失败次数过多，IP已被锁定 {settings.security_login_lockout_duration_minutes} 分钟",
                    lockout_minutes=settings.security_login_lockout_duration_minutes
                )
            
            return LoginResponse(
                success=False,
                message=f"用户名或密码错误。剩余尝试次数：{settings.security_login_max_attempts - new_failure_count}"
            )
        
        # 登录成功，重置失败记录
        rate_limiter.reset_login_failures(client_ip)
        
        # 创建访问令牌
        access_token = create_access_token(user["username"])
        
        log_security_event("login_success", client_ip, {
            "username": user["username"]
        })
        
        logger.info(f"用户登录成功: {user['username']} from {client_ip}")
        
        return LoginResponse(
            success=True,
            message="登录成功",
            token=access_token,
            username=user["username"]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"登录处理失败: {e}")
        
        log_security_event("login_error", client_ip, {
            "username": login_request.username,
            "error": str(e)
        })
        
        raise HTTPException(status_code=500, detail="登录处理失败")


@router.get("/verify", summary="验证令牌")
async def verify_user_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """
    验证JWT令牌有效性
    """
    try:
        if not credentials:
            raise HTTPException(
                status_code=401,
                detail="缺少认证凭据"
            )
        
        user = verify_token(credentials.credentials)
        if not user:
            raise HTTPException(
                status_code=401,
                detail="令牌无效或已过期"
            )
        
        return {
            "success": True,
            "message": "令牌有效",
            "username": user["username"],
            "permissions": user["permissions"]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"令牌验证失败: {e}")
        raise HTTPException(status_code=500, detail="令牌验证失败")


@router.get("/captcha", summary="获取验证码", response_model=CaptchaResponse)
async def get_captcha():
    """
    获取验证码图片
    """
    try:
        captcha_text, captcha_image = captcha_service.generate_captcha()
        captcha_token = rate_limiter.generate_captcha_token(captcha_text)
        
        return CaptchaResponse(
            success=True,
            captcha_image=captcha_image,
            captcha_token=captcha_token
        )
    except Exception as e:
        logger.error(f"生成验证码失败: {e}")
        raise HTTPException(status_code=500, detail="生成验证码失败")


@router.get("/security-status", summary="获取安全状态")
async def get_security_status(request: Request):
    """
    获取当前IP的安全状态
    """
    client_ip = get_client_ip(request)
    security_info = rate_limiter.get_security_info(client_ip)
    
    return {
        "success": True,
        "data": security_info
    }


@router.post("/logout", summary="用户登出")
async def logout(request: Request):
    """
    用户登出（前端清除token即可）
    """
    client_ip = get_client_ip(request)
    
    log_security_event("user_logout", client_ip)
    
    return {
        "success": True,
        "message": "登出成功"
    }
