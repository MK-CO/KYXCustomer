"""
API认证中间件
"""
import hashlib
import secrets
import jwt as pyjwt
from datetime import datetime, timedelta
from fastapi import HTTPException, Security, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from config.settings import settings

# HTTP Bearer认证
security = HTTPBearer()

# 硬编码的用户凭据
USERS = {
    "kyx_ai": {
        "password_hash": hashlib.sha256("kyx123456+".encode()).hexdigest(),
        "username": "kyx_ai",
        "permissions": ["read", "write", "admin"]
    }
}

# JWT配置（从设置文件获取）
def get_jwt_config():
    from config.settings import settings
    return {
        "secret_key": settings.security_jwt_secret_key,
        "algorithm": "HS256",
        "expire_hours": settings.security_jwt_expire_hours
    }


def verify_api_key(credentials: HTTPAuthorizationCredentials = Security(security)) -> bool:
    """
    验证API密钥
    
    Args:
        credentials: 认证凭据
        
    Returns:
        验证结果
        
    Raises:
        HTTPException: 认证失败
    """
    if not credentials:
        raise HTTPException(
            status_code=401,
            detail="缺少认证凭据",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # 验证API密钥
    if credentials.credentials != settings.api_key:
        raise HTTPException(
            status_code=401,
            detail="API密钥无效",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    return True


def authenticate_user(username: str, password: str) -> dict:
    """
    验证用户凭据
    
    Args:
        username: 用户名
        password: 密码
        
    Returns:
        用户信息或None
    """
    user = USERS.get(username)
    if not user:
        return None
    
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    if password_hash != user["password_hash"]:
        return None
    
    return user


def create_access_token(username: str) -> str:
    """
    创建JWT访问令牌
    
    Args:
        username: 用户名
        
    Returns:
        JWT令牌
    """
    jwt_config = get_jwt_config()
    expire = datetime.utcnow() + timedelta(hours=jwt_config["expire_hours"])
    to_encode = {
        "sub": username,
        "exp": expire,
        "iat": datetime.utcnow()
    }
    encoded_jwt = pyjwt.encode(to_encode, jwt_config["secret_key"], algorithm=jwt_config["algorithm"])
    return encoded_jwt


def verify_token(token: str) -> dict:
    """
    验证JWT令牌
    
    Args:
        token: JWT令牌
        
    Returns:
        用户信息或None
    """
    try:
        jwt_config = get_jwt_config()
        payload = pyjwt.decode(token, jwt_config["secret_key"], algorithms=[jwt_config["algorithm"]])
        username: str = payload.get("sub")
        if username is None:
            return None
        
        user = USERS.get(username)
        if user is None:
            return None
            
        return user
    except pyjwt.PyJWTError:
        return None


def verify_api_key_or_token(credentials: HTTPAuthorizationCredentials = Security(security)) -> dict:
    """
    验证API密钥或JWT令牌
    
    Args:
        credentials: 认证凭据
        
    Returns:
        用户信息
        
    Raises:
        HTTPException: 认证失败
    """
    if not credentials:
        raise HTTPException(
            status_code=401,
            detail="缺少认证凭据",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # 首先尝试验证API密钥
    if credentials.credentials == settings.api_key:
        return {
            "username": "api_user",
            "authenticated": True,
            "permissions": ["read", "write", "admin"],
            "auth_type": "api_key"
        }
    
    # 然后尝试验证JWT令牌
    user = verify_token(credentials.credentials)
    if user:
        return {
            "username": user["username"],
            "authenticated": True,
            "permissions": user["permissions"],
            "auth_type": "jwt"
        }
    
    raise HTTPException(
        status_code=401,
        detail="认证凭据无效",
        headers={"WWW-Authenticate": "Bearer"},
    )


def get_current_user(user_info: dict = Depends(verify_api_key_or_token)) -> dict:
    """
    获取当前用户信息
    
    Args:
        user_info: 用户信息
        
    Returns:
        用户信息
    """
    return user_info
