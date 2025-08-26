"""
数据库连接管理器 - 解决连接泄漏问题
"""
import logging
from contextlib import contextmanager
from typing import Generator
from sqlalchemy.orm import Session
from .database import SessionLocal

logger = logging.getLogger(__name__)


@contextmanager
def get_db_session() -> Generator[Session, None, None]:
    """
    安全的数据库会话上下文管理器
    
    使用方式:
    with get_db_session() as db:
        # 使用db进行数据库操作
        pass
    # 自动关闭连接
    """
    db = SessionLocal()
    try:
        logger.debug("📄 创建数据库会话")
        yield db
    except Exception as e:
        logger.error(f"❌ 数据库操作失败: {e}")
        db.rollback()
        raise
    finally:
        logger.debug("🔒 关闭数据库会话")
        db.close()


def get_db_session_sync() -> Session:
    """
    同步获取数据库会话（需要手动关闭）
    
    注意：使用此方法必须在finally块中调用 db.close()
    建议使用 get_db_session() 上下文管理器
    """
    return SessionLocal()
