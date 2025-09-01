"""
数据库连接和会话管理
"""
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool

from config.settings import settings

# 创建数据库引擎 - 大幅增加连接池以支持高并发
engine = create_engine(
    settings.database_url,
    poolclass=QueuePool,
    pool_size=30,        # 增加到30个基础连接
    max_overflow=50,     # 增加到50个溢出连接，总计80个连接
    pool_pre_ping=True,
    pool_recycle=3600,
    pool_timeout=30,     # 添加连接超时
    pool_reset_on_return='commit',  # 返回时重置连接状态
    echo=settings.log_sql_enabled,  # 根据配置决定是否显示SQL日志
    # 🔥 修复：添加明确的字符集配置
    connect_args={
        "charset": "utf8mb4",
        "init_command": "SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci"
    }
)

# 创建会话工厂
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 创建基础模型类
Base = declarative_base()


def get_db():
    """获取数据库会话"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def create_tables():
    """创建所有表"""
    Base.metadata.create_all(bind=engine)


def drop_tables():
    """删除所有表"""
    Base.metadata.drop_all(bind=engine)
