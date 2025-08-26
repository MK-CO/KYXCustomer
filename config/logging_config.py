"""
日志配置模块 - 优化日志显示和文件持久化
"""
import logging
import sys
import os
from logging.handlers import RotatingFileHandler
from typing import Dict


class LoggingConfig:
    """日志配置管理器"""
    
    @staticmethod
    def setup_logging():
        """设置日志配置"""
        # 创建logs目录 - 确保在项目根目录下
        # 获取项目根目录：从config目录向上一级
        config_dir = os.path.dirname(__file__)
        project_root = os.path.dirname(config_dir)
        log_dir = os.path.join(project_root, 'logs')
        
        # 确保logs目录存在且有写权限
        try:
            os.makedirs(log_dir, exist_ok=True)
            # 测试写权限
            test_file = os.path.join(log_dir, '.test_write')
            with open(test_file, 'w') as f:
                f.write('test')
            os.remove(test_file)
            print(f"日志目录已创建: {log_dir}")
        except Exception as e:
            print(f"创建日志目录失败: {e}")
            # 如果项目根目录不可写，使用当前工作目录
            log_dir = os.path.join(os.getcwd(), 'logs')
            os.makedirs(log_dir, exist_ok=True)
            print(f"使用工作目录作为日志目录: {log_dir}")
        
        # 配置根日志记录器
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            handlers=[
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        # 关闭或降低第三方库的日志级别
        logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
        logging.getLogger('sqlalchemy.dialects').setLevel(logging.WARNING)  
        logging.getLogger('sqlalchemy.pool').setLevel(logging.WARNING)
        logging.getLogger('sqlalchemy.orm').setLevel(logging.WARNING)
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('requests').setLevel(logging.WARNING)
        logging.getLogger('httpcore').setLevel(logging.WARNING)
        logging.getLogger('httpx').setLevel(logging.WARNING)
        
        # 保持应用自身的日志为INFO级别
        logging.getLogger('app').setLevel(logging.INFO)
        logging.getLogger('main').setLevel(logging.INFO)
        logging.getLogger('uvicorn.access').setLevel(logging.WARNING)
        
        # 自定义格式化器
        class ChineseFormatter(logging.Formatter):
            """支持中文友好显示的格式化器"""
            
            def format(self, record):
                # 为不同级别添加中文标识
                level_map = {
                    'DEBUG': '🔍',
                    'INFO': '📝', 
                    'WARNING': '⚠️',
                    'ERROR': '❌',
                    'CRITICAL': '🚨'
                }
                
                original_levelname = record.levelname
                record.levelname = level_map.get(original_levelname, original_levelname)
                
                # 完全移除模块名，只保留消息
                record.name = ""
                
                result = super().format(record)
                record.levelname = original_levelname  # 恢复原始级别名
                return result
        
        # 应用中文格式化器到控制台处理器
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(ChineseFormatter(
            '%(asctime)s %(levelname)s %(message)s',
            datefmt='%H:%M:%S'
        ))
        
        # 创建文件处理器 - 应用日志
        try:
            app_log_file = os.path.join(log_dir, 'app.log')
            app_file_handler = RotatingFileHandler(
                app_log_file, 
                maxBytes=10*1024*1024,  # 10MB
                backupCount=5,
                encoding='utf-8'
            )
            app_file_handler.setFormatter(logging.Formatter(
                '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            ))
            print(f"应用日志文件: {app_log_file}")
        except Exception as e:
            print(f"创建应用日志处理器失败: {e}")
            app_file_handler = None
        
        # 创建错误日志文件处理器
        try:
            error_log_file = os.path.join(log_dir, 'error.log')
            error_file_handler = RotatingFileHandler(
                error_log_file,
                maxBytes=10*1024*1024,  # 10MB
                backupCount=5,
                encoding='utf-8'
            )
            error_file_handler.setLevel(logging.ERROR)
            error_file_handler.setFormatter(logging.Formatter(
                '%(asctime)s [%(levelname)s] %(name)s: %(message)s\n%(pathname)s:%(lineno)d\n%(funcName)s()\n',
                datefmt='%Y-%m-%d %H:%M:%S'
            ))
            print(f"错误日志文件: {error_log_file}")
        except Exception as e:
            print(f"创建错误日志处理器失败: {e}")
            error_file_handler = None
        
        # 获取根日志记录器并配置处理器
        root_logger = logging.getLogger()
        root_logger.handlers.clear()
        root_logger.addHandler(console_handler)    # 控制台输出
        
        # 只有成功创建的处理器才添加
        if app_file_handler:
            root_logger.addHandler(app_file_handler)   # 应用日志文件
        if error_file_handler:
            root_logger.addHandler(error_file_handler) # 错误日志文件
            
        root_logger.setLevel(logging.INFO)
        
        # 记录日志系统初始化完成
        logger = logging.getLogger(__name__)
        logger.info(f"日志系统初始化完成，日志目录: {log_dir}")


# 初始化日志配置
def init_logging():
    """初始化日志配置"""
    LoggingConfig.setup_logging()
