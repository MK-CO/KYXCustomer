"""
配置管理模块 - 基于Properties文件
"""
from pathlib import Path
from .properties_loader import config


class Settings:
    """应用配置类"""
    
    @property
    def app_name(self) -> str:
        return config.get_str("app.name", "AI Platform Smart")
    
    @property
    def app_version(self) -> str:
        return config.get_str("app.version", "1.0.0")
    
    @property
    def environment(self) -> str:
        return config.get_str("app.environment", "local")
    
    @environment.setter
    def environment(self, value: str):
        """设置环境变量（运行时）"""
        import os
        os.environ["APP_ENVIRONMENT"] = value
    
    @property
    def debug(self) -> bool:
        return config.get_bool("app.debug", True)
    
    @property
    def log_level(self) -> str:
        return config.get_str("app.log.level", "INFO")
    
    @property
    def log_sql_enabled(self) -> bool:
        return config.get_bool("app.log.sql.enabled", False)
    
    # API配置
    @property
    def api_host(self) -> str:
        return config.get_str("api.host", "0.0.0.0")
    
    @property
    def api_port(self) -> int:
        return config.get_int("api.port", 8000)
    
    @property
    def api_prefix(self) -> str:
        return config.get_str("api.prefix", "/api/v1")
    
    @property
    def api_key(self) -> str:
        return config.get_str("api.key", "ai-platform-smart-2024")
    
    # 数据库配置
    @property
    def db_config(self) -> dict:
        """根据环境获取数据库配置"""
        return config.get_db_config(self.environment)
    
    @property
    def db_host(self) -> str:
        return self.db_config["host"]
    
    @property
    def db_port(self) -> int:
        return self.db_config["port"]
    
    @property
    def db_name(self) -> str:
        return self.db_config["name"]
    
    @property
    def db_user(self) -> str:
        return self.db_config["user"]
    
    @property
    def db_password(self) -> str:
        return self.db_config["password"]
    
    @property
    def database_url(self) -> str:
        """获取数据库连接URL"""
        return f"mysql+pymysql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}?charset=utf8mb4"
    
    # LLM配置
    @property
    def llm_provider(self) -> str:
        return config.get_str("llm.provider", "volcengine")
    
    @property
    def llm_config(self) -> dict:
        """获取LLM配置"""
        return config.get_llm_config(self.llm_provider)
    
    @property
    def volcengine_api_key(self) -> str:
        return config.get_str("volcengine.api.key", "")
    
    @property
    def volcengine_endpoint(self) -> str:
        return config.get_str("volcengine.endpoint", "")
    
    @property
    def volcengine_model(self) -> str:
        return config.get_str("volcengine.model", "kimi-k2-250711")
    
    @property
    def volcengine_model_alternate(self) -> str:
        return config.get_str("volcengine.model.alternate", "deepseek-r1-250120")
    
    @property
    def volcengine_region(self) -> str:
        return config.get_str("volcengine.region", "cn-beijing")
    
    @property
    def siliconflow_api_key(self) -> str:
        return config.get_str("siliconflow.api.key", "")
    
    @property
    def siliconflow_base_url(self) -> str:
        return config.get_str("siliconflow.base.url", "https://api.siliconflow.cn/v1")
    
    @property
    def siliconflow_model(self) -> str:
        return config.get_str("siliconflow.model", "Qwen/QwQ-32B")
    
    @property
    def siliconflow_model_kimi(self) -> str:
        return config.get_str("siliconflow.model.kimi", "moonshotai/Kimi-K2-Instruct")
    
    @property
    def siliconflow_model_glm4(self) -> str:
        return config.get_str("siliconflow.model.glm4", "zai-org/GLM-4.5V")
    
    @property
    def siliconflow_model_deepseek(self) -> str:
        return config.get_str("siliconflow.model.deepseek", "Pro/deepseek-ai/DeepSeek-V3")
    
    @property
    def siliconflow_model_deepseek_r1_8b(self) -> str:
        return config.get_str("siliconflow.model.deepseek.r1.8b", "deepseek-ai/DeepSeek-R1-0528-Qwen3-8B")
    
    @property
    def siliconflow_model_preferred(self) -> str:
        return config.get_str("siliconflow.model.preferred", "default")
    
    # 分析配置
    @property
    def min_text_length(self) -> int:
        return config.get_int("analysis.min.text.length", 10)
    
    @property
    def max_text_length(self) -> int:
        return config.get_int("analysis.max.text.length", 10000)
    
    @property
    def responsibility_threshold(self) -> float:
        return config.get_float("analysis.responsibility.threshold", 0.7)
    
    # 检测引擎配置
    @property
    def detection_keyword_weight_multiplier(self) -> float:
        return config.get_float("detection.keyword.weight.multiplier", 1.0)
    
    @property
    def detection_pattern_weight_multiplier(self) -> float:
        return config.get_float("detection.pattern.weight.multiplier", 0.8)
    
    @property
    def detection_confidence_threshold(self) -> float:
        return config.get_float("detection.confidence.threshold", 0.6)
    
    # 调度器配置已移除 - 现在完全由数据库任务配置控制
    
    # 数据处理配置
    @property
    def data_extractor_limit_default(self) -> int:
        return config.get_int("data.extractor.limit.default", 1000)
    
    @property
    def data_extractor_max_total(self) -> int:
        """数据抽取最大总量限制，0表示无限制"""
        return config.get_int("data.extractor.max.total", 0)
    
    @property
    def data_extractor_max_batches(self) -> int:
        """数据抽取最大批次数限制，0表示无限制"""
        return config.get_int("data.extractor.max.batches", 0)
    
    @property
    def data_extractor_hours_back_default(self) -> int:
        return config.get_int("data.extractor.hours.back.default", 24)
    
    @property
    def data_batch_size_max(self) -> int:
        return config.get_int("data.batch.size.max", 50)
    
    # 并发处理配置
    @property
    def concurrency_max_workers(self) -> int:
        return config.get_int("concurrency.max.workers", 10)
    
    @property
    def concurrency_analysis_batch_size(self) -> int:
        return config.get_int("concurrency.analysis.batch.size", 20)
    
    @property
    def concurrency_analysis_max_concurrent(self) -> int:
        return config.get_int("concurrency.analysis.max.concurrent", 8)
    
    @property
    def concurrency_api_workers(self) -> int:
        return config.get_int("concurrency.api.workers", 4)
    
    @property
    def concurrency_background_workers(self) -> int:
        return config.get_int("concurrency.background.workers", 6)
    
    # 路径配置
    @property
    def project_root(self) -> Path:
        """获取项目根目录"""
        return Path(__file__).parent.parent
    
    @property
    def data_dir(self) -> Path:
        """获取数据目录"""
        return self.project_root / "data"
    
    @property
    def logs_dir(self) -> Path:
        """获取日志目录"""
        return self.project_root / "logs"
    
    # 安全防护配置
    @property
    def security_rate_limit_enabled(self) -> bool:
        return config.get_bool("security.rate.limit.enabled", True)
    
    @property
    def security_rate_limit_requests_per_minute(self) -> int:
        return config.get_int("security.rate.limit.requests.per.minute", 10)
    
    @property
    def security_rate_limit_requests_per_hour(self) -> int:
        return config.get_int("security.rate.limit.requests.per.hour", 100)
    
    @property
    def security_rate_limit_requests_per_day(self) -> int:
        return config.get_int("security.rate.limit.requests.per.day", 1000)
    
    # 登录安全配置
    @property
    def security_login_max_attempts(self) -> int:
        return config.get_int("security.login.max.attempts", 5)
    
    @property
    def security_login_lockout_duration_minutes(self) -> int:
        return config.get_int("security.login.lockout.duration.minutes", 30)
    
    @property
    def security_login_captcha_threshold(self) -> int:
        return config.get_int("security.login.captcha.threshold", 3)
    
    @property
    def security_login_ip_whitelist(self) -> str:
        return config.get_str("security.login.ip.whitelist", "")
    
    @property
    def security_login_enable_captcha(self) -> bool:
        return config.get_bool("security.login.enable.captcha", True)
    
    # JWT安全配置
    @property
    def security_jwt_secret_key(self) -> str:
        return config.get_str("security.jwt.secret.key", "kyx-ai-platform-smart-2024-super-secret-key-v2")
    
    @property
    def security_jwt_expire_hours(self) -> int:
        return config.get_int("security.jwt.expire.hours", 24)
    
    @property
    def security_jwt_refresh_enable(self) -> bool:
        return config.get_bool("security.jwt.refresh.enable", True)
    
    # 防护模式配置
    @property
    def security_protection_mode(self) -> str:
        return config.get_str("security.protection.mode", "normal")


# 全局配置实例
settings = Settings()
