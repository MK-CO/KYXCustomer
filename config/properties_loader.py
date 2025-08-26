"""
Properties文件配置加载器
"""
import os
from pathlib import Path
from typing import Any, Dict, Optional


class PropertiesLoader:
    """Properties文件加载器"""
    
    def __init__(self, properties_file: str = "application.properties"):
        """
        初始化配置加载器
        
        Args:
            properties_file: properties文件路径
        """
        self.properties_file = properties_file
        self.properties = {}
        self._load_properties()
    
    def _load_properties(self):
        """加载properties文件"""
        # 获取项目根目录
        project_root = Path(__file__).parent.parent
        properties_path = project_root / self.properties_file
        
        if not properties_path.exists():
            print(f"警告: 配置文件 {properties_path} 不存在，使用默认配置")
            return
        
        try:
            with open(properties_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    
                    # 跳过空行和注释行
                    if not line or line.startswith('#'):
                        continue
                    
                    # 解析键值对
                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        
                        # 处理空值
                        if value == '':
                            value = None
                        
                        self.properties[key] = value
                    else:
                        print(f"警告: 第{line_num}行格式错误: {line}")
            
            print(f"成功加载配置文件: {properties_path}")
            
        except Exception as e:
            print(f"加载配置文件失败: {e}")
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        获取配置值
        
        Args:
            key: 配置键
            default: 默认值
            
        Returns:
            配置值
        """
        # 优先从环境变量获取
        env_key = key.replace('.', '_').upper()
        env_value = os.getenv(env_key)
        if env_value is not None:
            return self._convert_value(env_value)
        
        # 从properties文件获取
        value = self.properties.get(key, default)
        return self._convert_value(value) if value is not None else default
    
    def get_str(self, key: str, default: str = "") -> str:
        """获取字符串值"""
        return str(self.get(key, default))
    
    def get_int(self, key: str, default: int = 0) -> int:
        """获取整数值"""
        try:
            return int(self.get(key, default))
        except (ValueError, TypeError):
            return default
    
    def get_float(self, key: str, default: float = 0.0) -> float:
        """获取浮点数值"""
        try:
            return float(self.get(key, default))
        except (ValueError, TypeError):
            return default
    
    def get_bool(self, key: str, default: bool = False) -> bool:
        """获取布尔值"""
        value = self.get(key, default)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ('true', 'yes', '1', 'on')
        return bool(value)
    
    def _convert_value(self, value: str) -> Any:
        """
        转换值类型
        
        Args:
            value: 字符串值
            
        Returns:
            转换后的值
        """
        if not isinstance(value, str):
            return value
        
        # 布尔值
        if value.lower() in ('true', 'false'):
            return value.lower() == 'true'
        
        # 整数
        if value.isdigit() or (value.startswith('-') and value[1:].isdigit()):
            return int(value)
        
        # 浮点数
        try:
            if '.' in value:
                return float(value)
        except ValueError:
            pass
        
        return value
    
    def get_db_config(self, environment: str = "local") -> Dict[str, Any]:
        """
        获取数据库配置
        
        Args:
            environment: 环境名称 (local/prod)
            
        Returns:
            数据库配置字典
        """
        prefix = f"db.{environment}"
        
        return {
            "host": self.get_str(f"{prefix}.host", "localhost"),
            "port": self.get_int(f"{prefix}.port", 3306),
            "name": self.get_str(f"{prefix}.name", "ai_platform_smart"),
            "user": self.get_str(f"{prefix}.user", "root"),
            "password": self.get_str(f"{prefix}.password", "")
        }
    
    def get_llm_config(self, provider: Optional[str] = None) -> Dict[str, Any]:
        """
        获取LLM配置
        
        Args:
            provider: LLM提供商名称
            
        Returns:
            LLM配置字典
        """
        if provider is None:
            provider = self.get_str("llm.provider", "volcengine")
        
        if provider == "volcengine":
            return {
                "provider": "volcengine",
                "api_key": self.get_str("volcengine.api.key"),
                "endpoint": self.get_str("volcengine.endpoint"),
                "model": self.get_str("volcengine.model", "kimi-k2-250711"),
                "model_alternate": self.get_str("volcengine.model.alternate", "deepseek-r1-250120"),
                "region": self.get_str("volcengine.region", "cn-beijing")
            }
        elif provider == "siliconflow":
            return {
                "provider": "siliconflow",
                "api_key": self.get_str("siliconflow.api.key"),
                "base_url": self.get_str("siliconflow.base.url", "https://api.siliconflow.cn/v1"),
                "model": self.get_str("siliconflow.model", "Qwen/QwQ-32B"),
                "model_kimi": self.get_str("siliconflow.model.kimi", "moonshotai/Kimi-K2-Instruct"),
                "model_glm4": self.get_str("siliconflow.model.glm4", "zai-org/GLM-4.5V"),
                "model_deepseek": self.get_str("siliconflow.model.deepseek", "Pro/deepseek-ai/DeepSeek-V3"),
                "model_deepseek_r1_8b": self.get_str("siliconflow.model.deepseek.r1.8b", "deepseek-ai/DeepSeek-R1-0528-Qwen3-8B"),
                "model_preferred": self.get_str("siliconflow.model.preferred", "default")
            }
        else:
            raise ValueError(f"不支持的LLM提供商: {provider}")
    
    def get_all_properties(self) -> Dict[str, Any]:
        """获取所有配置"""
        return self.properties.copy()
    
    def reload_properties(self):
        """重新加载配置文件"""
        print("重新加载配置文件...")
        self.properties.clear()
        self._load_properties()
        return True


# 全局配置实例
config = PropertiesLoader()
