"""
LLM工厂类
"""
from typing import Optional
from .base import BaseLLMProvider
from .volcengine_provider import VolcengineProvider
from .siliconflow_provider import SiliconflowProvider
from config.settings import settings


class LLMFactory:
    """LLM提供商工厂类"""
    
    @staticmethod
    def create_provider(provider_name: Optional[str] = None) -> BaseLLMProvider:
        """
        创建LLM提供商实例
        
        Args:
            provider_name: 提供商名称，如果为None则使用配置中的默认提供商
            
        Returns:
            LLM提供商实例
            
        Raises:
            ValueError: 不支持的提供商
        """
        provider_name = provider_name or settings.llm_provider
        
        if provider_name == "volcengine":
            if not settings.volcengine_api_key:
                raise ValueError("火山大模型API密钥未配置")
            
            return VolcengineProvider(
                api_key=settings.volcengine_api_key,
                endpoint=settings.volcengine_endpoint,
                model=settings.volcengine_model,
                region=settings.volcengine_region
            )
        
        elif provider_name == "siliconflow":
            if not settings.siliconflow_api_key:
                raise ValueError("SiliconFlow API密钥未配置")
            
            # 构建完整的SiliconFlow配置
            siliconflow_config = {
                "model": settings.siliconflow_model,
                "model_kimi": settings.siliconflow_model_kimi,
                "model_glm4": settings.siliconflow_model_glm4,
                "model_deepseek": settings.siliconflow_model_deepseek,
                "model_deepseek_r1_8b": settings.siliconflow_model_deepseek_r1_8b,
                "model_preferred": settings.siliconflow_model_preferred
            }
            
            return SiliconflowProvider(
                api_key=settings.siliconflow_api_key,
                base_url=settings.siliconflow_base_url,
                model=settings.siliconflow_model,
                config=siliconflow_config
            )
        
        else:
            raise ValueError(f"不支持的LLM提供商: {provider_name}")
    
    @staticmethod
    def get_available_providers() -> list:
        """
        获取可用的提供商列表
        
        Returns:
            可用提供商列表
        """
        providers = []
        
        if settings.volcengine_api_key:
            providers.append({
                "name": "volcengine",
                "display_name": "火山大模型",
                "model": settings.volcengine_model
            })
        
        if settings.siliconflow_api_key:
            providers.append({
                "name": "siliconflow", 
                "display_name": "SiliconFlow",
                "model": settings.siliconflow_model
            })
        
        return providers


# 全局LLM服务实例
def get_llm_provider() -> BaseLLMProvider:
    """获取默认LLM提供商实例"""
    return LLMFactory.create_provider()
