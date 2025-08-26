"""
SiliconFlow LLM提供商
"""
import json
import aiohttp
from typing import Dict, List, Optional, Any
from .base import BaseLLMProvider


class SiliconflowProvider(BaseLLMProvider):
    """SiliconFlow提供商"""
    
    def __init__(self, api_key: str, base_url: str = "https://api.siliconflow.cn/v1", model: str = "Qwen/Qwen2.5-7B-Instruct", config: dict = None):
        """
        初始化SiliconFlow提供商
        
        Args:
            api_key: API密钥
            base_url: API基础URL
            model: 默认模型名称
            config: 模型配置字典，包含所有可用模型
        """
        super().__init__(api_key)
        self.base_url = base_url.rstrip('/')
        self.default_model = model
        self.config = config or {}
        
        # 模型映射表
        self.model_mapping = {
            "default": self.config.get("model", model),
            "glm4": self.config.get("model_glm4", "zai-org/GLM-4.5V"),
            "deepseek": self.config.get("model_deepseek", "Pro/deepseek-ai/DeepSeek-V3"),
            "deepseek-r1-8b": self.config.get("model_deepseek_r1_8b", "deepseek-ai/DeepSeek-R1-0528-Qwen3-8B")
        }
        
        # 根据preferred配置设置当前使用的模型
        preferred = self.config.get("model_preferred", "default")
        if preferred in self.model_mapping:
            self.current_model = self.model_mapping[preferred]
        else:
            self.current_model = self.default_model
    
    def get_model_by_type(self, model_type: str = None) -> str:
        """
        根据模型类型获取具体模型名称
        
        Args:
            model_type: 模型类型 (default, kimi, glm4, deepseek, deepseek-r1-8b)
            
        Returns:
            具体的模型名称
        """
        if model_type and model_type in self.model_mapping:
            return self.model_mapping[model_type]
        return self.current_model
    
    async def chat_completion(
        self,
        messages: List[Dict[str, str]],
        model: Optional[str] = None,
        model_type: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: int = 2000,
        **kwargs
    ) -> Dict[str, Any]:
        """
        SiliconFlow聊天完成接口（OpenAI兼容格式）
        
        Args:
            messages: 对话消息列表
            model: 具体模型名称，优先级最高
            model_type: 模型类型 (default, kimi, glm4, deepseek, deepseek-r1-8b)
            temperature: 温度参数
            max_tokens: 最大token数
            **kwargs: 其他参数
        """
        # 模型选择优先级：具体模型名 > 模型类型 > 当前模型 > 默认模型
        if model:
            selected_model = model
        elif model_type:
            selected_model = self.get_model_by_type(model_type)
        else:
            selected_model = self.current_model or self.default_model
        
        # 构建请求数据（OpenAI格式）
        request_data = {
            "model": selected_model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": False
        }
        request_data.update(kwargs)
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        url = f"{self.base_url}/chat/completions"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    json=request_data,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status == 200:
                        result = await response.json()
                        return {
                            "success": True,
                            "content": result.get("choices", [{}])[0].get("message", {}).get("content", ""),
                            "usage": result.get("usage", {}),
                            "model": selected_model,
                            "raw_response": result
                        }
                    else:
                        error_text = await response.text()
                        return {
                            "success": False,
                            "error": f"HTTP {response.status}: {error_text}",
                            "content": ""
                        }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "content": ""
            }
    
    async def analyze_responsibility_evasion(
        self,
        conversation_text: str,
        context: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        使用SiliconFlow分析规避责任行为
        """
        prompt = self._build_responsibility_prompt(conversation_text, context or "")
        
        messages = [
            {"role": "system", "content": "你是一个专业的客服对话分析师，擅长识别对话中的规避责任行为。请严格按照要求的JSON格式返回分析结果。"},
            {"role": "user", "content": prompt}
        ]
        
        response = await self.chat_completion(
            messages=messages,
            temperature=0.2,  # 低温度确保更稳定的分析结果
            max_tokens=1500
        )
        
        if not response["success"]:
            return {
                "success": False,
                "error": response["error"],
                "analysis": {}
            }
        
        try:
            # 尝试解析JSON响应 - 防止NoneType错误
            content = (response.get("content") or "").strip()
            
            # 提取JSON部分
            if "```json" in content:
                start = content.find("```json") + 7
                end = content.find("```", start)
                json_str = content[start:end].strip()
            elif "```" in content and "{" in content:
                # 处理没有语言标识的代码块
                start = content.find("```") + 3
                end = content.find("```", start)
                json_str = content[start:end].strip()
            elif "{" in content and "}" in content:
                start = content.find("{")
                end = content.rfind("}") + 1
                json_str = content[start:end]
            else:
                json_str = content
            
            analysis = json.loads(json_str)
            
            return {
                "success": True,
                "analysis": analysis,
                "raw_response": response
            }
        except json.JSONDecodeError as e:
            return {
                "success": False,
                "error": f"JSON解析错误: {e}",
                "raw_content": response["content"],
                "analysis": {}
            }
    
    async def sentiment_analysis(self, text: str) -> Dict[str, Any]:
        """
        使用SiliconFlow进行情感分析
        """
        prompt = self._build_sentiment_prompt(text)
        
        messages = [
            {"role": "system", "content": "你是一个专业的情感分析师，请严格按照要求的JSON格式返回分析结果。"},
            {"role": "user", "content": prompt}
        ]
        
        response = await self.chat_completion(
            messages=messages,
            temperature=0.2,
            max_tokens=800
        )
        
        if not response["success"]:
            return {
                "success": False,
                "error": response["error"],
                "analysis": {}
            }
        
        try:
            # 解析情感分析结果 - 防止NoneType错误
            content = (response.get("content") or "").strip()
            
            if "```json" in content:
                start = content.find("```json") + 7
                end = content.find("```", start)
                json_str = content[start:end].strip()
            elif "```" in content and "{" in content:
                start = content.find("```") + 3
                end = content.find("```", start)
                json_str = content[start:end].strip()
            elif "{" in content and "}" in content:
                start = content.find("{")
                end = content.rfind("}") + 1
                json_str = content[start:end]
            else:
                json_str = content
            
            analysis = json.loads(json_str)
            
            return {
                "success": True,
                "analysis": analysis,
                "raw_response": response
            }
        except json.JSONDecodeError as e:
            return {
                "success": False,
                "error": f"JSON解析错误: {e}",
                "raw_content": response["content"],
                "analysis": {}
            }
