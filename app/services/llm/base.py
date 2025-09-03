"""
LLM基础抽象类
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any


class BaseLLMProvider(ABC):
    """LLM提供商基础抽象类"""
    
    def __init__(self, api_key: str, **kwargs):
        """
        初始化LLM提供商
        
        Args:
            api_key: API密钥
            **kwargs: 其他配置参数
        """
        self.api_key = api_key
        self.config = kwargs
    
    @abstractmethod
    async def chat_completion(
        self,
        messages: List[Dict[str, str]],
        model: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: int = 2000,
        **kwargs
    ) -> Dict[str, Any]:
        """
        聊天完成接口
        
        Args:
            messages: 消息列表，格式 [{"role": "user", "content": "..."}]
            model: 模型名称
            temperature: 温度参数
            max_tokens: 最大token数
            **kwargs: 其他参数
            
        Returns:
            LLM响应结果
        """
        pass
    
    @abstractmethod
    async def analyze_responsibility_evasion(
        self,
        conversation_text: str,
        context: Optional[str] = None,
        few_shot_examples: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        分析对话中的规避责任行为
        
        Args:
            conversation_text: 对话文本
            context: 上下文信息
            few_shot_examples: 动态few-shot示例列表
            
        Returns:
            分析结果
        """
        pass
    
    @abstractmethod
    async def sentiment_analysis(
        self,
        text: str
    ) -> Dict[str, Any]:
        """
        情感分析
        
        Args:
            text: 待分析文本
            
        Returns:
            情感分析结果
        """
        pass
    
    def _build_responsibility_prompt(self, conversation_text: str, context: str = "") -> str:
        """
        构建规避责任检测的提示词
        
        Args:
            conversation_text: 对话文本
            context: 上下文
            
        Returns:
            提示词
        """
        prompt = f"""
你是一个专业的客服对话分析师，请分析以下客服对话内容，识别是否存在规避责任的行为。

推卸责任的常见表现包括：
1. 推卸责任：将问题归咎于其他部门、供应商或外部因素
2. 部门推脱：声称问题不属于自己部门管辖，让客户找其他部门  
3. 外部推责：将责任完全归咎于外部因素，如供应商、厂家等
4. 否认责任：明确表示公司不承担任何责任
5. 消极回应：对客户问题给出无能为力、没办法等消极回应

请分析以下对话内容：
```
{conversation_text}
```

{f"相关上下文信息：{context}" if context else ""}

请从以下几个维度进行分析，并返回JSON格式结果：
1. 是否存在规避责任行为（true/false）
2. 规避责任的具体表现（如果存在）
3. 风险等级（low/medium/high）
4. 置信度（0-1）
5. 相关证据句子
6. 改进建议

返回格式：
{{
    "has_evasion": true/false,
    "evasion_types": "推卸责任", // 只能是"推卸责任"或空字符串  
    "risk_level": "low/medium/high",
    "confidence_score": 0.8,
    "evidence_sentences": ["具体的问题句子"],
    "improvement_suggestions": ["改进建议"],
    "sentiment": "positive/negative/neutral",
    "sentiment_intensity": 0.0
}}
"""
        return prompt
    
    def _build_sentiment_prompt(self, text: str) -> str:
        """
        构建情感分析的提示词
        
        Args:
            text: 待分析文本
            
        Returns:
            提示词
        """
        prompt = f"""
请分析以下文本的情感倾向，并返回JSON格式的结果：

文本内容：
```
{text}
```

请从以下维度分析：
1. 整体情感倾向（positive/negative/neutral）
2. 情感强度（0-1，1表示情感最强烈）
3. 具体情感类别（如愤怒、满意、失望等）
4. 情感关键词

返回格式：
{{
    "sentiment": "positive/negative/neutral",
    "intensity": 0.8,
    "emotion_categories": ["愤怒", "失望"],
    "keywords": ["关键词1", "关键词2"]
}}
"""
        return prompt
