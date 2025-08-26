"""
火山大模型LLM提供商 - 更新版本
"""
import json
import aiohttp
import logging
from typing import Dict, List, Optional, Any
from .base import BaseLLMProvider
from app.models.denoise import safe_json_dumps

logger = logging.getLogger(__name__)


class VolcengineProvider(BaseLLMProvider):
    """火山大模型提供商"""
    
    def __init__(self, api_key: str, endpoint: str, model: str = "skylark2-pro-4k", region: str = "cn-beijing"):
        """
        初始化火山大模型提供商
        
        Args:
            api_key: API密钥
            endpoint: 服务端点
            model: 模型名称
            region: 地区
        """
        super().__init__(api_key)
        self.endpoint = endpoint
        self.model = model
        self.region = region
    
    async def chat_completion(
        self,
        messages: List[Dict[str, str]],
        model: Optional[str] = None,
        temperature: float = 0.7,
        max_tokens: int = 2000,
        **kwargs
    ) -> Dict[str, Any]:
        """
        火山大模型聊天完成接口
        
        Args:
            messages: 消息列表
            model: 模型名称（可选）
            temperature: 温度参数
            max_tokens: 最大token数
            
        Returns:
            LLM响应结果
        """
        try:
            # 构建请求负载
            payload = {
                "model": model or self.model,
                "messages": messages,
                "temperature": temperature,
                "max_tokens": max_tokens,
                **kwargs
            }
            
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}"
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.endpoint,
                    headers=headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=60)
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # 解析响应
                        if data.get("choices") and len(data["choices"]) > 0:
                            content = data["choices"][0]["message"]["content"]
                            
                            return {
                                "success": True,
                                "content": content,
                                "model": payload["model"],
                                "usage": data.get("usage", {}),
                                "raw_response": data
                            }
                        else:
                            return {
                                "success": False,
                                "error": "响应格式异常：缺少choices",
                                "content": ""
                            }
                    else:
                        error_text = await response.text()
                        return {
                            "success": False,
                            "error": f"HTTP {response.status}: {error_text}",
                            "content": ""
                        }
                        
        except Exception as e:
            logger.error(f"火山大模型调用异常: {e}")
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
        使用火山大模型分析规避责任行为
        """
        prompt = self._build_enhanced_responsibility_prompt(conversation_text, context or "")
        
        messages = [
            {"role": "user", "content": prompt}
        ]
        
        response = await self.chat_completion(
            messages=messages,
            temperature=0.3,  # 低温度确保更稳定的分析结果
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
        情感分析
        """
        prompt = self._build_sentiment_prompt(text)
        
        messages = [
            {"role": "user", "content": prompt}
        ]
        
        response = await self.chat_completion(
            messages=messages,
            temperature=0.2,
            max_tokens=500
        )
        
        if not response["success"]:
            return {
                "success": False,
                "error": response["error"],
                "analysis": {}
            }
        
        try:
            content = (response.get("content") or "").strip()
            
            # 提取JSON部分
            if "```json" in content:
                start = content.find("```json") + 7
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
    
    def _build_enhanced_responsibility_prompt(self, conversation_text: str, context: str = "") -> str:
        """
        构建增强的规避责任检测提示词
        """
        few_shot_examples = [
            {
                "conversation": "门店: 车主一直催贴膜进度，又来了，怎么样了？\n客服: 这个需要时间处理，让车主耐心等待。",
                "analysis": {
                    "has_evasion": True,
                    "risk_level": "high",
                    "confidence_score": 0.85,
                    "evasion_types": ["紧急催促", "模糊回应"],
                    "evidence_sentences": ["车主一直催贴膜进度，又来了，怎么样了", "这个需要时间处理，让车主耐心等待"],
                    "improvement_suggestions": ["应具体回应车主的催促，提供明确的完成时间，如'师傅今天下午3点完成贴膜'"]
                }
            },
            {
                "conversation": "门店: 车主投诉配件质量，要退款了\n客服: 这不是我们的问题，是厂家的配件质量问题，让车主直接找供应商。",
                "analysis": {
                    "has_evasion": True,
                    "risk_level": "high",
                    "confidence_score": 0.95,
                    "evasion_types": ["投诉纠纷", "推卸责任"],
                    "evidence_sentences": ["车主投诉配件质量，要退款了", "这不是我们的问题，是厂家的配件质量问题"],
                    "improvement_suggestions": ["面对投诉和退款要求，门店应承担售后责任，协助处理而不是推卸给厂家"]
                }
            },
            {
                "conversation": "师傅: 又来催了，撕心裂肺的，搞快点弄完\n门店: 知道了，赶紧搞定",
                "analysis": {
                    "has_evasion": True,
                    "risk_level": "high",
                    "confidence_score": 0.9,
                    "evasion_types": ["不当用词表达"],
                    "evidence_sentences": ["又来催了，撕心裂肺的，搞快点弄完", "赶紧搞定"],
                    "improvement_suggestions": ["应使用专业用语，如'车主比较着急，请加快处理速度'，避免'撕'、'搞'等不当表达"]
                }
            },
            {
                "conversation": "门店: 有纠纷单，客诉12315了\n客服: 翘单吧，能拖就拖一天是一天。",
                "analysis": {
                    "has_evasion": True,
                    "risk_level": "high",
                    "confidence_score": 0.98,
                    "evasion_types": ["投诉纠纷", "拖延处理"],
                    "evidence_sentences": ["有纠纷单，客诉12315了", "翘单吧，能拖就拖一天是一天"],
                    "improvement_suggestions": ["严禁故意拖延处理客诉和12315投诉，应立即响应和解决"]
                }
            },
            {
                "conversation": "门店: 车主加急联系，速度催结果，有进展了吗？\n客服: 已经在跟进了，会尽快给答复。",
                "analysis": {
                    "has_evasion": True,
                    "risk_level": "medium",
                    "confidence_score": 0.75,
                    "evasion_types": ["紧急催促", "模糊回应"],
                    "evidence_sentences": ["车主加急联系，速度催结果，有进展了吗", "已经在跟进了，会尽快给答复"],
                    "improvement_suggestions": ["面对加急催促，应提供具体的进展情况和预计完成时间"]
                }
            },
            {
                "conversation": "门店: 车主咨询全车贴膜价格和质保期\n客服: 全车贴膜1800元，质保2年，包括材料和人工，预计明天上午完成安装。",
                "analysis": {
                    "has_evasion": False,
                    "risk_level": "low",
                    "confidence_score": 0.1,
                    "evasion_types": [],
                    "evidence_sentences": [],
                    "improvement_suggestions": []
                }
            }
        ]
        
        few_shot_text = "\n\n".join([
            f"对话示例{i+1}:\n{example['conversation']}\n分析结果:\n{safe_json_dumps(example['analysis'], ensure_ascii=False)}"
            for i, example in enumerate(few_shot_examples)
        ])
        
        prompt = f"""
你是一个专业的汽车服务行业质量分析专家，请分析以下师傅、门店、客服之间的对话中是否存在规避责任的行为。

🚨 重点检测关键词（高风险信号）：
**紧急催促类**: 撕、催、紧急、又来了、怎么样了、有进展了吗、加急联系、速度
**投诉纠纷类**: 纠纷单、投诉、退款了、结果、12315、客诉、翘单
**推卸责任类**: 不是我们问题、找厂家、师傅负责、不归我们管
**拖延处理类**: 翘单、逃单、能拖就拖、故意拖延
**不当用词类**: 搞快点、烦死了、撕心裂肺、赶紧搞定

在汽车服务行业中，规避责任的表现包括：
1. **紧急催促响应不当**：面对车主"催"、"撕"、"又来了"等急迫表达，给出敷衍回应
2. **投诉纠纷推卸**：遇到"纠纷单"、"投诉"、"退款了"、"12315"、"客诉"时直接推卸
3. **推卸责任**：将问题完全推给师傅、厂家、供应商，拒绝承担服务责任
4. **拖延处理**：面对投诉采用"翘单"、"能拖就拖"等消极态度
5. **不当用词**：使用"撕"、"搞"、"烦死了"等非专业表达

⚠️ 核心检测标准：
- 出现"撕、催、紧急、又来了、怎么样了、有进展了吗"等词汇的对话需重点分析
- "纠纷单、投诉、退款了、12315、客诉、翘单"为高风险关键词，出现时置信度应≥0.8
- "速度、加急联系、结果"等催促词汇结合模糊回应的，置信度应≥0.7
- 正常业务咨询（价格、质保等）给出具体信息的，不算规避责任

分析要求：
1. **优先检测上述核心关键词**，这些是汽车服务行业的高风险信号
2. 重点关注对催促、投诉、纠纷的响应态度
3. 评估风险级别：low（无风险）、medium（中等风险）、high（高风险）  
4. 核心关键词命中时，置信度应≥0.7
5. 列出具体的证据句子
6. 给出针对性改进建议

{few_shot_text}

现在请分析以下对话：
{conversation_text}

{f"关键词筛选上下文：{context}" if context else ""}

请严格按照以下JSON格式返回分析结果：
{{
    "has_evasion": boolean,
    "risk_level": "low|medium|high",
    "confidence_score": float,
    "evasion_types": [string],
    "evidence_sentences": [string],
    "improvement_suggestions": [string],
    "sentiment": "positive|negative|neutral",
    "sentiment_intensity": float
}}
"""
        return prompt
