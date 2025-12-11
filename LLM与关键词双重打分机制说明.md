# LLM与关键词双重打分机制详解

> 本文档详细说明系统如何通过关键词匹配和LLM智能分析相结合，准确判定客服责任问题。

## 📋 目录

1. [整体流程概览](#整体流程概览)
2. [第一阶段：关键词粗筛](#第一阶段关键词粗筛)
3. [第二阶段：LLM深度分析](#第二阶段llm深度分析)
4. [第三阶段：结果融合](#第三阶段结果融合)
5. [证据结构化标准](#证据结构化标准)
6. [最终判定逻辑](#最终判定逻辑)
7. [实际案例解析](#实际案例解析)

---

## 整体流程概览

系统采用**正则匹配 + LLM分析**的两阶段策略：

```
对话内容
   ↓
[第一阶段] 关键词粗筛 (keyword_screening)
   ├── 正则匹配：命中关键词/模式
   ├── 置信度计算：基于权重累加
   └── 判定：is_suspicious + confidence_score
   ↓
[筛选决策] confidence_score >= 0.3 ?
   ├── 是 → 进入LLM深度分析
   └── 否 → 判定为低风险，不保存
   ↓
[第二阶段] LLM深度分析
   ├── 输入：对话 + 正则证据上下文 + Few-shot示例
   ├── 分析：责任判定 + 风险评估 + 证据提取
   └── 输出：has_evasion + risk_level + evidence_sentences
   ↓
[第三阶段] 结果融合
   ├── 证据关联：正则证据 ↔ LLM证据
   ├── 置信度调整：综合两者结果
   └── 最终判定：保存 or 跳过
```

---

## 第一阶段：关键词粗筛

### 1.1 配置加载

**数据来源**：数据库动态加载（支持热更新）

```python
# 关键词配置表结构
ai_keyword_categories       # 分类表
├── category_key           # 分类键名（如 responsibility_evasion）
├── category_name          # 中文名称（如 推卸责任）
├── category_type          # 类型：analysis（分析用）
├── is_enabled             # 启用状态
└── sort_order             # 排序

ai_keyword_configs         # 关键词配置表
├── category_id            # 所属分类
├── keyword_type           # 类型：keyword（关键词）/ pattern（正则）/ exclusion（排除）
├── keyword_value          # 关键词/正则内容
├── weight                 # 权重
└── risk_level             # 风险级别：low/medium/high
```

**关键词配置示例**：

```python
{
    "推卸责任": {
        "keywords": [
            "不是我们的问题", "不是我们负责", "找其他部门",
            "联系供应商", "厂家问题", "找师傅"
        ],
        "patterns": [
            r"(不是|不属于).*(我们|门店|本店).*(问题|责任|负责)",
            r"(这是|属于).*(厂家|师傅|供应商).*(问题|责任)",
            r"(找|联系|去问).*(师傅|厂家|供应商)"
        ],
        "weight": 1.0,         # 权重系数
        "risk_level": "high"   # 风险级别
    },
    "拖延处理": {
        "keywords": ["翘单", "逃单", "一直拖", "故意拖"],
        "patterns": [
            r"(翘单|逃单).{0,10}(了|呢)",
            r"(拖着|一直拖|故意拖).*(不处理|不解决)"
        ],
        "weight": 1.1,
        "risk_level": "high"
    },
    "模糊回应": {
        "keywords": ["需要时间", "耐心等待", "已经在处理"],
        "patterns": [
            r"(需要时间|要等)(?![^，。！？；]*[具体时间|明确])"
        ],
        "exclusions": [  # 排除条件：包含这些内容则不算命中
            r"(预计|大概).*(时间|小时|分钟)",
            r"(\d+).*(小时|分钟|天).*内"
        ],
        "weight": 0.6,
        "risk_level": "medium"
    }
}
```

### 1.2 匹配计算

**关键词匹配权重**（代码位置：`stage2_analysis_service.py:2043-2113`）

```python
def keyword_screening(self, conversation_text: str, db: Session = None):
    """关键词粗筛"""
    matched_categories = []
    total_score = 0.0

    for category, config in keywords_config.items():
        category_score = 0.0

        # 检查排除条件
        if "exclusions" in config:
            for exclusion_pattern in config["exclusions"]:
                if re.search(exclusion_pattern, conversation_text):
                    excluded = True
                    break

        if not excluded:
            # 关键词匹配：每个关键词 +0.1
            for keyword in config["keywords"]:
                if keyword in conversation_text:
                    category_score += 0.1

            # 正则匹配：每个模式 +0.2
            for pattern in config["patterns"]:
                if re.search(pattern, conversation_text):
                    category_score += 0.2

        # 应用权重系数
        if category_score > 0:
            weighted_score = category_score * config["weight"]
            total_score += weighted_score
            matched_categories.append(category)

    # 置信度判定：total_score > 0.3 且有匹配类别
    is_suspicious = total_score > 0.3 and len(matched_categories) > 0

    return {
        "is_suspicious": is_suspicious,
        "confidence_score": min(total_score, 1.0),
        "matched_categories": matched_categories
    }
```

**打分规则解析**：

| 匹配类型 | 基础分数 | 权重调整 | 示例 |
|---------|---------|---------|------|
| 关键词命中 | +0.1 | × weight | "不是我们的问题" → 0.1 × 1.0 = 0.1 |
| 正则模式命中 | +0.2 | × weight | 匹配`(不是).*(我们).*(责任)` → 0.2 × 1.0 = 0.2 |
| 多个关键词 | 累加 | - | 命中3个关键词 → 0.1×3 = 0.3 |
| 排除规则 | 整个分类清零 | - | 虽然命中"需要时间"，但包含"预计2小时" → 0分 |

**阈值判定**：
- `confidence_score >= 0.3` → 进入LLM分析
- `confidence_score < 0.3` → 判定为低风险，不保存

### 1.3 证据提取

**结构化证据对象**（代码位置：`stage2_analysis_service.py:1266-1446`）

系统会提取每个匹配到的关键词/模式的详细信息：

```python
{
    "rule_type": "keyword",              # 规则类型
    "rule_name": "推卸责任",             # 分类名称
    "category": "推卸责任",
    "matched_keyword": "不是我们的问题",  # 匹配的关键词
    "matched_pattern": null,             # 正则表达式（关键词匹配时为null）
    "matched_text": "不是我们的问题",     # 实际匹配的文本
    "message_content": "这不是我们的问题，是厂家的配件质量问题",  # 原始消息
    "conversation_context": "[2024-01-15 10:30] 客服(张三): 这不是我们的问题，是厂家的配件质量问题",
    "highlighted_context": "[2024-01-15 10:30] 客服(张三): 这【不是我们的问题】，是厂家的配件质量问题",
    "config_id": 123,
    "message_index": 5,
    "user_type": "service",
    "user_name": "张三",
    "match_start_pos": 2,
    "match_end_pos": 10,
    "llm_analysis": {                    # LLM分析信息（初始状态）
        "llm_confirmed": false,
        "llm_risk_assessment": "unknown",
        "regex_matched": true
    },
    "evidence_status": "regex_matched"
}
```

---

## 第二阶段：LLM深度分析

### 2.1 Few-shot示例选择

**动态选择策略**（代码位置：`stage2_analysis_service.py:2232-2249`）

系统根据关键词命中的分类，动态选择对应的Few-shot示例：

```python
# 分类映射
category_key_mapping = {
    "紧急催促": "urgent_urging",
    "投诉纠纷": "complaint_dispute",
    "推卸责任": "responsibility_evasion",
    "拖延处理": "delay_handling",
    "不当用词": "inappropriate_wording"
}

# 如果命中"推卸责任"分类，则选择该分类的专属示例
few_shot_examples = self._get_category_few_shot_examples(db, ["responsibility_evasion"])
```

**Few-shot示例库**（代码位置：`stage2_analysis_service.py:1063-1210`）

```python
"responsibility_evasion": [  # 推卸责任示例（5个）
    {
        "conversation": "车主: 贴膜有气泡要求重新处理\n客服: 这不是我们门店的问题，是师傅技术问题，你直接找安装师傅负责。",
        "analysis": {
            "has_evasion": True,
            "risk_level": "high",
            "confidence_score": 0.95,
            "evasion_types": "推卸责任",
            "evidence_sentences": ["这不是我们门店的问题，是师傅技术问题", "你直接找安装师傅负责"],
            "improvement_suggestions": ["门店应承担服务责任，协调师傅重新处理"]
        }
    },
    # ... 另外4个示例
]
```

### 2.2 Prompt构建

**完整Prompt结构**（代码位置：`stage2_analysis_service.py:2115-2170`）

```
[系统角色定义]
你是专业的汽车服务行业质量分析专家

[规避责任定义]
1. 推卸责任：将问题推给师傅、厂家、供应商
2. 模糊回应：不提供具体时间、师傅安排
3. 拖延处理：故意延长处理时间
4. 不当用词：使用"车主烦人"等非专业表达

[重点关注]
⚠️ 配件质量问题推给"厂家"、"供应商"
⚠️ 贴膜、安装问题推给"师傅自己负责"
⚠️ 对推卸责任行为，置信度应 >= 0.8

[Few-shot示例]
示例1: [推卸责任 - high风险 - 0.95置信度]
对话: ...
分析: ...

示例2: ...

[证据上下文] ⭐ 关键优化点
=== 正则匹配发现的关键证据 ===
总计发现 3 条证据，涉及类别: 推卸责任, 模糊回应

📂 推卸责任 (2条):
  1. [关键词匹配] "不是我们的问题"
     对话: [2024-01-15 10:30] 客服(张三): 这【不是我们的问题】，是厂家的配件质量问题
  2. [正则匹配] 模式: (找|联系).*(师傅|厂家) -> "找安装师傅"
     对话: [2024-01-15 10:32] 客服(张三): 你直接【找安装师傅】负责

=== 分析要求 ===
请基于以上证据，结合完整对话内容：
1. 确认这些证据是否真的表明存在问题行为
2. 评估严重程度和风险级别
3. 判断是否存在规避责任行为

[待分析对话]
门店: 车主说贴膜有气泡要求重新处理
客服: 这不是我们门店的问题，是师傅技术问题，你直接找安装师傅负责。

[输出格式]
{
    "has_evasion": boolean,
    "risk_level": "low|medium|high",
    "confidence_score": float,
    "evasion_types": string,
    "evidence_sentences": [string],
    "improvement_suggestions": [string]
}
```

### 2.3 LLM分析输出

LLM会基于以上完整上下文返回分析结果：

```json
{
    "has_evasion": true,
    "risk_level": "high",
    "confidence_score": 0.95,
    "evasion_types": "推卸责任",
    "evidence_sentences": [
        "这不是我们门店的问题，是师傅技术问题",
        "你直接找安装师傅负责"
    ],
    "improvement_suggestions": [
        "门店应承担服务责任，协调师傅重新处理，而不是直接推卸给师傅"
    ],
    "sentiment": "negative",
    "sentiment_intensity": 0.8
}
```

---

## 第三阶段：结果融合

### 3.1 证据关联

**关联算法**（代码位置：`stage2_analysis_service.py:1565-1667`）

系统会将正则匹配的证据与LLM识别的证据进行相似度匹配：

```python
def _enhance_evidence_with_llm_analysis(detailed_evidence, llm_analysis):
    """将LLM分析结果关联到每条正则证据"""

    for evidence in detailed_evidence:
        message_content = evidence["message_content"]

        # 计算与LLM证据的相似度
        for llm_sentence in llm_analysis["evidence_sentences"]:
            similarity = calculate_similarity(message_content, llm_sentence)

            if similarity > 0.3:  # 匹配阈值
                evidence["llm_analysis"].update({
                    "llm_confirmed": True,              # LLM确认
                    "llm_risk_assessment": "high",
                    "llm_match_score": 0.85,           # 匹配度
                    "llm_evidence_match": llm_sentence,
                    "evidence_status": "regex_hit_llm_confirmed"  # 双重确认
                })
```

**相似度计算规则**：

```python
def _calculate_evidence_similarity(message_content, llm_sentence):
    """计算证据相似度"""

    # 1. 完全包含 → 1.0
    if message_content in llm_sentence or llm_sentence in message_content:
        return 1.0

    # 2. 关键词重叠度（Jaccard相似度）
    message_words = set(message_content.split())
    llm_words = set(llm_sentence.split())
    jaccard_score = len(intersection) / len(union)

    # 3. 长度相似度调整
    length_ratio = min(len1, len2) / max(len1, len2)

    # 综合评分：70%词重叠 + 30%长度
    final_score = jaccard_score * 0.7 + length_ratio * 0.3
    return final_score
```

### 3.2 证据状态标记

融合后的证据会被标记为不同状态：

```python
"evidence_status": {
    "regex_matched":           # 仅正则匹配到
    "regex_hit_llm_confirmed": # 正则命中 + LLM确认
    "regex_hit_llm_category_match":  # 正则命中 + LLM类别匹配
    "regex_hit_llm_normal":    # 正则命中但LLM认为正常
    "regex_hit_llm_low_risk":  # 正则命中但LLM低风险
    "llm_identified":          # LLM独立识别（无正则匹配）
}
```

### 3.3 置信度融合

**融合策略**（代码位置：`stage2_analysis_service.py:1500-1563`）

```python
def _merge_regex_and_llm_results(keyword_result, detailed_evidence, llm_analysis):
    """融合正则匹配和LLM分析结果"""

    # 基础：使用LLM的风险判定
    merged_result = {
        "has_evasion": llm_analysis["has_evasion"],
        "risk_level": llm_analysis["risk_level"],
        "confidence_score": llm_analysis["confidence_score"]
    }

    # 调整1：如果LLM置信度过低，使用正则置信度
    if merged_result["confidence_score"] < 0.5 and keyword_result["confidence_score"] > 0.5:
        merged_result["confidence_score"] = min(keyword_result["confidence_score"], 0.8)

    # 调整2：如果LLM未识别规避责任，但正则匹配到"推卸责任"且高置信度
    if not merged_result["has_evasion"] and "推卸责任" in keyword_result["matched_categories"]:
        if merged_result["confidence_score"] > 0.7:
            merged_result["has_evasion"] = True
            merged_result["evasion_types"] = "推卸责任"

    return merged_result
```

**置信度决策矩阵**：

| 正则置信度 | LLM置信度 | LLM判定 | 最终判定 | 最终置信度 |
|-----------|----------|---------|---------|-----------|
| 0.5 | 0.9 | has_evasion=True | 有责任 | 0.9 |
| 0.6 | 0.4 | has_evasion=False | 无责任 | 0.6（使用正则） |
| 0.8 | 0.8 | has_evasion=False，但匹配"推卸责任" | 有责任（二次确认） | 0.8 |
| 0.2 | - | （未进入LLM） | 低风险 | 0.2 |

---

## 证据结构化标准

### 完整证据对象示例

```json
{
    "rule_type": "keyword",
    "rule_name": "推卸责任",
    "category": "推卸责任",
    "matched_keyword": "不是我们的问题",
    "matched_pattern": null,
    "matched_text": "不是我们的问题",
    "message_content": "这不是我们的问题，是厂家的配件质量问题",
    "conversation_context": "[2024-01-15 10:30] 客服(张三): 这不是我们的问题，是厂家的配件质量问题",
    "highlighted_context": "[2024-01-15 10:30] 客服(张三): 这【不是我们的问题】，是厂家的配件质量问题",
    "config_id": 123,
    "message_index": 5,
    "message_id": 98765,
    "user_type": "service",
    "user_name": "张三",
    "create_time": "2024-01-15 10:30:00",
    "match_start_pos": 2,
    "match_end_pos": 10,
    "evidence_timestamp": "2024-01-15T10:30:00",
    "llm_analysis": {
        "llm_confirmed": true,                # LLM确认此证据
        "llm_risk_assessment": "high",        # LLM风险评估
        "llm_analysis_reason": "LLM识别此内容属于推卸责任行为",
        "llm_match_score": 0.95,             # 与LLM证据的匹配度
        "llm_evidence_match": "这不是我们门店的问题，是师傅技术问题",
        "llm_suggestion": "门店应承担服务责任，协调师傅重新处理",
        "regex_matched": true,               # 正则匹配成功
        "llm_overridden": false,             # LLM未覆盖正则结果
        "confidence_explanation": "正则匹配命中 '推卸责任' 分类，LLM分析确认存在问题行为"
    },
    "analysis_timestamp": "2024-01-15T10:31:00",
    "evidence_status": "regex_hit_llm_confirmed"
}
```

### 证据字段说明

| 字段 | 类型 | 说明 | 来源 |
|-----|------|------|------|
| `rule_type` | string | keyword/pattern/llm_analysis | 正则/LLM |
| `matched_keyword` | string | 匹配的关键词 | 关键词配置 |
| `matched_pattern` | string | 匹配的正则表达式 | 正则配置 |
| `matched_text` | string | 实际匹配的文本片段 | 消息内容 |
| `conversation_context` | string | 完整消息显示（含时间+角色） | 消息解析 |
| `highlighted_context` | string | 高亮显示匹配部分 | 自动生成 |
| `llm_confirmed` | boolean | LLM是否确认此证据 | LLM分析 |
| `llm_match_score` | float | 与LLM证据的相似度 | 相似度算法 |
| `evidence_status` | string | 证据状态标记 | 融合结果 |

---

## 最终判定逻辑

### 5.1 保存决策

**决策树**（代码位置：`stage2_analysis_service.py:507-643`）

```
分析结果
   ↓
[检查1] skip_save标记 == True?
   └── 是 → 不保存（低风险）
   ↓
[检查2] risk_level == "low" && has_evasion == False?
   └── 是 → 不保存（低风险无规避）
   ↓
[检查3] risk_level == "medium" or "high"?
   └── 是 → 保存到数据库
```

**保存条件总结**：

| 风险级别 | 规避责任 | 是否保存 | 原因 |
|---------|---------|---------|------|
| low | False | ❌ 不保存 | 正常对话 |
| low | True | ✅ 保存 | 虽然低风险但有规避行为 |
| medium | False | ✅ 保存 | 中等风险需要记录 |
| medium | True | ✅ 保存 | 中等风险+规避行为 |
| high | 任意 | ✅ 保存 | 高风险必须记录 |

### 5.2 数据库记录

**保存字段**（代码位置：`stage2_analysis_service.py:685-785`）

```sql
INSERT INTO ai_work_comment_analysis_results (
    work_id,
    order_id,
    order_no,
    -- 基础判定
    has_evasion,              -- 是否规避责任：1/0
    risk_level,               -- 风险级别：low/medium/high
    confidence_score,         -- 置信度：0-1
    evasion_types,            -- 规避类型（JSON）

    -- 证据数据
    evidence_sentences,       -- 结构化证据对象数组（JSON）
    improvement_suggestions,  -- 改进建议（JSON）

    -- 关键词匹配
    keyword_screening_score,  -- 关键词置信度
    matched_categories,       -- 匹配的类别（逗号分隔）
    matched_keywords,         -- 匹配的关键词详情（JSON）
    is_suspicious,            -- 关键词是否可疑：1/0

    -- LLM分析
    llm_provider,             -- LLM提供商
    llm_model,                -- LLM模型
    llm_tokens_used,          -- Token消耗
    llm_raw_response,         -- LLM原始响应（JSON）

    -- 原始数据
    conversation_text,        -- 完整对话文本
    analysis_details          -- 完整分析结果（JSON）
)
```

---

## 实际案例解析

### 案例1：推卸责任 - 高风险

**对话内容**：
```
门店: 车主说贴膜有气泡要求重新处理
客服: 这不是我们门店的问题，是师傅技术问题，你直接找安装师傅负责。
```

**分析过程**：

#### 阶段1：关键词粗筛

```python
匹配结果：
- 类别：推卸责任
- 关键词：["不是我们的问题"]（+0.1）
- 正则：r"(不是).*(我们|门店).*(问题|责任)"（+0.2）
- 正则：r"(找|联系).*(师傅)"（+0.2）
- 权重：1.0

计算：
category_score = 0.1 + 0.2 + 0.2 = 0.5
weighted_score = 0.5 × 1.0 = 0.5
total_score = 0.5

判定：
is_suspicious = True（0.5 > 0.3）
confidence_score = 0.5
```

#### 阶段2：LLM分析

```
输入Prompt包含：
1. Few-shot示例（5个推卸责任案例）
2. 证据上下文：
   - 关键词匹配："不是我们的问题"
   - 正则匹配："不是我们门店的问题"
   - 正则匹配："找安装师傅"

LLM输出：
{
    "has_evasion": true,
    "risk_level": "high",
    "confidence_score": 0.95,
    "evasion_types": "推卸责任",
    "evidence_sentences": [
        "这不是我们门店的问题，是师傅技术问题",
        "你直接找安装师傅负责"
    ]
}
```

#### 阶段3：结果融合

```python
证据关联：
- 正则证据1: "不是我们的问题"
  ↔ LLM证据1: "这不是我们门店的问题，是师傅技术问题"
  相似度: 0.9 → llm_confirmed = True

- 正则证据2: 匹配"找安装师傅"
  ↔ LLM证据2: "你直接找安装师傅负责"
  相似度: 0.85 → llm_confirmed = True

最终结果：
{
    "has_evasion": true,
    "risk_level": "high",
    "confidence_score": 0.95,  # 使用LLM高置信度
    "evidence_sentences": [
        {
            "rule_type": "keyword",
            "matched_keyword": "不是我们的问题",
            "llm_analysis": {
                "llm_confirmed": true,
                "llm_match_score": 0.9,
                "evidence_status": "regex_hit_llm_confirmed"
            }
        },
        {
            "rule_type": "pattern",
            "matched_pattern": r"(找|联系).*(师傅)",
            "llm_analysis": {
                "llm_confirmed": true,
                "llm_match_score": 0.85,
                "evidence_status": "regex_hit_llm_confirmed"
            }
        }
    ]
}

保存决策：✅ 保存（high风险）
```

---

### 案例2：模糊回应 - 但提供了具体时间

**对话内容**：
```
车主: 订单什么时候能处理完？
客服: 这个需要时间处理，预计今天下午3点完成。
```

**分析过程**：

#### 阶段1：关键词粗筛

```python
匹配结果：
- 类别：模糊回应
- 关键词：["需要时间"]（+0.1）
- 正则：r"需要时间"（+0.2）
- 排除规则命中：r"(预计|大概).*(时间|小时)" → 整个分类清零

计算：
category_score = 0 (被排除规则清零)
total_score = 0

判定：
is_suspicious = False（0 < 0.3）
confidence_score = 0
```

**结果**：未进入LLM分析，直接判定为低风险，不保存。

**为什么不保存**？
虽然包含"需要时间"这个模糊回应关键词，但同时提供了具体时间"今天下午3点"，触发了排除规则，证明这是正常的服务说明，不是问题行为。

---

### 案例3：关键词命中但LLM判定为正常

**对话内容**：
```
车主: 我的订单有问题
客服: 不是系统问题，我帮您查一下订单，请稍等。
```

**分析过程**：

#### 阶段1：关键词粗筛

```python
匹配结果：
- 类别：推卸责任
- 关键词：["不是"]（部分匹配）
- 正则：r"(不是).*(我们|门店).*(问题|责任)"（未完全匹配）

计算：
category_score = 0.1
weighted_score = 0.1 × 1.0 = 0.1
total_score = 0.1

判定：
is_suspicious = False（0.1 < 0.3）
```

**结果**：未达到阈值，不进入LLM分析，判定为低风险。

**正确判定的原因**：
- "不是系统问题"后面紧跟"我帮您查一下"，表明客服在主动解决问题
- 关键词"不是"需要结合上下文判断，单独匹配分数不够
- 阈值机制有效防止了误判

---

### 案例4：LLM独立识别（无正则匹配）

**对话内容**：
```
车主: 配件安装后有异响
客服: 那您先凑合用着吧，等哪天我们有空了再说。
```

**分析过程**：

#### 阶段1：关键词粗筛

```python
匹配结果：
- 没有命中任何配置的关键词
- 没有匹配任何正则模式

计算：
total_score = 0

判定：
is_suspicious = False
```

**理论上应该跳过，但如果强制进入LLM分析**：

#### 阶段2：LLM分析

```
LLM输出：
{
    "has_evasion": true,
    "risk_level": "medium",
    "confidence_score": 0.75,
    "evasion_types": "拖延处理",
    "evidence_sentences": [
        "那您先凑合用着吧，等哪天我们有空了再说"
    ]
}
```

**实际系统行为**：
由于未达到0.3阈值，这个对话在当前配置下**不会进入LLM分析**，会被判定为低风险。

**如何改进**？
- 方案1：添加关键词"凑合用"、"等有空"到配置
- 方案2：降低阈值至0.2（但可能增加误报）
- 方案3：对所有对话都进行LLM分析（成本高）

当前系统选择**方案1**：持续优化关键词配置库，平衡准确率和成本。

---

## 关键设计亮点

### 1. 双重验证机制

```
正则匹配（快速筛选） + LLM分析（准确判定）
        ↓                        ↓
    高召回率                  高准确率
```

- **正则匹配**：保证不漏掉潜在问题（召回率）
- **LLM分析**：避免误判正常对话（准确率）

### 2. 证据可追溯

每个判定都有完整的证据链：
```
原始消息 → 正则匹配 → LLM确认 → 最终判定
   ↓           ↓          ↓          ↓
保存完整     高亮显示    相似度     结构化JSON
```

### 3. 动态Few-shot

根据正则匹配的分类，选择对应的Few-shot示例：
- 命中"推卸责任" → 使用5个推卸责任案例
- 命中"拖延处理" → 使用拖延处理案例
- 提高LLM判定的准确性

### 4. 分级保存策略

```
低风险 → 不保存（节省存储）
中风险 → 保存（需要关注）
高风险 → 保存（重点处理）
```

### 5. 排除规则机制

避免正常业务用语被误判：
- "需要时间" + "预计2小时" → 正常
- "不是我们的问题" + "我帮您协调" → 正常

---

## 总结

**为什么能准确判定客服责任？**

1. **关键词配置全面**：覆盖推卸责任、拖延处理、模糊回应等多种场景
2. **正则模式精准**：不仅匹配关键词，还匹配语法结构
3. **排除规则完善**：避免误判正常业务用语
4. **LLM深度理解**：理解上下文语义，避免断章取义
5. **Few-shot引导**：提供行业特定案例，提高判定准确性
6. **双重验证机制**：正则+LLM互相印证
7. **证据结构化**：完整记录判定依据，可追溯可解释
8. **阈值机制**：0.3的阈值平衡召回率和准确率

**核心公式**：

```
最终置信度 = {
    LLM置信度                      if LLM置信度 >= 0.5
    min(正则置信度, 0.8)           if LLM置信度 < 0.5 且正则置信度 > 0.5
    LLM置信度                      if 正则命中"推卸责任" 且 LLM置信度 > 0.7
}

是否保存 = (风险级别 != "low") OR (has_evasion == True)
```

---

## 常见问题

### Q1: 为什么有些明显的问题没有被识别？

**A**: 可能原因：
1. 关键词配置不全 → 解决：添加新关键词
2. 正则模式未覆盖 → 解决：添加新模式
3. 未达到0.3阈值 → 解决：检查权重配置

### Q2: 为什么有些正常对话被误判？

**A**: 可能原因：
1. 排除规则不完善 → 解决：添加排除模式
2. LLM分析偏差 → 解决：优化Few-shot示例
3. 阈值设置过低 → 解决：提高阈值

### Q3: 如何调整系统的敏感度？

**A**: 调整方法：
- 提高敏感度：降低阈值至0.2，增加关键词
- 降低敏感度：提高阈值至0.4，增加排除规则
- 平衡调整：优化权重配置（weight字段）

### Q4: 证据为什么是结构化的？

**A**: 结构化的好处：
1. 可追溯：完整记录判定依据
2. 可解释：展示给用户/管理员
3. 可优化：统计误判原因，改进配置
4. 可审计：合规性要求

---

## 附录

### A. 配置示例完整版

参考：`app/services/keyword_config_manager.py`

### B. 数据库表结构

参考：`app/models/analysis.py`

### C. 核心代码位置索引

| 功能 | 文件 | 行号 |
|-----|------|------|
| 关键词粗筛 | stage2_analysis_service.py | 2043-2113 |
| LLM分析 | stage2_analysis_service.py | 2174-2388 |
| 结果融合 | stage2_analysis_service.py | 1500-1563 |
| 证据提取 | stage2_analysis_service.py | 1266-1446 |
| 保存决策 | stage2_analysis_service.py | 507-643 |
| Few-shot库 | stage2_analysis_service.py | 1063-1210 |

---

**文档版本**: v1.0
**生成日期**: 2024-01-15
**维护者**: KYX智能分析系统团队
