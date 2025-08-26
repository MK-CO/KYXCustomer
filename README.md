# AI Platform Smart - 智能客服对话分析系统

## 系统简介

AI Platform Smart 是一个基于大语言模型（LLM）和检索增强生成（RAG）技术的智能客服对话分析系统，专门用于分析客服聊天记录，检测客服人员是否存在规避责任的行为。

## 核心功能

1. **定时数据抽取**: 自动从MySQL数据库抽取聊天记录，按工单ID分组
2. **高精度检测**: 使用关键词粗筛 + LLM few-shot精判的两阶段检测
3. **规避责任识别**: 识别推卸责任、拖延处理、模糊回应、否认责任、敷衍态度等行为
4. **JSON标准输出**: LLM返回结构化JSON结果，便于处理和分析
5. **批量MySQL分析**: 对工单聊天记录进行批量分析和存储
6. **Properties配置**: 使用标准properties文件进行配置管理

## 技术架构

- **Web框架**: FastAPI
- **数据库**: MySQL (SQLAlchemy ORM)
- **LLM支持**: 火山大模型 + SiliconFlow
- **检测引擎**: 关键词粗筛 + LLM few-shot精判
- **任务调度**: 内置定时调度器
- **配置管理**: Properties文件 + 环境变量

## 项目结构

```
ai-platform-smart/
├── app/
│   ├── api/                    # API接口
│   │   ├── analysis.py         # 分析相关接口
│   │   └── system.py          # 系统管理接口
│   ├── core/                   # 核心组件
│   │   └── scheduler.py       # 定时任务调度器
│   ├── db/                     # 数据库
│   │   └── database.py        # 数据库连接管理
│   ├── models/                 # 数据模型
│   │   └── conversation.py    # 对话和分析结果模型
│   └── services/              # 业务服务
│       ├── data_extractor.py  # 数据抽取服务
│       ├── batch_analyzer.py  # 批量分析服务
│       ├── detection_engine.py # 高精度检测引擎
│       └── llm/               # LLM适配器
│           ├── base.py        # LLM基础抽象类
│           ├── volcengine_provider.py    # 火山大模型适配器
│           ├── siliconflow_provider.py  # SiliconFlow适配器
│           └── llm_factory.py # LLM工厂类
├── config/                     # 配置文件
│   ├── properties_loader.py  # Properties加载器
│   └── settings.py            # 应用配置
├── application.properties      # 主配置文件
├── data/                       # 数据目录
├── logs/                       # 日志目录
├── requirements.txt           # 依赖包
├── main.py                   # 主程序入口
└── README.md                 # 说明文档
```

## 环境配置

### 1. 安装依赖

```bash
cd ai-platform-smart
pip install -r requirements.txt
```

### 2. 配置文件设置

系统使用`application.properties`文件进行配置，也支持通过环境变量覆盖：

```properties
# 修改 application.properties 文件中的关键配置：

# 数据库配置
db.local.password=your_local_password
db.prod.host=your_prod_host
db.prod.password=your_prod_password

# LLM配置
llm.provider=volcengine
volcengine.api.key=your_volcengine_api_key
volcengine.endpoint=your_volcengine_endpoint

# 或使用 SiliconFlow
# llm.provider=siliconflow
# siliconflow.api.key=your_siliconflow_api_key

# API安全配置
api.key=your_secure_api_key
```

**环境变量覆盖示例：**
```bash
# 通过环境变量覆盖配置
export DB_LOCAL_PASSWORD=your_password
export VOLCENGINE_API_KEY=your_volcengine_key
export API_KEY=your_secure_key
```

### 3. 数据库表结构

系统会自动创建以下数据表：
- `analysis_results`: 分析结果表（简化版）

原始聊天记录表需要包含以下字段：
```sql
CREATE TABLE chat_records (
    id INT PRIMARY KEY,
    order_id VARCHAR(64),      -- 工单ID
    session_id VARCHAR(128),   -- 会话ID
    user_type VARCHAR(16),     -- 用户类型: customer, service, system
    user_id VARCHAR(64),       -- 用户ID
    message_content TEXT,      -- 消息内容
    message_type VARCHAR(16),  -- 消息类型
    created_time DATETIME,     -- 创建时间
    is_processed TINYINT       -- 是否已处理
);
```

## 使用方法

### 1. 启动方式

#### 仅启动API服务（开发环境）
```bash
python main.py --mode api --env local
```

#### 仅启动调度器
```bash
python main.py --mode scheduler --env prod
```

#### 同时启动API和调度器（生产环境）
```bash
python main.py --mode both --env prod
```

### 2. API接口

系统启动后，可访问以下接口：

- **API文档**: `http://localhost:8000/docs`
- **系统信息**: `GET /api/v1/system/info`
- **健康检查**: `GET /api/v1/system/health`

#### 主要API接口

**所有API都需要添加认证头：**
```bash
Authorization: Bearer ai-platform-smart-2024
```

1. **手动触发分析**
```bash
curl -X POST "http://localhost:8000/api/v1/analysis/manual/run?limit=100" \
  -H "Authorization: Bearer ai-platform-smart-2024"
```

2. **分析指定工单**
```bash
curl -X POST "http://localhost:8000/api/v1/analysis/order/{order_id}" \
  -H "Authorization: Bearer ai-platform-smart-2024"
```

3. **查询分析结果**
```bash
curl -X GET "http://localhost:8000/api/v1/analysis/results?risk_level=high&limit=50" \
  -H "Authorization: Bearer ai-platform-smart-2024"
```

4. **获取统计信息**
```bash
curl -X GET "http://localhost:8000/api/v1/analysis/statistics?start_date=2024-01-01" \
  -H "Authorization: Bearer ai-platform-smart-2024"
```

5. **获取高风险对话**
```bash
curl -X GET "http://localhost:8000/api/v1/analysis/high-risk?limit=20" \
  -H "Authorization: Bearer ai-platform-smart-2024"
```

6. **获取检测引擎配置**
```bash
curl -X GET "http://localhost:8000/api/v1/system/detection/config" \
  -H "Authorization: Bearer ai-platform-smart-2024"
```

7. **测试检测引擎**
```bash
curl -X POST "http://localhost:8000/api/v1/system/detection/test" \
  -H "Authorization: Bearer ai-platform-smart-2024" \
  -H "Content-Type: application/json" \
  -d '{"text": "这不是我们的问题，你去找厂家吧"}'
```

8. **获取指定类别关键词**
```bash
curl -X GET "http://localhost:8000/api/v1/system/detection/keywords/推卸责任" \
  -H "Authorization: Bearer ai-platform-smart-2024"
```

### 3. 调度器管理

#### 查看调度器状态
```bash
GET /api/v1/system/scheduler/status
```

#### 启动/停止调度器
```bash
POST /api/v1/system/scheduler/start
POST /api/v1/system/scheduler/stop
```

#### 调整任务间隔
```bash
POST /api/v1/system/scheduler/intervals
{
    "batch_analysis_interval": 300,  # 5分钟
    "cleanup_interval": 3600        # 1小时
}
```

### 4. 知识库管理

#### 查看知识库信息
```bash
GET /api/v1/system/rag/knowledge
```

#### 添加正面例子
```bash
POST /api/v1/system/rag/add-example
{
    "example_type": "positive",
    "dialogue": "客户：商品有问题\n客服：非常抱歉，我们立即为您处理",
    "reason": "主动承担责任",
    "keywords": ["抱歉", "立即处理"]
}
```

#### 添加负面例子
```bash
POST /api/v1/system/rag/add-example
{
    "example_type": "negative",
    "dialogue": "客户：商品有问题\n客服：这不是我们的问题，去找厂家",
    "evasion_type": "推卸责任",
    "risk_level": "high",
    "keywords": ["不是我们的问题", "去找厂家"]
}
```

#### 添加敏感词
```bash
POST /api/v1/system/rag/add-keyword
{
    "keyword": "不关我们的事",
    "category": "推卸责任",
    "weight": 1.0
}
```

## 高精度检测流程

### 1. 关键词粗筛
- **5大类别关键词**：推卸责任、拖延处理、模糊回应、否认责任、敷衍态度
- **正则模式匹配**：识别复杂的规避表达
- **权重计算**：根据关键词类型和数量计算初步风险评分

### 2. LLM Few-shot精判
- **Few-shot示例**：提供正面和负面样例供LLM参考
- **结构化提示词**：包含检测规则、样例和分析要求
- **JSON标准输出**：确保结果格式统一，便于后续处理

### 3. 工作流程
1. **数据抽取**: 定时从聊天记录表抽取未处理的记录
2. **分组处理**: 按工单ID将聊天记录分组
3. **关键词粗筛**: 快速识别疑似问题对话
4. **LLM精判**: 使用大语言模型进行深度分析
5. **结果解析**: 解析LLM返回的JSON结果
6. **存储结果**: 将分析结果存储到数据库
7. **标记完成**: 将处理过的聊天记录标记为已处理

## 分析维度

### 规避责任检测
- **推卸责任**: 将问题归咎于其他部门或第三方
- **拖延处理**: 故意延长处理时间
- **模糊回应**: 给出模棱两可的答复
- **否认责任**: 明确表示不承担责任
- **敷衍态度**: 随意应付客户

### 情感分析
- **情感倾向**: positive/negative/neutral
- **情感强度**: 0-1的数值评分
- **情感类别**: 具体的情感标签

### 风险等级
- **High**: 明确的规避责任行为
- **Medium**: 可能存在问题的表达
- **Low**: 正常的客服回复

## 安全配置

### API认证
- **所有API端点**（除了`/`和`/health`）都需要Bearer token认证
- 默认API密钥：`ai-platform-smart-2024`（生产环境请务必修改）
- 通过环境变量`API_KEY`进行配置

### 安全建议
1. **生产环境必须修改API_KEY**
2. **使用HTTPS**部署（生产环境）
3. **限制访问IP**（推荐使用nginx等反向代理）
4. **定期轮换API密钥**
5. **监控API访问日志**

## 注意事项

1. **数据库配置**: 确保原始聊天记录表包含必要字段
2. **LLM配置**: 至少配置一个可用的LLM提供商
3. **API安全**: 生产环境务必修改默认API密钥
4. **资源监控**: 批量分析可能消耗较多LLM tokens
5. **定时任务**: 生产环境建议使用进程管理器（如supervisor）

## 扩展开发

1. **添加新的LLM提供商**: 继承`BaseLLMProvider`类
2. **自定义分析维度**: 修改提示词和结果解析逻辑
3. **增加数据源**: 扩展`DataExtractor`类
4. **优化RAG**: 添加更多知识库数据

## 常见问题

Q: 如何切换LLM提供商？
A: 修改配置文件中的`LLM_PROVIDER`参数，并确保对应的API密钥已配置。

Q: API返回401错误怎么办？
A: 检查Authorization头是否正确，格式为`Bearer YOUR_API_KEY`。

Q: 分析结果不准确怎么办？
A: 可以通过添加更多正反例子和敏感词来改善分析效果。

Q: 如何处理大量历史数据？
A: 可以调整批量分析的limit参数，分批处理历史数据。

Q: 如何修改API密钥？
A: 通过环境变量`API_KEY`或配置文件修改，重启服务生效。

## 技术支持

如有问题，请检查：
1. 配置文件是否正确
2. 数据库连接是否正常
3. LLM API是否可访问
4. 日志文件中的错误信息
# KYXCustomer
