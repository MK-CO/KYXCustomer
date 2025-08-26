-- 工单回复分析系统数据表
-- 适配 t_work_comment 表结构的分析结果表

-- 工单回复分析结果表
CREATE TABLE `ai_work_comment_analysis_results` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `work_id` bigint(20) NOT NULL COMMENT '工单ID',
  `order_id` bigint(20) DEFAULT NULL COMMENT '订单ID（从工单表关联获取）',
  `order_no` varchar(100) DEFAULT NULL COMMENT '订单编号（从工单表关联获取）',
  `session_id` varchar(128) DEFAULT NULL COMMENT '会话ID（同work_id）',
  
  -- 会话基本信息
  `session_start_time` datetime DEFAULT NULL COMMENT '会话开始时间',
  `session_end_time` datetime DEFAULT NULL COMMENT '会话结束时间',
  `total_comments` int(11) DEFAULT '0' COMMENT '总回复数',
  `customer_comments` int(11) DEFAULT '0' COMMENT '客户回复数',
  `service_comments` int(11) DEFAULT '0' COMMENT '客服回复数',
  
  -- 规避责任检测结果
  `has_evasion` tinyint(1) DEFAULT '0' COMMENT '是否规避责任 0/1',
  `risk_level` varchar(16) DEFAULT 'low' COMMENT '风险级别: low/medium/high',
  `confidence_score` decimal(4,3) DEFAULT '0.000' COMMENT '置信度评分',
  `evasion_types` text COMMENT '规避类型列表(JSON)',
  `evidence_sentences` text COMMENT '证据句子列表(JSON)',
  `improvement_suggestions` text COMMENT '改进建议列表(JSON)',
  
  -- 关键词粗筛结果
  `keyword_screening_score` decimal(4,3) DEFAULT '0.000' COMMENT '关键词筛选评分',
  `matched_categories` varchar(255) DEFAULT NULL COMMENT '匹配的关键词类别',
  `matched_keywords` text DEFAULT NULL COMMENT '匹配的关键词详情(JSON格式): {"推卸责任": {"keywords": ["撕", "催"], "patterns": ["催.*了"]}}',
  `is_suspicious` tinyint(1) DEFAULT '0' COMMENT '关键词粗筛是否疑似',
  
  -- 情感分析结果
  `sentiment` varchar(16) DEFAULT 'neutral' COMMENT '情感: positive/negative/neutral',
  `sentiment_intensity` decimal(4,3) DEFAULT '0.000' COMMENT '情感强度',
  
  -- 原始数据
  `conversation_text` longtext COMMENT '完整对话文本',
  `llm_raw_response` longtext COMMENT 'LLM原始响应',
  `analysis_details` longtext COMMENT '完整分析结果(JSON)',
  `analysis_note` varchar(255) DEFAULT NULL COMMENT '分析备注（如：评论为空、LLM分析失败等）',
  
  -- LLM调用信息
  `llm_provider` varchar(32) DEFAULT NULL COMMENT 'LLM提供商',
  `llm_model` varchar(64) DEFAULT NULL COMMENT 'LLM模型名称',
  `llm_tokens_used` int(11) DEFAULT '0' COMMENT 'LLM消耗token数',
  
  -- 时间戳
  `analysis_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '分析执行时间',
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_work_id` (`work_id`),
  KEY `idx_work_id_analysis` (`work_id`),
  KEY `idx_order_id_analysis` (`order_id`),
  KEY `idx_order_no_analysis` (`order_no`),
  KEY `idx_risk_level_analysis` (`risk_level`),
  KEY `idx_has_evasion_analysis` (`has_evasion`),
  KEY `idx_analysis_time_analysis` (`analysis_time`),
  KEY `idx_session_start_time_analysis` (`session_start_time`),
  KEY `idx_confidence_score_analysis` (`confidence_score`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='工单回复分析结果表';


-- 风险预警配置表
CREATE TABLE `ai_work_risk_alert_config` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `alert_name` varchar(64) NOT NULL COMMENT '预警名称',
  `alert_type` varchar(32) NOT NULL COMMENT '预警类型: evasion/sentiment/keywords',
  `risk_level` varchar(16) NOT NULL COMMENT '触发风险级别',
  `confidence_threshold` decimal(4,3) DEFAULT '0.800' COMMENT '置信度阈值',
  `enabled` tinyint(1) DEFAULT '1' COMMENT '是否启用',
  `alert_channels` varchar(255) DEFAULT NULL COMMENT '预警渠道: email,sms,webhook',
  `alert_recipients` text COMMENT '预警接收人列表(JSON)',
  `alert_template` text COMMENT '预警模板',
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  
  PRIMARY KEY (`id`),
  KEY `idx_alert_type` (`alert_type`),
  KEY `idx_risk_level` (`risk_level`),
  KEY `idx_enabled` (`enabled`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='风险预警配置表';

-- 预警记录表
CREATE TABLE `ai_work_risk_alert_logs` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `work_id` bigint(20) NOT NULL COMMENT '工单ID',
  `alert_config_id` int(11) NOT NULL COMMENT '预警配置ID',
  `alert_type` varchar(32) NOT NULL COMMENT '预警类型',
  `risk_level` varchar(16) NOT NULL COMMENT '风险级别',
  `confidence_score` decimal(4,3) DEFAULT '0.000' COMMENT '置信度评分',
  `alert_content` text COMMENT '预警内容',
  `alert_status` varchar(16) DEFAULT 'pending' COMMENT '预警状态: pending/sent/failed',
  `sent_channels` varchar(255) DEFAULT NULL COMMENT '已发送渠道',
  `sent_at` datetime DEFAULT NULL COMMENT '发送时间',
  `error_message` varchar(500) DEFAULT NULL COMMENT '错误信息',
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  
  PRIMARY KEY (`id`),
  KEY `idx_work_id_alert` (`work_id`),
  KEY `idx_alert_config_id` (`alert_config_id`),
  KEY `idx_alert_type_alert` (`alert_type`),
  KEY `idx_alert_status` (`alert_status`),
  KEY `idx_created_at_alert` (`created_at`),
  FOREIGN KEY (`alert_config_id`) REFERENCES `ai_work_risk_alert_config` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='预警记录表';

-- 工单AI处理待处理表（关联关系表）
CREATE TABLE `ai_work_pending_analysis` (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `work_id` bigint NOT NULL COMMENT '工单ID',
  `work_table_name` varchar(32) NOT NULL COMMENT '工单表名(如:t_work_2025)',
  `comment_table_name` varchar(32) NOT NULL COMMENT '评论表名(如:t_work_comment_2025)',
  `extract_date` date NOT NULL COMMENT '抽取日期',
  `create_time` datetime NOT NULL COMMENT '工单创建时间',
  `work_type` varchar(20) DEFAULT NULL COMMENT '工单类型(WorkTypeEnum)',
  `work_state` varchar(15) DEFAULT NULL COMMENT '工单状态(WorkStateEnum)',
  `create_by` varchar(64) DEFAULT NULL COMMENT '发起账号',
  `create_name` varchar(100) DEFAULT NULL COMMENT '发起人名称',
  
  -- AI处理状态
  `ai_status` varchar(20) DEFAULT 'PENDING' COMMENT 'AI处理状态: PENDING(待处理)/PROCESSING(处理中)/COMPLETED(已完成)/FAILED(失败)',
  `ai_process_start_time` datetime DEFAULT NULL COMMENT 'AI处理开始时间',
  `ai_process_end_time` datetime DEFAULT NULL COMMENT 'AI处理结束时间',
  `ai_error_message` text COMMENT 'AI处理错误信息',
  `ai_retry_count` int DEFAULT 0 COMMENT 'AI处理重试次数',
  
  -- 数据统计
  `comment_count` int DEFAULT 0 COMMENT '评论数量',
  `has_comments` tinyint(1) DEFAULT 0 COMMENT '是否有评论数据',
  
  -- 时间戳
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间',
  
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_work_id_extract_date` (`work_id`, `extract_date`),
  KEY `idx_extract_date` (`extract_date`),
  KEY `idx_ai_status` (`ai_status`),
  KEY `idx_work_table_name` (`work_table_name`),
  KEY `idx_create_time` (`create_time`),
  KEY `idx_ai_process_time` (`ai_process_start_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='工单AI处理待处理表-关联关系表';

-- 插入默认预警配置
INSERT INTO `ai_work_risk_alert_config` (`alert_name`, `alert_type`, `risk_level`, `confidence_threshold`, `alert_channels`, `alert_recipients`, `alert_template`) VALUES
('高风险规避责任检测', 'evasion', 'high', 0.850, 'email,webhook', '["admin@company.com"]', '检测到工单${work_id}存在高风险规避责任行为，置信度：${confidence_score}'),
('中风险规避责任检测', 'evasion', 'medium', 0.700, 'email', '["manager@company.com"]', '检测到工单${work_id}存在中风险规避责任行为，置信度：${confidence_score}'),
('强烈负面情感检测', 'sentiment', 'high', 0.800, 'email,sms', '["service@company.com"]', '检测到工单${work_id}存在强烈负面情感，请及时关注处理');
