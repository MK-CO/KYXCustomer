-- 为工单回复分析结果表增加首次响应统计字段
ALTER TABLE `ai_work_comment_analysis_results`
  ADD COLUMN `first_response_seconds` int(11) DEFAULT NULL COMMENT '首次客服响应耗时(秒)' AFTER `service_comments`,
  ADD COLUMN `first_response_timeout` tinyint(1) DEFAULT '0' COMMENT '首次响应是否超时' AFTER `first_response_seconds`,
  ADD COLUMN `first_response_rule_seconds` int(11) DEFAULT '120' COMMENT '首次响应超时阈值(秒)' AFTER `first_response_timeout`,
  ADD COLUMN `first_customer_message_time` datetime DEFAULT NULL COMMENT '商家首条消息时间' AFTER `first_response_rule_seconds`,
  ADD COLUMN `first_service_reply_time` datetime DEFAULT NULL COMMENT '客服首次回复时间' AFTER `first_customer_message_time`;

-- 添加超时索引，便于筛选
ALTER TABLE `ai_work_comment_analysis_results`
  ADD INDEX `idx_first_response_timeout` (`first_response_timeout`);
