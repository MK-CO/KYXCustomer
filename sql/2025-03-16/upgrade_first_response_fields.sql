-- 一次性升级脚本：添加首次响应相关字段及索引（售后首响检测）
-- 适用表：ai_work_comment_analysis_results

ALTER TABLE `ai_work_comment_analysis_results`
  ADD COLUMN IF NOT EXISTS `first_response_seconds` int(11) DEFAULT NULL COMMENT '首次客服响应耗时(秒)' AFTER `service_comments`,
  ADD COLUMN IF NOT EXISTS `first_response_timeout` tinyint(1) DEFAULT '0' COMMENT '首次响应是否超时' AFTER `first_response_seconds`,
  ADD COLUMN IF NOT EXISTS `first_response_rule_seconds` int(11) DEFAULT '120' COMMENT '首次响应超时阈值(秒)' AFTER `first_response_timeout`,
  ADD COLUMN IF NOT EXISTS `first_customer_message_time` datetime DEFAULT NULL COMMENT '商家首条消息时间' AFTER `first_response_rule_seconds`,
  ADD COLUMN IF NOT EXISTS `first_service_reply_time` datetime DEFAULT NULL COMMENT '客服首次回复时间' AFTER `first_customer_message_time`;

ALTER TABLE `ai_work_comment_analysis_results`
  ADD INDEX IF NOT EXISTS `idx_first_response_timeout` (`first_response_timeout`);
