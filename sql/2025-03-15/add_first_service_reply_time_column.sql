-- 新增客服首次回复时间字段
ALTER TABLE `ai_work_comment_analysis_results`
  ADD COLUMN `first_service_reply_time` datetime DEFAULT NULL COMMENT '客服首次回复时间' AFTER `first_customer_message_time`;
