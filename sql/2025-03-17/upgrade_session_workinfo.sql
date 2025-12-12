-- 升级 ai_work_comment_analysis_results，补充会话与工单信息
-- 兼容低版本 MySQL：不使用 IF NOT EXISTS，请只执行一次，或先检查列是否存在

-- 添加工单创建时间
ALTER TABLE `ai_work_comment_analysis_results`
  ADD COLUMN `work_create_time` datetime DEFAULT NULL COMMENT '工单创建时间' AFTER `sentiment_intensity`;

-- 添加首次客服回复姓名
ALTER TABLE `ai_work_comment_analysis_results`
  ADD COLUMN `first_service_reply_name` varchar(100) DEFAULT NULL COMMENT '首次客服回复姓名' AFTER `first_service_reply_time`;

-- session_id 字段已存在，如需确保非空可自行更新。
