-- 为 ai_work_comment_analysis_results 表添加 analysis_note 字段
-- 执行日期: 2025-08-18
-- 目的: 支持分析备注信息（如：评论为空、LLM分析失败等）

ALTER TABLE ai_work_comment_analysis_results 
ADD COLUMN analysis_note varchar(255) DEFAULT NULL COMMENT '分析备注（如：评论为空、LLM分析失败等）';

-- 验证字段是否添加成功
-- DESCRIBE ai_work_comment_analysis_results;
