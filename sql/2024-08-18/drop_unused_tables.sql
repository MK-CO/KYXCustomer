-- 删除不需要的数据表
-- 执行前请确保已备份相关数据

-- 删除会话分析结果表（已由工单分析结果表替代）
DROP TABLE IF EXISTS `ai_session_analysis_results`;

-- 删除会话评论表（已由工单分析结果表替代）
DROP TABLE IF EXISTS `ai_session_comments`;

-- 删除分析执行记录表（不再需要）
DROP TABLE IF EXISTS `ai_analysis_execution_logs`;

-- 删除工单分析执行记录表（如果存在）
DROP TABLE IF EXISTS `ai_work_analysis_execution_logs`;

-- 为工单分析结果表添加analysis_note字段（如果不存在）
ALTER TABLE `ai_work_comment_analysis_results` 
ADD COLUMN `analysis_note` varchar(255) DEFAULT NULL COMMENT '分析备注（如：评论为空、LLM分析失败等）';
