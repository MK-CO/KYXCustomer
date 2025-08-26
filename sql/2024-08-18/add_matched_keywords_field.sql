-- 为ai_work_comment_analysis_results表添加matched_keywords字段
-- 用于存储匹配到的具体关键词详情

ALTER TABLE `ai_work_comment_analysis_results` 
ADD COLUMN `matched_keywords` text DEFAULT NULL COMMENT '匹配的关键词详情(JSON)' 
AFTER `matched_categories`;

-- 为新字段添加注释说明
ALTER TABLE `ai_work_comment_analysis_results` 
MODIFY COLUMN `matched_keywords` text DEFAULT NULL COMMENT '匹配的关键词详情(JSON格式): {"推卸责任": {"keywords": ["撕", "催"], "patterns": ["催.*了"]}, "拖延处理": {"keywords": ["速度"], "patterns": []}}';
