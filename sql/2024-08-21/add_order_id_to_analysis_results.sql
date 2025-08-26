-- 为工单回复分析结果表添加订单ID字段
-- 从t_work_order_2025表中匹配工单ID和订单ID
-- 注意：工单表按年份分表，当前示例使用2025年的表

-- 1. 添加order_id列
ALTER TABLE `ai_work_comment_analysis_results` 
ADD COLUMN `order_id` bigint(20) DEFAULT NULL COMMENT '订单ID（从工单表关联获取）' 
AFTER `work_id`;

-- 2. 为order_id列添加索引以提高查询性能
ALTER TABLE `ai_work_comment_analysis_results` 
ADD INDEX `idx_order_id_analysis` (`order_id`);

-- 3. 更新现有记录的order_id（从t_work_order_2025表匹配）
-- 注意：这里使用LEFT JOIN确保即使匹配不到也不会丢失分析结果记录
UPDATE `ai_work_comment_analysis_results` ar 
LEFT JOIN `t_work_order_2025` wo ON ar.work_id = wo.id 
SET ar.order_id = wo.order_id 
WHERE ar.order_id IS NULL;

-- 4. 验证更新结果
SELECT 
    COUNT(*) as total_records,
    COUNT(order_id) as records_with_order_id,
    COUNT(*) - COUNT(order_id) as records_without_order_id
FROM `ai_work_comment_analysis_results`;
