-- 为工单回复分析结果表添加订单编号字段
-- 从t_work_order_2025表中匹配工单ID获取订单ID和订单编号

-- 1. 添加order_no列
ALTER TABLE `ai_work_comment_analysis_results` 
ADD COLUMN `order_no` varchar(100) DEFAULT NULL COMMENT '订单编号（从工单表关联获取）' 
AFTER `order_id`;

-- 2. 为order_no列添加索引以提高查询性能
ALTER TABLE `ai_work_comment_analysis_results` 
ADD INDEX `idx_order_no_analysis` (`order_no`);

-- 3. 更新现有记录的order_no（从t_work_order_2025表匹配）
-- 注意：这里使用LEFT JOIN确保即使匹配不到也不会丢失分析结果记录
UPDATE `ai_work_comment_analysis_results` ar 
LEFT JOIN `t_work_order_2025` wo ON ar.work_id = wo.id 
SET ar.order_no = wo.order_no 
WHERE ar.order_no IS NULL;

-- 4. 验证更新结果
SELECT 
    COUNT(*) as total_records,
    COUNT(order_id) as records_with_order_id,
    COUNT(order_no) as records_with_order_no,
    COUNT(*) - COUNT(order_id) as records_without_order_id,
    COUNT(*) - COUNT(order_no) as records_without_order_no
FROM `ai_work_comment_analysis_results`;
