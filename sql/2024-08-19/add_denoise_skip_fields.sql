-- 添加去燥和跳过记录字段到任务执行记录表
-- 用于更准确地统计任务执行结果

ALTER TABLE `ai_task_execution_records` 
ADD COLUMN `skipped_records` int(11) DEFAULT 0 COMMENT '跳过的记录数（去燥、重复等）' AFTER `failed_records`,
ADD COLUMN `denoised_records` int(11) DEFAULT 0 COMMENT '去燥处理的记录数' AFTER `skipped_records`,
ADD COLUMN `duplicate_records` int(11) DEFAULT 0 COMMENT '重复跳过的记录数' AFTER `denoised_records`;

-- 更新现有记录，计算跳过的记录数（基于总数-成功-失败的差值）
UPDATE `ai_task_execution_records` 
SET 
    `skipped_records` = GREATEST(0, COALESCE(`total_records`, 0) - COALESCE(`success_records`, 0) - COALESCE(`failed_records`, 0)),
    `denoised_records` = 0,
    `duplicate_records` = 0
WHERE 
    `skipped_records` IS NULL 
    AND `total_records` > 0 
    AND (`success_records` + `failed_records`) < `total_records`;

-- 添加索引优化查询性能
CREATE INDEX `idx_records_stats` ON `ai_task_execution_records` (`total_records`, `success_records`, `failed_records`, `skipped_records`);

-- 显示更新结果
SELECT 
    task_id,
    task_name,
    total_records,
    success_records,
    failed_records,
    skipped_records,
    (success_records + failed_records + skipped_records) as calculated_total,
    updated_at
FROM `ai_task_execution_records` 
WHERE skipped_records > 0 
ORDER BY updated_at DESC 
LIMIT 10;
