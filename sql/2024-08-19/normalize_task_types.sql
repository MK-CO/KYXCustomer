-- 统一任务类型命名，与任务配置保持一致

-- 首先为执行记录表添加task_config_key字段（如果还没有的话）
ALTER TABLE ai_task_execution_records 
ADD COLUMN IF NOT EXISTS task_config_key VARCHAR(100) NULL 
COMMENT '关联的任务配置键（对应ai_task_configs.task_key）' 
AFTER task_type;

-- 添加索引（如果还没有的话）
ALTER TABLE ai_task_execution_records 
ADD INDEX IF NOT EXISTS idx_task_config_key (task_config_key);

-- 统一任务类型和配置键的映射
UPDATE ai_task_execution_records 
SET 
    task_config_key = CASE 
        WHEN task_type IN ('batch_analysis', 'manual_analysis', 'full_task', 'full_task_range') THEN 'customer_service_analysis'
        WHEN task_type IN ('cleanup', 'manual_cleanup') THEN 'system_cleanup'
        WHEN task_type = 'manual_extraction' THEN 'customer_service_analysis'
        ELSE task_config_key
    END,
    task_type = CASE 
        WHEN task_type IN ('full_task', 'full_task_range') THEN 'batch_analysis'
        WHEN task_type = 'manual_cleanup' THEN 'cleanup'
        ELSE task_type
    END
WHERE task_config_key IS NULL OR task_type IN ('full_task', 'full_task_range', 'manual_cleanup');

-- 显示更新结果
SELECT 
    task_config_key,
    task_type,
    COUNT(*) as record_count,
    MIN(start_time) as earliest_record,
    MAX(start_time) as latest_record
FROM ai_task_execution_records 
GROUP BY task_config_key, task_type 
ORDER BY task_config_key, task_type;
