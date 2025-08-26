-- 为任务执行记录表添加任务配置关联字段
ALTER TABLE ai_task_execution_records 
ADD COLUMN task_config_key VARCHAR(100) NULL 
COMMENT '关联的任务配置键（对应ai_task_configs.task_key）' 
AFTER task_type;

-- 添加索引
ALTER TABLE ai_task_execution_records 
ADD INDEX idx_task_config_key (task_config_key);

-- 更新现有记录，根据任务类型映射到任务配置键
UPDATE ai_task_execution_records 
SET task_config_key = CASE 
    WHEN task_type IN ('batch_analysis', 'manual_analysis') THEN 'customer_service_analysis'
    WHEN task_type = 'cleanup' THEN 'system_cleanup'
    ELSE NULL
END
WHERE task_config_key IS NULL;

-- 添加外键约束（可选，如果需要严格的数据一致性）
-- ALTER TABLE ai_task_execution_records 
-- ADD CONSTRAINT fk_task_config 
-- FOREIGN KEY (task_config_key) REFERENCES ai_task_configs(task_key) 
-- ON DELETE SET NULL ON UPDATE CASCADE;
