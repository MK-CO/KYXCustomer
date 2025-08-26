-- 任务配置表
CREATE TABLE IF NOT EXISTS ai_task_configs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '主键ID',
    task_key VARCHAR(100) NOT NULL UNIQUE COMMENT '任务唯一标识',
    task_name VARCHAR(200) NOT NULL COMMENT '任务名称',
    task_description TEXT COMMENT '任务描述',
    task_type ENUM('scheduled', 'manual', 'both') DEFAULT 'scheduled' COMMENT '任务类型：scheduled-仅定时, manual-仅手动, both-两者都支持',
    is_enabled BOOLEAN DEFAULT TRUE COMMENT '是否启用',
    schedule_interval BIGINT NOT NULL DEFAULT 14400 COMMENT '调度间隔（秒），默认4小时',
    schedule_cron VARCHAR(100) DEFAULT NULL COMMENT 'Cron表达式（可选）',
    max_concurrent INTEGER DEFAULT 1 COMMENT '最大并发数',
    default_batch_size INTEGER DEFAULT 50 COMMENT '默认批次大小',
    task_handler VARCHAR(200) NOT NULL COMMENT '任务处理器类路径',
    task_params JSON COMMENT '任务参数配置',
    priority INTEGER DEFAULT 5 COMMENT '优先级（1-10，数字越小优先级越高）',
    timeout_seconds INTEGER DEFAULT 3600 COMMENT '超时时间（秒）',
    retry_times INTEGER DEFAULT 0 COMMENT '重试次数',
    last_execution_time DATETIME COMMENT '最后执行时间',
    next_execution_time DATETIME COMMENT '下次执行时间',
    execution_count BIGINT DEFAULT 0 COMMENT '执行次数',
    success_count BIGINT DEFAULT 0 COMMENT '成功次数',
    failure_count BIGINT DEFAULT 0 COMMENT '失败次数',
    created_by VARCHAR(100) DEFAULT 'system' COMMENT '创建者',
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    
    INDEX idx_task_key (task_key),
    INDEX idx_is_enabled (is_enabled),
    INDEX idx_task_type (task_type),
    INDEX idx_next_execution (next_execution_time),
    INDEX idx_priority (priority)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='AI任务配置表';

-- 插入默认的客服记录解析任务配置
INSERT INTO ai_task_configs (
    task_key,
    task_name,
    task_description,
    task_type,
    is_enabled,
    schedule_interval,
    max_concurrent,
    default_batch_size,
    task_handler,
    task_params,
    priority,
    timeout_seconds,
    retry_times,
    created_by
) VALUES (
    'customer_service_analysis',
    '客服记录分析任务',
    '定时分析客服工单记录，提取关键信息和情感分析',
    'both',
    TRUE,
    14400,  -- 4小时 = 14400秒
    1,
    50,
    'batch_analysis',
    JSON_OBJECT(
        'description', '定时执行的批量分析任务，遵循先抽取后分析的流程',
        'extraction_limit', 4000,
        'analysis_batch_size', 50,
        'max_concurrent', 5
    ),
    1,
    7200,   -- 2小时超时
    1,
    'system'
) ON DUPLICATE KEY UPDATE
    task_name = VALUES(task_name),
    task_description = VALUES(task_description),
    updated_at = CURRENT_TIMESTAMP;

-- 可以添加更多默认任务配置
INSERT INTO ai_task_configs (
    task_key,
    task_name,
    task_description,
    task_type,
    is_enabled,
    schedule_interval,
    max_concurrent,
    default_batch_size,
    task_handler,
    task_params,
    priority,
    timeout_seconds,
    retry_times,
    created_by
) VALUES (
    'system_cleanup',
    '系统数据清理任务',
    '定时清理过期的数据和日志文件',
    'scheduled',
    TRUE,
    86400,  -- 24小时
    1,
    100,
    'cleanup',
    JSON_OBJECT(
        'cleanup_days', 30,
        'cleanup_tables', JSON_ARRAY('ai_work_pending_analysis', 'ai_task_execution_records')
    ),
    5,
    1800,   -- 30分钟超时
    2,
    'system'
) ON DUPLICATE KEY UPDATE
    task_name = VALUES(task_name),
    task_description = VALUES(task_description),
    updated_at = CURRENT_TIMESTAMP;
