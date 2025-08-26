-- APScheduler数据库表创建脚本
-- 用于存储APScheduler的作业信息

-- 创建APScheduler作业表
CREATE TABLE IF NOT EXISTS apscheduler_jobs (
    id VARCHAR(191) NOT NULL PRIMARY KEY,
    next_run_time DOUBLE(25,6) NULL,
    job_state LONGBLOB NOT NULL,
    
    INDEX ix_apscheduler_jobs_next_run_time (next_run_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 插入APScheduler表创建记录
INSERT INTO db_migration_log (script_name, executed_at, description) 
VALUES ('create_apscheduler_table.sql', NOW(), 'APScheduler作业表创建，支持调度器持久化存储');
