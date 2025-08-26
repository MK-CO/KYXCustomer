-- AI任务执行记录表
CREATE TABLE IF NOT EXISTS `ai_task_execution_records` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '记录ID',
  `task_id` varchar(64) NOT NULL COMMENT '任务唯一标识符',
  `task_name` varchar(100) NOT NULL COMMENT '任务名称',
  `task_type` varchar(50) NOT NULL COMMENT '任务类型 (batch_analysis, work_extraction, cleanup, manual)',
  `trigger_type` varchar(20) NOT NULL DEFAULT 'scheduled' COMMENT '触发类型 (scheduled, manual)',
  `trigger_user` varchar(50) DEFAULT NULL COMMENT '手动触发用户（手动执行时）',
  `start_time` datetime NOT NULL COMMENT '任务开始时间',
  `end_time` datetime DEFAULT NULL COMMENT '任务结束时间',
  `duration_seconds` int(11) DEFAULT NULL COMMENT '执行时长（秒）',
  `status` varchar(20) NOT NULL DEFAULT 'running' COMMENT '任务状态 (running, completed, failed, cancelled)',
  `process_stage` varchar(50) DEFAULT NULL COMMENT '当前处理阶段',
  `total_records` int(11) DEFAULT 0 COMMENT '总处理记录数',
  `processed_records` int(11) DEFAULT 0 COMMENT '已处理记录数',
  `success_records` int(11) DEFAULT 0 COMMENT '成功处理记录数',
  `failed_records` int(11) DEFAULT 0 COMMENT '失败处理记录数',
  `extracted_records` int(11) DEFAULT 0 COMMENT '抽取记录数（工单抽取任务）',
  `analyzed_records` int(11) DEFAULT 0 COMMENT '分析记录数（分析任务）',
  `batch_size` int(11) DEFAULT NULL COMMENT '批次大小',
  `max_concurrent` int(11) DEFAULT NULL COMMENT '最大并发数',
  `error_message` text COMMENT '错误信息',
  `execution_details` json DEFAULT NULL COMMENT '执行详情（JSON格式）',
  `performance_stats` json DEFAULT NULL COMMENT '性能统计（JSON格式）',
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_task_id` (`task_id`),
  INDEX `idx_task_type_status` (`task_type`, `status`),
  INDEX `idx_start_time` (`start_time`),
  INDEX `idx_status` (`status`),
  INDEX `idx_trigger_type` (`trigger_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='AI任务执行记录表';

-- 插入示例数据（可选）
INSERT INTO `ai_task_execution_records` (
  `task_id`, `task_name`, `task_type`, `trigger_type`, `start_time`, `end_time`, 
  `duration_seconds`, `status`, `total_records`, `success_records`, `execution_details`
) VALUES 
(
  'BATCH_20241201_120000_abc123', 
  '定时批量分析任务', 
  'batch_analysis', 
  'scheduled', 
  '2024-12-01 12:00:00', 
  '2024-12-01 12:15:30', 
  930, 
  'completed', 
  4000, 
  3850, 
  JSON_OBJECT(
    'extraction_statistics', JSON_OBJECT('extracted', 4000, 'inserted', 4000),
    'analysis_statistics', JSON_OBJECT('successful_analyses', 3850, 'failed_analyses', 150),
    'message', '定时任务执行完成'
  )
),
(
  'MANUAL_20241201_140000_def456', 
  '手动执行分析任务', 
  'batch_analysis', 
  'manual', 
  '2024-12-01 14:00:00', 
  '2024-12-01 14:08:45', 
  525, 
  'completed', 
  500, 
  485, 
  JSON_OBJECT(
    'trigger_user', 'admin',
    'analysis_statistics', JSON_OBJECT('successful_analyses', 485, 'failed_analyses', 15),
    'message', '手动分析完成'
  )
);
