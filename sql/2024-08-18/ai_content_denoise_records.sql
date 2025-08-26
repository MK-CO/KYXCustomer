-- 创建内容去噪记录表
-- 用于存储工单评论去噪处理的统计信息

DROP TABLE IF EXISTS `ai_content_denoise_records`;

CREATE TABLE `ai_content_denoise_records` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `work_id` bigint(20) NOT NULL COMMENT '工单ID',
  `batch_id` varchar(64) DEFAULT NULL COMMENT '批处理ID，用于标识同一批次处理的工单',
  `original_comment_count` int(11) NOT NULL DEFAULT '0' COMMENT '原始评论数量',
  `filtered_comment_count` int(11) NOT NULL DEFAULT '0' COMMENT '过滤后评论数量',
  `removed_comment_count` int(11) NOT NULL DEFAULT '0' COMMENT '被移除的评论数量',
  `filter_rate` decimal(5,2) NOT NULL DEFAULT '0.00' COMMENT '过滤率（百分比）',
  `filter_reasons` json DEFAULT NULL COMMENT '过滤原因统计（JSON格式）',
  `removed_details` json DEFAULT NULL COMMENT '被移除评论的详细信息（JSON格式，前10条）',
  `processing_time_ms` int(11) DEFAULT NULL COMMENT '处理耗时（毫秒）',
  `denoise_version` varchar(32) DEFAULT 'v1.0' COMMENT '去噪模型版本',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_work_id` (`work_id`),
  KEY `idx_batch_id` (`batch_id`),
  KEY `idx_created_at` (`created_at`),
  KEY `idx_filter_rate` (`filter_rate`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='内容去噪记录表';

-- 创建去噪批次统计表
DROP TABLE IF EXISTS `ai_denoise_batch_statistics`;

CREATE TABLE `ai_denoise_batch_statistics` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `batch_id` varchar(64) NOT NULL COMMENT '批处理ID',
  `total_work_orders` int(11) NOT NULL DEFAULT '0' COMMENT '总工单数',
  `processed_work_orders` int(11) NOT NULL DEFAULT '0' COMMENT '已处理工单数',
  `total_original_comments` int(11) NOT NULL DEFAULT '0' COMMENT '原始评论总数',
  `total_filtered_comments` int(11) NOT NULL DEFAULT '0' COMMENT '过滤后评论总数',
  `total_removed_comments` int(11) NOT NULL DEFAULT '0' COMMENT '移除评论总数',
  `overall_filter_rate` decimal(5,2) NOT NULL DEFAULT '0.00' COMMENT '整体过滤率',
  `global_filter_reasons` json DEFAULT NULL COMMENT '全局过滤原因统计',
  `processing_start_time` timestamp NULL DEFAULT NULL COMMENT '处理开始时间',
  `processing_end_time` timestamp NULL DEFAULT NULL COMMENT '处理结束时间',
  `total_processing_time_ms` int(11) DEFAULT NULL COMMENT '总处理耗时（毫秒）',
  `denoise_version` varchar(32) DEFAULT 'v1.0' COMMENT '去噪模型版本',
  `status` enum('PROCESSING','COMPLETED','FAILED') DEFAULT 'PROCESSING' COMMENT '处理状态',
  `error_message` text DEFAULT NULL COMMENT '错误信息',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_batch_id` (`batch_id`),
  KEY `idx_status` (`status`),
  KEY `idx_created_at` (`created_at`),
  KEY `idx_overall_filter_rate` (`overall_filter_rate`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='去噪批次统计表';

-- 插入示例数据（可选）
-- INSERT INTO `ai_denoise_batch_statistics` (
--   `batch_id`, `total_work_orders`, `processed_work_orders`, 
--   `total_original_comments`, `total_filtered_comments`, `total_removed_comments`,
--   `overall_filter_rate`, `status`
-- ) VALUES (
--   'batch_20250115_001', 100, 100, 
--   1250, 980, 270,
--   21.60, 'COMPLETED'
-- );
