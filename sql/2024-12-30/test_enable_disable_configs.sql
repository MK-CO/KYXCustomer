-- 关键词配置启用/禁用状态测试脚本
-- 创建日期：2024-12-30
-- 用途：测试和验证关键词配置的启用/禁用功能

-- 1. 查看当前所有配置的启用状态
SELECT '=== 分析关键词分类启用状态 ===' as info;
SELECT 
    category_key,
    category_name,
    category_type,
    is_enabled,
    CASE WHEN is_enabled = 1 THEN '启用' ELSE '禁用' END as status,
    sort_order
FROM ai_keyword_categories 
WHERE category_type = 'analysis'
ORDER BY sort_order;

SELECT '=== 分析关键词配置启用状态统计 ===' as info;
SELECT 
    cat.category_name,
    COUNT(*) as total_configs,
    SUM(kc.is_enabled) as enabled_configs,
    COUNT(*) - SUM(kc.is_enabled) as disabled_configs
FROM ai_keyword_configs kc
JOIN ai_keyword_categories cat ON kc.category_id = cat.id
WHERE cat.category_type = 'analysis'
GROUP BY cat.id, cat.category_name
ORDER BY cat.sort_order;

SELECT '=== 去噪模式配置启用状态统计 ===' as info;
SELECT 
    pattern_type,
    COUNT(*) as total_patterns,
    SUM(is_enabled) as enabled_patterns,
    COUNT(*) - SUM(is_enabled) as disabled_patterns
FROM ai_denoise_patterns
GROUP BY pattern_type
ORDER BY pattern_type;

-- 2. 测试禁用某些配置（可选，用于测试）
-- 注意：运行前请备份数据！

-- 禁用一个关键词配置进行测试
-- UPDATE ai_keyword_configs 
-- SET is_enabled = 0 
-- WHERE keyword_value = '撕' AND keyword_type = 'keyword'
-- LIMIT 1;

-- 禁用一个去噪模式进行测试
-- UPDATE ai_denoise_patterns 
-- SET is_enabled = 0 
-- WHERE pattern_name = '测试内容'
-- LIMIT 1;

-- 3. 验证查询是否正确过滤启用状态
SELECT '=== 验证分析关键词查询过滤 ===' as info;
SELECT 
    cat.category_name,
    kc.keyword_type,
    kc.keyword_value,
    kc.is_enabled
FROM ai_keyword_configs kc
JOIN ai_keyword_categories cat ON kc.category_id = cat.id
WHERE cat.category_type = 'analysis' 
AND cat.is_enabled = 1 
AND kc.is_enabled = 1
ORDER BY cat.sort_order, kc.keyword_type, kc.id
LIMIT 10;

SELECT '=== 验证去噪模式查询过滤 ===' as info;
SELECT 
    pattern_type,
    pattern_name,
    pattern_value,
    is_enabled
FROM ai_denoise_patterns
WHERE is_enabled = 1
ORDER BY pattern_type, sort_order
LIMIT 10;

-- 4. 恢复测试数据（如果进行了禁用测试）
-- UPDATE ai_keyword_configs SET is_enabled = 1 WHERE is_enabled = 0;
-- UPDATE ai_denoise_patterns SET is_enabled = 1 WHERE is_enabled = 0;
-- UPDATE ai_keyword_categories SET is_enabled = 1 WHERE is_enabled = 0;

-- 5. 查看配置统计总览
SELECT '=== 配置统计总览 ===' as info;
SELECT 
    '分析关键词分类' as config_type,
    COUNT(*) as total,
    SUM(is_enabled) as enabled,
    COUNT(*) - SUM(is_enabled) as disabled
FROM ai_keyword_categories 
WHERE category_type = 'analysis'

UNION ALL

SELECT 
    '分析关键词配置' as config_type,
    COUNT(*) as total,
    SUM(kc.is_enabled) as enabled,
    COUNT(*) - SUM(kc.is_enabled) as disabled
FROM ai_keyword_configs kc
JOIN ai_keyword_categories cat ON kc.category_id = cat.id
WHERE cat.category_type = 'analysis'

UNION ALL

SELECT 
    '去噪模式配置' as config_type,
    COUNT(*) as total,
    SUM(is_enabled) as enabled,
    COUNT(*) - SUM(is_enabled) as disabled
FROM ai_denoise_patterns;

-- 6. 创建一个临时视图来查看启用配置的详细信息
CREATE OR REPLACE VIEW v_enabled_analysis_config AS
SELECT 
    cat.category_key,
    cat.category_name,
    cat.sort_order as category_order,
    kc.id as config_id,
    kc.keyword_type,
    kc.keyword_value,
    kc.weight,
    kc.risk_level,
    kc.description,
    kc.is_enabled
FROM ai_keyword_configs kc
JOIN ai_keyword_categories cat ON kc.category_id = cat.id
WHERE cat.category_type = 'analysis' 
AND cat.is_enabled = 1 
AND kc.is_enabled = 1
ORDER BY cat.sort_order, kc.keyword_type, kc.id;

-- 查看启用配置的详细信息
SELECT '=== 当前启用的分析配置详情 ===' as info;
SELECT * FROM v_enabled_analysis_config LIMIT 20;

-- 清理临时视图
-- DROP VIEW IF EXISTS v_enabled_analysis_config;
