-- 去噪模式配置数据
-- 创建日期：2024-12-30
-- 用途：插入去噪过滤器的配置数据

-- 插入正常操作模式配置
INSERT INTO `ai_denoise_patterns` (`pattern_name`, `pattern_type`, `pattern_value`, `description`, `action`, `sort_order`) VALUES
-- 正常操作模式
('工单关闭操作', 'normal_operation', '【完结】.*?关闭工单', '工单正常关闭操作', 'filter_out', 1),
('自动完结工单', 'normal_operation', '【自动完结工单】.*?(已撤单|订单已派单|已完成|已关闭|自动关闭|系统关闭)', '系统自动完结工单操作', 'filter_out', 2),
('自动完结通知', 'normal_operation', '【自动完结工单】.*', '系统自动完结通知', 'filter_out', 3),
('系统状态更新', 'normal_operation', '【.*?】.*?(状态|更新|变更)', '系统自动状态更新', 'filter_out', 4),
('工单创建通知', 'normal_operation', '工单.*?(创建|提交|生成)', '工单创建系统通知', 'filter_out', 5),
('自动分配通知', 'normal_operation', '(自动分配|系统分配|已分配给)', '系统自动分配通知', 'filter_out', 6),
('催单提醒', 'normal_operation', '(催单|提醒|超时)', '系统催单提醒', 'filter_out', 7),
('订单状态变更', 'normal_operation', '.*?(已撤单|订单已派单|已派单|订单状态|派单成功|撤单成功)', '订单状态自动变更通知', 'filter_out', 8);

-- 插入无效数据模式配置  
INSERT INTO `ai_denoise_patterns` (`pattern_name`, `pattern_type`, `pattern_value`, `description`, `action`, `sort_order`) VALUES
-- 无效数据模式
('重复数字', 'invalid_data', '^(\d)\1{2,}$', '匹配 111, 222, 1111 等重复数字串', 'filter_out', 1),
('单字符重复', 'invalid_data', '^(.)\1{2,}$', '匹配 aaa, BBB 等重复单字符', 'filter_out', 2),
('纯数字短内容', 'invalid_data', '^[\d\s]{1,5}$', '1-5位纯数字内容', 'filter_out', 3),
('测试内容', 'invalid_data', '^(test|测试|TEST|Test|\.\.\.|。。。)$', '明显的测试内容', 'filter_out', 4),
('空白或符号', 'invalid_data', '^[\s\-_=+\*\.]{1,10}$', '只含空白字符或简单符号', 'filter_out', 5),
('意义不明的短内容', 'invalid_data', '^[a-zA-Z]{1,3}$', '1-3个字母的过短无意义内容', 'filter_out', 6);

-- 插入系统关键词配置
INSERT INTO `ai_denoise_patterns` (`pattern_name`, `pattern_type`, `pattern_value`, `description`, `action`, `sort_order`) VALUES
-- 系统关键词
('系统关键词_系统', 'system_keyword', '系统', '系统相关关键词', 'filter_out', 1),
('系统关键词_自动', 'system_keyword', '自动', '自动相关关键词', 'filter_out', 2),
('系统关键词_通知', 'system_keyword', '通知', '通知相关关键词', 'filter_out', 3),
('系统关键词_提醒', 'system_keyword', '提醒', '提醒相关关键词', 'filter_out', 4),
('系统关键词_分配', 'system_keyword', '分配', '分配相关关键词', 'filter_out', 5),
('系统关键词_转派', 'system_keyword', '转派', '转派相关关键词', 'filter_out', 6),
('系统关键词_完结', 'system_keyword', '【完结】', '完结状态关键词', 'filter_out', 7),
('系统关键词_处理中', 'system_keyword', '【处理中】', '处理中状态关键词', 'filter_out', 8),
('系统关键词_待处理', 'system_keyword', '【待处理】', '待处理状态关键词', 'filter_out', 9),
('系统关键词_已分配', 'system_keyword', '【已分配】', '已分配状态关键词', 'filter_out', 10),
('系统关键词_自动完结工单', 'system_keyword', '【自动完结工单】', '自动完结工单关键词', 'filter_out', 11),
('系统关键词_工单创建', 'system_keyword', '工单创建', '工单创建关键词', 'filter_out', 12),
('系统关键词_工单关闭', 'system_keyword', '工单关闭', '工单关闭关键词', 'filter_out', 13),
('系统关键词_状态变更', 'system_keyword', '状态变更', '状态变更关键词', 'filter_out', 14),
('系统关键词_优先级调整', 'system_keyword', '优先级调整', '优先级调整关键词', 'filter_out', 15),
('系统关键词_已撤单', 'system_keyword', '已撤单', '已撤单关键词', 'filter_out', 16),
('系统关键词_订单已派单', 'system_keyword', '订单已派单', '订单已派单关键词', 'filter_out', 17),
('系统关键词_派单成功', 'system_keyword', '派单成功', '派单成功关键词', 'filter_out', 18),
('系统关键词_撤单成功', 'system_keyword', '撤单成功', '撤单成功关键词', 'filter_out', 19),
('系统关键词_订单状态', 'system_keyword', '订单状态', '订单状态关键词', 'filter_out', 20);
