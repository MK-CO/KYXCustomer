-- 关键词和正则规则配置表结构
-- 创建日期：2024-12-30
-- 用途：将硬编码的关键词和正则表达式改为数据库配置

-- 1. 配置分类表
CREATE TABLE `ai_keyword_categories` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `category_key` varchar(64) NOT NULL COMMENT '分类键名（英文）',
  `category_name` varchar(128) NOT NULL COMMENT '分类中文名称',
  `category_type` varchar(32) NOT NULL COMMENT '配置类型: analysis(分析用), denoise(去噪用)',
  `description` text COMMENT '分类描述',
  `is_enabled` tinyint(1) DEFAULT '1' COMMENT '是否启用',
  `sort_order` int(11) DEFAULT '0' COMMENT '排序权重',
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_category_key` (`category_key`),
  KEY `idx_category_type` (`category_type`),
  KEY `idx_is_enabled` (`is_enabled`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='关键词配置分类表';

-- 2. 关键词配置表
CREATE TABLE `ai_keyword_configs` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `category_id` int(11) NOT NULL COMMENT '分类ID',
  `keyword_type` varchar(32) NOT NULL COMMENT '关键词类型: keyword(关键词), pattern(正则模式), exclusion(排除条件)',
  `keyword_value` text NOT NULL COMMENT '关键词内容或正则表达式',
  `weight` decimal(4,2) DEFAULT '1.00' COMMENT '权重',
  `risk_level` varchar(16) DEFAULT 'medium' COMMENT '风险级别: low/medium/high',
  `description` varchar(255) DEFAULT NULL COMMENT '说明描述',
  `is_enabled` tinyint(1) DEFAULT '1' COMMENT '是否启用',
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  
  PRIMARY KEY (`id`),
  KEY `idx_category_id` (`category_id`),
  KEY `idx_keyword_type` (`keyword_type`),
  KEY `idx_is_enabled` (`is_enabled`),
  KEY `idx_risk_level` (`risk_level`),
  FOREIGN KEY (`category_id`) REFERENCES `ai_keyword_categories` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='关键词配置表';

-- 3. 去噪模式配置表
CREATE TABLE `ai_denoise_patterns` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `pattern_name` varchar(128) NOT NULL COMMENT '模式名称',
  `pattern_type` varchar(32) NOT NULL COMMENT '模式类型: normal_operation(正常操作), invalid_data(无效数据), system_keyword(系统关键词)',
  `pattern_value` text NOT NULL COMMENT '模式内容（正则表达式或关键词）',
  `description` varchar(255) DEFAULT NULL COMMENT '模式描述',
  `action` varchar(32) DEFAULT 'filter_out' COMMENT '动作: filter_out(过滤掉), keep(保留)',
  `is_enabled` tinyint(1) DEFAULT '1' COMMENT '是否启用',
  `sort_order` int(11) DEFAULT '0' COMMENT '排序权重',
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  
  PRIMARY KEY (`id`),
  KEY `idx_pattern_type` (`pattern_type`),
  KEY `idx_is_enabled` (`is_enabled`),
  KEY `idx_sort_order` (`sort_order`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='去噪模式配置表';

-- 4. 配置版本管理表
CREATE TABLE `ai_keyword_config_versions` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `version_name` varchar(64) NOT NULL COMMENT '版本名称',
  `version_description` text COMMENT '版本描述',
  `config_data` longtext COMMENT '配置数据快照(JSON)',
  `is_active` tinyint(1) DEFAULT '0' COMMENT '是否为当前活跃版本',
  `created_by` varchar(64) DEFAULT NULL COMMENT '创建者',
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  
  PRIMARY KEY (`id`),
  KEY `idx_is_active` (`is_active`),
  KEY `idx_created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='关键词配置版本管理表';

-- 插入默认分析配置分类
INSERT INTO `ai_keyword_categories` (`category_key`, `category_name`, `category_type`, `description`, `sort_order`) VALUES
('urgent_urging', '紧急催促', 'analysis', '用于检测客户紧急催促的关键词和模式', 1),
('complaint_dispute', '投诉纠纷', 'analysis', '用于检测投诉纠纷相关的关键词和模式', 2),
('responsibility_shifting', '推卸责任', 'analysis', '用于检测推卸责任行为的关键词和模式', 3),
('delay_processing', '拖延处理', 'analysis', '用于检测拖延处理行为的关键词和模式', 4),
('inappropriate_expression', '不当用词表达', 'analysis', '用于检测不当用词表达的关键词和模式', 5),
('vague_response', '模糊回应', 'analysis', '用于检测模糊回应的关键词和模式', 6);

-- 插入紧急催促类关键词
INSERT INTO `ai_keyword_configs` (`category_id`, `keyword_type`, `keyword_value`, `weight`, `risk_level`, `description`) VALUES
-- 紧急催促关键词
(1, 'keyword', '撕', 0.90, 'high', '强烈催促词汇'),
(1, 'keyword', '催', 0.90, 'high', '催促词汇'),
(1, 'keyword', '紧急', 0.90, 'high', '紧急词汇'),
(1, 'keyword', '加急联系', 0.90, 'high', '加急联系'),
(1, 'keyword', '速度', 0.90, 'high', '催促速度'),
(1, 'keyword', '又来了', 0.90, 'high', '重复催促'),
(1, 'keyword', '怎么样了', 0.90, 'high', '询问进展'),
(1, 'keyword', '有进展了吗', 0.90, 'high', '询问进展'),

-- 紧急催促正则模式
(1, 'pattern', '(催|撕).{0,5}(催|撕)', 0.90, 'high', '连续催促模式'),
(1, 'pattern', '(又|一直).*(催|撕|来了)', 0.90, 'high', '重复催促模式'),
(1, 'pattern', '(怎么样|进展).{0,10}(了|啊|呢|吗)', 0.90, 'high', '询问进展模式'),
(1, 'pattern', '(紧急|加急).*(联系|处理|解决)', 0.90, 'high', '紧急处理模式'),
(1, 'pattern', '(速度|快点).*(处理|解决|搞定)', 0.90, 'high', '催促处理速度'),
(1, 'pattern', '(有|没有).*(进展|结果|消息).*(了|吗|呢)', 0.90, 'high', '询问结果模式');

-- 插入投诉纠纷类关键词
INSERT INTO `ai_keyword_configs` (`category_id`, `keyword_type`, `keyword_value`, `weight`, `risk_level`, `description`) VALUES
-- 投诉纠纷关键词
(2, 'keyword', '纠纷单', 1.20, 'high', '纠纷单'),
(2, 'keyword', '投诉', 1.20, 'high', '投诉'),
(2, 'keyword', '退款了', 1.20, 'high', '退款'),
(2, 'keyword', '结果', 1.20, 'high', '催促结果'),
(2, 'keyword', '12315', 1.20, 'high', '消费者投诉热线'),
(2, 'keyword', '客诉', 1.20, 'high', '客户投诉'),
(2, 'keyword', '翘单', 1.20, 'high', '翘单'),

-- 投诉纠纷正则模式
(2, 'pattern', '(纠纷|投诉).*(单|了|啊|呢)', 1.20, 'high', '纠纷投诉模式'),
(2, 'pattern', '(退款|退钱).*(了|啊|呢)', 1.20, 'high', '退款模式'),
(2, 'pattern', '(客诉|投诉).*12315', 1.20, 'high', '投诉12315模式'),
(2, 'pattern', '(翘单|逃单).{0,10}(了|呢)', 1.20, 'high', '翘单模式'),
(2, 'pattern', '(结果|进展).*(不知道|不清楚|没消息|怎么样)', 1.20, 'high', '催促结果模式'),
(2, 'pattern', '12315.*(投诉|举报|客诉)', 1.20, 'high', '12315投诉模式');

-- 插入推卸责任类关键词
INSERT INTO `ai_keyword_configs` (`category_id`, `keyword_type`, `keyword_value`, `weight`, `risk_level`, `description`) VALUES
-- 推卸责任关键词
(3, 'keyword', '不是我们的问题', 1.00, 'high', '推卸责任表达'),
(3, 'keyword', '不是我们负责', 1.00, 'high', '推卸责任表达'),
(3, 'keyword', '不关我们事', 1.00, 'high', '推卸责任表达'),
(3, 'keyword', '找其他部门', 1.00, 'high', '推卸责任表达'),
(3, 'keyword', '联系供应商', 1.00, 'high', '推卸责任表达'),
(3, 'keyword', '厂家问题', 1.00, 'high', '推卸责任表达'),
(3, 'keyword', '配件问题', 1.00, 'high', '推卸责任表达'),
(3, 'keyword', '找师傅', 1.00, 'high', '推卸责任表达'),
(3, 'keyword', '师傅负责', 1.00, 'high', '推卸责任表达'),
(3, 'keyword', '找安装师傅', 1.00, 'high', '推卸责任表达'),
(3, 'keyword', '不是门店责任', 1.00, 'high', '推卸责任表达'),
(3, 'keyword', '这是厂家的', 1.00, 'high', '推卸责任表达'),
(3, 'keyword', '原厂保修', 1.00, 'high', '推卸责任表达'),
(3, 'keyword', '找4S店', 1.00, 'high', '推卸责任表达'),
(3, 'keyword', '不归我们管', 1.00, 'high', '推卸责任表达'),
(3, 'keyword', '系统问题', 1.00, 'high', '推卸责任表达'),
(3, 'keyword', '总部决定', 1.00, 'high', '推卸责任表达'),
(3, 'keyword', '没办法', 1.00, 'high', '推卸责任表达'),
(3, 'keyword', '无能为力', 1.00, 'high', '推卸责任表达'),
(3, 'keyword', '爱莫能助', 1.00, 'high', '推卸责任表达'),
(3, 'keyword', '无可奈何', 1.00, 'high', '推卸责任表达'),
(3, 'keyword', '我们也很无奈', 1.00, 'high', '推卸责任表达'),

-- 推卸责任正则模式
(3, 'pattern', '(不是|不属于).*(我们|门店|本店).*(问题|责任|负责)', 1.00, 'high', '推卸责任模式'),
(3, 'pattern', '(这是|属于).*(厂家|师傅|供应商|原厂).*(问题|责任)', 1.00, 'high', '推卸责任模式'),
(3, 'pattern', '(找|联系|去问).*(师傅|厂家|供应商|4S店|原厂)', 1.00, 'high', '推卸责任模式'),
(3, 'pattern', '(师傅|安装师傅).*(自己|负责|承担).*(责任|问题)', 1.00, 'high', '推卸责任模式'),
(3, 'pattern', '(配件|产品).*(质量|问题).*找.*(厂家|供应商)', 1.00, 'high', '推卸责任模式'),
(3, 'pattern', '(贴膜|安装|维修).*(问题|效果).*找.*(师傅|技师)', 1.00, 'high', '推卸责任模式'),
(3, 'pattern', '(保修|售后).*找.*(原厂|4S店|厂家)', 1.00, 'high', '推卸责任模式'),
(3, 'pattern', '(没办法|无能为力|爱莫能助|无可奈何).*解决', 1.00, 'high', '推卸责任模式'),
(3, 'pattern', '这个.*不归.*(我们|门店).*管', 1.00, 'high', '推卸责任模式');

-- 插入拖延处理类关键词
INSERT INTO `ai_keyword_configs` (`category_id`, `keyword_type`, `keyword_value`, `weight`, `risk_level`, `description`) VALUES
-- 拖延处理关键词
(4, 'keyword', '翘单', 1.10, 'high', '翘单'),
(4, 'keyword', '逃单', 1.10, 'high', '逃单'),
(4, 'keyword', '一直拖', 1.10, 'high', '一直拖延'),
(4, 'keyword', '故意拖', 1.10, 'high', '故意拖延'),
(4, 'keyword', '拖着不处理', 1.10, 'high', '拖着不处理'),
(4, 'keyword', '不想处理', 1.10, 'high', '不想处理'),

-- 拖延处理正则模式
(4, 'pattern', '(翘单|逃单).{0,10}(了|呢)(?![^，。！？；]*[处理解决完成])', 1.10, 'high', '翘单逃单模式'),
(4, 'pattern', '(拖着|一直拖|故意拖).*(不处理|不解决)', 1.10, 'high', '故意拖延模式'),
(4, 'pattern', '(不想|不愿意).*(处理|解决|管)', 1.10, 'high', '不想处理模式'),
(4, 'pattern', '(能拖|继续拖).*(就拖|一天)', 1.10, 'high', '能拖就拖模式');

-- 插入不当用词表达类关键词
INSERT INTO `ai_keyword_configs` (`category_id`, `keyword_type`, `keyword_value`, `weight`, `risk_level`, `description`) VALUES
-- 不当用词表达关键词
(5, 'keyword', '搞快点', 0.80, 'medium', '不当用词'),
(5, 'keyword', '快点搞', 0.80, 'medium', '不当用词'),
(5, 'keyword', '急死了', 0.80, 'medium', '不当用词'),
(5, 'keyword', '催死了', 0.80, 'medium', '不当用词'),
(5, 'keyword', '烦死了', 0.80, 'medium', '不当用词'),
(5, 'keyword', '撕', 0.80, 'medium', '不当用词'),
(5, 'keyword', '赶紧搞', 0.80, 'medium', '不当用词'),
(5, 'keyword', '搞定', 0.80, 'medium', '不当用词'),
(5, 'keyword', '又来催', 0.80, 'medium', '不当用词'),
(5, 'keyword', '车主烦人', 0.80, 'medium', '不当用词'),
(5, 'keyword', '师傅拖拉', 0.80, 'medium', '不当用词'),

-- 不当用词表达正则模式
(5, 'pattern', '(搞|弄).*(快|定|好)', 0.80, 'medium', '不当用词模式'),
(5, 'pattern', '(急|催|烦|撕).*(死了|要命)', 0.80, 'medium', '不当用词模式'),
(5, 'pattern', '(又|一直).*(催|撕|来了)', 0.80, 'medium', '不当用词模式'),
(5, 'pattern', '(车主|客户).*(烦人|烦死|麻烦死)', 0.80, 'medium', '不当用词模式'),
(5, 'pattern', '(师傅|技师).*(拖拉|磨叽|慢吞吞|烦人)', 0.80, 'medium', '不当用词模式'),
(5, 'pattern', '(赶紧|快点).*(搞|弄|处理)', 0.80, 'medium', '不当用词模式');

-- 插入模糊回应类关键词
INSERT INTO `ai_keyword_configs` (`category_id`, `keyword_type`, `keyword_value`, `weight`, `risk_level`, `description`) VALUES
-- 模糊回应关键词
(6, 'keyword', '需要时间', 0.60, 'medium', '模糊回应'),
(6, 'keyword', '耐心等待', 0.60, 'medium', '模糊回应'),
(6, 'keyword', '已经在处理', 0.60, 'medium', '模糊回应'),
(6, 'keyword', '尽快联系', 0.60, 'medium', '模糊回应'),
(6, 'keyword', '正在处理中', 0.60, 'medium', '模糊回应'),
(6, 'keyword', '会尽快', 0.60, 'medium', '模糊回应'),
(6, 'keyword', '稍等一下', 0.60, 'medium', '模糊回应'),
(6, 'keyword', '马上处理', 0.60, 'medium', '模糊回应'),

-- 模糊回应正则模式
(6, 'pattern', '(这个|这种).*(需要时间|要等)(?![^，。！？；]*[具体时间|明确|预计])', 0.60, 'medium', '模糊时间回应'),
(6, 'pattern', '(已经在|正在).*(处理|跟进)(?![^，。！？；]*[具体|详细|明确])', 0.60, 'medium', '模糊处理回应'),
(6, 'pattern', '(会|将).*(尽快|马上)(?![^，。！？；]*[今天|明天|\d+小时])', 0.60, 'medium', '模糊时间承诺'),
(6, 'pattern', '(请|您).*(耐心|稍等)(?![^，。！？；]*[\d+分钟|\d+小时])', 0.60, 'medium', '模糊等待回应'),

-- 模糊回应排除条件
(6, 'exclusion', '(预计|大概|估计).*(时间|小时|分钟|天)', 0.60, 'medium', '排除有具体时间预估的回应'),
(6, 'exclusion', '(具体|详细).*(时间|进度)', 0.60, 'medium', '排除有具体进度说明的回应'),
(6, 'exclusion', '(\d+).*(小时|分钟|天).*内', 0.60, 'medium', '排除有具体时间承诺的回应'),
(6, 'exclusion', '(今天|明天|本周).*(完成|处理)', 0.60, 'medium', '排除有具体完成时间的回应');
