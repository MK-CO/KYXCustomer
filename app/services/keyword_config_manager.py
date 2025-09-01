"""
关键词配置管理器
用于管理数据库中的关键词和正则规则配置
"""
import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime

logger = logging.getLogger(__name__)


class KeywordConfigManager:
    """关键词配置管理器"""
    
    def __init__(self):
        """初始化配置管理器"""
        self._cache = {}
        self._cache_timestamp = None
        self._cache_ttl = 300  # 缓存5分钟

    def _is_cache_valid(self) -> bool:
        """检查缓存是否有效"""
        if not self._cache or not self._cache_timestamp:
            return False
        
        time_diff = (datetime.now() - self._cache_timestamp).total_seconds()
        return time_diff < self._cache_ttl

    def _update_cache(self, config_data: Dict[str, Any]):
        """更新缓存"""
        self._cache = config_data
        self._cache_timestamp = datetime.now()

    def get_analysis_keywords_config(self, db: Session, use_cache: bool = True) -> Dict[str, Dict[str, Any]]:
        """
        获取分析用关键词配置（仅返回启用的配置）
        
        Args:
            db: 数据库会话
            use_cache: 是否使用缓存
            
        Returns:
            分析关键词配置字典
        """
        if use_cache and self._is_cache_valid() and 'analysis' in self._cache:
            logger.debug("使用缓存的分析关键词配置")
            return self._cache['analysis']

        logger.info("从数据库加载分析关键词配置")
        
        try:
            # 查询所有分析类型的配置分类
            sql = """
            SELECT 
                c.id as category_id,
                c.category_key,
                c.category_name,
                c.description,
                c.sort_order
            FROM ai_keyword_categories c
            WHERE c.category_type = 'analysis' 
            AND c.is_enabled = 1
            ORDER BY c.sort_order
            """
            
            categories = db.execute(text(sql)).fetchall()
            
            if not categories:
                logger.warning("未找到启用的分析配置分类")
                return {}
            
            config = {}
            
            for category in categories:
                category_id = category.category_id
                category_key = category.category_key
                category_name = category.category_name
                
                # 查询该分类下的关键词配置
                keyword_sql = """
                SELECT 
                    id,
                    keyword_type,
                    keyword_value,
                    weight,
                    risk_level,
                    description
                FROM ai_keyword_configs 
                WHERE category_id = :category_id 
                AND is_enabled = 1
                ORDER BY id
                """
                
                keywords = db.execute(text(keyword_sql), {"category_id": category_id}).fetchall()
                
                # 分类组织关键词
                keywords_list = []
                patterns_list = []
                exclusions_list = []
                weight = 1.0
                risk_level = "medium"
                
                for keyword in keywords:
                    if keyword.keyword_type == 'keyword':
                        keywords_list.append(keyword.keyword_value)
                    elif keyword.keyword_type == 'pattern':
                        patterns_list.append(keyword.keyword_value)
                    elif keyword.keyword_type == 'exclusion':
                        exclusions_list.append(keyword.keyword_value)
                    
                    # 使用第一个关键词的权重和风险级别作为分类的默认值
                    if weight == 1.0:
                        weight = float(keyword.weight) if keyword.weight else 1.0
                    if risk_level == "medium":
                        risk_level = keyword.risk_level or "medium"
                
                # 构建配置项
                category_config = {
                    "keywords": keywords_list,
                    "patterns": patterns_list,
                    "weight": weight,
                    "risk_level": risk_level
                }
                
                # 添加排除条件（如果有）
                if exclusions_list:
                    category_config["exclusions"] = exclusions_list
                
                config[category_name] = category_config
                
                logger.debug(f"加载分类 {category_name}: {len(keywords_list)} 个关键词, {len(patterns_list)} 个模式")
            
            # 更新缓存
            if use_cache:
                cache_data = self._cache.copy() if self._cache else {}
                cache_data['analysis'] = config
                self._update_cache(cache_data)
            
            logger.info(f"成功加载 {len(config)} 个分析关键词配置分类")
            return config
            
        except Exception as e:
            logger.error(f"加载分析关键词配置失败: {e}")
            # 返回空配置而不是抛出异常，确保系统稳定运行
            return {}

    def get_denoise_patterns(self, db: Session, pattern_type: str = None, use_cache: bool = True) -> List[Dict[str, Any]]:
        """
        获取去噪模式配置
        
        Args:
            db: 数据库会话
            pattern_type: 模式类型过滤 (normal_operation, invalid_data, system_keyword)
            use_cache: 是否使用缓存
            
        Returns:
            去噪模式配置列表
        """
        cache_key = f"denoise_{pattern_type or 'all'}"
        
        if use_cache and self._is_cache_valid() and cache_key in self._cache:
            logger.debug(f"使用缓存的去噪模式配置: {pattern_type}")
            return self._cache[cache_key]

        logger.info(f"从数据库加载去噪模式配置: {pattern_type}")
        
        try:
            sql = """
            SELECT 
                pattern_name,
                pattern_type,
                pattern_value,
                description,
                action,
                sort_order
            FROM ai_denoise_patterns 
            WHERE is_enabled = 1
            """
            
            params = {}
            if pattern_type:
                sql += " AND pattern_type = :pattern_type"
                params["pattern_type"] = pattern_type
            
            sql += " ORDER BY sort_order, id"
            
            patterns = db.execute(text(sql), params).fetchall()
            
            pattern_list = []
            for pattern in patterns:
                pattern_dict = {
                    "name": pattern.pattern_name,
                    "pattern": pattern.pattern_value,
                    "description": pattern.description,
                    "action": pattern.action,
                    "pattern_type": pattern.pattern_type
                }
                pattern_list.append(pattern_dict)
            
            # 更新缓存
            if use_cache:
                cache_data = self._cache.copy() if self._cache else {}
                cache_data[cache_key] = pattern_list
                self._update_cache(cache_data)
            
            logger.info(f"成功加载 {len(pattern_list)} 个去噪模式配置")
            return pattern_list
            
        except Exception as e:
            logger.error(f"加载去噪模式配置失败: {e}")
            return []

    def get_system_keywords(self, db: Session, use_cache: bool = True) -> List[str]:
        """
        获取系统关键词列表
        
        Args:
            db: 数据库会话
            use_cache: 是否使用缓存
            
        Returns:
            系统关键词列表
        """
        if use_cache and self._is_cache_valid() and 'system_keywords' in self._cache:
            logger.debug("使用缓存的系统关键词")
            return self._cache['system_keywords']

        logger.info("从数据库加载系统关键词")
        
        try:
            sql = """
            SELECT pattern_value
            FROM ai_denoise_patterns 
            WHERE pattern_type = 'system_keyword' 
            AND is_enabled = 1
            ORDER BY sort_order, id
            """
            
            keywords = db.execute(text(sql)).fetchall()
            keyword_list = [keyword.pattern_value for keyword in keywords]
            
            # 更新缓存
            if use_cache:
                cache_data = self._cache.copy() if self._cache else {}
                cache_data['system_keywords'] = keyword_list
                self._update_cache(cache_data)
            
            logger.info(f"成功加载 {len(keyword_list)} 个系统关键词")
            return keyword_list
            
        except Exception as e:
            logger.error(f"加载系统关键词失败: {e}")
            return []

    def clear_cache(self):
        """清空缓存"""
        self._cache = {}
        self._cache_timestamp = None
        logger.info("关键词配置缓存已清空")

    def reload_config(self, db: Session) -> Dict[str, Any]:
        """
        重新加载所有配置
        
        Args:
            db: 数据库会话
            
        Returns:
            加载结果
        """
        logger.info("开始重新加载所有关键词配置")
        
        try:
            # 清空缓存
            self.clear_cache()
            
            # 重新加载各种配置
            analysis_config = self.get_analysis_keywords_config(db, use_cache=False)
            normal_operation_patterns = self.get_denoise_patterns(db, "normal_operation", use_cache=False)
            invalid_data_patterns = self.get_denoise_patterns(db, "invalid_data", use_cache=False)
            system_keywords = self.get_system_keywords(db, use_cache=False)
            
            result = {
                "success": True,
                "message": "配置重新加载成功",
                "statistics": {
                    "analysis_categories": len(analysis_config),
                    "normal_operation_patterns": len(normal_operation_patterns),
                    "invalid_data_patterns": len(invalid_data_patterns),
                    "system_keywords": len(system_keywords)
                }
            }
            
            logger.info(f"配置重新加载完成: {result['statistics']}")
            return result
            
        except Exception as e:
            logger.error(f"重新加载配置失败: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": "配置重新加载失败"
            }

    # ==================== 配置管理方法 ====================
    
    def add_keyword_config(
        self, 
        db: Session, 
        category_key: str, 
        keyword_type: str, 
        keyword_value: str,
        weight: float = 1.0,
        risk_level: str = "medium",
        description: str = None
    ) -> bool:
        """
        添加关键词配置
        
        Args:
            db: 数据库会话
            category_key: 分类键名
            keyword_type: 关键词类型 (keyword, pattern, exclusion)
            keyword_value: 关键词内容
            weight: 权重
            risk_level: 风险级别
            description: 描述
            
        Returns:
            是否成功
        """
        try:
            # 查找分类ID
            category_sql = "SELECT id FROM ai_keyword_categories WHERE category_key = :category_key"
            category = db.execute(text(category_sql), {"category_key": category_key}).fetchone()
            
            if not category:
                logger.error(f"未找到分类: {category_key}")
                return False
            
            # 插入关键词配置
            insert_sql = """
            INSERT INTO ai_keyword_configs (
                category_id, keyword_type, keyword_value, weight, risk_level, description
            ) VALUES (
                :category_id, :keyword_type, :keyword_value, :weight, :risk_level, :description
            )
            """
            
            db.execute(text(insert_sql), {
                "category_id": category.id,
                "keyword_type": keyword_type,
                "keyword_value": keyword_value,
                "weight": weight,
                "risk_level": risk_level,
                "description": description
            })
            
            db.commit()
            
            # 清空缓存
            self.clear_cache()
            
            logger.info(f"成功添加关键词配置: {category_key} - {keyword_value}")
            return True
            
        except Exception as e:
            logger.error(f"添加关键词配置失败: {e}")
            db.rollback()
            return False

    def update_keyword_config(
        self,
        db: Session,
        config_id: int,
        **kwargs
    ) -> bool:
        """
        更新关键词配置
        
        Args:
            db: 数据库会话
            config_id: 配置ID
            **kwargs: 要更新的字段
            
        Returns:
            是否成功
        """
        try:
            # 构建更新SQL
            update_fields = []
            params = {"config_id": config_id}
            
            allowed_fields = ["keyword_value", "weight", "risk_level", "description", "is_enabled"]
            
            for field, value in kwargs.items():
                if field in allowed_fields:
                    update_fields.append(f"{field} = :{field}")
                    params[field] = value
            
            if not update_fields:
                logger.warning("没有有效的更新字段")
                return False
            
            update_sql = f"""
            UPDATE ai_keyword_configs 
            SET {', '.join(update_fields)}, updated_at = NOW()
            WHERE id = :config_id
            """
            
            result = db.execute(text(update_sql), params)
            
            if result.rowcount == 0:
                logger.warning(f"未找到ID为 {config_id} 的配置")
                return False
            
            db.commit()
            
            # 清空缓存
            self.clear_cache()
            
            logger.info(f"成功更新关键词配置: {config_id}")
            return True
            
        except Exception as e:
            logger.error(f"更新关键词配置失败: {e}")
            db.rollback()
            return False

    def delete_keyword_config(self, db: Session, config_id: int) -> bool:
        """
        删除关键词配置
        
        Args:
            db: 数据库会话
            config_id: 配置ID
            
        Returns:
            是否成功
        """
        try:
            delete_sql = "DELETE FROM ai_keyword_configs WHERE id = :config_id"
            result = db.execute(text(delete_sql), {"config_id": config_id})
            
            if result.rowcount == 0:
                logger.warning(f"未找到ID为 {config_id} 的配置")
                return False
            
            db.commit()
            
            # 清空缓存
            self.clear_cache()
            
            logger.info(f"成功删除关键词配置: {config_id}")
            return True
            
        except Exception as e:
            logger.error(f"删除关键词配置失败: {e}")
            db.rollback()
            return False

    def get_config_statistics(self, db: Session) -> Dict[str, Any]:
        """
        获取详细的配置统计信息（包括启用/禁用状态）
        
        Args:
            db: 数据库会话
            
        Returns:
            配置统计信息
        """
        try:
            stats = {}
            
            # 统计分析关键词分类
            category_sql = """
            SELECT 
                is_enabled,
                COUNT(*) as count
            FROM ai_keyword_categories 
            WHERE category_type = 'analysis'
            GROUP BY is_enabled
            """
            category_results = db.execute(text(category_sql)).fetchall()
            
            stats["analysis_categories"] = {
                "enabled": 0,
                "disabled": 0,
                "total": 0
            }
            for result in category_results:
                if result.is_enabled:
                    stats["analysis_categories"]["enabled"] = result.count
                else:
                    stats["analysis_categories"]["disabled"] = result.count
                stats["analysis_categories"]["total"] += result.count
            
            # 统计分析关键词配置
            keyword_sql = """
            SELECT 
                kc.is_enabled,
                COUNT(*) as count
            FROM ai_keyword_configs kc
            JOIN ai_keyword_categories cat ON kc.category_id = cat.id
            WHERE cat.category_type = 'analysis'
            GROUP BY kc.is_enabled
            """
            keyword_results = db.execute(text(keyword_sql)).fetchall()
            
            stats["analysis_keywords"] = {
                "enabled": 0,
                "disabled": 0,
                "total": 0
            }
            for result in keyword_results:
                if result.is_enabled:
                    stats["analysis_keywords"]["enabled"] = result.count
                else:
                    stats["analysis_keywords"]["disabled"] = result.count
                stats["analysis_keywords"]["total"] += result.count
            
            # 统计去噪模式
            denoise_sql = """
            SELECT 
                pattern_type,
                is_enabled,
                COUNT(*) as count
            FROM ai_denoise_patterns
            GROUP BY pattern_type, is_enabled
            """
            denoise_results = db.execute(text(denoise_sql)).fetchall()
            
            stats["denoise_patterns"] = {}
            for result in denoise_results:
                pattern_type = result.pattern_type
                if pattern_type not in stats["denoise_patterns"]:
                    stats["denoise_patterns"][pattern_type] = {
                        "enabled": 0,
                        "disabled": 0,
                        "total": 0
                    }
                
                if result.is_enabled:
                    stats["denoise_patterns"][pattern_type]["enabled"] = result.count
                else:
                    stats["denoise_patterns"][pattern_type]["disabled"] = result.count
                stats["denoise_patterns"][pattern_type]["total"] += result.count
            
            logger.info("获取配置统计信息成功")
            return {
                "success": True,
                "data": stats
            }
            
        except Exception as e:
            logger.error(f"获取配置统计信息失败: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    def export_config(self, db: Session) -> Dict[str, Any]:
        """
        导出配置数据
        
        Args:
            db: 数据库会话
            
        Returns:
            配置数据
        """
        try:
            # 导出分析配置
            analysis_config = self.get_analysis_keywords_config(db, use_cache=False)
            
            # 导出去噪配置
            normal_patterns = self.get_denoise_patterns(db, "normal_operation", use_cache=False)
            invalid_patterns = self.get_denoise_patterns(db, "invalid_data", use_cache=False)
            system_keywords = self.get_system_keywords(db, use_cache=False)
            
            export_data = {
                "export_time": datetime.now().isoformat(),
                "analysis_config": analysis_config,
                "denoise_config": {
                    "normal_operation_patterns": normal_patterns,
                    "invalid_data_patterns": invalid_patterns,
                    "system_keywords": system_keywords
                }
            }
            
            logger.info("配置数据导出成功")
            return {
                "success": True,
                "data": export_data
            }
            
        except Exception as e:
            logger.error(f"导出配置数据失败: {e}")
            return {
                "success": False,
                "error": str(e)
            }


# 全局关键词配置管理器实例
keyword_config_manager = KeywordConfigManager()
