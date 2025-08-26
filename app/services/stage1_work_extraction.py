"""
第一阶段：工单数据抽取服务
从t_work表抽取工单ID，获取评论数据，维护AI处理状态
"""
import logging
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set
from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError, IntegrityError

from app.db.database import get_db
from config.settings import settings

logger = logging.getLogger(__name__)


class Stage1WorkExtractionService:
    """第一阶段：工单数据抽取服务"""
    
    def __init__(self):
        """初始化第一阶段服务"""
        self.work_table_base_name = "t_work"
        self.comment_table_base_name = "t_work_comment"
        self.pending_table_name = "ai_work_pending_analysis"
        self.current_year = datetime.now().year
        self._table_cache = {}
        self._cache_expire_time = None
        self._cache_duration = timedelta(hours=1)
    
    # ==================== 表名管理方法 ====================
    
    def get_work_table_name(self, year: int = None) -> str:
        """获取工单表名"""
        if year is None:
            year = self.current_year
        return f"{self.work_table_base_name}_{year}"
    
    def get_comment_table_name(self, year: int = None) -> str:
        """获取评论表名"""
        if year is None:
            year = self.current_year
        return f"{self.comment_table_base_name}_{year}"
    
    def discover_work_tables(self, db: Session) -> List[str]:
        """发现所有工单分表"""
        logger.info("=== 开始发现工单分表 ===")
        try:
            sql = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = DATABASE() 
            AND table_name LIKE :pattern
            AND table_type = 'BASE TABLE'
            ORDER BY table_name DESC
            """
            
            pattern = f"{self.work_table_base_name}_%"
            logger.info(f"执行SQL查询发现工单表，匹配模式: {pattern}")
            logger.debug(f"SQL查询: {sql}")
            
            result = db.execute(text(sql), {"pattern": pattern})
            all_tables = [row[0] for row in result.fetchall()]
            
            logger.info(f"✓ 查询到 {len(all_tables)} 个候选工单表: {all_tables}")
            
            # 验证表名格式，只接受 t_work_YYYY 格式
            year_pattern = re.compile(rf'^{re.escape(self.work_table_base_name)}_(\d{{4}})$')
            valid_tables = []
            for table in all_tables:
                match = year_pattern.match(table)
                if match:
                    year = int(match.group(1))
                    if 2020 <= year <= 2030:
                        valid_tables.append(table)
                        logger.debug(f"✓ 验证表 {table} 有效，年份: {year}")
                    else:
                        logger.warning(f"⚠️ 表 {table} 年份 {year} 超出有效范围 (2020-2030)")
                else:
                    logger.warning(f"⚠️ 表 {table} 格式不匹配年份模式，跳过")
            
            logger.info(f"✓ 发现 {len(valid_tables)} 个有效工单表: {valid_tables}")
            if not valid_tables:
                logger.warning(f"⚠️ 未发现任何有效工单表，将使用默认表: {self.work_table_base_name}_{self.current_year}")
                return [f"{self.work_table_base_name}_{self.current_year}"]
            
            return valid_tables
            
        except Exception as e:
            logger.error(f"❌ 发现工单分表失败: {e}")
            default_table = f"{self.work_table_base_name}_{self.current_year}"
            logger.info(f"使用默认工单表: {default_table}")
            return [default_table]
    
    def discover_comment_tables(self, db: Session) -> List[str]:
        """发现所有评论分表"""
        logger.info("=== 开始发现评论分表 ===")
        current_time = datetime.now()
        
        # 检查缓存
        if (self._cache_expire_time and 
            current_time < self._cache_expire_time and 
            self._table_cache):
            logger.info(f"✓ 使用缓存的评论表数据: {list(self._table_cache.keys())}")
            return list(self._table_cache.keys())
        
        try:
            sql = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = DATABASE() 
            AND table_name LIKE :pattern
            ORDER BY table_name DESC
            """
            
            pattern = f"{self.comment_table_base_name}_%"
            logger.info(f"执行SQL查询发现评论表，匹配模式: {pattern}")
            logger.debug(f"SQL查询: {sql}")
            
            result = db.execute(text(sql), {"pattern": pattern})
            tables = [row[0] for row in result.fetchall()]
            
            logger.info(f"✓ 查询到 {len(tables)} 个评论表: {tables}")
            
            # 验证表名格式并缓存
            year_pattern = re.compile(rf'^{re.escape(self.comment_table_base_name)}_(\d{{4}})$')
            valid_tables = []
            for table in tables:
                match = year_pattern.match(table)
                if match:
                    year = int(match.group(1))
                    if 2020 <= year <= 2030:
                        self._table_cache[table] = year
                        valid_tables.append(table)
                        logger.debug(f"✓ 验证表 {table} 有效，年份: {year}")
                    else:
                        logger.warning(f"⚠️ 表 {table} 年份 {year} 超出有效范围 (2020-2030)")
                else:
                    logger.warning(f"⚠️ 表 {table} 格式不匹配年份模式")
            
            self._cache_expire_time = current_time + self._cache_duration
            logger.info(f"✓ 缓存 {len(valid_tables)} 个有效评论表，缓存时间: {self._cache_duration}")
            
            if not valid_tables:
                logger.warning(f"⚠️ 未发现任何有效评论表，将使用默认表: {self.comment_table_base_name}_{self.current_year}")
                return [f"{self.comment_table_base_name}_{self.current_year}"]
            
            return valid_tables
            
        except Exception as e:
            logger.error(f"❌ 发现评论分表失败: {e}")
            default_table = f"{self.comment_table_base_name}_{self.current_year}"
            logger.info(f"使用默认评论表: {default_table}")
            return [default_table]
    
    def check_table_exists(self, db: Session, table_name: str) -> bool:
        """检查表是否存在"""
        logger.debug(f"🔍 检查表是否存在: {table_name}")
        try:
            sql = """
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = DATABASE() 
            AND table_name = :table_name
            """
            logger.debug(f"SQL查询: {sql}")
            logger.debug(f"参数: table_name = {table_name}")
            
            result = db.execute(text(sql), {"table_name": table_name})
            exists = result.fetchone() is not None
            
            if exists:
                logger.debug(f"✓ 表 {table_name} 存在")
            else:
                logger.warning(f"⚠️ 表 {table_name} 不存在")
            
            return exists
            
        except Exception as e:
            logger.error(f"❌ 检查表 {table_name} 是否存在时出错: {e}")
            return False
    
    # ==================== 工单数据抽取方法 ====================
    
    def _batch_extract_work_orders_by_time_range(
        self,
        db: Session,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        target_date: Optional[datetime] = None,
        days_back: int = 1
    ) -> List[Dict[str, Any]]:
        """重构：先查询总数量，然后固定次数批量抽取工单数据"""
        
        # 确定时间范围
        if start_time is not None and end_time is not None:
            actual_start_time = start_time
            actual_end_time = end_time
        elif target_date is not None:
            actual_start_time = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
            actual_end_time = actual_start_time + timedelta(days=1)
        else:
            target_date = datetime.now() - timedelta(days=days_back)
            actual_start_time = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
            actual_end_time = actual_start_time + timedelta(days=1)
        
        batch_size = settings.data_extractor_limit_default
        
        logger.info(f"📊 重构后批量抽取配置:")
        logger.info(f"  ⏰ 时间范围: {actual_start_time} ~ {actual_end_time}")
        logger.info(f"  📦 批次大小: {batch_size}条/批")
        
        # 1. 先查询符合条件的工单总数量
        try:
            target_year = actual_start_time.year
            work_table_name = self.get_work_table_name(target_year)
            
            # 验证表是否存在
            if not self.check_table_exists(db, work_table_name):
                logger.warning(f"⚠️ 工单表 {work_table_name} 不存在，使用当前年份表")
                work_table_name = self.get_work_table_name()
                if not self.check_table_exists(db, work_table_name):
                    logger.error(f"❌ 工单表 {work_table_name} 不存在")
                    return []
            
            count_sql = f"""
            SELECT COUNT(*) as total_count
            FROM {work_table_name}
            WHERE create_time >= :start_time 
            AND create_time < :end_time
            AND deleted = 0
            AND state = 'FINISH'
            """
            
            logger.info(f"🔍 查询符合条件的工单总数量...")
            count_result = db.execute(text(count_sql), {
                "start_time": actual_start_time,
                "end_time": actual_end_time
            })
            total_count = count_result.fetchone()[0]
            
            logger.info(f"📊 查询到符合条件的工单总数: {total_count}条")
            
            if total_count == 0:
                logger.info("⚠️ 没有符合条件的工单需要抽取")
                return []
            
            # 2. 计算需要的固定循环次数
            total_batches = (total_count + batch_size - 1) // batch_size  # 向上取整
            logger.info(f"📊 计算批次数: 总计{total_count}条 ÷ {batch_size}条/批 = {total_batches}批次")
            
            # 应用配置限制
            max_total_setting = settings.data_extractor_max_total
            max_batches_setting = settings.data_extractor_max_batches
            
            if max_total_setting > 0 and total_count > max_total_setting:
                total_count = max_total_setting
                total_batches = (total_count + batch_size - 1) // batch_size
                logger.info(f"📊 应用配置限制: 最大总量{max_total_setting}条，调整为{total_batches}批次")
            
            if max_batches_setting > 0 and total_batches > max_batches_setting:
                total_batches = max_batches_setting
                logger.info(f"📊 应用配置限制: 最大批次{max_batches_setting}批次")
            
        except Exception as e:
            logger.error(f"❌ 查询工单总数失败: {e}")
            return []
        
        # 3. 固定次数循环抽取
        all_work_orders = []
        current_offset = 0
        
        for batch_num in range(1, total_batches + 1):
            logger.info(f"🔄 执行第 {batch_num}/{total_batches} 批次抽取 (偏移: {current_offset})")
            
            batch_orders = self.extract_work_orders_by_time_range(
                db, actual_start_time, actual_end_time, None, 1, 
                limit=batch_size, offset=current_offset
            )
            
            if not batch_orders:
                logger.info(f"✅ 第 {batch_num} 批次无数据，提前完成")
                break
            
            all_work_orders.extend(batch_orders)
            current_offset += len(batch_orders)
            
            logger.info(f"📈 第 {batch_num}/{total_batches} 批次完成: 本批 {len(batch_orders)}条，累计 {len(all_work_orders)}条")
        
        logger.info(f"📊 固定次数批量抽取完成: 计划 {total_batches} 批次，实际 {batch_num if 'batch_num' in locals() else 0} 批次，总计 {len(all_work_orders)}条工单")
        return all_work_orders
    
    def extract_work_orders_by_time_range(
        self,
        db: Session,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        target_date: Optional[datetime] = None,
        days_back: int = 1,
        limit: Optional[int] = None,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """根据时间范围抽取工单"""
        logger.info("=" * 50)
        logger.info("🚀 开始根据时间范围抽取工单")
        
        # 处理时间范围参数的优先级：start_time/end_time > target_date > days_back
        if start_time is not None and end_time is not None:
            # 使用指定的时间范围
            logger.info(f"📅 使用指定时间范围: {start_time} - {end_time}")
        elif target_date is not None:
            # 使用指定日期的整天
            start_time = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_time = start_time + timedelta(days=1)
            logger.info(f"📅 使用指定日期的整天: {target_date.date()}")
        else:
            # 使用days_back计算
            target_date = datetime.now() - timedelta(days=days_back)
            start_time = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
            end_time = start_time + timedelta(days=1)
            logger.info(f"📅 使用days_back={days_back}计算时间范围")
        
        # 确保时间范围有效
        if start_time >= end_time:
            logger.error(f"❌ 时间范围无效: start_time={start_time} >= end_time={end_time}")
            return []
        
        logger.info(f"📅 最终抽取工单时间范围: {start_time.strftime('%Y-%m-%d %H:%M:%S')} - {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"⏰ 时间跨度: {(end_time - start_time).total_seconds() / 3600:.1f} 小时")
        
        # 根据开始时间确定目标年份（如果跨年，使用开始时间的年份）
        target_year = start_time.year
        work_table_name = self.get_work_table_name(target_year)
        comment_table_name = self.get_comment_table_name(target_year)
        
        logger.info(f"🎯 预期使用表: 工单表={work_table_name}, 评论表={comment_table_name}")
        
        # 验证工单表是否存在
        logger.info("🔍 验证工单表是否存在...")
        available_tables = self.discover_work_tables(db)
        if work_table_name not in available_tables:
            logger.warning(f"⚠️ 工单表 {work_table_name} 不存在，使用当前年份表")
            work_table_name = self.get_work_table_name()
            comment_table_name = self.get_comment_table_name()
            logger.info(f"🔄 调整后使用表: 工单表={work_table_name}, 评论表={comment_table_name}")
        else:
            logger.info(f"✓ 工单表 {work_table_name} 存在，继续使用")
        
        # 再次确认表存在
        logger.info(f"🔍 最终确认表 {work_table_name} 是否存在...")
        if not self.check_table_exists(db, work_table_name):
            logger.error(f"❌ 工单表 {work_table_name} 不存在，抽取失败")
            return []
        
        try:
            # 应用限制配置
            if limit is None:
                limit = settings.data_extractor_limit_default
            
            sql = f"""
            SELECT 
                id as work_id,
                create_time,
                type as work_type,
                state as work_state,
                create_by,
                create_name,
                level,
                order_by,
                order_name
            FROM {work_table_name}
            WHERE create_time >= :start_time 
            AND create_time < :end_time
            AND deleted = 0
            AND state = 'FINISH'
            ORDER BY create_time DESC
            LIMIT :limit OFFSET :offset
            """
            
            logger.info(f"📝 执行工单查询 (限制:{limit}条, 偏移:{offset})")
            logger.info(f"⏰ 时间范围: {start_time} ~ {end_time}")
            
            result = db.execute(text(sql), {
                "start_time": start_time,
                "end_time": end_time,
                "limit": limit,
                "offset": offset
            })
            
            logger.info("⚡ SQL查询执行完成，正在处理结果...")
            
            work_orders = []
            row_count = 0
            for row in result:
                row_count += 1
                work_order = {
                    "work_id": row.work_id,
                    "work_table_name": work_table_name,
                    "comment_table_name": comment_table_name,
                    "extract_date": start_time.date(),  # 使用开始时间的日期作为抽取日期
                    "start_time": start_time,  # 添加时间范围信息
                    "end_time": end_time,
                    "create_time": row.create_time,
                    "work_type": row.work_type,
                    "work_state": row.work_state,
                    "create_by": row.create_by,
                    "create_name": row.create_name,
                    "level": getattr(row, 'level', None),
                    "order_by": getattr(row, 'order_by', None),
                    "order_name": getattr(row, 'order_name', None)
                }
                work_orders.append(work_order)
                
                if row_count <= 5:  # 只打印前5条记录的详细信息
                    logger.debug(f"📋 工单 #{row_count}: ID={row.work_id}, 创建时间={row.create_time}, 类型={row.work_type}, 状态={row.work_state}")
                elif row_count == 6:
                    logger.debug("... (后续工单信息省略)")
            
            logger.info(f"✅ 从表 {work_table_name} 成功抽取到 {len(work_orders)} 个工单")
            
            if len(work_orders) == 0:
                logger.warning(f"⚠️ 在时间范围 {start_time} - {end_time} 内未找到任何工单")
                logger.info("💡 请检查:")
                logger.info("   1. 时间范围是否正确")
                logger.info("   2. 工单表中是否有数据")
                logger.info("   3. deleted=0 条件是否过滤了所有记录")
            
            return work_orders
            
        except Exception as e:
            logger.error(f"❌ 从表 {work_table_name} 抽取工单失败: {e}")
            logger.error(f"错误详情: {str(e)}")
            return []
    
    def extract_work_orders_by_date(
        self,
        db: Session,
        target_date: Optional[datetime] = None,
        days_back: int = 1
    ) -> List[Dict[str, Any]]:
        """根据日期抽取工单（兼容方法）"""
        logger.warning("⚠️ extract_work_orders_by_date 方法已过时，请使用 extract_work_orders_by_time_range")
        return self.extract_work_orders_by_time_range(
            db=db,
            target_date=target_date,
            days_back=days_back
        )
    
    # ==================== 待处理表管理方法 ====================
    
    def insert_pending_analysis_records(
        self,
        db: Session,
        work_orders: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """插入待处理分析记录"""
        logger.info("=" * 50)
        logger.info("💾 开始插入待处理分析记录")
        
        if not work_orders:
            logger.warning("⚠️ 没有工单数据需要插入")
            return {
                "success": True,
                "inserted": 0,
                "skipped": 0,
                "errors": 0,
                "total": 0,
                "message": "没有工单数据需要插入"
            }
        
        logger.info(f"📊 准备插入 {len(work_orders)} 个工单到待处理表: {self.pending_table_name}")
        
        success_count = 0
        skip_count = 0
        error_count = 0
        
        # 🔥 修复：使用批量提交机制，避免长时间事务阻塞API
        batch_size = 50  # 每50条记录提交一次
        
        for i, work_order in enumerate(work_orders, 1):
            work_id = work_order["work_id"]
            logger.debug(f"🔄 处理工单 {i}/{len(work_orders)}: ID={work_id}")
            
            try:
                # 检查是否已存在
                check_sql = f"""
                SELECT id FROM {self.pending_table_name}
                WHERE work_id = :work_id AND extract_date = :extract_date
                LIMIT 1
                """
                
                logger.debug(f"🔍 检查工单 {work_id} 是否已存在...")
                logger.debug(f"检查SQL: {check_sql}")
                
                existing = db.execute(text(check_sql), {
                    "work_id": work_order["work_id"],
                    "extract_date": work_order["extract_date"]
                }).fetchone()
                
                if existing:
                    skip_count += 1
                    logger.debug(f"⏭️ 工单 {work_id} 已存在，跳过插入")
                    continue
                
                # 🔥 修复：在插入前实时查询评论数量
                comment_count = self.get_work_comment_count(db, work_order["work_id"], work_order["comment_table_name"])
                has_comments = 1 if comment_count > 0 else 0
                
                # 插入新记录
                insert_sql = f"""
                INSERT INTO {self.pending_table_name} (
                    work_id, work_table_name, comment_table_name, extract_date,
                    create_time, work_type, work_state, create_by, create_name,
                    ai_status, comment_count, has_comments, created_at
                ) VALUES (
                    :work_id, :work_table_name, :comment_table_name, :extract_date,
                    :create_time, :work_type, :work_state, :create_by, :create_name,
                    'PENDING', :comment_count, :has_comments, :created_at
                )
                """
                
                logger.debug(f"💾 插入工单 {work_id} 到待处理表，评论数: {comment_count}")
                logger.debug(f"插入SQL: {insert_sql}")
                
                db.execute(text(insert_sql), {
                    "work_id": work_order["work_id"],
                    "work_table_name": work_order["work_table_name"],
                    "comment_table_name": work_order["comment_table_name"],
                    "extract_date": work_order["extract_date"],
                    "create_time": work_order["create_time"],
                    "work_type": work_order["work_type"],
                    "work_state": work_order["work_state"],
                    "create_by": work_order["create_by"],
                    "create_name": work_order["create_name"],
                    "comment_count": comment_count,  # 🔥 新增：实际评论数量
                    "has_comments": has_comments,   # 🔥 新增：是否有评论标识
                    "created_at": datetime.now()
                })
                
                success_count += 1
                logger.debug(f"✅ 工单 {work_id} 插入成功")
                
                # 🔥 批量提交机制：每处理batch_size条记录或到达最后一条时提交
                if success_count % batch_size == 0 or i == len(work_orders):
                    try:
                        db.commit()
                        logger.info(f"💾 批量提交: 已处理 {i}/{len(work_orders)} 条记录 (成功:{success_count}, 跳过:{skip_count}, 错误:{error_count})")
                    except Exception as commit_error:
                        db.rollback()
                        logger.error(f"❌ 批量提交失败: {commit_error}")
                        error_count += 1
                
            except IntegrityError as e:
                skip_count += 1
                logger.debug(f"⏭️ 工单 {work_id} 违反唯一约束，跳过: {e}")
                db.rollback()
                continue
            except Exception as e:
                error_count += 1
                logger.error(f"❌ 插入工单 {work_id} 到待处理表失败: {e}")
                logger.error(f"错误详情: {str(e)}")
                db.rollback()
                continue
        
        return {
            "success": True,
            "inserted": success_count,
            "skipped": skip_count,
            "errors": error_count,
            "total": len(work_orders),
            "message": f"插入完成: 成功{success_count}, 跳过{skip_count}, 错误{error_count}"
        }
    
    def get_pending_work_orders(
        self,
        db: Session,
        ai_status: str = 'PENDING',
        limit: int = 100,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """获取待处理的工单
        
        Args:
            db: 数据库会话
            ai_status: AI处理状态
            limit: 限制数量
            start_date: 开始时间（按create_time过滤）
            end_date: 结束时间（按create_time过滤）
        """
        try:
            # 构建WHERE条件
            where_conditions = ["ai_status = :ai_status"]
            params = {
                "ai_status": ai_status,
                "limit": limit
            }
            
            # 🔥 新增：支持按工单创建时间范围过滤
            if start_date:
                where_conditions.append("create_time >= :start_date")
                params["start_date"] = start_date
            
            if end_date:
                where_conditions.append("create_time <= :end_date")
                params["end_date"] = end_date
            
            sql = f"""
            SELECT 
                id, work_id, work_table_name, comment_table_name,
                extract_date, create_time, work_type, work_state,
                create_by, create_name, ai_status, comment_count,
                has_comments, ai_retry_count, created_at
            FROM {self.pending_table_name}
            WHERE {' AND '.join(where_conditions)}
            ORDER BY created_at ASC
            LIMIT :limit
            """
            
            result = db.execute(text(sql), params)
            
            pending_orders = []
            for row in result:
                pending_orders.append({
                    "id": row.id,
                    "work_id": row.work_id,
                    "work_table_name": row.work_table_name,
                    "comment_table_name": row.comment_table_name,
                    "extract_date": row.extract_date,
                    "create_time": row.create_time,
                    "work_type": row.work_type,
                    "work_state": row.work_state,
                    "create_by": row.create_by,
                    "create_name": row.create_name,
                    "ai_status": row.ai_status,
                    "comment_count": row.comment_count,
                    "has_comments": bool(row.has_comments),
                    "ai_retry_count": row.ai_retry_count,
                    "created_at": row.created_at
                })
            
            # 构建日志信息
            time_range_info = ""
            if start_date or end_date:
                time_parts = []
                if start_date:
                    time_parts.append(f"从{start_date.date()}")
                if end_date:
                    time_parts.append(f"到{end_date.date()}")
                time_range_info = f" ({' '.join(time_parts)})"
            
            logger.info(f"获取到 {len(pending_orders)} 个状态为 {ai_status} 的工单{time_range_info}")
            return pending_orders
            
        except Exception as e:
            logger.error(f"获取待处理工单失败: {e}")
            return []
    
    def update_work_order_ai_status(
        self,
        db: Session,
        work_id: int,
        ai_status: str,
        error_message: Optional[str] = None,
        comment_count: Optional[int] = None,
        has_comments: Optional[bool] = None
    ) -> bool:
        """更新工单AI处理状态"""
        try:
            update_fields = ["ai_status = :ai_status", "updated_at = :updated_at"]
            params = {
                "work_id": work_id,
                "ai_status": ai_status,
                "updated_at": datetime.now()
            }
            
            if ai_status == 'PROCESSING':
                update_fields.append("ai_process_start_time = :start_time")
                params["start_time"] = datetime.now()
            elif ai_status in ['COMPLETED', 'FAILED']:
                update_fields.append("ai_process_end_time = :end_time")
                params["end_time"] = datetime.now()
            
            if error_message is not None:
                update_fields.append("ai_error_message = :error_message")
                params["error_message"] = error_message
            
            if comment_count is not None:
                update_fields.append("comment_count = :comment_count")
                params["comment_count"] = comment_count
            
            if has_comments is not None:
                update_fields.append("has_comments = :has_comments")
                params["has_comments"] = has_comments
            
            if ai_status == 'FAILED':
                update_fields.append("ai_retry_count = ai_retry_count + 1")
            
            sql = f"""
            UPDATE {self.pending_table_name}
            SET {', '.join(update_fields)}
            WHERE work_id = :work_id
            """
            
            result = db.execute(text(sql), params)
            db.commit()
            
            if result.rowcount > 0:
                logger.info(f"工单 {work_id} AI状态更新为: {ai_status}")
                return True
            else:
                logger.warning(f"工单 {work_id} 更新失败，未找到记录")
                return False
                
        except Exception as e:
            db.rollback()
            logger.error(f"更新工单 {work_id} AI状态失败: {e}")
            return False
    
    def reset_failed_work_orders_for_retry(
        self,
        db: Session,
        work_ids: List[int] = None,
        limit: int = None
    ) -> int:
        """重置FAILED状态的工单为PENDING，以便重新分析"""
        try:
            # 构建WHERE条件
            where_conditions = ["ai_status = 'FAILED'"]
            params = {}
            
            if work_ids:
                # 重置指定的工单
                placeholders = ','.join([f':work_id_{i}' for i in range(len(work_ids))])
                where_conditions.append(f"work_id IN ({placeholders})")
                for i, work_id in enumerate(work_ids):
                    params[f'work_id_{i}'] = work_id
            
            where_clause = " AND ".join(where_conditions)
            limit_clause = f"LIMIT {limit}" if limit else ""
            
            # 重置状态和错误信息
            sql = f"""
            UPDATE {self.pending_table_name}
            SET 
                ai_status = 'PENDING',
                ai_error_message = NULL,
                updated_at = NOW()
            WHERE {where_clause}
            {limit_clause}
            """
            
            result = db.execute(text(sql), params)
            db.commit()
            
            reset_count = result.rowcount
            if reset_count > 0:
                logger.info(f"✅ 成功重置 {reset_count} 个FAILED状态工单为PENDING")
            else:
                logger.info("⚠️ 没有找到需要重置的FAILED状态工单")
            
            return reset_count
            
        except Exception as e:
            db.rollback()
            logger.error(f"❌ 重置FAILED状态工单失败: {e}")
            return 0
    
    # ==================== 评论数据处理方法 ====================
    
    def get_work_comments(
        self,
        db: Session,
        work_id: int,
        comment_table_name: str
    ) -> List[Dict[str, Any]]:
        """获取指定工单的所有评论记录（仅处理人评论，oper=1）"""
        try:
            sql = f"""
            SELECT 
                id,
                work_id,
                user_type,
                user_id,
                name,
                content,
                create_time,
                oper,
                image,
                reissue
            FROM {comment_table_name}
            WHERE work_id = :work_id 
            AND deleted = 0
            AND oper = 1
            ORDER BY create_time ASC
            """
            
            result = db.execute(text(sql), {"work_id": work_id})
            
            comments = []
            for row in result:
                comments.append({
                    "id": row.id,
                    "work_id": row.work_id,
                    "user_type": row.user_type,
                    "user_id": row.user_id,
                    "name": row.name,
                    "content": row.content,
                    "create_time": row.create_time,
                    "oper": bool(row.oper) if row.oper is not None else False,
                    "image": row.image,
                    "reissue": row.reissue,
                    "source_table": comment_table_name
                })
            
            logger.info(f"从表 {comment_table_name} 获取工单 {work_id} 的 {len(comments)} 条处理人评论（oper=1）")
            return comments
            
        except Exception as e:
            logger.error(f"从表 {comment_table_name} 获取工单 {work_id} 评论失败: {e}")
            return []
    
    def build_conversation_text(self, comments: List[Dict[str, Any]]) -> str:
        """构建工单对话文本"""
        if not comments:
            return ""
        
        conversation_parts = []
        
        for comment in comments:
            user_type = comment.get("user_type", "")
            name = comment.get("name", "")
            content = str(comment.get("content") or "")  # 防止NoneType错误
            oper = comment.get("oper", False)
            create_time = comment.get("create_time", "")
            
            # 确定角色显示名称
            if user_type == "customer":
                role = "客户"
            elif user_type == "service" or oper:
                role = "客服"
            elif user_type == "system":
                role = "系统"
            else:
                role = user_type or "未知"
            
            # 如果有名称，添加到角色后面
            if name:
                role_display = f"{role}({name})"
            else:
                role_display = role
            
            # 添加时间戳
            time_str = ""
            if create_time:
                if isinstance(create_time, datetime):
                    time_str = create_time.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    time_str = str(create_time)
            
            # 构建对话行
            if time_str:
                conversation_parts.append(f"[{time_str}] {role_display}: {content}")
            else:
                conversation_parts.append(f"{role_display}: {content}")
        
        return "\n".join(conversation_parts)
    
    def get_work_comment_count(
        self,
        db: Session,
        work_id: int,
        comment_table_name: str
    ) -> int:
        """获取工单评论数量"""
        logger.debug(f"🔍 获取工单 {work_id} 的评论数量，表: {comment_table_name}")
        try:
            # 先检查评论表是否存在
            if not self.check_table_exists(db, comment_table_name):
                logger.warning(f"⚠️ 评论表 {comment_table_name} 不存在，返回评论数量为0")
                return 0
            
            sql = f"""
            SELECT COUNT(*) as comment_count
            FROM {comment_table_name}
            WHERE work_id = :work_id AND deleted = 0
            """
            
            logger.debug(f"SQL查询: {sql}")
            logger.debug(f"参数: work_id={work_id}")
            
            result = db.execute(text(sql), {"work_id": work_id})
            count = result.fetchone().comment_count
            
            logger.debug(f"✅ 工单 {work_id} 评论数量: {count}")
            return count
            
        except Exception as e:
            logger.error(f"❌ 获取工单 {work_id} 评论数量失败: {e}")
            logger.error(f"错误详情: {str(e)}")
            return 0
    
    # ==================== 主要业务流程方法 ====================
    
    def extract_work_data_by_time_range(
        self,
        db: Session,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        target_date: Optional[datetime] = None,
        days_back: int = 1
    ) -> Dict[str, Any]:
        """第一阶段：根据时间范围循环批量抽取工单数据并插入待处理表"""
        logger.info("🚀 开始数据抽取（循环批量模式）")
        logger.info(f"📋 参数: start_time={start_time}, end_time={end_time}, target_date={target_date}, days_back={days_back}")
        
        try:
            # 1. 循环批量抽取工单数据
            logger.info("📝 步骤1: 循环批量抽取工单数据")
            all_work_orders = self._batch_extract_work_orders_by_time_range(
                db, start_time, end_time, target_date, days_back
            )
            
            # 确定实际使用的时间范围
            if start_time is not None and end_time is not None:
                actual_start_time = start_time
                actual_end_time = end_time
                actual_target_date = start_time.date()
            elif target_date is not None:
                actual_target_date = target_date.date()
                actual_start_time = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
                actual_end_time = actual_start_time + timedelta(days=1)
            else:
                actual_target_date = (datetime.now() - timedelta(days=days_back)).date()
                actual_start_time = datetime.combine(actual_target_date, datetime.min.time())
                actual_end_time = actual_start_time + timedelta(days=1)
            
            if not all_work_orders:
                logger.warning("⚠️ 没有找到需要抽取的工单")
                return {
                    "success": True,
                    "stage": "第一阶段：工单数据抽取（时间范围）",
                    "target_date": actual_target_date.strftime("%Y-%m-%d"),
                    "time_range": {
                        "start_time": actual_start_time.strftime("%Y-%m-%d %H:%M:%S"),
                        "end_time": actual_end_time.strftime("%Y-%m-%d %H:%M:%S")
                    },
                    "days_back": days_back,
                    "statistics": {"extracted": 0, "inserted": 0, "skipped": 0, "updated": 0},
                    "message": "没有找到需要抽取的工单"
                }
            
            logger.info(f"✅ 步骤1完成: 抽取到 {len(all_work_orders)} 个工单")
            
            # 2. 插入待处理表
            logger.info("📝 步骤2: 插入待处理表")
            insert_result = self.insert_pending_analysis_records(db, all_work_orders)
            inserted_count = insert_result.get("inserted", 0)
            skipped_count = insert_result.get("skipped", 0)
            logger.info(f"✅ 步骤2完成: {insert_result.get('message', '未知结果')}")
            
            # 3. 🔥 优化：查询评论统计信息（插入时已正确设置，无需重复更新）
            logger.info("📝 步骤3: 统计评论信息")
            updated_count = inserted_count  # 插入时已正确设置评论统计
            comment_stats = {"with_comments": 0, "without_comments": 0, "total_comments": 0}
            
            if inserted_count > 0:
                try:
                    # 🔥 优化：直接从数据库查询统计信息，避免重复计算
                    stats_sql = f"""
                    SELECT 
                        COUNT(CASE WHEN has_comments = 1 THEN 1 END) as with_comments,
                        COUNT(CASE WHEN has_comments = 0 THEN 1 END) as without_comments,
                        SUM(comment_count) as total_comments
                    FROM {self.pending_table_name}
                    WHERE created_at >= :start_time
                    """
                    
                    # 使用当前批次的开始时间作为查询条件
                    batch_start = datetime.now() - timedelta(minutes=10)  # 假设批次在10分钟内完成
                    result = db.execute(text(stats_sql), {"start_time": batch_start})
                    row = result.fetchone()
                    
                    if row:
                        comment_stats["with_comments"] = row.with_comments or 0
                        comment_stats["without_comments"] = row.without_comments or 0
                        comment_stats["total_comments"] = row.total_comments or 0
                        logger.debug(f"📊 查询得到评论统计: {comment_stats}")
                    
                except Exception as e:
                    logger.warning(f"⚠️ 查询评论统计失败，使用默认值: {e}")
                    # 如果查询失败，使用保守估计
                    comment_stats["with_comments"] = inserted_count
                    comment_stats["without_comments"] = 0
                    comment_stats["total_comments"] = inserted_count * 3  # 估算平均3条评论
            
            logger.info(f"✅ 步骤3完成: 插入时已正确设置评论统计，处理 {updated_count} 条记录")
            
            result = {
                "extracted": len(all_work_orders),
                "inserted": inserted_count,
                "skipped": skipped_count,  # 🔥 新增：跳过的记录数
                "updated": updated_count
            }
            
            # 打印详细统计信息
            logger.info("=" * 50)
            logger.info("📊 第一阶段数据抽取完成统计:")
            logger.info(f"  📥 抽取工单数: {result['extracted']}")
            logger.info(f"  💾 插入记录数: {result['inserted']}")
            logger.info(f"  ⏭️ 跳过记录数: {result['skipped']}")  # 🔥 新增日志
            logger.info(f"  🔄 更新记录数: {result['updated']}")
            logger.info(f"  💬 有评论工单: {comment_stats['with_comments']}")
            logger.info(f"  💭 无评论工单: {comment_stats['without_comments']}")
            logger.info(f"  📝 总评论数量: {comment_stats['total_comments']}")
            logger.info("=" * 50)
            
            return {
                "success": True,
                "stage": "第一阶段：工单数据抽取（时间范围）",
                "target_date": actual_target_date.strftime("%Y-%m-%d"),
                "time_range": {
                    "start_time": actual_start_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "end_time": actual_end_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "duration_hours": round((actual_end_time - actual_start_time).total_seconds() / 3600, 1)
                },
                "days_back": days_back,
                "statistics": result,
                "comment_statistics": comment_stats,
                "message": f"成功抽取 {result['extracted']} 个工单，插入 {result['inserted']} 条记录，更新 {result['updated']} 条评论统计"
            }
            
        except Exception as e:
            logger.error(f"❌ 第一阶段数据抽取失败: {e}")
            logger.error(f"错误详情: {str(e)}")
            import traceback
            logger.error(f"错误堆栈: {traceback.format_exc()}")
            return {
                "success": False,
                "stage": "第一阶段：工单数据抽取",
                "error": str(e),
                "message": "数据抽取失败"
            }
    
    def extract_daily_work_data(
        self,
        db: Session,
        target_date: Optional[datetime] = None,
        days_back: int = 1
    ) -> Dict[str, Any]:
        """第一阶段：抽取指定日期的工单数据并插入待处理表（兼容方法）"""
        logger.warning("⚠️ extract_daily_work_data 方法已过时，请使用 extract_work_data_by_time_range")
        return self.extract_work_data_by_time_range(
            db=db,
            target_date=target_date,
            days_back=days_back
        )
    
    def get_extraction_statistics(self, db: Session) -> Dict[str, Any]:
        """获取数据抽取统计信息"""
        try:
            sql = f"""
            SELECT 
                ai_status,
                COUNT(*) as count,
                COUNT(CASE WHEN has_comments = 1 THEN 1 END) as with_comments,
                COUNT(CASE WHEN has_comments = 0 THEN 1 END) as without_comments,
                AVG(comment_count) as avg_comment_count,
                MAX(created_at) as latest_extract,
                MIN(created_at) as earliest_extract
            FROM {self.pending_table_name}
            GROUP BY ai_status
            """
            
            result = db.execute(text(sql))
            
            statistics = {}
            total_count = 0
            total_with_comments = 0
            total_without_comments = 0
            
            for row in result:
                status = row.ai_status
                count = row.count
                with_comments = row.with_comments or 0
                without_comments = row.without_comments or 0
                
                statistics[status] = {
                    "count": count,
                    "with_comments": with_comments,
                    "without_comments": without_comments,
                    "avg_comment_count": float(row.avg_comment_count or 0),
                    "latest_extract": row.latest_extract,
                    "earliest_extract": row.earliest_extract
                }
                
                total_count += count
                total_with_comments += with_comments
                total_without_comments += without_comments
            
            return {
                "success": True,
                "by_status": statistics,
                "totals": {
                    "total_work_orders": total_count,
                    "total_with_comments": total_with_comments,
                    "total_without_comments": total_without_comments,
                    "comment_coverage_rate": round(total_with_comments / total_count * 100, 2) if total_count > 0 else 0
                }
            }
            
        except Exception as e:
            logger.error(f"获取抽取统计信息失败: {e}")
            return {
                "success": False,
                "error": str(e)
            }


# 全局第一阶段服务实例
stage1_service = Stage1WorkExtractionService()
