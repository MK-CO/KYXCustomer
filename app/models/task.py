"""
任务执行记录数据模型
"""
import json
import uuid
from datetime import datetime
from typing import Dict, Any, Optional, List
from sqlalchemy import text
from sqlalchemy.orm import Session

import logging

logger = logging.getLogger(__name__)


class TaskExecutionRecord:
    """任务执行记录模型"""
    
    def __init__(self):
        self.table_name = "ai_task_execution_records"
    
    def generate_task_id(self, task_type: str, trigger_type: str = "scheduled") -> str:
        """生成任务ID"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        unique_id = str(uuid.uuid4())[:8]
        prefix = "MANUAL" if trigger_type == "manual" else "SCHED"
        return f"{prefix}_{task_type.upper()}_{timestamp}_{unique_id}"
    
    def create_task_record(
        self,
        db: Session,
        task_name: str,
        task_type: str,
        trigger_type: str = "scheduled",
        trigger_user: Optional[str] = None,
        batch_size: Optional[int] = None,
        max_concurrent: Optional[int] = None,
        execution_details: Optional[Dict[str, Any]] = None,
        task_config_key: Optional[str] = None
    ) -> str:
        """创建任务记录"""
        try:
            task_id = self.generate_task_id(task_type, trigger_type)
            start_time = datetime.now()
            
            sql = f"""
            INSERT INTO {self.table_name} (
                task_id, task_name, task_type, task_config_key, trigger_type, trigger_user,
                start_time, status, batch_size, max_concurrent, 
                execution_details, created_at, updated_at
            ) VALUES (
                :task_id, :task_name, :task_type, :task_config_key, :trigger_type, :trigger_user,
                :start_time, 'running', :batch_size, :max_concurrent,
                :execution_details, :created_at, :updated_at
            )
            """
            
            # 转换执行详情为JSON字符串
            details_json = json.dumps(execution_details, ensure_ascii=False) if execution_details else None
            
            params = {
                "task_id": task_id,
                "task_name": task_name,
                "task_type": task_type,
                "task_config_key": task_config_key,
                "trigger_type": trigger_type,
                "trigger_user": trigger_user,
                "start_time": start_time,
                "batch_size": batch_size,
                "max_concurrent": max_concurrent,
                "execution_details": details_json,
                "created_at": start_time,
                "updated_at": start_time
            }
            
            db.execute(text(sql), params)
            db.commit()
            
            logger.info(f"✅ 创建任务记录成功: {task_id} ({task_name})")
            return task_id
            
        except Exception as e:
            logger.error(f"❌ 创建任务记录失败: {e}")
            db.rollback()
            raise e
    
    def update_task_progress(
        self,
        db: Session,
        task_id: str,
        status: Optional[str] = None,
        process_stage: Optional[str] = None,
        total_records: Optional[int] = None,
        processed_records: Optional[int] = None,
        success_records: Optional[int] = None,
        failed_records: Optional[int] = None,
        skipped_records: Optional[int] = None,
        denoised_records: Optional[int] = None,
        duplicate_records: Optional[int] = None,
        extracted_records: Optional[int] = None,
        analyzed_records: Optional[int] = None,
        execution_details: Optional[Dict[str, Any]] = None,
        performance_stats: Optional[Dict[str, Any]] = None
    ) -> bool:
        """更新任务进度"""
        try:
            update_fields = ["updated_at = :updated_at"]
            params = {
                "task_id": task_id,
                "updated_at": datetime.now()
            }
            
            if status is not None:
                update_fields.append("status = :status")
                params["status"] = status
            
            if process_stage is not None:
                update_fields.append("process_stage = :process_stage")
                params["process_stage"] = process_stage
            
            if total_records is not None:
                update_fields.append("total_records = :total_records")
                params["total_records"] = total_records
            
            if processed_records is not None:
                update_fields.append("processed_records = :processed_records")
                params["processed_records"] = processed_records
            
            if success_records is not None:
                update_fields.append("success_records = :success_records")
                params["success_records"] = success_records
            
            if failed_records is not None:
                update_fields.append("failed_records = :failed_records")
                params["failed_records"] = failed_records
            
            if skipped_records is not None:
                update_fields.append("skipped_records = :skipped_records")
                params["skipped_records"] = skipped_records
            
            if denoised_records is not None:
                update_fields.append("denoised_records = :denoised_records")
                params["denoised_records"] = denoised_records
            
            if duplicate_records is not None:
                update_fields.append("duplicate_records = :duplicate_records")
                params["duplicate_records"] = duplicate_records
            
            if extracted_records is not None:
                update_fields.append("extracted_records = :extracted_records")
                params["extracted_records"] = extracted_records
            
            if analyzed_records is not None:
                update_fields.append("analyzed_records = :analyzed_records")
                params["analyzed_records"] = analyzed_records
            
            if execution_details is not None:
                update_fields.append("execution_details = :execution_details")
                params["execution_details"] = json.dumps(execution_details, ensure_ascii=False)
            
            if performance_stats is not None:
                update_fields.append("performance_stats = :performance_stats")
                params["performance_stats"] = json.dumps(performance_stats, ensure_ascii=False)
            
            sql = f"""
            UPDATE {self.table_name}
            SET {', '.join(update_fields)}
            WHERE task_id = :task_id
            """
            
            result = db.execute(text(sql), params)
            db.commit()
            
            if result.rowcount > 0:
                logger.debug(f"📊 更新任务进度: {task_id}")
                return True
            else:
                logger.warning(f"⚠️ 任务记录不存在: {task_id}")
                return False
                
        except Exception as e:
            logger.error(f"❌ 更新任务进度失败: {task_id}, {e}")
            db.rollback()
            return False
    
    def complete_task(
        self,
        db: Session,
        task_id: str,
        status: str = "completed",
        error_message: Optional[str] = None,
        execution_details: Optional[Dict[str, Any]] = None,
        performance_stats: Optional[Dict[str, Any]] = None
    ) -> bool:
        """完成任务记录"""
        try:
            end_time = datetime.now()
            
            # 先获取开始时间以计算时长
            start_time_sql = f"SELECT start_time FROM {self.table_name} WHERE task_id = :task_id"
            start_result = db.execute(text(start_time_sql), {"task_id": task_id}).fetchone()
            
            if not start_result:
                logger.error(f"❌ 任务记录不存在: {task_id}")
                return False
            
            start_time = start_result[0]
            duration_seconds = int((end_time - start_time).total_seconds())
            
            update_fields = [
                "status = :status",
                "end_time = :end_time", 
                "duration_seconds = :duration_seconds",
                "updated_at = :updated_at"
            ]
            
            params = {
                "task_id": task_id,
                "status": status,
                "end_time": end_time,
                "duration_seconds": duration_seconds,
                "updated_at": end_time
            }
            
            if error_message is not None:
                update_fields.append("error_message = :error_message")
                params["error_message"] = error_message
            
            if execution_details is not None:
                update_fields.append("execution_details = :execution_details")
                params["execution_details"] = json.dumps(execution_details, ensure_ascii=False)
            
            if performance_stats is not None:
                update_fields.append("performance_stats = :performance_stats")
                params["performance_stats"] = json.dumps(performance_stats, ensure_ascii=False)
            
            sql = f"""
            UPDATE {self.table_name}
            SET {', '.join(update_fields)}
            WHERE task_id = :task_id
            """
            
            result = db.execute(text(sql), params)
            db.commit()
            
            if result.rowcount > 0:
                status_icon = "✅" if status == "completed" else "❌"
                logger.info(f"{status_icon} 任务完成: {task_id}, 状态: {status}, 耗时: {duration_seconds}秒")
                return True
            else:
                logger.error(f"❌ 更新任务记录失败: {task_id}")
                return False
                
        except Exception as e:
            logger.error(f"❌ 完成任务记录失败: {task_id}, {e}")
            db.rollback()
            return False
    
    def get_task_record(self, db: Session, task_id: str) -> Optional[Dict[str, Any]]:
        """获取单个任务记录"""
        try:
            sql = f"""
            SELECT 
                id, task_id, task_name, task_type, task_config_key, trigger_type, trigger_user,
                start_time, end_time, duration_seconds, status, process_stage,
                total_records, processed_records, success_records, failed_records,
                skipped_records, denoised_records, duplicate_records,
                extracted_records, analyzed_records, batch_size, max_concurrent,
                error_message, execution_details, performance_stats,
                created_at, updated_at
            FROM {self.table_name}
            WHERE task_id = :task_id
            """
            
            result = db.execute(text(sql), {"task_id": task_id}).fetchone()
            
            if result:
                return self._format_task_record(result)
            return None
            
        except Exception as e:
            logger.error(f"❌ 获取任务记录失败: {task_id}, {e}")
            return None
    
    def get_task_records(
        self,
        db: Session,
        limit: int = 50,
        offset: int = 0,
        task_type: Optional[str] = None,
        status: Optional[str] = None,
        trigger_type: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """获取任务记录列表"""
        try:
            where_conditions = []
            params = {"limit": limit, "offset": offset}
            
            if task_type:
                where_conditions.append("task_type = :task_type")
                params["task_type"] = task_type
            
            if status:
                where_conditions.append("status = :status")
                params["status"] = status
            
            if trigger_type:
                where_conditions.append("trigger_type = :trigger_type")
                params["trigger_type"] = trigger_type
            
            if start_date:
                where_conditions.append("start_time >= :start_date")
                params["start_date"] = start_date
            
            if end_date:
                where_conditions.append("start_time <= :end_date")
                params["end_date"] = end_date
            
            where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
            
            sql = f"""
            SELECT 
                id, task_id, task_name, task_type, task_config_key, trigger_type, trigger_user,
                start_time, end_time, duration_seconds, status, process_stage,
                total_records, processed_records, success_records, failed_records,
                skipped_records, denoised_records, duplicate_records,
                extracted_records, analyzed_records, batch_size, max_concurrent,
                error_message, execution_details, performance_stats,
                created_at, updated_at
            FROM {self.table_name}
            WHERE {where_clause}
            ORDER BY start_time DESC
            LIMIT :limit OFFSET :offset
            """
            
            result = db.execute(text(sql), params)
            
            records = []
            for row in result:
                records.append(self._format_task_record(row))
            
            return records
            
        except Exception as e:
            logger.error(f"❌ 获取任务记录列表失败: {e}")
            return []
    
    def get_task_by_id(self, db: Session, task_id: str) -> Optional[Dict[str, Any]]:
        """根据任务ID获取单个任务记录"""
        try:
            sql = f"""
            SELECT * FROM {self.table_name}
            WHERE task_id = :task_id
            """
            
            result = db.execute(text(sql), {"task_id": task_id})
            row = result.fetchone()
            
            if not row:
                return None
            
            # 解析JSON字段
            execution_details = {}
            performance_stats = {}
            
            if row.execution_details:
                try:
                    execution_details = json.loads(row.execution_details)
                except:
                    execution_details = {"raw": row.execution_details}
            
            if row.performance_stats:
                try:
                    performance_stats = json.loads(row.performance_stats)
                except:
                    performance_stats = {"raw": row.performance_stats}
            
            # 计算完成率（确保不超过100%）
            completion_rate = 0.0
            if row.total_records and row.total_records > 0:
                processed = min(row.processed_records or 0, row.total_records)
                completion_rate = processed / row.total_records * 100
            
            # 计算成功率
            success_rate = 0.0
            if row.processed_records and row.processed_records > 0:
                success_rate = (row.success_records or 0) / row.processed_records * 100
            
            # 计算跳过的记录数（兼容旧数据）
            skipped_records = getattr(row, 'skipped_records', 0) or 0
            denoised_records = getattr(row, 'denoised_records', 0) or 0
            duplicate_records = getattr(row, 'duplicate_records', 0) or 0
            
            # 如果没有跳过记录数据，从总数中计算
            if skipped_records == 0 and row.total_records and (row.success_records or 0) + (row.failed_records or 0) < row.total_records:
                skipped_records = row.total_records - (row.success_records or 0) - (row.failed_records or 0)
            
            # 计算实际处理率和成功率
            actual_processed = (row.success_records or 0) + (row.failed_records or 0)
            actual_completion_rate = 0.0
            if row.total_records and row.total_records > 0:
                actual_completion_rate = actual_processed / row.total_records * 100

            return {
                "id": row.id,
                "task_id": row.task_id,
                "task_name": row.task_name,
                "task_type": row.task_type,
                "trigger_type": row.trigger_type,
                "trigger_user": row.trigger_user,
                "start_time": row.start_time.isoformat() if row.start_time else None,
                "end_time": row.end_time.isoformat() if row.end_time else None,
                "duration_seconds": row.duration_seconds,
                "status": row.status,
                "process_stage": row.process_stage,
                "total_records": row.total_records,
                "processed_records": row.processed_records,
                "success_records": row.success_records,
                "failed_records": row.failed_records,
                "skipped_records": skipped_records,  # 新增：跳过的记录数
                "denoised_records": denoised_records,  # 新增：去燥的记录数
                "duplicate_records": duplicate_records,  # 新增：重复的记录数
                "extracted_records": row.extracted_records,
                "analyzed_records": row.analyzed_records,
                "batch_size": row.batch_size,
                "max_concurrent": row.max_concurrent,
                "error_message": row.error_message,
                "execution_details": execution_details,
                "performance_stats": performance_stats,
                "completion_rate": round(completion_rate, 2),  # 原始完成率
                "actual_completion_rate": round(actual_completion_rate, 2),  # 实际处理完成率
                "success_rate": round(success_rate, 2),
                "created_at": row.created_at.isoformat() if row.created_at else None,
                "updated_at": row.updated_at.isoformat() if row.updated_at else None
            }
            
        except Exception as e:
            logger.error(f"❌ 获取任务记录失败: {task_id}, {e}")
            return None
    
    def get_task_statistics(self, db: Session, days: int = 7) -> Dict[str, Any]:
        """获取任务统计信息"""
        try:
            # 按状态统计
            status_sql = f"""
            SELECT status, COUNT(*) as count
            FROM {self.table_name}
            WHERE start_time >= DATE_SUB(NOW(), INTERVAL :days DAY)
            GROUP BY status
            """
            
            status_result = db.execute(text(status_sql), {"days": days})
            status_stats = {row[0]: row[1] for row in status_result}
            
            # 按任务类型统计
            type_sql = f"""
            SELECT task_type, COUNT(*) as count, 
                   AVG(duration_seconds) as avg_duration,
                   SUM(success_records) as total_success,
                   SUM(failed_records) as total_failed
            FROM {self.table_name}
            WHERE start_time >= DATE_SUB(NOW(), INTERVAL :days DAY)
            GROUP BY task_type
            """
            
            type_result = db.execute(text(type_sql), {"days": days})
            type_stats = {}
            for row in type_result:
                type_stats[row[0]] = {
                    "count": row[1],
                    "avg_duration": float(row[2] or 0),
                    "total_success": row[3] or 0,
                    "total_failed": row[4] or 0
                }
            
            # 最近执行记录
            recent_sql = f"""
            SELECT task_id, task_name, status, start_time, duration_seconds
            FROM {self.table_name}
            ORDER BY start_time DESC
            LIMIT 10
            """
            
            recent_result = db.execute(text(recent_sql))
            recent_tasks = []
            for row in recent_result:
                recent_tasks.append({
                    "task_id": row[0],
                    "task_name": row[1],
                    "status": row[2],
                    "start_time": row[3].isoformat() if row[3] else None,
                    "duration_seconds": row[4]
                })
            
            return {
                "status_statistics": status_stats,
                "type_statistics": type_stats,
                "recent_tasks": recent_tasks,
                "period_days": days
            }
            
        except Exception as e:
            logger.error(f"❌ 获取任务统计失败: {e}")
            return {}
    
    def _format_task_record(self, row) -> Dict[str, Any]:
        """格式化任务记录"""
        try:
            # 解析JSON字段
            execution_details = None
            performance_stats = None
            
            if row.execution_details:
                try:
                    execution_details = json.loads(row.execution_details)
                except:
                    execution_details = {"raw": row.execution_details}
            
            if row.performance_stats:
                try:
                    performance_stats = json.loads(row.performance_stats)
                except:
                    performance_stats = {"raw": row.performance_stats}
            
            # 计算跳过的记录数（兼容旧数据）
            skipped_records = getattr(row, 'skipped_records', 0) or 0
            denoised_records = getattr(row, 'denoised_records', 0) or 0
            duplicate_records = getattr(row, 'duplicate_records', 0) or 0
            
            # 如果没有跳过记录数据，从总数中计算
            if skipped_records == 0 and row.total_records and (row.success_records or 0) + (row.failed_records or 0) < row.total_records:
                skipped_records = row.total_records - (row.success_records or 0) - (row.failed_records or 0)
            
            # 🔥 修复进度计算逻辑：
            # 1. actual_processed: 实际执行过的记录数（成功+失败，不包括跳过）
            # 2. total_processed: 使用数据库中的processed_records字段（如果存在），否则计算所有处理过的记录数
            # 3. completion_rate: 总体完成率，包括跳过的记录
            actual_processed = (row.success_records or 0) + (row.failed_records or 0)
            
            # 🔥 优先使用数据库中的processed_records字段（这是API层正确维护的值）
            if hasattr(row, 'processed_records') and row.processed_records is not None:
                total_processed = row.processed_records
            else:
                # 向后兼容：如果没有processed_records字段，则重新计算
                total_processed = actual_processed + skipped_records
            
            # 计算完成率（包括跳过的记录，确保不超过100%）
            completion_rate = 0.0
            if row.total_records and row.total_records > 0:
                # 🔥 修复：区分抽取阶段和分析阶段的进度计算
                process_stage = getattr(row, 'process_stage', '')
                
                # 如果是抽取阶段或刚完成抽取，且实际处理记录为0，则进度应该是0%
                if (process_stage and ('抽取' in process_stage or '开始分析' in process_stage) 
                    and actual_processed == 0):
                    completion_rate = 0.0
                else:
                    processed = min(total_processed, row.total_records)
                    completion_rate = processed / row.total_records * 100
            elif row.total_records == 0:
                # 🔥 修复：当总数为0时，进度应该是100%（没有任务要处理时认为已完成）
                completion_rate = 100.0
            
            # 计算实际处理完成率（不包括跳过的记录）
            actual_completion_rate = 0.0
            if row.total_records and row.total_records > 0:
                actual_completion_rate = actual_processed / row.total_records * 100
            
            # 计算成功率（基于实际处理的记录）
            success_rate = 0.0
            if actual_processed > 0:
                success_rate = (row.success_records or 0) / actual_processed * 100

            return {
                "id": row.id,
                "task_id": row.task_id,
                "task_name": row.task_name,
                "task_type": row.task_type,
                "task_config_key": getattr(row, 'task_config_key', None),
                "trigger_type": row.trigger_type,
                "trigger_user": row.trigger_user,
                "start_time": row.start_time.isoformat() if row.start_time else None,
                "end_time": row.end_time.isoformat() if row.end_time else None,
                "duration_seconds": row.duration_seconds,
                "status": row.status,
                "process_stage": row.process_stage,
                "total_records": row.total_records,
                "processed_records": total_processed,  # 🔥 修复：使用实际的总处理数（包括跳过）
                "success_records": row.success_records,
                "failed_records": row.failed_records,
                "skipped_records": skipped_records,  # 新增：跳过的记录数
                "denoised_records": denoised_records,  # 新增：去燥的记录数
                "duplicate_records": duplicate_records,  # 新增：重复的记录数
                "extracted_records": row.extracted_records,
                "analyzed_records": row.analyzed_records,
                "batch_size": row.batch_size,
                "max_concurrent": row.max_concurrent,
                "error_message": row.error_message,
                "execution_details": execution_details,
                "performance_stats": performance_stats,
                "completion_rate": round(completion_rate, 2),  # 原始完成率
                "actual_completion_rate": round(actual_completion_rate, 2),  # 实际处理完成率
                "success_rate": round(success_rate, 2),
                "created_at": row.created_at.isoformat() if row.created_at else None,
                "updated_at": row.updated_at.isoformat() if row.updated_at else None
            }
            
        except Exception as e:
            logger.error(f"❌ 格式化任务记录失败: {e}")
            return {"error": "格式化失败"}
    
    def cleanup_old_records(self, db: Session, days_to_keep: int = 30) -> int:
        """清理旧的任务记录"""
        try:
            sql = f"""
            DELETE FROM {self.table_name}
            WHERE start_time < DATE_SUB(NOW(), INTERVAL :days_to_keep DAY)
            AND status IN ('completed', 'failed', 'cancelled')
            """
            
            result = db.execute(text(sql), {"days_to_keep": days_to_keep})
            db.commit()
            
            deleted_count = result.rowcount
            logger.info(f"🧹 清理了 {deleted_count} 条 {days_to_keep} 天前的任务记录")
            
            return deleted_count
            
        except Exception as e:
            logger.error(f"❌ 清理旧任务记录失败: {e}")
            db.rollback()
            return 0


# 全局任务记录实例
task_record = TaskExecutionRecord()
