"""
去噪记录数据模型
用于管理内容去噪处理的记录和统计信息
"""
import json
import logging
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def safe_json_dumps(obj: Any, ensure_ascii: bool = False) -> str:
    """
    安全的JSON序列化函数，处理datetime、Decimal等不可序列化的对象
    """
    from decimal import Decimal
    
    def json_serializer(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            # 将Decimal转换为float，保持数值精度
            return float(obj)
        raise TypeError(f"Type {type(obj)} not serializable")
    
    return json.dumps(obj, ensure_ascii=ensure_ascii, default=json_serializer)


class DenoiseRecordManager:
    """去噪记录管理器"""
    
    def __init__(self):
        """初始化管理器"""
        self.records_table = "ai_content_denoise_records"
        self.batch_table = "ai_denoise_batch_statistics"
        self.denoise_version = "v1.0"
    
    def generate_batch_id(self) -> str:
        """生成批次ID"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        return f"batch_{timestamp}_{unique_id}"
    
    def create_batch_record(
        self,
        db: Session,
        batch_id: str,
        total_work_orders: int
    ) -> bool:
        """创建批次记录"""
        try:
            sql = f"""
            INSERT INTO {self.batch_table} (
                batch_id, total_work_orders, processed_work_orders,
                total_original_comments, total_filtered_comments, total_removed_comments,
                overall_filter_rate, processing_start_time, denoise_version, status
            ) VALUES (
                :batch_id, :total_work_orders, 0,
                0, 0, 0,
                0.00, :start_time, :version, 'PROCESSING'
            )
            """
            
            db.execute(text(sql), {
                "batch_id": batch_id,
                "total_work_orders": total_work_orders,
                "start_time": datetime.now(),
                "version": self.denoise_version
            })
            db.commit()
            
            logger.info(f"✅ 创建批次记录: {batch_id}, 工单数: {total_work_orders}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 创建批次记录失败: {e}")
            db.rollback()
            return False
    
    def save_work_order_denoise_record(
        self,
        db: Session,
        work_id: int,
        batch_id: str,
        denoise_result: Dict[str, Any],
        processing_time_ms: Optional[int] = None
    ) -> bool:
        """保存单个工单的去噪记录"""
        try:
            # 计算过滤率
            original_count = denoise_result.get("original_count", 0)
            filtered_count = denoise_result.get("filtered_count", 0)
            removed_count = denoise_result.get("removed_count", 0)
            filter_rate = (removed_count / original_count * 100) if original_count > 0 else 0.0
            
            # 提取过滤原因和详细信息
            filter_statistics = denoise_result.get("filter_statistics", {})
            filter_reasons = filter_statistics.get("filter_reasons", {})
            removed_details = filter_statistics.get("removed_details", [])
            
            sql = f"""
            INSERT INTO {self.records_table} (
                work_id, batch_id, original_comment_count, filtered_comment_count,
                removed_comment_count, filter_rate, filter_reasons, removed_details,
                processing_time_ms, denoise_version
            ) VALUES (
                :work_id, :batch_id, :original_count, :filtered_count,
                :removed_count, :filter_rate, :filter_reasons, :removed_details,
                :processing_time_ms, :version
            )
            """
            
            db.execute(text(sql), {
                "work_id": work_id,
                "batch_id": batch_id,
                "original_count": original_count,
                "filtered_count": filtered_count,
                "removed_count": removed_count,
                "filter_rate": round(filter_rate, 2),
                "filter_reasons": safe_json_dumps(filter_reasons, ensure_ascii=False) if filter_reasons else None,
                "removed_details": safe_json_dumps(removed_details, ensure_ascii=False) if removed_details else None,
                "processing_time_ms": processing_time_ms,
                "version": self.denoise_version
            })
            
            logger.debug(f"📋 保存工单 {work_id} 去噪记录: {original_count} -> {filtered_count}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 保存工单 {work_id} 去噪记录失败: {e}")
            return False
    
    def update_batch_statistics(
        self,
        db: Session,
        batch_id: str,
        statistics: Dict[str, Any],
        status: str = "COMPLETED",
        error_message: Optional[str] = None
    ) -> bool:
        """更新批次统计信息"""
        try:
            processing_time_ms = statistics.get("total_processing_time_ms")
            
            sql = f"""
            UPDATE {self.batch_table}
            SET 
                processed_work_orders = :processed_orders,
                total_original_comments = :original_comments,
                total_filtered_comments = :filtered_comments,
                total_removed_comments = :removed_comments,
                overall_filter_rate = :filter_rate,
                global_filter_reasons = :filter_reasons,
                processing_end_time = :end_time,
                total_processing_time_ms = :processing_time_ms,
                status = :status,
                error_message = :error_message,
                updated_at = :updated_at
            WHERE batch_id = :batch_id
            """
            
            db.execute(text(sql), {
                "batch_id": batch_id,
                "processed_orders": statistics.get("total_work_orders", 0),
                "original_comments": statistics.get("total_original_comments", 0),
                "filtered_comments": statistics.get("total_filtered_comments", 0),
                "removed_comments": statistics.get("total_removed_comments", 0),
                "filter_rate": round(statistics.get("overall_filter_rate", 0.0), 2),
                "filter_reasons": safe_json_dumps(statistics.get("filter_reasons", {}), ensure_ascii=False),
                "end_time": datetime.now(),
                "processing_time_ms": processing_time_ms,
                "status": status,
                "error_message": error_message,
                "updated_at": datetime.now()
            })
            db.commit()
            
            logger.info(f"✅ 更新批次统计: {batch_id}, 状态: {status}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 更新批次统计失败: {e}")
            db.rollback()
            return False
    
    def get_batch_statistics(
        self,
        db: Session,
        batch_id: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """获取批次统计信息"""
        try:
            where_clause = "WHERE batch_id = :batch_id" if batch_id else ""
            
            sql = f"""
            SELECT 
                id, batch_id, total_work_orders, processed_work_orders,
                total_original_comments, total_filtered_comments, total_removed_comments,
                overall_filter_rate, global_filter_reasons,
                processing_start_time, processing_end_time, total_processing_time_ms,
                denoise_version, status, error_message, created_at
            FROM {self.batch_table}
            {where_clause}
            ORDER BY created_at DESC
            LIMIT :limit
            """
            
            params = {"limit": limit}
            if batch_id:
                params["batch_id"] = batch_id
            
            result = db.execute(text(sql), params)
            
            statistics = []
            for row in result:
                stat = {
                    "id": row.id,
                    "batch_id": row.batch_id,
                    "total_work_orders": row.total_work_orders,
                    "processed_work_orders": row.processed_work_orders,
                    "total_original_comments": row.total_original_comments,
                    "total_filtered_comments": row.total_filtered_comments,
                    "total_removed_comments": row.total_removed_comments,
                    "overall_filter_rate": float(row.overall_filter_rate),
                    "global_filter_reasons": json.loads(row.global_filter_reasons) if row.global_filter_reasons else {},
                    "processing_start_time": row.processing_start_time,
                    "processing_end_time": row.processing_end_time,
                    "total_processing_time_ms": row.total_processing_time_ms,
                    "denoise_version": row.denoise_version,
                    "status": row.status,
                    "error_message": row.error_message,
                    "created_at": row.created_at
                }
                statistics.append(stat)
            
            logger.info(f"📊 获取到 {len(statistics)} 条批次统计记录")
            return statistics
            
        except Exception as e:
            logger.error(f"❌ 获取批次统计失败: {e}")
            return []
    
    def get_work_order_denoise_records(
        self,
        db: Session,
        work_id: Optional[int] = None,
        batch_id: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """获取工单去噪记录"""
        try:
            where_conditions = []
            params = {"limit": limit}
            
            if work_id:
                where_conditions.append("work_id = :work_id")
                params["work_id"] = work_id
            
            if batch_id:
                where_conditions.append("batch_id = :batch_id")
                params["batch_id"] = batch_id
            
            where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            sql = f"""
            SELECT 
                id, work_id, batch_id, original_comment_count, filtered_comment_count,
                removed_comment_count, filter_rate, filter_reasons, removed_details,
                processing_time_ms, denoise_version, created_at
            FROM {self.records_table}
            {where_clause}
            ORDER BY created_at DESC
            LIMIT :limit
            """
            
            result = db.execute(text(sql), params)
            
            records = []
            for row in result:
                record = {
                    "id": row.id,
                    "work_id": row.work_id,
                    "batch_id": row.batch_id,
                    "original_comment_count": row.original_comment_count,
                    "filtered_comment_count": row.filtered_comment_count,
                    "removed_comment_count": row.removed_comment_count,
                    "filter_rate": float(row.filter_rate),
                    "filter_reasons": json.loads(row.filter_reasons) if row.filter_reasons else {},
                    "removed_details": json.loads(row.removed_details) if row.removed_details else [],
                    "processing_time_ms": row.processing_time_ms,
                    "denoise_version": row.denoise_version,
                    "created_at": row.created_at
                }
                records.append(record)
            
            logger.info(f"📋 获取到 {len(records)} 条工单去噪记录")
            return records
            
        except Exception as e:
            logger.error(f"❌ 获取工单去噪记录失败: {e}")
            return []
    
    def get_denoise_summary(self, db: Session, days: int = 7) -> Dict[str, Any]:
        """获取去噪统计摘要"""
        try:
            # 计算时间范围
            end_date = datetime.now()
            start_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0)
            start_date = start_date.replace(day=start_date.day - days + 1)
            
            # 批次统计
            batch_sql = f"""
            SELECT 
                COUNT(*) as total_batches,
                SUM(total_work_orders) as total_work_orders,
                SUM(total_original_comments) as total_original_comments,
                SUM(total_filtered_comments) as total_filtered_comments,
                SUM(total_removed_comments) as total_removed_comments,
                AVG(overall_filter_rate) as avg_filter_rate
            FROM {self.batch_table}
            WHERE created_at >= :start_date
            AND status = 'COMPLETED'
            """
            
            batch_result = db.execute(text(batch_sql), {"start_date": start_date}).fetchone()
            
            # 工单记录统计
            record_sql = f"""
            SELECT 
                COUNT(*) as total_records,
                AVG(filter_rate) as avg_work_order_filter_rate,
                MAX(filter_rate) as max_filter_rate,
                MIN(filter_rate) as min_filter_rate
            FROM {self.records_table}
            WHERE created_at >= :start_date
            """
            
            record_result = db.execute(text(record_sql), {"start_date": start_date}).fetchone()
            
            # 热门过滤原因统计
            reasons_sql = f"""
            SELECT filter_reasons
            FROM {self.records_table}
            WHERE created_at >= :start_date
            AND filter_reasons IS NOT NULL
            """
            
            reasons_result = db.execute(text(reasons_sql), {"start_date": start_date})
            
            # 聚合过滤原因
            all_reasons = {}
            for row in reasons_result:
                if row.filter_reasons:
                    reasons = json.loads(row.filter_reasons)
                    for reason, count in reasons.items():
                        all_reasons[reason] = all_reasons.get(reason, 0) + count
            
            # 排序获取前10个热门原因
            top_reasons = sorted(all_reasons.items(), key=lambda x: x[1], reverse=True)[:10]
            
            summary = {
                "time_range": {
                    "start_date": start_date.strftime("%Y-%m-%d"),
                    "end_date": end_date.strftime("%Y-%m-%d"),
                    "days": days
                },
                "batch_statistics": {
                    "total_batches": batch_result.total_batches or 0,
                    "total_work_orders": batch_result.total_work_orders or 0,
                    "total_original_comments": batch_result.total_original_comments or 0,
                    "total_filtered_comments": batch_result.total_filtered_comments or 0,
                    "total_removed_comments": batch_result.total_removed_comments or 0,
                    "avg_filter_rate": round(float(batch_result.avg_filter_rate or 0), 2)
                },
                "record_statistics": {
                    "total_records": record_result.total_records or 0,
                    "avg_work_order_filter_rate": round(float(record_result.avg_work_order_filter_rate or 0), 2),
                    "max_filter_rate": round(float(record_result.max_filter_rate or 0), 2),
                    "min_filter_rate": round(float(record_result.min_filter_rate or 0), 2)
                },
                "top_filter_reasons": [{"reason": reason, "count": count} for reason, count in top_reasons]
            }
            
            logger.info(f"📊 生成去噪统计摘要: {days}天内 {summary['batch_statistics']['total_batches']} 个批次")
            return summary
            
        except Exception as e:
            logger.error(f"❌ 获取去噪统计摘要失败: {e}")
            return {
                "error": str(e),
                "time_range": {"days": days},
                "batch_statistics": {},
                "record_statistics": {},
                "top_filter_reasons": []
            }


# 全局去噪记录管理器实例
denoise_record_manager = DenoiseRecordManager()
