"""
任务配置数据模型
"""
import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from sqlalchemy import text
from sqlalchemy.orm import Session

import logging

logger = logging.getLogger(__name__)


class TaskConfig:
    """任务配置模型"""
    
    def __init__(self):
        self.table_name = "ai_task_configs"
    
    def get_all_tasks(self, db: Session, enabled_only: bool = False) -> List[Dict[str, Any]]:
        """获取所有任务配置"""
        try:
            where_clause = "WHERE is_enabled = TRUE" if enabled_only else ""
            
            sql = f"""
            SELECT 
                id, task_key, task_name, task_description, task_type, is_enabled,
                schedule_interval, schedule_cron, max_concurrent, default_batch_size,
                task_handler, task_params, priority, timeout_seconds, retry_times,
                last_execution_time, next_execution_time, execution_count, 
                success_count, failure_count, created_by, created_at, updated_at
            FROM {self.table_name}
            {where_clause}
            ORDER BY priority ASC, task_name ASC
            """
            
            result = db.execute(text(sql))
            
            tasks = []
            for row in result:
                tasks.append(self._format_task_config(row))
            
            return tasks
            
        except Exception as e:
            logger.error(f"❌ 获取任务配置列表失败: {e}")
            return []
    
    def get_task_by_key(self, db: Session, task_key: str) -> Optional[Dict[str, Any]]:
        """根据任务键获取任务配置"""
        try:
            sql = f"""
            SELECT 
                id, task_key, task_name, task_description, task_type, is_enabled,
                schedule_interval, schedule_cron, max_concurrent, default_batch_size,
                task_handler, task_params, priority, timeout_seconds, retry_times,
                last_execution_time, next_execution_time, execution_count, 
                success_count, failure_count, created_by, created_at, updated_at
            FROM {self.table_name}
            WHERE task_key = :task_key
            """
            
            result = db.execute(text(sql), {"task_key": task_key})
            row = result.fetchone()
            
            if row:
                return self._format_task_config(row)
            return None
            
        except Exception as e:
            logger.error(f"❌ 获取任务配置失败: {task_key}, {e}")
            return None
    
    def get_enabled_scheduled_tasks(self, db: Session) -> List[Dict[str, Any]]:
        """获取启用的定时任务"""
        try:
            sql = f"""
            SELECT 
                id, task_key, task_name, task_description, task_type, is_enabled,
                schedule_interval, schedule_cron, max_concurrent, default_batch_size,
                task_handler, task_params, priority, timeout_seconds, retry_times,
                last_execution_time, next_execution_time, execution_count, 
                success_count, failure_count, created_by, created_at, updated_at
            FROM {self.table_name}
            WHERE is_enabled = TRUE 
            AND task_type IN ('scheduled', 'both')
            ORDER BY priority ASC, task_name ASC
            """
            
            result = db.execute(text(sql))
            
            tasks = []
            for row in result:
                tasks.append(self._format_task_config(row))
            
            return tasks
            
        except Exception as e:
            logger.error(f"❌ 获取启用的定时任务失败: {e}")
            return []
    
    def update_task_enabled_status(self, db: Session, task_key: str, is_enabled: bool) -> bool:
        """更新任务启用状态"""
        try:
            sql = f"""
            UPDATE {self.table_name}
            SET is_enabled = :is_enabled, updated_at = CURRENT_TIMESTAMP
            WHERE task_key = :task_key
            """
            
            result = db.execute(text(sql), {
                "task_key": task_key,
                "is_enabled": is_enabled
            })
            db.commit()
            
            if result.rowcount > 0:
                status_text = "启用" if is_enabled else "禁用"
                logger.info(f"✅ 任务 {task_key} 已{status_text}")
                return True
            else:
                logger.warning(f"⚠️ 任务不存在: {task_key}")
                return False
                
        except Exception as e:
            logger.error(f"❌ 更新任务状态失败: {task_key}, {e}")
            db.rollback()
            return False
    
    def update_task_execution_stats(
        self,
        db: Session,
        task_key: str,
        last_execution_time: Optional[datetime] = None,
        next_execution_time: Optional[datetime] = None,
        success: Optional[bool] = None
    ) -> bool:
        """更新任务执行统计"""
        try:
            update_fields = ["updated_at = CURRENT_TIMESTAMP"]
            params = {"task_key": task_key}
            
            if last_execution_time:
                update_fields.append("last_execution_time = :last_execution_time")
                params["last_execution_time"] = last_execution_time
            
            if next_execution_time:
                update_fields.append("next_execution_time = :next_execution_time")
                params["next_execution_time"] = next_execution_time
            
            # 只有在success不为None时才更新计数
            if success is not None:
                # 更新执行次数
                update_fields.append("execution_count = execution_count + 1")
                
                # 更新成功/失败次数
                if success:
                    update_fields.append("success_count = success_count + 1")
                else:
                    update_fields.append("failure_count = failure_count + 1")
            
            sql = f"""
            UPDATE {self.table_name}
            SET {', '.join(update_fields)}
            WHERE task_key = :task_key
            """
            
            result = db.execute(text(sql), params)
            db.commit()
            
            if result.rowcount > 0:
                status_msg = ""
                if success is True:
                    status_msg = " - 成功"
                elif success is False:
                    status_msg = " - 失败"
                logger.debug(f"📊 更新任务执行统计: {task_key}{status_msg}")
                return True
            else:
                logger.warning(f"⚠️ 任务不存在: {task_key}")
                return False
                
        except Exception as e:
            logger.error(f"❌ 更新任务执行统计失败: {task_key}, {e}")
            db.rollback()
            return False
    
    def update_task_config(
        self,
        db: Session,
        task_key: str,
        updates: Dict[str, Any]
    ) -> bool:
        """更新任务配置"""
        try:
            if not updates:
                return True
            
            update_fields = []
            params = {"task_key": task_key}
            
            # 允许更新的字段
            allowed_fields = {
                'task_name', 'task_description', 'task_type', 'is_enabled',
                'schedule_interval', 'schedule_cron', 'max_concurrent', 
                'default_batch_size', 'task_params', 'priority', 
                'timeout_seconds', 'retry_times'
            }
            
            for field, value in updates.items():
                if field in allowed_fields:
                    update_fields.append(f"{field} = :{field}")
                    
                    # 处理JSON字段
                    if field == 'task_params' and isinstance(value, dict):
                        params[field] = json.dumps(value, ensure_ascii=False)
                    else:
                        params[field] = value
            
            if not update_fields:
                logger.warning(f"⚠️ 没有有效的更新字段: {task_key}")
                return False
            
            update_fields.append("updated_at = CURRENT_TIMESTAMP")
            
            sql = f"""
            UPDATE {self.table_name}
            SET {', '.join(update_fields)}
            WHERE task_key = :task_key
            """
            
            result = db.execute(text(sql), params)
            db.commit()
            
            if result.rowcount > 0:
                logger.info(f"✅ 更新任务配置: {task_key}")
                return True
            else:
                logger.warning(f"⚠️ 任务不存在: {task_key}")
                return False
                
        except Exception as e:
            logger.error(f"❌ 更新任务配置失败: {task_key}, {e}")
            db.rollback()
            return False
    
    def create_task_config(
        self,
        db: Session,
        task_key: str,
        task_name: str,
        task_description: str,
        task_handler: str,
        task_type: str = "scheduled",
        is_enabled: bool = True,
        schedule_interval: int = 3600,
        **kwargs
    ) -> bool:
        """创建任务配置"""
        try:
            # 检查任务是否已存在
            existing = self.get_task_by_key(db, task_key)
            if existing:
                logger.warning(f"⚠️ 任务已存在: {task_key}")
                return False
            
            # 准备参数
            params = {
                "task_key": task_key,
                "task_name": task_name,
                "task_description": task_description,
                "task_type": task_type,
                "is_enabled": is_enabled,
                "schedule_interval": schedule_interval,
                "task_handler": task_handler,
                "schedule_cron": kwargs.get("schedule_cron"),
                "max_concurrent": kwargs.get("max_concurrent", 1),
                "default_batch_size": kwargs.get("default_batch_size", 50),
                "priority": kwargs.get("priority", 5),
                "timeout_seconds": kwargs.get("timeout_seconds", 3600),
                "retry_times": kwargs.get("retry_times", 0),
                "created_by": kwargs.get("created_by", "system")
            }
            
            # 处理task_params
            task_params = kwargs.get("task_params", {})
            if task_params:
                params["task_params"] = json.dumps(task_params, ensure_ascii=False)
            else:
                params["task_params"] = None
            
            sql = f"""
            INSERT INTO {self.table_name} (
                task_key, task_name, task_description, task_type, is_enabled,
                schedule_interval, schedule_cron, max_concurrent, default_batch_size,
                task_handler, task_params, priority, timeout_seconds, retry_times,
                created_by, created_at, updated_at
            ) VALUES (
                :task_key, :task_name, :task_description, :task_type, :is_enabled,
                :schedule_interval, :schedule_cron, :max_concurrent, :default_batch_size,
                :task_handler, :task_params, :priority, :timeout_seconds, :retry_times,
                :created_by, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
            """
            
            db.execute(text(sql), params)
            db.commit()
            
            logger.info(f"✅ 创建任务配置: {task_key}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 创建任务配置失败: {task_key}, {e}")
            db.rollback()
            return False
    
    def delete_task_config(self, db: Session, task_key: str) -> bool:
        """删除任务配置"""
        try:
            sql = f"DELETE FROM {self.table_name} WHERE task_key = :task_key"
            
            result = db.execute(text(sql), {"task_key": task_key})
            db.commit()
            
            if result.rowcount > 0:
                logger.info(f"✅ 删除任务配置: {task_key}")
                return True
            else:
                logger.warning(f"⚠️ 任务不存在: {task_key}")
                return False
                
        except Exception as e:
            logger.error(f"❌ 删除任务配置失败: {task_key}, {e}")
            db.rollback()
            return False
    
    def get_tasks_due_for_execution(self, db: Session, current_time: datetime = None) -> List[Dict[str, Any]]:
        """获取需要执行的任务"""
        try:
            if not current_time:
                current_time = datetime.now()
            
            sql = f"""
            SELECT 
                id, task_key, task_name, task_description, task_type, is_enabled,
                schedule_interval, schedule_cron, max_concurrent, default_batch_size,
                task_handler, task_params, priority, timeout_seconds, retry_times,
                last_execution_time, next_execution_time, execution_count, 
                success_count, failure_count, created_by, created_at, updated_at
            FROM {self.table_name}
            WHERE is_enabled = TRUE 
            AND task_type IN ('scheduled', 'both')
            AND (
                next_execution_time IS NULL 
                OR next_execution_time <= :current_time
                OR last_execution_time IS NULL
                OR TIMESTAMPDIFF(SECOND, last_execution_time, :current_time) >= schedule_interval
            )
            ORDER BY priority ASC, task_name ASC
            """
            
            result = db.execute(text(sql), {"current_time": current_time})
            
            tasks = []
            for row in result:
                task_config = self._format_task_config(row)
                
                # 计算下次执行时间
                if task_config["last_execution_time"]:
                    last_time = datetime.fromisoformat(task_config["last_execution_time"].replace('Z', '+00:00'))
                    next_time = last_time + timedelta(seconds=task_config["schedule_interval"])
                    
                    # 如果还没到执行时间，跳过
                    if next_time > current_time:
                        continue
                
                tasks.append(task_config)
            
            return tasks
            
        except Exception as e:
            logger.error(f"❌ 获取待执行任务失败: {e}")
            return []
    
    def _format_task_config(self, row) -> Dict[str, Any]:
        """格式化任务配置"""
        try:
            # 解析JSON字段
            task_params = {}
            if row.task_params:
                try:
                    task_params = json.loads(row.task_params)
                except:
                    task_params = {"raw": row.task_params}
            
            return {
                "id": row.id,
                "task_key": row.task_key,
                "task_name": row.task_name,
                "task_description": row.task_description,
                "task_type": row.task_type,
                "is_enabled": bool(row.is_enabled),
                "status": "生效" if bool(row.is_enabled) else "不生效",  # 简化状态显示
                "schedule_interval": row.schedule_interval,
                "schedule_cron": row.schedule_cron,
                "max_concurrent": row.max_concurrent,
                "default_batch_size": row.default_batch_size,
                "task_handler": row.task_handler,
                "task_params": task_params,
                "priority": row.priority,
                "timeout_seconds": row.timeout_seconds,
                "retry_times": row.retry_times,
                "last_execution_time": row.last_execution_time.isoformat() if row.last_execution_time else None,
                "next_execution_time": row.next_execution_time.isoformat() if row.next_execution_time else None,
                "execution_count": row.execution_count,
                "success_count": row.success_count,
                "failure_count": row.failure_count,
                # 移除了success_rate字段
                "created_by": row.created_by,
                "created_at": row.created_at.isoformat() if row.created_at else None,
                "updated_at": row.updated_at.isoformat() if row.updated_at else None
            }
            
        except Exception as e:
            logger.error(f"❌ 格式化任务配置失败: {e}")
            return {"error": "格式化失败"}


# 全局任务配置实例
task_config = TaskConfig()
