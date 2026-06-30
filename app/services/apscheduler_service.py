"""
APScheduler 调度器服务 - 替代原有调度器实现
"""
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED
from config.settings import settings
from app.core.master_switch import master_switch

logger = logging.getLogger(__name__)


class APSchedulerService:
    """APScheduler 调度器服务"""
    
    def __init__(self):
        # 配置作业存储（使用现有数据库）
        jobstores = {
            'default': SQLAlchemyJobStore(
                url=settings.database_url, 
                tablename='apscheduler_jobs',
                metadata=None
            )
        }
        
        # 🔥 优化：增加执行器的线程池大小，提升并发处理能力
        import os
        cpu_count = os.cpu_count() or 4
        
        # 根据CPU核数和系统配置动态设置线程池大小
        scheduler_workers = max(20, cpu_count * 3)  # 至少20个，或CPU核数的3倍
        
        # 配置执行器
        executors = {
            'default': ThreadPoolExecutor(max_workers=scheduler_workers),
        }
        
        logger.info(f"🔧 APScheduler执行器配置: {scheduler_workers} 个线程 (CPU核数: {cpu_count})")
        
        # 作业默认配置
        job_defaults = {
            'coalesce': True,  # 合并多个错过的作业执行为一次
            'max_instances': 3,  # 最大实例数
            'misfire_grace_time': 86400 * 7  # 允许7天延迟（604800秒），支持长周期任务
        }
        
        # 创建调度器
        self.scheduler = AsyncIOScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone='Asia/Shanghai'
        )
        
        # 添加事件监听器
        self._setup_event_listeners()
        
        self._running = False
        self._job_configs = {}  # 缓存任务配置
    
    def _setup_event_listeners(self):
        """设置事件监听器"""
        
        def job_executed(event):
            logger.info(f"✅ 任务执行完成: {event.job_id}")
        
        def job_error(event):
            logger.error(f"❌ 任务执行失败: {event.job_id}, 错误: {event.exception}")
        
        def job_missed(event):
            logger.warning(f"⚠️ 任务错过执行: {event.job_id}")
        
        self.scheduler.add_listener(job_executed, EVENT_JOB_EXECUTED)
        self.scheduler.add_listener(job_error, EVENT_JOB_ERROR)
        self.scheduler.add_listener(job_missed, EVENT_JOB_MISSED)
    
    async def start(self):
        """启动调度器"""
        if not self._running:
            try:
                self.scheduler.start()
                self._running = True
                logger.info("🚀 APScheduler 调度器已启动")
                
                # 启动后立即加载任务配置
                await self._load_and_register_tasks()
                
            except Exception as e:
                logger.error(f"❌ APScheduler 启动失败: {e}")
                raise
    
    async def stop(self):
        """停止调度器"""
        if self._running:
            try:
                self.scheduler.shutdown(wait=False)
                self._running = False
                logger.info("⏹️ APScheduler 调度器已停止")
            except Exception as e:
                logger.error(f"❌ APScheduler 停止失败: {e}")
    
    async def _load_and_register_tasks(self):
        """从数据库加载任务配置并注册到APScheduler"""
        try:
            from app.db.connection_manager import get_db_session
            from app.models.task_config import task_config
            
            # 🔥 修复：使用连接管理器防止连接泄漏
            with get_db_session() as db:
                # 获取所有启用的任务配置
                configs = task_config.get_all_tasks(db, enabled_only=True)
                
                for config in configs:
                    await self._register_task(config)
                
                logger.info(f"📋 加载了 {len(configs)} 个启用任务")
                    
        except Exception as e:
            logger.error(f"❌ 加载任务配置失败: {e}")
    
    async def _register_task(self, config: Dict[str, Any]):
        """注册单个任务到APScheduler - 支持cron和interval两种触发器"""
        try:
            task_key = config.get("task_key")
            task_name = config.get("task_name", task_key)
            schedule_cron = config.get("schedule_cron")
            schedule_interval = config.get("schedule_interval", 3600)
            
            # 根据配置选择触发器类型
            job_kwargs = {
                'func': 'app.services.apscheduler_service:execute_scheduled_task',
                'args': [task_key],
                'id': task_key,
                'name': task_name,
                'replace_existing': True
            }
            
            # 🚀 优先使用cron表达式，如果没有则使用interval
            if schedule_cron and schedule_cron.strip():
                # 使用cron触发器
                try:
                    # 验证cron表达式格式（简单验证）
                    cron_parts = schedule_cron.strip().split()
                    if len(cron_parts) != 5:
                        raise ValueError(f"cron表达式必须是5个字段: {schedule_cron}")
                    
                    minute, hour, day, month, day_of_week = cron_parts
                    job_kwargs.update({
                        'trigger': 'cron',
                        'minute': minute,
                        'hour': hour,
                        'day': day,
                        'month': month,
                        'day_of_week': day_of_week,
                        'timezone': 'Asia/Shanghai'
                    })
                    
                    trigger_desc = f"cron({schedule_cron})"
                    logger.info(f"➕ 注册CRON任务: {task_name} - {trigger_desc}")
                    
                except Exception as e:
                    logger.error(f"❌ 无效的cron表达式: {schedule_cron}, 错误: {e}")
                    logger.info(f"🔄 降级使用interval触发器: {schedule_interval}秒")
                    # 降级到interval模式
                    job_kwargs.update({
                        'trigger': 'interval',
                        'seconds': schedule_interval
                    })
                    trigger_desc = f"每{schedule_interval}秒"
            else:
                # 使用interval触发器
                # 🔥 修复：添加 start_date，确保长周期任务正确调度
                # 如果有 next_execution_time，从该时间开始；否则立即开始
                start_time = None
                if config.get("next_execution_time"):
                    try:
                        from datetime import datetime
                        start_time = datetime.fromisoformat(config["next_execution_time"].replace('Z', '+00:00'))
                        logger.info(f"📅 使用已保存的下次执行时间: {start_time}")
                    except:
                        pass

                job_kwargs.update({
                    'trigger': 'interval',
                    'seconds': schedule_interval,
                    'start_date': start_time  # 指定开始时间，确保调度连续性
                })
                trigger_desc = f"每{schedule_interval}秒"
                logger.info(f"➕ 注册间隔任务: {task_name} - {trigger_desc}")
            
            # 注册任务到APScheduler
            self.scheduler.add_job(**job_kwargs)
            
            # 缓存配置
            self._job_configs[task_key] = config
            
            # 🔥 注册后立即同步下次执行时间到数据库（如果可能）
            try:
                from app.db.connection_manager import get_db_session
                
                # 🔥 修复：使用连接管理器防止连接泄漏
                with get_db_session() as db:
                    await self._sync_single_task_time(db, task_key)
            except Exception as e:
                logger.debug(f"注册任务时同步时间失败: {task_key}, {e}")
            
        except Exception as e:
            logger.error(f"❌ 注册任务失败: {config.get('task_key')}, {e}")
    
    async def _execute_task(self, config: Dict[str, Any]):
        """执行具体任务 - 完整流程（抽取前一天数据+分析）"""
        task_key = config.get("task_key")
        task_id = f"APS_{task_key}_{int(datetime.now().timestamp())}"

        try:
            from app.db.connection_manager import get_db_session
            from app.models.task import task_record
            from app.services.stage1_work_extraction import stage1_service
            from app.services.stage2_analysis_service import stage2_service
            from sqlalchemy import text

            # 🔥 计算前一天的时间范围
            yesterday = datetime.now() - timedelta(days=1)
            target_date = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

            logger.info(f"📅 定时任务目标日期: {target_date.strftime('%Y-%m-%d')}")

            # 🔥 修复：使用连接管理器防止连接泄漏
            with get_db_session() as db:
                # 创建任务记录
                db_task_id = task_record.create_task_record(
                    db=db,
                    task_name=config.get("task_name", "APScheduler定时任务（抽取+分析）"),
                    task_type="batch_analysis",
                    trigger_type="scheduled",
                    batch_size=settings.concurrency_analysis_batch_size,
                    max_concurrent=settings.concurrency_analysis_max_concurrent,
                    task_config_key=task_key,
                    execution_details={
                        "description": "APScheduler定时任务：抽取前一天数据+分析",
                        "apscheduler_task_id": task_id,
                        "target_date": target_date.strftime('%Y-%m-%d')
                    }
                )

                logger.info(f"🚀 APScheduler任务开始: {db_task_id}")

                # ========== 阶段1：数据抽取（前一天） ==========
                task_record.update_task_progress(
                    db=db,
                    task_id=db_task_id,
                    process_stage="数据抽取中"
                )

                extraction_result = stage1_service.extract_work_data_by_time_range(
                    db=db,
                    target_date=target_date
                )

                if not extraction_result.get("success"):
                    raise Exception(f"数据抽取失败: {extraction_result.get('message', '未知错误')}")

                stats = extraction_result.get("statistics", {})
                extracted = stats.get("extracted", 0)
                inserted = stats.get("inserted", 0)
                skipped = stats.get("skipped", 0)

                task_record.update_task_progress(
                    db=db,
                    task_id=db_task_id,
                    process_stage="数据抽取完成，开始分析",
                    extracted_records=extracted,
                    skipped_records=skipped,
                    duplicate_records=skipped
                )

                logger.info(f"✅ 阶段1完成: 抽取{extracted}条，插入{inserted}条，跳过重复{skipped}条")

                # ========== 阶段2：批量分析 ==========
                pending_count_sql = f"""
                SELECT COUNT(*) as count
                FROM {stage1_service.pending_table_name}
                WHERE ai_status = 'PENDING'
                """
                pending_result = db.execute(text(pending_count_sql))
                total_pending_orders = pending_result.fetchone()[0]

                task_record.update_task_progress(
                    db=db,
                    task_id=db_task_id,
                    process_stage="批量分析中",
                    total_records=total_pending_orders,
                    processed_records=0
                )

                analysis_result = await stage2_service.process_pending_analysis_queue(db)

                if not analysis_result.get("success"):
                    logger.warning(f"⚠️ 分析阶段有错误: {analysis_result.get('error', '未知')}")

                # 任务完成
                task_record.complete_task(
                    db=db,
                    task_id=db_task_id,
                    status="completed",
                    execution_details={
                        "apscheduler_task_id": task_id,
                        "target_date": target_date.strftime('%Y-%m-%d'),
                        "extraction": {"extracted": extracted, "inserted": inserted, "skipped": skipped},
                        "analysis": analysis_result,
                        "completed_at": datetime.now().isoformat()
                    }
                )

                logger.info(f"✅ APScheduler任务完成: {db_task_id}")

                # 更新任务配置的执行统计
                await self._update_task_stats(db, task_key, success=True)
                    
        except Exception as e:
            logger.error(f"❌ APScheduler任务执行失败: {task_key}, {e}")
            
            # 更新失败统计
            try:
                from app.db.connection_manager import get_db_session
                # 🔥 修复：使用连接管理器防止连接泄漏
                with get_db_session() as db:
                    await self._update_task_stats(db, task_key, success=False)
            except:
                pass
    
    async def _update_task_stats(self, db, task_key: str, success: bool):
        """更新任务执行统计"""
        try:
            from app.models.task_config import task_config
            
            # 获取当前配置
            config = task_config.get_task_by_key(db, task_key)
            if config:
                execution_count = config.get("execution_count", 0) + 1
                
                if success:
                    success_count = config.get("success_count", 0) + 1
                    failure_count = config.get("failure_count", 0)
                else:
                    success_count = config.get("success_count", 0)
                    failure_count = config.get("failure_count", 0) + 1
                
                # 🔥 获取APScheduler中任务的下次执行时间
                next_run_time = None
                try:
                    job = self.scheduler.get_job(task_key)
                    if job and job.next_run_time:
                        next_run_time = job.next_run_time
                except:
                    pass
                
                # 🔥 使用专门的执行统计更新方法，支持时间同步
                success_update = task_config.update_task_execution_stats(
                    db=db,
                    task_key=task_key,
                    last_execution_time=datetime.now(),
                    next_execution_time=next_run_time,
                    success=success
                )
                
                if success_update:
                    next_time_str = next_run_time.isoformat() if next_run_time else "无"
                    logger.debug(f"📊 更新任务统计: {task_key}, 成功:{success_count}, 失败:{failure_count}, 下次执行:{next_time_str}")
                else:
                    logger.warning(f"⚠️ 更新任务统计失败: {task_key}")
                
        except Exception as e:
            logger.error(f"❌ 更新任务统计失败: {task_key}, {e}")
    
    def add_interval_job(self, func, seconds: int, job_id: str, name: str = None, **kwargs):
        """添加间隔任务"""
        try:
            self.scheduler.add_job(
                func=func,
                trigger='interval',
                seconds=seconds,
                id=job_id,
                name=name or job_id,
                replace_existing=True,
                **kwargs
            )
            logger.info(f"➕ 添加间隔任务: {job_id} (每{seconds}秒)")
        except Exception as e:
            logger.error(f"❌ 添加间隔任务失败: {job_id}, {e}")
    
    def add_cron_job(self, func, cron_expression: str, job_id: str, name: str = None, **kwargs):
        """添加CRON任务"""
        try:
            # 解析CRON表达式
            parts = cron_expression.split()
            if len(parts) == 5:
                minute, hour, day, month, day_of_week = parts
                
                self.scheduler.add_job(
                    func=func,
                    trigger='cron',
                    minute=minute,
                    hour=hour,
                    day=day,
                    month=month,
                    day_of_week=day_of_week,
                    id=job_id,
                    name=name or job_id,
                    replace_existing=True,
                    **kwargs
                )
                logger.info(f"➕ 添加CRON任务: {job_id} ({cron_expression})")
            else:
                logger.error(f"❌ 无效的CRON表达式: {cron_expression}")
        except Exception as e:
            logger.error(f"❌ 添加CRON任务失败: {job_id}, {e}")
    
    def remove_job(self, job_id: str):
        """删除任务"""
        try:
            self.scheduler.remove_job(job_id)
            if job_id in self._job_configs:
                del self._job_configs[job_id]
            logger.info(f"➖ 删除任务: {job_id}")
        except Exception as e:
            logger.warning(f"删除任务失败: {job_id}, {e}")
    
    def pause_job(self, job_id: str):
        """暂停任务"""
        try:
            self.scheduler.pause_job(job_id)
            logger.info(f"⏸️ 暂停任务: {job_id}")
        except Exception as e:
            logger.error(f"❌ 暂停任务失败: {job_id}, {e}")
    
    def resume_job(self, job_id: str):
        """恢复任务"""
        try:
            self.scheduler.resume_job(job_id)
            logger.info(f"▶️ 恢复任务: {job_id}")
        except Exception as e:
            logger.error(f"❌ 恢复任务失败: {job_id}, {e}")
    
    def get_jobs(self):
        """获取所有任务"""
        return self.scheduler.get_jobs()
    
    def get_job_status(self, job_id: str):
        """获取任务状态"""
        job = self.scheduler.get_job(job_id)
        if job:
            return {
                "id": job.id,
                "name": job.name,
                "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
                "trigger": str(job.trigger),
                "pending": job.pending if hasattr(job, 'pending') else False
            }
        return None
    
    def get_status(self) -> Dict[str, Any]:
        """获取调度器状态（保持与原有接口兼容）"""
        jobs = self.get_jobs()
        
        # 🔥 智能状态检查：总开关开启但APScheduler未运行时自动修复
        needs_restart = False
        if master_switch.enabled and not self._running:
            needs_restart = True
            logger.warning("🔧 检测到总开关已开启但APScheduler未运行，标记为需要重启")
        
        # 构建任务状态
        configured_tasks = {}
        for job in jobs:
            config = self._job_configs.get(job.id, {})
            trigger_type, trigger_desc = self._analyze_trigger(job.trigger)
            
            # 🔥 使用APScheduler的实时时间，而不是缓存的配置时间
            next_execution_time = job.next_run_time.isoformat() if job.next_run_time else None
            
            configured_tasks[job.id] = {
                "task_name": job.name,
                "status": "生效" if master_switch.enabled else "总开关关闭",
                "is_enabled": True,  # APScheduler中的任务都是启用的
                "trigger_type": trigger_type,  # 🚀 新增：触发器类型（cron/interval）
                "trigger_description": trigger_desc,  # 🚀 新增：触发器描述
                "schedule_interval": self._extract_interval_from_trigger(job.trigger),
                "schedule_cron": config.get("schedule_cron"),  # 🚀 新增：cron表达式
                "last_execution": config.get("last_execution_time"),
                "next_execution": next_execution_time,  # 🔥 使用APScheduler实时时间
                "next_execution_time": next_execution_time,  # 🔥 为兼容性添加此字段
                "execution_count": config.get("execution_count", 0),
                "success_count": config.get("success_count", 0),
                "failure_count": config.get("failure_count", 0)
            }
        
        # 诊断状态
        diagnosis_status = "healthy"
        diagnosis_message = "APScheduler调度器状态正常"
        
        if needs_restart:
            diagnosis_status = "needs_restart"
            diagnosis_message = "总开关已开启但调度器未运行，建议重启调度器"
        
        return {
            "is_running": self._running and master_switch.enabled,
            "scheduler_enabled": True,
            "master_switch_enabled": master_switch.enabled,
            "effective_status": "running" if (self._running and master_switch.enabled) else "stopped",
            "current_time": datetime.now().isoformat(),
            "scheduler_type": "apscheduler_driven",
            "check_interval": 60,  # APScheduler自动管理
            "configured_tasks": configured_tasks,
            "active_tasks": {
                "apscheduler_main": {
                    "done": False,
                    "cancelled": False,
                    "running": self._running
                }
            },
            "task_summary": {
                "total_configured": len(jobs),
                "currently_running": 1 if self._running else 0,
                "enabled_tasks": len(jobs)
            },
            "master_switch": master_switch.get_status(),
            "health_check": {
                "main_scheduler_task_exists": True,
                "main_scheduler_task_running": self._running,
                "status_consistent": True,
                "is_stop_event_set": False,
                "last_enabled_count": len(jobs)
            },
            "diagnosis": {
                "status": diagnosis_status,
                "message": diagnosis_message,
                "needs_restart": needs_restart  # 🔥 新增：是否需要重启标志
            }
        }
    
    def _extract_interval_from_trigger(self, trigger) -> int:
        """从触发器提取间隔时间（秒）"""
        try:
            if hasattr(trigger, 'interval'):
                return int(trigger.interval.total_seconds())
        except:
            pass
        return 3600  # 默认1小时
    
    def _analyze_trigger(self, trigger) -> tuple:
        """分析触发器类型和描述"""
        try:
            trigger_name = trigger.__class__.__name__.lower()
            
            if 'cron' in trigger_name:
                # CRON触发器
                try:
                    # 尝试重构cron表达式
                    fields = []
                    for field_name in ['minute', 'hour', 'day', 'month', 'day_of_week']:
                        field = getattr(trigger, field_name, None)
                        if field is not None:
                            fields.append(str(field))
                        else:
                            fields.append('*')
                    
                    cron_expr = ' '.join(fields)
                    return 'cron', f"CRON: {cron_expr}"
                except:
                    return 'cron', "CRON触发器"
                    
            elif 'interval' in trigger_name:
                # 间隔触发器
                try:
                    if hasattr(trigger, 'interval'):
                        seconds = int(trigger.interval.total_seconds())
                        return 'interval', f"每{self._format_interval_display(seconds)}"
                    return 'interval', "间隔触发器"
                except:
                    return 'interval', "间隔触发器"
                    
            else:
                return 'unknown', str(trigger)
                
        except Exception as e:
            logger.debug(f"分析触发器失败: {e}")
            return 'unknown', "未知触发器"
    
    def _format_interval_display(self, seconds: int) -> str:
        """格式化间隔时间显示"""
        if seconds < 60:
            return f"{seconds}秒"
        elif seconds < 3600:
            minutes = seconds // 60
            return f"{minutes}分钟"
        elif seconds < 86400:
            hours = seconds // 3600
            return f"{hours}小时"
        else:
            days = seconds // 86400
            return f"{days}天"
    
    async def reload_task_configs(self):
        """重新加载任务配置"""
        logger.info("🔄 重新加载任务配置")
        
        # 清除现有任务
        for job in self.get_jobs():
            self.remove_job(job.id)
        
        # 重新加载
        await self._load_and_register_tasks()
    
    async def toggle_task_enabled(self, task_key: str, enabled: bool) -> bool:
        """切换任务启用状态"""
        try:
            if enabled:
                # 从数据库重新加载配置并注册
                from app.db.connection_manager import get_db_session
                from app.models.task_config import task_config
                
                # 🔥 修复：使用连接管理器防止连接泄漏
                with get_db_session() as db:
                    config = task_config.get_task_by_key(db, task_key)
                    if config and config.get("is_enabled"):
                        await self._register_task(config)
                        return True
            else:
                # 暂停任务
                self.pause_job(task_key)
                return True
                
        except Exception as e:
            logger.error(f"❌ 切换任务状态失败: {task_key}, {e}")
            
        return False
    
    async def update_task_config(self, task_key: str, updates: Dict[str, Any]) -> bool:
        """更新任务配置并重新注册到APScheduler"""
        try:
            from app.db.connection_manager import get_db_session
            from app.models.task_config import task_config
            
            # 🔥 修复：使用连接管理器防止连接泄漏
            with get_db_session() as db:
                # 更新数据库中的任务配置
                success = task_config.update_task_config(db, task_key, updates)
                
                if success:
                    # 重新注册任务到APScheduler
                    config = task_config.get_task_by_key(db, task_key)
                    if config and config.get("is_enabled"):
                        # 删除旧任务
                        self.remove_job(task_key)
                        # 重新注册新任务
                        await self._register_task(config)
                        logger.info(f"🔄 任务配置已更新并重新注册: {task_key}")
                        
                        # 🔥 自动同步下次执行时间到数据库
                        await self._sync_single_task_time(db, task_key)
                    
                    return True
                    
        except Exception as e:
            logger.error(f"❌ 更新任务配置失败: {task_key}, {e}")
            
        return False
    
    async def _sync_single_task_time(self, db, task_key: str):
        """同步单个任务的执行时间到数据库"""
        try:
            from app.models.task_config import task_config
            from datetime import datetime
            
            # 获取APScheduler中任务的下次执行时间
            job = self.scheduler.get_job(task_key)
            if job and job.next_run_time:
                # 🔥 使用专门的执行统计更新方法，而不是通用的配置更新方法
                success = task_config.update_task_execution_stats(
                    db=db,
                    task_key=task_key,
                    next_execution_time=job.next_run_time  # 直接传入datetime对象
                )
                
                if success:
                    next_execution_time_str = job.next_run_time.isoformat()
                    logger.info(f"✅ 自动同步任务时间: {task_key} -> {next_execution_time_str}")
                    # 更新内存缓存
                    if task_key in self._job_configs:
                        self._job_configs[task_key]["next_execution_time"] = next_execution_time_str
                else:
                    logger.warning(f"⚠️ 同步任务时间失败: {task_key}")
            else:
                logger.debug(f"📝 任务无下次执行时间: {task_key}")
                
        except Exception as e:
            logger.warning(f"⚠️ 同步单个任务时间失败: {task_key}, {e}")
    
    async def sync_all_task_times(self):
        """同步所有任务的执行时间到数据库"""
        try:
            from app.db.connection_manager import get_db_session
            from app.models.task_config import task_config
            
            # 🔥 修复：使用连接管理器防止连接泄漏
            with get_db_session() as db:
                jobs = self.get_jobs()
                synced_count = 0
                
                for job in jobs:
                    try:
                        if job.next_run_time:
                            # 🔥 使用专门的执行统计更新方法
                            success = task_config.update_task_execution_stats(
                                db=db,
                                task_key=job.id,
                                next_execution_time=job.next_run_time
                            )
                            
                            if success:
                                synced_count += 1
                                next_execution_time = job.next_run_time.isoformat()
                                logger.debug(f"📊 同步任务时间: {job.id} -> {next_execution_time}")
                    except Exception as e:
                        logger.warning(f"⚠️ 同步任务时间失败: {job.id}, {e}")
                
                logger.info(f"🔄 同步完成: {synced_count}/{len(jobs)} 个任务时间已更新")
                return synced_count
                    
        except Exception as e:
            logger.error(f"❌ 同步任务时间失败: {e}")
            return 0


# 全局APScheduler实例
apscheduler_service = APSchedulerService()


# ======================== CRON表达式工具函数 ========================

def validate_cron_expression(cron_expr: str) -> Dict[str, Any]:
    """
    验证cron表达式格式
    返回: {"valid": bool, "message": str, "next_runs": List[str]}
    """
    try:
        if not cron_expr or not cron_expr.strip():
            return {"valid": False, "message": "cron表达式不能为空"}
        
        cron_expr = cron_expr.strip()
        parts = cron_expr.split()
        
        if len(parts) != 5:
            return {
                "valid": False, 
                "message": f"cron表达式必须包含5个字段 (分 时 日 月 周)，当前有{len(parts)}个字段"
            }
        
        minute, hour, day, month, day_of_week = parts
        
        # 基本字段验证
        validations = [
            _validate_cron_field(minute, 0, 59, "分钟"),
            _validate_cron_field(hour, 0, 23, "小时"),
            _validate_cron_field(day, 1, 31, "日"),
            _validate_cron_field(month, 1, 12, "月"),
            _validate_cron_field(day_of_week, 0, 6, "周几")
        ]
        
        for validation in validations:
            if not validation["valid"]:
                return validation
        
        # 尝试创建APScheduler CronTrigger来验证
        try:
            from apscheduler.triggers.cron import CronTrigger
            from datetime import datetime
            
            trigger = CronTrigger(
                minute=minute,
                hour=hour,
                day=day,
                month=month,
                day_of_week=day_of_week,
                timezone='Asia/Shanghai'
            )
            
            # 获取接下来的3次执行时间作为示例
            now = datetime.now()
            next_runs = []
            for i in range(3):
                next_run = trigger.get_next_fire_time(now, now)
                if next_run:
                    next_runs.append(next_run.strftime('%Y-%m-%d %H:%M:%S'))
                    now = next_run
                else:
                    break
            
            return {
                "valid": True,
                "message": "cron表达式格式正确",
                "next_runs": next_runs,
                "description": _describe_cron_expression(cron_expr)
            }
            
        except Exception as e:
            return {"valid": False, "message": f"cron表达式语法错误: {str(e)}"}
            
    except Exception as e:
        return {"valid": False, "message": f"验证失败: {str(e)}"}

def _validate_cron_field(field: str, min_val: int, max_val: int, field_name: str) -> Dict[str, Any]:
    """验证单个cron字段"""
    try:
        if field == '*':
            return {"valid": True}
        
        # 处理范围 (如 1-5)
        if '-' in field:
            try:
                start, end = field.split('-')
                start, end = int(start), int(end)
                if start < min_val or end > max_val or start > end:
                    return {"valid": False, "message": f"{field_name}范围无效: {field}"}
                return {"valid": True}
            except:
                return {"valid": False, "message": f"{field_name}范围格式错误: {field}"}
        
        # 处理步长 (如 */5)
        if '/' in field:
            try:
                base, step = field.split('/')
                if base != '*':
                    base_val = int(base)
                    if base_val < min_val or base_val > max_val:
                        return {"valid": False, "message": f"{field_name}基值超出范围: {base}"}
                step_val = int(step)
                if step_val <= 0:
                    return {"valid": False, "message": f"{field_name}步长必须大于0: {step}"}
                return {"valid": True}
            except:
                return {"valid": False, "message": f"{field_name}步长格式错误: {field}"}
        
        # 处理列表 (如 1,3,5)
        if ',' in field:
            try:
                values = [int(x.strip()) for x in field.split(',')]
                for val in values:
                    if val < min_val or val > max_val:
                        return {"valid": False, "message": f"{field_name}值超出范围: {val}"}
                return {"valid": True}
            except:
                return {"valid": False, "message": f"{field_name}列表格式错误: {field}"}
        
        # 处理单个数值
        try:
            val = int(field)
            if val < min_val or val > max_val:
                return {"valid": False, "message": f"{field_name}值超出范围({min_val}-{max_val}): {val}"}
            return {"valid": True}
        except:
            return {"valid": False, "message": f"{field_name}数值格式错误: {field}"}
            
    except Exception as e:
        return {"valid": False, "message": f"{field_name}验证失败: {str(e)}"}

def _describe_cron_expression(cron_expr: str) -> str:
    """生成cron表达式的中文描述"""
    try:
        minute, hour, day, month, day_of_week = cron_expr.split()
        
        # 简单的描述逻辑
        descriptions = []
        
        if minute == '0' and hour != '*':
            descriptions.append("每小时整点")
        elif minute != '*':
            descriptions.append(f"第{minute}分钟")
        
        if hour != '*':
            if ',' in hour:
                descriptions.append(f"在{hour}点")
            elif '-' in hour:
                descriptions.append(f"在{hour}点之间")
            else:
                descriptions.append(f"在{hour}点")
        
        if day != '*':
            descriptions.append(f"每月{day}号")
        
        if month != '*':
            descriptions.append(f"在{month}月")
        
        if day_of_week != '*':
            weekdays = ['周日', '周一', '周二', '周三', '周四', '周五', '周六']
            if ',' in day_of_week:
                days = [weekdays[int(d)] for d in day_of_week.split(',') if d.isdigit() and 0 <= int(d) <= 6]
                descriptions.append(f"在{','.join(days)}")
            elif day_of_week.isdigit() and 0 <= int(day_of_week) <= 6:
                descriptions.append(f"在{weekdays[int(day_of_week)]}")
        
        if not descriptions:
            return "每分钟执行"
        
        return " ".join(descriptions) + "执行"
        
    except:
        return "自定义时间执行"

# ======================== 可序列化的任务执行函数 ========================

def execute_scheduled_task(task_key: str):
    """
    可序列化的任务执行函数，供APScheduler调用
    这个函数必须是顶级函数，不能是类方法或lambda，以便APScheduler能够序列化
    """
    import asyncio
    from app.core.master_switch import master_switch
    
    logger.info(f"🎯 APScheduler调用任务执行: {task_key}")
    
    # 检查总开关状态
    if not master_switch.enabled:
        logger.debug(f"🔒 总开关关闭，跳过任务: {task_key}")
        return
    
    # 获取任务配置
    config = apscheduler_service._job_configs.get(task_key)
    if not config:
        logger.error(f"❌ 未找到任务配置: {task_key}")
        return
    
    # 在新的事件循环中执行异步任务
    try:
        # APScheduler在线程池中运行，需要创建新的事件循环
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        try:
            new_loop.run_until_complete(apscheduler_service._execute_task(config))
        finally:
            new_loop.close()
    except Exception as e:
        logger.error(f"❌ 执行定时任务失败: {task_key}, {e}")
