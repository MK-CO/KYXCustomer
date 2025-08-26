"""
并发处理管理器 - 解决worker太少导致前端卡顿的问题
"""
import asyncio
import logging
import concurrent.futures
from typing import Any, Callable, Optional
from functools import wraps

from config.settings import settings

logger = logging.getLogger(__name__)


class ConcurrencyManager:
    """并发处理管理器"""
    
    def __init__(self):
        self._thread_pool = None
        self._process_pool = None
        self._initialized = False
    
    def initialize(self):
        """初始化线程池和进程池"""
        if self._initialized:
            return
            
        # 🔥 优化：增加线程池大小，更好地处理I/O密集型任务（如数据库操作、API调用）
        import os
        cpu_count = os.cpu_count() or 4
        
        # 线程池：适合I/O密集型任务，可以设置较大的数量
        thread_workers = max(settings.concurrency_max_workers, cpu_count * 4)  # 至少CPU核数的4倍
        
        self._thread_pool = concurrent.futures.ThreadPoolExecutor(
            max_workers=thread_workers,
            thread_name_prefix="ai-platform-worker"
        )
        
        # 进程池：适合CPU密集型任务，不要超过CPU核数
        process_workers = min(settings.concurrency_background_workers, cpu_count)
        
        self._process_pool = concurrent.futures.ProcessPoolExecutor(
            max_workers=process_workers
        )
        
        self._initialized = True
        logger.info(f"🚀 并发管理器已初始化:")
        logger.info(f"  📊 线程池大小: {thread_workers} (CPU核数: {cpu_count})")
        logger.info(f"  ⚙️ 进程池大小: {process_workers}")
        logger.info(f"  🎯 推荐配置: I/O密集型用线程池，CPU密集型用进程池")
    
    async def run_in_thread(self, func: Callable, *args, **kwargs) -> Any:
        """在线程池中运行I/O密集型任务"""
        if not self._initialized:
            self.initialize()
            
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._thread_pool, func, *args, **kwargs)
    
    async def run_in_process(self, func: Callable, *args, **kwargs) -> Any:
        """在进程池中运行CPU密集型任务"""
        if not self._initialized:
            self.initialize()
            
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._process_pool, func, *args, **kwargs)
    
    def shutdown(self, wait: bool = True):
        """关闭所有executor"""
        if self._thread_pool:
            self._thread_pool.shutdown(wait=wait)
            self._thread_pool = None
            
        if self._process_pool:
            self._process_pool.shutdown(wait=wait)
            self._process_pool = None
            
        self._initialized = False
        logger.info("🛑 并发管理器已关闭")
    
    @property
    def thread_pool_status(self) -> dict:
        """获取线程池状态"""
        if not self._thread_pool:
            return {"initialized": False}
            
        return {
            "initialized": True,
            "max_workers": self._thread_pool._max_workers,
            "active_threads": len(self._thread_pool._threads),
            "pending_tasks": self._thread_pool._work_queue.qsize() if hasattr(self._thread_pool, '_work_queue') else 0
        }


# 全局并发管理器实例
concurrency_manager = ConcurrencyManager()


def run_in_background(func: Callable) -> Callable:
    """装饰器：在后台线程中运行函数，避免阻塞主线程"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        return await concurrency_manager.run_in_thread(func, *args, **kwargs)
    return wrapper


def run_cpu_intensive(func: Callable) -> Callable:
    """装饰器：在进程池中运行CPU密集型函数"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        return await concurrency_manager.run_in_process(func, *args, **kwargs)
    return wrapper


class AsyncTaskManager:
    """异步任务管理器 - 用于管理长时间运行的后台任务"""
    
    def __init__(self):
        self.running_tasks = {}
        self.task_results = {}
    
    async def submit_task(self, task_id: str, coro, timeout: Optional[float] = None):
        """提交异步任务"""
        if task_id in self.running_tasks:
            logger.warning(f"任务 {task_id} 已存在，跳过提交")
            return False
            
        task = asyncio.create_task(coro)
        self.running_tasks[task_id] = task
        
        logger.info(f"📋 提交异步任务: {task_id}")
        
        # 添加任务完成回调
        def task_done_callback(task):
            try:
                if task.cancelled():
                    logger.info(f"⏹ 任务 {task_id} 已取消")
                    result = {"status": "cancelled", "result": None}
                elif task.exception():
                    logger.error(f"❌ 任务 {task_id} 执行失败: {task.exception()}")
                    result = {"status": "failed", "error": str(task.exception())}
                else:
                    logger.info(f"✅ 任务 {task_id} 执行完成")
                    result = {"status": "completed", "result": task.result()}
                    
                self.task_results[task_id] = result
                
            except Exception as e:
                logger.error(f"任务回调处理失败: {e}")
            finally:
                # 清理运行中的任务记录
                self.running_tasks.pop(task_id, None)
        
        task.add_done_callback(task_done_callback)
        return True
    
    def cancel_task(self, task_id: str) -> bool:
        """取消任务"""
        if task_id not in self.running_tasks:
            return False
            
        task = self.running_tasks[task_id]
        return task.cancel()
    
    def get_task_status(self, task_id: str) -> dict:
        """获取任务状态"""
        if task_id in self.running_tasks:
            task = self.running_tasks[task_id]
            return {
                "status": "running",
                "done": task.done(),
                "cancelled": task.cancelled()
            }
        elif task_id in self.task_results:
            return self.task_results[task_id]
        else:
            return {"status": "not_found"}
    
    def cleanup_completed_tasks(self, max_age_hours: int = 24):
        """清理已完成的任务结果"""
        # 简单实现：清理所有已完成的任务
        # 在生产环境中，可以根据时间戳进行更精细的清理
        cleaned_count = len(self.task_results)
        self.task_results.clear()
        logger.info(f"🧹 清理了 {cleaned_count} 个已完成的任务结果")
    
    @property
    def status(self) -> dict:
        """获取任务管理器状态"""
        return {
            "running_tasks": len(self.running_tasks),
            "completed_tasks": len(self.task_results),
            "running_task_ids": list(self.running_tasks.keys())
        }


# 全局异步任务管理器
async_task_manager = AsyncTaskManager()
