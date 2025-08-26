"""
系统相关API端点 - 简化版
"""
import logging
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)

from app.db.database import get_db
from app.services.apscheduler_service import apscheduler_service
from app.core.concurrency import concurrency_manager, async_task_manager
from app.core.auth import get_current_user
from app.core.master_switch import master_switch  # 🔥 新增：独立的总开关管理器

router = APIRouter(prefix="/system", tags=["system"])


@router.get("/health")
async def health_check(db: Session = Depends(get_db)):
    """
    健康检查
    """
    try:
        # 检查数据库连接
        db.execute(text("SELECT 1"))
        db_status = "healthy"
    except Exception as e:
        db_status = f"error: {str(e)}"
    
    # 🚀 使用APScheduler状态检查
    scheduler_detailed_status = apscheduler_service.get_status()
    
    # 基于APScheduler状态和总开关确定显示状态
    master_switch = scheduler_detailed_status.get("master_switch_enabled", True)
    effective_status = scheduler_detailed_status.get("effective_status", "stopped")
    
    if not master_switch:
        scheduler_status = "disabled"  # 总开关关闭
    elif scheduler_detailed_status.get("diagnosis", {}).get("status") == "inconsistent":
        scheduler_status = "inconsistent"  # 状态不一致
    else:
        scheduler_status = effective_status  # 使用实际有效状态
    
    health_status = {
        "status": "healthy" if db_status == "healthy" else "unhealthy",
        "timestamp": datetime.now().isoformat(),
        "components": {
            "database": db_status,
            "scheduler": scheduler_status
        }
    }
    
    # 如果有组件不健康，返回503状态码
    if health_status["status"] == "unhealthy":
        raise HTTPException(status_code=503, detail=health_status)
    
    return health_status


@router.get("/scheduler/status")
async def get_scheduler_status(current_user: dict = Depends(get_current_user)):
    """
    获取调度器状态 - APScheduler版本
    """
    try:
        # 🔥 使用APScheduler替代原有调度器
        status = apscheduler_service.get_status()
        return {
            "success": True,
            "scheduler_status": status
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取调度器状态失败: {str(e)}")


@router.post("/scheduler/start")
async def start_scheduler(current_user: dict = Depends(get_current_user)):
    """
    启动调度器 - APScheduler版本
    """
    try:
        if apscheduler_service._running:
            return {
                "success": True,
                "message": "APScheduler调度器已在运行中"
            }
        
        await apscheduler_service.start()
        return {
            "success": True,
            "message": "APScheduler调度器已启动，自动执行配置的分析任务"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"启动调度器失败: {str(e)}")


@router.post("/scheduler/stop")
async def stop_scheduler(current_user: dict = Depends(get_current_user)):
    """
    停止调度器 - APScheduler版本
    """
    try:
        await apscheduler_service.stop()
        return {
            "success": True,
            "message": "APScheduler调度器已停止"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"停止调度器失败: {str(e)}")


@router.get("/scheduler/health")
async def get_scheduler_health(current_user: dict = Depends(get_current_user)):
    """
    获取调度器健康状态和诊断信息
    """
    try:
        status = apscheduler_service.get_status()
        return {
            "success": True,
            "scheduler_health": {
                "is_running": status["is_running"],
                "health_check": status.get("health_check", {}),
                "diagnosis": status.get("diagnosis", {}),
                "task_summary": status.get("task_summary", {}),
                "enabled_tasks": status.get("task_summary", {}).get("enabled_tasks", 0),
                "recommendations": _get_scheduler_recommendations(status)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取调度器健康状态失败: {str(e)}")


@router.get("/scheduler/master-switch")
async def get_master_switch_status(current_user: dict = Depends(get_current_user)):
    """
    获取调度器总开关状态 - 支持调度器停止时查看状态
    """
    try:
        switch_status = master_switch.get_status()  # 🔥 使用独立管理器
        
        return {
            "success": True,
            "master_switch": switch_status,
            "message": f"调度器总开关当前为：{switch_status['status_text']}",
            "note": "总开关独立运行，调度器停止时也可查看和修改"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取调度器总开关状态失败: {str(e)}")


@router.post("/scheduler/master-switch/enable")
async def enable_master_switch(current_user: dict = Depends(get_current_user)):
    """
    开启调度器总开关 - 智能启动调度器
    """
    try:
        username = current_user.get("username", "unknown")
        success = master_switch.enable()  # 🔥 使用独立管理器
        
        if success:
            logger.info(f"🔓 用户 {username} 开启了调度器总开关")
            
            # 🚀 智能启动：检查APScheduler状态并自动启动
            scheduler_started = False
            auto_restart_attempted = False
            
            # 🔥 使用APScheduler：如果未运行则启动
            if not apscheduler_service._running:
                try:
                    await apscheduler_service.start()
                    scheduler_started = True
                    auto_restart_attempted = True
                    logger.info(f"🚀 开启总开关时自动启动APScheduler调度器 (用户: {username})")
                except Exception as e:
                    logger.error(f"❌ 自动启动APScheduler调度器失败: {e}")
                    auto_restart_attempted = True
            
            # 检查最终状态
            main_scheduler_running = apscheduler_service._running
            
            message = "调度器总开关已开启"
            if scheduler_started:
                message += " 并自动启动APScheduler调度器"
            elif auto_restart_attempted and not main_scheduler_running:
                message += " 但APScheduler启动失败，请检查日志"
            
            return {
                "success": True,
                "message": message,
                "master_switch": master_switch.get_status(),
                "scheduler_auto_started": scheduler_started,
                "scheduler_running": main_scheduler_running,
                "auto_restart_attempted": auto_restart_attempted,
                "note": "智能开关：总开关开启时会自动启动APScheduler调度器"
            }
        else:
            raise Exception("设置总开关失败")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"开启调度器总开关失败: {str(e)}")


@router.post("/scheduler/master-switch/disable")
async def disable_master_switch(current_user: dict = Depends(get_current_user)):
    """
    关闭调度器总开关 - 智能控制（不自动停止调度器）
    """
    try:
        username = current_user.get("username", "unknown")
        success = master_switch.disable()  # 🔥 使用独立管理器
        
        if success:
            logger.info(f"🔒 用户 {username} 关闭了调度器总开关")
            main_scheduler_running = apscheduler_service._running
            
            return {
                "success": True,
                "message": "调度器总开关已关闭",
                "master_switch": master_switch.get_status(),
                "scheduler_running": main_scheduler_running,
                "note": "总开关已关闭，APScheduler将不会执行任务但继续监控。如需完全停止，请调用停止调度器API。"
            }
        else:
            raise Exception("设置总开关失败")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"关闭调度器总开关失败: {str(e)}")


@router.post("/scheduler/master-switch/toggle")
async def toggle_master_switch(current_user: dict = Depends(get_current_user)):
    """
    切换调度器总开关状态 - 支持调度器停止时操作
    """
    try:
        username = current_user.get("username", "unknown")
        toggle_result = master_switch.toggle()  # 🔥 使用独立管理器
        
        if toggle_result["success"]:
            action = toggle_result["action"]
            logger.info(f"🔄 用户 {username} 切换调度器总开关: {action}")
            return {
                "success": True,
                "message": f"调度器总开关已{action}",
                "master_switch": master_switch.get_status(),
                "previous_state": toggle_result["previous_state"],
                "new_state": toggle_result["new_state"],
                "note": f"总开关已{action}，调度器停止时也可正常切换"
            }
        else:
            raise Exception("切换总开关失败")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"切换调度器总开关失败: {str(e)}")


def _get_scheduler_recommendations(status: dict) -> list:
    """根据调度器状态提供建议"""
    recommendations = []
    
    # 检查是否有状态不一致
    diagnosis = status.get("diagnosis", {})
    if diagnosis.get("status") == "inconsistent":
        recommendations.append("⚠️ 调度器状态不一致，建议重启调度器")
    elif diagnosis.get("status") == "auto_fixed":
        recommendations.append("✅ 状态不一致问题已自动修复")
    
    # 检查是否有启用的任务
    enabled_tasks = status.get("task_summary", {}).get("enabled_tasks", 0)
    if enabled_tasks == 0:
        recommendations.append("💡 没有启用的任务，可以在任务配置中启用需要的任务")
    
    # 检查主调度任务状态
    health_check = status.get("health_check", {})
    if not health_check.get("main_scheduler_task_exists"):
        recommendations.append("❌ 主调度任务不存在，建议重启调度器")
    elif not health_check.get("main_scheduler_task_running"):
        recommendations.append("⚠️ 主调度任务已停止，建议检查日志或重启调度器")
    
    if not recommendations:
        recommendations.append("✅ 调度器运行正常")
    
    return recommendations


# ======================== 性能监控API ========================

@router.get("/performance/status")
async def get_performance_status(current_user: dict = Depends(get_current_user)):
    """
    获取系统性能状态
    """
    try:
        # 获取并发管理器状态
        thread_pool_status = concurrency_manager.thread_pool_status
        
        # 获取异步任务管理器状态
        task_manager_status = async_task_manager.status
        
        # 🚀 获取APScheduler状态
        scheduler_status = apscheduler_service.get_status()
        
        performance_status = {
            "timestamp": datetime.now().isoformat(),
            "concurrency": {
                "thread_pool": thread_pool_status,
                "async_tasks": task_manager_status
            },
            "scheduler": {
                "is_running": scheduler_status["is_running"],
                "master_switch_enabled": scheduler_status.get("master_switch_enabled", True),  # 🔥 新增：总开关状态
                "effective_status": scheduler_status.get("effective_status", "stopped"),  # 🔥 新增：实际状态
                "active_tasks": scheduler_status["task_summary"]["currently_running"],
                "enabled_tasks": scheduler_status["task_summary"]["enabled_tasks"],
                # 🔥 新增：状态诊断信息
                "health_check": scheduler_status.get("health_check", {}),
                "diagnosis": scheduler_status.get("diagnosis", {}),
                "master_switch": scheduler_status.get("master_switch", {})  # 🔥 新增：总开关详情
            },
            "memory_info": {
                "description": "内存使用情况需要额外监控工具"
            }
        }
        
        return {
            "success": True,
            "performance_status": performance_status
        }
        
    except Exception as e:
        logger.error(f"获取性能状态失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取性能状态失败: {str(e)}")


@router.get("/performance/tasks")
async def get_running_tasks(current_user: dict = Depends(get_current_user)):
    """
    获取当前运行的后台任务
    """
    try:
        task_manager_status = async_task_manager.status
        
        # 获取每个运行中任务的详细状态
        running_tasks_detail = {}
        for task_id in task_manager_status["running_task_ids"]:
            task_status = async_task_manager.get_task_status(task_id)
            running_tasks_detail[task_id] = task_status
        
        return {
            "success": True,
            "running_tasks": running_tasks_detail,
            "summary": {
                "total_running": task_manager_status["running_tasks"],
                "total_completed": task_manager_status["completed_tasks"]
            }
        }
        
    except Exception as e:
        logger.error(f"获取运行任务失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取运行任务失败: {str(e)}")


@router.post("/performance/tasks/{task_id}/cancel")
async def cancel_background_task(
    task_id: str,
    current_user: dict = Depends(get_current_user)
):
    """
    取消指定的后台任务
    """
    try:
        success = async_task_manager.cancel_task(task_id)
        
        if success:
            return {
                "success": True,
                "message": f"任务 {task_id} 已取消"
            }
        else:
            return {
                "success": False,
                "message": f"任务 {task_id} 不存在或无法取消"
            }
            
    except Exception as e:
        logger.error(f"取消任务失败: {e}")
        raise HTTPException(status_code=500, detail=f"取消任务失败: {str(e)}")


@router.post("/performance/cleanup")
async def cleanup_completed_tasks(current_user: dict = Depends(get_current_user)):
    """
    清理已完成的任务记录
    """
    try:
        async_task_manager.cleanup_completed_tasks()
        
        return {
            "success": True,
            "message": "已清理完成的任务记录"
        }
        
    except Exception as e:
        logger.error(f"清理任务记录失败: {e}")
        raise HTTPException(status_code=500, detail=f"清理任务记录失败: {str(e)}")


# 调度器现在完全由数据库任务配置自动控制，无需手动切换



