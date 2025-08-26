"""
任务管理API接口
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.db.database import get_db
from app.core.auth import get_current_user
from app.models.task import task_record
from app.services.apscheduler_service import apscheduler_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tasks", tags=["任务管理"])


@router.get("/records", summary="获取任务记录列表")
async def get_task_records(
    limit: int = Query(50, description="每页记录数", ge=1, le=200),
    offset: int = Query(0, description="偏移量", ge=0),
    task_type: Optional[str] = Query(None, description="任务类型筛选"),
    status: Optional[str] = Query(None, description="状态筛选"),
    trigger_type: Optional[str] = Query(None, description="触发类型筛选"),
    start_date: Optional[str] = Query(None, description="开始日期筛选 (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="结束日期筛选 (YYYY-MM-DD)"),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    获取任务执行记录列表
    
    - **limit**: 每页记录数 (1-200)
    - **offset**: 偏移量
    - **task_type**: 任务类型筛选 (batch_analysis, manual_analysis, cleanup等)
    - **status**: 状态筛选 (running, completed, failed, cancelled)
    - **trigger_type**: 触发类型筛选 (scheduled, manual)
    - **start_date**: 开始日期筛选
    - **end_date**: 结束日期筛选
    """
    try:
        # 解析日期参数
        start_datetime = None
        end_datetime = None
        
        if start_date:
            try:
                start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(status_code=400, detail="开始日期格式错误，请使用 YYYY-MM-DD 格式")
        
        if end_date:
            try:
                end_datetime = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
            except ValueError:
                raise HTTPException(status_code=400, detail="结束日期格式错误，请使用 YYYY-MM-DD 格式")
        
        # 获取任务记录
        records = task_record.get_task_records(
            db=db,
            limit=limit,
            offset=offset,
            task_type=task_type,
            status=status,
            trigger_type=trigger_type,
            start_date=start_datetime,
            end_date=end_datetime
        )
        
        # 🔥 为每个任务记录添加终止状态标识
        enhanced_records = []
        for record in records:
            enhanced_record = record.copy()
            
            # 添加是否可以终止的标识
            task_status = record.get("status", "")
            enhanced_record["can_terminate"] = task_status in ["running", "pending"]
            
            # 添加状态显示友好文本
            status_display_map = {
                "running": "运行中",
                "completed": "已完成", 
                "failed": "失败",
                "cancelled": "已取消",
                "pending": "等待中"
            }
            enhanced_record["status_display"] = status_display_map.get(task_status, task_status)
            
            enhanced_records.append(enhanced_record)
        
        return {
            "success": True,
            "data": enhanced_records,
            "pagination": {
                "limit": limit,
                "offset": offset,
                "total": len(records)
            },
            "filters": {
                "task_type": task_type,
                "status": status,
                "trigger_type": trigger_type,
                "start_date": start_date,
                "end_date": end_date
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取任务记录失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取任务记录失败: {str(e)}")


@router.get("/records/{task_id}", summary="获取单个任务记录详情")
async def get_task_record_detail(
    task_id: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """获取指定任务的详细记录"""
    try:
        record = task_record.get_task_record(db, task_id)
        
        if not record:
            raise HTTPException(status_code=404, detail=f"任务记录不存在: {task_id}")
        
        # 🔥 为任务记录添加终止状态标识
        enhanced_record = record.copy()
        task_status = record.get("status", "")
        enhanced_record["can_terminate"] = task_status in ["running", "pending"]
        
        # 添加状态显示友好文本
        status_display_map = {
            "running": "运行中",
            "completed": "已完成", 
            "failed": "失败",
            "cancelled": "已取消",
            "pending": "等待中"
        }
        enhanced_record["status_display"] = status_display_map.get(task_status, task_status)
        
        return {
            "success": True,
            "data": enhanced_record
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取任务记录详情失败: {task_id}, {e}")
        raise HTTPException(status_code=500, detail=f"获取任务记录详情失败: {str(e)}")


@router.get("/status/{task_id}", summary="获取任务实时状态 - 供前端轮询使用")
async def get_task_status(
    task_id: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    获取任务实时状态 - 专为前端轮询设计
    
    **返回**: 
    - **status**: 任务状态 (running, completed, failed, cancelled, pending)
    - **progress**: 进度信息 (百分比、已处理数量等)
    - **stage**: 当前执行阶段
    - **message**: 状态描述信息
    - **can_terminate**: 是否可以终止
    - **last_update**: 最后更新时间
    """
    try:
        record = task_record.get_task_record(db, task_id)
        
        if not record:
            raise HTTPException(status_code=404, detail=f"任务不存在: {task_id}")
        
        task_status = record.get("status", "")
        process_stage = record.get("process_stage", "")
        
        # 计算进度百分比
        total_records = record.get("total_records", 0)
        processed_records = record.get("processed_records", 0)
        
        progress_percentage = 0
        if total_records > 0:
            progress_percentage = min(100, round((processed_records / total_records) * 100, 1))
        elif task_status == "completed":
            progress_percentage = 100
        
        # 构建进度信息
        progress_info = {
            "percentage": progress_percentage,
            "total_records": total_records,
            "processed_records": processed_records,
            "success_records": record.get("success_records", 0),
            "failed_records": record.get("failed_records", 0),
            "extracted_records": record.get("extracted_records", 0),
            "analyzed_records": record.get("analyzed_records", 0),
            "skipped_records": record.get("skipped_records", 0)
        }
        
        # 生成状态消息
        status_messages = {
            "running": f"正在执行 - {process_stage}" if process_stage else "正在执行",
            "completed": f"执行完成 - 处理了{processed_records}条记录",
            "failed": f"执行失败 - {record.get('error_message', '未知错误')}",
            "cancelled": f"已取消 - {record.get('error_message', '用户取消')}",
            "pending": "等待执行"
        }
        
        # 检查异步任务管理器中的状态
        from app.core.concurrency import async_task_manager
        async_task_status = None
        
        # 尝试从执行详情中获取async_task_id
        execution_details = record.get("execution_details", {})
        if isinstance(execution_details, str):
            import json
            try:
                execution_details = json.loads(execution_details)
            except:
                execution_details = {}
        
        # 检查是否有对应的异步任务
        async_task_id = None
        for running_task_id in async_task_manager.running_tasks.keys():
            if task_id in running_task_id:
                async_task_id = running_task_id
                break
        
        if async_task_id:
            async_task_status = async_task_manager.get_task_status(async_task_id)
        
        return {
            "success": True,
            "task_id": task_id,
            "status": task_status,
            "status_display": {
                "running": "运行中",
                "completed": "已完成", 
                "failed": "失败",
                "cancelled": "已取消",
                "pending": "等待中"
            }.get(task_status, task_status),
            "stage": process_stage,
            "progress": progress_info,
            "message": status_messages.get(task_status, f"状态: {task_status}"),
            "can_terminate": task_status in ["running", "pending"],
            "is_active": task_status in ["running", "pending"],
            "last_update": record.get("updated_at"),
            "created_at": record.get("created_at"),
            "async_task_info": {
                "async_task_id": async_task_id,
                "async_status": async_task_status
            } if async_task_id else None,
            "execution_details": execution_details
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取任务状态失败: {task_id}, {e}")
        raise HTTPException(status_code=500, detail=f"获取任务状态失败: {str(e)}")


@router.get("/statistics", summary="获取任务统计信息")
async def get_task_statistics(
    days: int = Query(7, description="统计天数", ge=1, le=90),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """获取任务执行统计信息"""
    try:
        stats = task_record.get_task_statistics(db, days)
        
        return {
            "success": True,
            "data": stats,
            "generated_at": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"获取任务统计失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取任务统计失败: {str(e)}")


@router.post("/manual-analysis", summary="手动执行分析任务 - 后台异步执行")
async def run_manual_analysis(
    limit: int = Query(200, description="分析记录数限制", ge=1, le=2000),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    手动触发分析任务 - 后台异步执行，立即返回任务ID
    
    - **limit**: 分析记录数限制 (1-2000)
    """
    try:
        # 检查是否有正在运行的任务
        running_tasks = task_record.get_task_records(
            db=db,
            limit=10,
            status="running"
        )
        
        if running_tasks:
            return {
                "success": False,
                "message": "有任务正在运行中，请等待完成后再执行",
                "running_tasks": [{"task_id": t["task_id"], "task_name": t["task_name"]} for t in running_tasks]
            }
        
        # 获取当前用户信息
        username = current_user.get("username", "unknown")
        
        logger.info(f"🔧 用户 {username} 手动触发分析任务，限制: {limit} 条")
        
        # 🔥 创建任务记录
        main_task_id = task_record.create_task_record(
            db=db,
            task_name="手动执行分析任务",
            task_type="manual_analysis",
            trigger_type="manual",
            trigger_user=username,
            task_config_key="customer_service_analysis",
            execution_details={
                "description": "手动触发的分析任务",
                "analysis_limit": limit,
                "requested_by": username
            }
        )
        
        # 🔥 提交到后台异步执行，立即返回任务ID
        from app.core.concurrency import async_task_manager
        import uuid
        
        async_task_id = f"manual_analysis_{uuid.uuid4().hex[:16]}"
        success = await async_task_manager.submit_task(
            async_task_id,
            run_manual_analysis_async(db, limit, username, main_task_id)
        )
        
        if not success:
            # 任务提交失败，更新数据库记录
            task_record.complete_task(
                db=db,
                task_id=main_task_id,
                status="failed", 
                error_message="任务提交到后台队列失败"
            )
            raise HTTPException(status_code=500, detail="任务提交失败")
        
        # 立即返回任务ID，不等待完成
        return {
            "success": True,
            "task_id": main_task_id,
            "async_task_id": async_task_id,
            "status": "submitted",
            "message": f"手动分析任务已提交到后台执行，限制: {limit} 条",
            "analysis_limit": limit,
            "check_status_url": f"/api/v1/tasks/status/{main_task_id}"
        }
        
    except Exception as e:
        logger.error(f"手动执行分析任务失败: {e}")
        raise HTTPException(status_code=500, detail=f"手动执行分析任务失败: {str(e)}")


@router.post("/manual-extraction", summary="手动执行数据抽取任务 - 后台异步执行")
async def run_manual_extraction(
    target_date: Optional[str] = Query(None, description="目标日期 (YYYY-MM-DD)，默认为昨天"),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    手动触发数据抽取任务 - 后台异步执行，立即返回任务ID
    
    - **target_date**: 目标日期，格式为 YYYY-MM-DD，默认为昨天
    """
    try:
        # 解析目标日期
        if target_date:
            try:
                target_datetime = datetime.strptime(target_date, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(status_code=400, detail="日期格式错误，请使用 YYYY-MM-DD 格式")
        else:
            target_datetime = datetime.now() - timedelta(days=1)
        
        # 获取当前用户信息
        username = current_user.get("username", "unknown")
        
        # 创建任务记录
        task_id = task_record.create_task_record(
            db=db,
            task_name="手动执行数据抽取任务",
            task_type="manual_extraction",
            trigger_type="manual",
            trigger_user=username,
            task_config_key="customer_service_analysis",
            execution_details={
                "description": "手动触发的数据抽取任务",
                "target_date": target_date or target_datetime.strftime("%Y-%m-%d"),
                "requested_by": username
            }
        )
        
        logger.info(f"🔧 用户 {username} 手动触发数据抽取任务: {task_id}, 目标日期: {target_datetime.date()}")
        
        # 🔥 提交到后台异步执行，立即返回任务ID
        from app.core.concurrency import async_task_manager
        import uuid
        
        async_task_id = f"manual_extraction_{uuid.uuid4().hex[:16]}"
        success = await async_task_manager.submit_task(
            async_task_id,
            run_manual_extraction_async(db, target_datetime, username, task_id)
        )
        
        if not success:
            # 任务提交失败，更新数据库记录
            task_record.complete_task(
                db=db,
                task_id=task_id,
                status="failed", 
                error_message="任务提交到后台队列失败"
            )
            raise HTTPException(status_code=500, detail="任务提交失败")
        
        # 立即返回任务ID，不等待完成
        return {
            "success": True,
            "task_id": task_id,
            "async_task_id": async_task_id,
            "status": "submitted",
            "message": f"数据抽取任务已提交到后台执行",
            "target_date": target_datetime.strftime("%Y-%m-%d"),
            "check_status_url": f"/api/v1/tasks/status/{task_id}"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"手动执行数据抽取任务失败: {e}")
        raise HTTPException(status_code=500, detail=f"手动执行数据抽取任务失败: {str(e)}")


@router.get("/scheduler/status", summary="获取调度器状态")
async def get_scheduler_status(
    current_user: dict = Depends(get_current_user)
):
    """获取APScheduler调度器状态"""
    try:
        status = apscheduler_service.get_status()
        
        return {
            "success": True,
            "data": status
        }
        
    except Exception as e:
        logger.error(f"获取调度器状态失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取调度器状态失败: {str(e)}")


@router.post("/cleanup", summary="清理旧任务记录")
async def cleanup_old_records(
    days_to_keep: int = Query(30, description="保留天数", ge=7, le=180),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """清理旧的任务记录"""
    try:
        username = current_user.get("username", "unknown")
        logger.info(f"🧹 用户 {username} 触发清理旧任务记录，保留 {days_to_keep} 天")
        
        deleted_count = task_record.cleanup_old_records(db, days_to_keep)
        
        return {
            "success": True,
            "message": f"成功清理 {deleted_count} 条 {days_to_keep} 天前的任务记录",
            "deleted_count": deleted_count,
            "days_to_keep": days_to_keep
        }
        
    except Exception as e:
        logger.error(f"清理旧任务记录失败: {e}")
        raise HTTPException(status_code=500, detail=f"清理旧任务记录失败: {str(e)}")


@router.post("/full-task", summary="执行完整任务流程 - 后台异步执行")
async def run_full_task(
    target_date: Optional[str] = Query(None, description="目标日期 (YYYY-MM-DD)，默认为昨天"),
    analysis_limit: int = Query(200, description="分析记录数限制", ge=1, le=2000),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    执行完整任务流程：先抽取后分析
    
    - **target_date**: 目标日期，格式为 YYYY-MM-DD，默认为昨天
    - **analysis_limit**: 分析记录数限制 (1-2000)
    """
    try:
        # 解析目标日期
        if target_date:
            try:
                target_datetime = datetime.strptime(target_date, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(status_code=400, detail="日期格式错误，请使用 YYYY-MM-DD 格式")
        else:
            target_datetime = datetime.now() - timedelta(days=1)
        
        # 获取当前用户信息
        username = current_user.get("username", "unknown")
        
        # 创建主任务记录
        main_task_id = task_record.create_task_record(
            db=db,
            task_name="完整任务流程（抽取+分析）",
            task_type="batch_analysis",
            trigger_type="manual",
            trigger_user=username,
            task_config_key="customer_service_analysis",
            execution_details={
                "description": "完整任务流程：先抽取后分析",
                "target_date": target_date or target_datetime.strftime("%Y-%m-%d"),
                "analysis_limit": analysis_limit,
                "requested_by": username
            }
        )
        
        logger.info(f"🚀 用户 {username} 触发完整任务流程: {main_task_id}")
        
        # 提交到后台异步执行，立即返回任务ID
        from app.core.concurrency import async_task_manager
        import uuid
        
        async_task_id = f"full_task_{uuid.uuid4().hex[:16]}"
        success = await async_task_manager.submit_task(
            async_task_id,
            run_full_task_async(db, target_datetime, analysis_limit, username, main_task_id)
        )
        
        if not success:
            # 任务提交失败，更新数据库记录
            task_record.complete_task(
                db=db,
                task_id=main_task_id,
                status="failed", 
                error_message="任务提交到后台队列失败"
            )
            raise HTTPException(status_code=500, detail="任务提交失败")
        
        # 立即返回任务ID，不等待完成
        return {
            "success": True,
            "task_id": main_task_id,
            "async_task_id": async_task_id,
            "status": "submitted",
            "message": f"完整任务流程已提交到后台执行",
            "target_date": target_datetime.strftime("%Y-%m-%d"),
            "analysis_limit": analysis_limit,
            "check_status_url": f"/api/v1/tasks/status/{main_task_id}"
        }
        

        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"完整任务流程启动失败: {e}")
        raise HTTPException(status_code=500, detail=f"完整任务流程启动失败: {str(e)}")


@router.post("/full-task-range", summary="执行完整任务流程（自定义时间范围+循环分析）- 后台异步执行")
async def run_full_task_range(
    start_time: str = Query(..., description="开始时间 (YYYY-MM-DDTHH:MM:SS)"),
    end_time: str = Query(..., description="结束时间 (YYYY-MM-DDTHH:MM:SS)"),
    loop_analysis: bool = Query(True, description="是否循环分析直到完成"),
    batch_size: int = Query(200, description="分析批次大小", ge=1, le=1000),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """
    执行完整任务流程：自定义时间范围抽取+循环分析 - 后台异步执行
    
    - **start_time**: 开始时间，格式为 YYYY-MM-DDTHH:MM:SS
    - **end_time**: 结束时间，格式为 YYYY-MM-DDTHH:MM:SS
    - **loop_analysis**: 是否循环分析（True=无限循环直到完成，False=仅处理1轮）
    - **batch_size**: 每批次分析的记录数 (建议50-200)
    
    **🔥 性能优化**: 
    - 已修复阻塞问题：采用异步分批处理，避免长时间阻塞
    - loop_analysis=true: 无限循环处理，直到所有PENDING数据分析完成
    - loop_analysis=false: 单批次处理，立即返回，适合快速处理场景
    
    **返回**: 立即返回任务ID，可通过 `/api/v1/tasks/status/{task_id}` 查询进度
    """
    try:
        # 解析时间
        try:
            start_datetime = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            end_datetime = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
        except ValueError:
            raise HTTPException(status_code=400, detail="时间格式错误，请使用 YYYY-MM-DDTHH:MM:SS 格式")
        
        if start_datetime >= end_datetime:
            raise HTTPException(status_code=400, detail="开始时间必须早于结束时间")
        
        # 获取当前用户信息
        username = current_user.get("username", "unknown")
        
        # 计算时间跨度
        duration_hours = round((end_datetime - start_datetime).total_seconds() / 3600, 2)
        
        # 创建主任务记录
        main_task_id = task_record.create_task_record(
            db=db,
            task_name="完整任务流程（自定义范围+循环分析）",
            task_type="batch_analysis",
            trigger_type="manual",
            trigger_user=username,
            task_config_key="customer_service_analysis",
            execution_details={
                "description": "完整任务流程：自定义时间范围抽取+循环分析",
                "start_time": start_time,
                "end_time": end_time,
                "duration_hours": duration_hours,
                "loop_analysis": loop_analysis,
                "batch_size": batch_size,
                "requested_by": username
            }
        )
        
        logger.info(f"🚀 用户 {username} 触发完整任务流程: {main_task_id}, 时间范围: {start_time} ~ {end_time}")
        
        # 🔥 修复：提交到后台异步执行，立即返回任务ID
        from app.core.concurrency import async_task_manager
        import uuid
        
        async_task_id = f"full_task_range_{uuid.uuid4().hex[:16]}"
        success = await async_task_manager.submit_task(
            async_task_id,
            run_full_task_range_async(db, start_datetime, end_datetime, loop_analysis, batch_size, username, main_task_id)
        )
        
        if not success:
            # 任务提交失败，更新数据库记录
            task_record.complete_task(
                db=db,
                task_id=main_task_id,
                status="failed", 
                error_message="任务提交到后台队列失败"
            )
            raise HTTPException(status_code=500, detail="任务提交失败")
        
        # 立即返回任务ID，不等待完成
        return {
            "success": True,
            "task_id": main_task_id,
            "async_task_id": async_task_id,
            "status": "submitted",
            "message": f"完整任务流程已提交到后台执行",
            "time_range": f"{start_time} ~ {end_time}",
            "duration_hours": duration_hours,
            "loop_analysis": loop_analysis,
            "batch_size": batch_size,
            "check_status_url": f"/api/v1/tasks/status/{main_task_id}"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"完整任务流程启动失败: {e}")
        raise HTTPException(status_code=500, detail=f"完整任务流程启动失败: {str(e)}")


async def run_full_task_range_async(
    db: Session, 
    start_datetime: datetime, 
    end_datetime: datetime, 
    loop_analysis: bool, 
    batch_size: int, 
    username: str, 
    main_task_id: str
):
    """异步执行完整任务流程（自定义时间范围+循环分析）"""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"🚀 开始异步完整任务流程: {main_task_id}, 用户: {username}")
        
        # 阶段1：固定次数分批数据抽取
        task_record.update_task_progress(
            db=db,
            task_id=main_task_id,
            process_stage="开始数据抽取 - 查询总数量"
        )
        
        from app.services.stage1_work_extraction import stage1_service
        
        # 重构：先查询总数量，然后固定次数循环
        async def async_extract_in_batches():
            import asyncio
            batch_size = 1000
            current_offset = 0
            total_extracted = 0
            total_inserted = 0
            total_skipped = 0
            
            logger.info(f"📊 重构后抽取配置: 每批{batch_size}条，先查询总数量再固定循环")
            
            # 1. 先查询需要抽取的工单总数量
            try:
                current_year = start_datetime.year
                work_table_name = f"t_work_{current_year}"
                
                count_sql = f"""
                SELECT COUNT(*) as total_count
                FROM {work_table_name}
                WHERE create_time >= :start_time 
                AND create_time < :end_time
                AND deleted = 0
                AND state = 'FINISH'
                """
                
                logger.info(f"🔍 查询工单总数量，时间范围: {start_datetime} ~ {end_datetime}")
                count_result = db.execute(text(count_sql), {
                    "start_time": start_datetime,
                    "end_time": end_datetime
                })
                total_count = count_result.fetchone()[0]
                
                logger.info(f"📊 查询到需要抽取的工单总数: {total_count}条")
                
                if total_count == 0:
                    logger.info("⚠️ 没有需要抽取的工单")
                    return {
                        "success": True,
                        "stage": "数据抽取",
                        "statistics": {"extracted": 0, "inserted": 0, "skipped": 0, "batches_processed": 0},
                        "message": "没有需要抽取的工单"
                    }
                
                # 2. 计算需要的循环次数
                total_batches = (total_count + batch_size - 1) // batch_size  # 向上取整
                logger.info(f"📊 计算批次数: 总计{total_count}条 ÷ {batch_size}条/批 = {total_batches}批次")
                
            except Exception as e:
                logger.error(f"❌ 查询工单总数失败: {e}")
                return {
                    "success": False,
                    "stage": "数据抽取",
                    "error": str(e),
                    "message": "查询工单总数失败"
                }
            
            # 3. 固定次数循环抽取
            for batch_num in range(1, total_batches + 1):
                logger.info(f"🔄 开始第{batch_num}/{total_batches}批抽取 (偏移: {current_offset})")
                
                # 更新任务进度
                task_record.update_task_progress(
                    db=db,
                    task_id=main_task_id,
                    process_stage=f"数据抽取中 - 第{batch_num}/{total_batches}批",
                    extracted_records=total_extracted
                )
                
                try:
                    # 分批抽取工单数据
                    batch_orders = stage1_service.extract_work_orders_by_time_range(
                        db=db,
                        start_time=start_datetime,
                        end_time=end_datetime,
                        limit=batch_size,
                        offset=current_offset
                    )
                    
                    if not batch_orders:
                        logger.info(f"✅ 第{batch_num}批无数据，提前完成")
                        break
                    
                    # 分批插入待处理表
                    insertion_result = stage1_service.insert_pending_analysis_records(
                        db=db, 
                        work_orders=batch_orders
                    )
                    
                    batch_inserted = insertion_result.get("inserted", 0)
                    batch_skipped = insertion_result.get("skipped", 0)
                    
                    total_extracted += len(batch_orders)
                    total_inserted += batch_inserted
                    total_skipped += batch_skipped
                    current_offset += len(batch_orders)
                    
                    logger.info(f"📈 第{batch_num}/{total_batches}批完成: 抽取{len(batch_orders)}条，插入{batch_inserted}条，跳过{batch_skipped}条")
                        
                except Exception as e:
                    logger.error(f"❌ 第{batch_num}批抽取失败: {e}")
                    continue
                
                # 异步让出执行权，避免长时间阻塞
                await asyncio.sleep(0.2)
            
            return {
                "success": True,
                "stage": "固定次数数据抽取",
                "statistics": {
                    "extracted": total_extracted,
                    "inserted": total_inserted, 
                    "skipped": total_skipped,
                    "batches_processed": total_batches,
                    "planned_batches": total_batches
                },
                "message": f"固定次数抽取完成: 计划{total_batches}批，抽取{total_extracted}条，插入{total_inserted}条"
            }
        
        extraction_result = await async_extract_in_batches()
        
        if not extraction_result.get("success"):
            raise Exception(f"数据抽取失败: {extraction_result.get('message', '未知错误')}")
        
        stats = extraction_result.get("statistics", {})
        extracted = stats.get("extracted", 0)
        inserted = stats.get("inserted", 0)
        skipped = stats.get("skipped", 0)
        
        # 更新抽取阶段完成状态
        task_record.update_task_progress(
            db=db,
            task_id=main_task_id,
            process_stage="数据抽取完成，准备开始分析",
            extracted_records=extracted,
            skipped_records=skipped,
            duplicate_records=skipped
        )
        
        logger.info(f"✅ 阶段1异步抽取完成: 抽取{extracted}条，插入{inserted}条，跳过重复{skipped}条")
        
        # 阶段2：循环分析（直到队列为空）
        from app.services.stage2_analysis_service import stage2_service
        
        # 🔥 在分析阶段开始时，查询实际的待分析工单数量（按时间范围过滤），设置正确的进度基准
        pending_count_sql = f"""
        SELECT COUNT(*) as count 
        FROM {stage1_service.pending_table_name} 
        WHERE ai_status = 'PENDING' 
        AND create_time >= :start_time 
        AND create_time <= :end_time
        """
        pending_result = db.execute(text(pending_count_sql), {
            "start_time": start_datetime,
            "end_time": end_datetime
        })
        total_pending_orders = pending_result.fetchone()[0]
        
        # 🔥 现在设置分析阶段的进度基准：基于实际待分析的工单数量
        task_record.update_task_progress(
            db=db,
            task_id=main_task_id,
            process_stage="循环分析中",
            total_records=total_pending_orders,
            processed_records=0
        )
        
        logger.info(f"📊 分析阶段开始: 待分析工单数={total_pending_orders}")
        
        total_successful = 0
        total_failed = 0
        total_analyzed = 0
        total_skipped = 0
        total_batches = 0
        
        # 🔥 优化：先查询pending表总数据量，合理设置循环次数
        current_cycle = 0
        
        # 重构：根据loop_analysis参数决定处理方式，先查询总数再固定循环
        if loop_analysis:
            logger.info("🔄 启用循环分析模式，重构为固定次数循环")
            
            # 1. 先查询pending表中的总工单数量
            initial_pending_result = db.execute(text(pending_count_sql), {
                "start_time": start_datetime,
                "end_time": end_datetime
            })
            total_pending_count = initial_pending_result.fetchone()[0]
            
            # 2. 计算需要的固定循环次数
            if total_pending_count > 0:
                total_cycles = (total_pending_count + batch_size - 1) // batch_size  # 向上取整
            else:
                total_cycles = 0
            
            logger.info(f"📊 重构后分析规划:")
            logger.info(f"  📥 总PENDING工单数: {total_pending_count}")
            logger.info(f"  📦 每批处理数量: {batch_size}")
            logger.info(f"  🔄 固定循环次数: {total_cycles}")
            
            if total_pending_count == 0:
                logger.info("📝 没有待处理的PENDING工单，跳过分析")
            else:
                # 3. 固定次数循环处理
                for cycle_num in range(1, total_cycles + 1):
                    current_cycle = cycle_num
                    logger.info(f"🔄 开始第 {cycle_num}/{total_cycles} 轮分析")
                    
                    # 检查当前PENDING状态的工单数量（用于日志）
                    pending_result = db.execute(text(pending_count_sql), {
                        "start_time": start_datetime,
                        "end_time": end_datetime
                    })
                    current_pending = pending_result.fetchone()[0]
                    
                    logger.info(f"📋 第{cycle_num}轮开始前: 剩余PENDING工单数={current_pending}")
                    
                    # 如果没有PENDING工单了，可以提前结束
                    if current_pending == 0:
                        logger.info("✅ 所有PENDING工单已处理完成，提前结束循环")
                        break
                    
                    # 执行分析队列处理
                    batch_result = await stage2_service.process_pending_analysis_queue(
                        db=db,
                        batch_size=batch_size,
                        max_concurrent=5,
                        start_date=start_datetime,
                        end_date=end_datetime
                    )
                    
                    # 累计统计
                    analysis_stats = batch_result.get("analysis_statistics", {})
                    batch_successful = analysis_stats.get("successful_analyses", 0)
                    batch_failed = analysis_stats.get("failed_analyses", 0)
                    batch_analyzed = analysis_stats.get("analyzed_orders", 0)
                    batch_skipped = analysis_stats.get("skipped_orders", 0)
                    
                    total_successful += batch_successful
                    total_failed += batch_failed
                    total_analyzed += batch_analyzed
                    total_skipped += batch_skipped
                    total_batches += 1
                    
                    # 更新任务进度
                    task_record.update_task_progress(
                        db=db,
                        task_id=main_task_id,
                        process_stage=f"分析第{cycle_num}/{total_cycles}轮",
                        success_records=total_successful,
                        failed_records=total_failed,
                        analyzed_records=total_analyzed
                    )
                    
                    logger.info(f"📈 第{cycle_num}/{total_cycles}轮完成: 成功{batch_successful}, 失败{batch_failed}, 分析{batch_analyzed}, 跳过{batch_skipped}")
                    logger.info(f"📊 累计进度: 成功{total_successful}, 失败{total_failed}, 累计跳过{total_skipped}")
                    
                    # 异步让出执行时间，避免长时间占用
                    import asyncio
                    await asyncio.sleep(0.5)
        else:
            logger.info("🎯 单批次分析模式，处理一批后立即返回")
            
            # 先查询pending表中的总工单数量
            initial_pending_result = db.execute(text(pending_count_sql), {
                "start_time": start_datetime,
                "end_time": end_datetime
            })
            total_pending_count = initial_pending_result.fetchone()[0]
            
            logger.info(f"📊 单批次分析情况:")
            logger.info(f"  📥 总PENDING工单数: {total_pending_count}")
            logger.info(f"  📦 本批处理数量: {batch_size}")
            logger.info(f"  📋 预计剩余轮次: {max(0, (total_pending_count - batch_size + batch_size - 1) // batch_size)}")
            
            current_cycle = 1
            
            # 🔥 修复：添加时间范围参数到分析队列处理
            batch_result = await stage2_service.process_pending_analysis_queue(
                db=db,
                batch_size=batch_size,
                max_concurrent=5,
                start_date=start_datetime,
                end_date=end_datetime
            )
            
            # 累计统计 - 修正字段名称
            analysis_stats = batch_result.get("analysis_statistics", {})
            batch_successful = analysis_stats.get("successful_analyses", 0)
            batch_failed = analysis_stats.get("failed_analyses", 0)
            batch_analyzed = analysis_stats.get("analyzed_orders", 0)
            batch_skipped = analysis_stats.get("skipped_orders", 0)
            
            total_successful += batch_successful
            total_failed += batch_failed
            total_analyzed += batch_analyzed
            total_skipped += batch_skipped
            total_batches += 1
            
            # 更新任务进度
            task_record.update_task_progress(
                db=db,
                task_id=main_task_id,
                process_stage=f"单批次分析完成",
                success_records=total_successful,
                failed_records=total_failed,
                analyzed_records=total_analyzed
            )
            
            logger.info(f"📈 单批次完成: 成功{batch_successful}, 失败{batch_failed}, 分析{batch_analyzed}, 跳过{batch_skipped}")
            logger.info("🎯 单批次模式，处理完成立即返回")
            
            if not batch_result.get("success"):
                logger.warning(f"⚠️ 第{current_cycle}轮分析失败: {batch_result.get('message', '未知错误')}")
            
            # 🔥 单批次模式已完成，无需继续处理
            logger.info("📝 单批次分析模式完成")
            
            # 🔥 修复进度计算：processed_records应该包含所有已处理的记录（成功+失败+跳过）
            final_total_processed = total_successful + total_failed + total_skipped
            
            # 更新进度
            task_record.update_task_progress(
                db=db,
                task_id=main_task_id,
                processed_records=final_total_processed,
                success_records=total_successful,
                failed_records=total_failed,
                analyzed_records=total_successful
            )
            

            
        # 🔥 新增：分析完成后检查剩余pending数量
        final_pending_result = db.execute(text(pending_count_sql), {
            "start_time": start_datetime,
            "end_time": end_datetime
        })
        final_pending_count = final_pending_result.fetchone()[0]
        
        logger.info("=" * 80)
        logger.info("📝 分析阶段完成总结:")
        logger.info(f"  🔄 完成循环轮次: {current_cycle}")
        logger.info(f"  ✅ 成功分析数量: {total_successful}")
        logger.info(f"  ❌ 分析失败数量: {total_failed}")
        logger.info(f"  ⏭️ 跳过处理数量: {total_skipped}")
        logger.info(f"  📋 剩余PENDING数: {final_pending_count}")
        if final_pending_count > 0:
            logger.warning(f"⚠️ 还有 {final_pending_count} 个工单需要继续处理")
        else:
            logger.info("🎉 所有PENDING工单已完成处理！")
        logger.info("=" * 80)
        
        # 构建最终执行详情
        duration_hours = round((end_datetime - start_datetime).total_seconds() / 3600, 2)
        final_details = {
            "extraction_phase": {
                "extracted": extracted,
                "inserted": inserted,
                "skipped": skipped,
                "duration_hours": duration_hours,
                "time_range": f"{start_datetime} ~ {end_datetime}"
            },
            "analysis_phase": {
                "total_cycles": current_cycle,  # 🔥 实际执行的循环次数
                "analyzed": total_analyzed,
                "successful": total_successful,
                "failed": total_failed,
                "skipped": total_skipped,
                "remaining_pending": final_pending_count,  # 🔥 新增：剩余pending数量
                "batch_size": batch_size,
                "loop_analysis": loop_analysis,
                "processing_mode": "无限循环分析" if loop_analysis else "单批次分析",  # 🔥 修复：更新处理模式
                "completion_status": "完成" if final_pending_count == 0 else f"未完成(剩余{final_pending_count}条)"  # 🔥 新增：完成状态
            },
            "performance_optimization": {  # 🔥 性能优化信息
                "blocking_prevention": "采用异步分批处理，避免长时间阻塞",
                "cycle_delay": "0.5秒",
                "termination_reason": "所有PENDING数据处理完成" if final_pending_count == 0 else f"处理完成(剩余{final_pending_count}条pending)"
            },
            "completion_summary": f"抽取{extracted}条，分析{current_cycle}轮，成功{total_successful}条，失败{total_failed}条，跳过{total_skipped}条，剩余{final_pending_count}条pending"
        }
        
        # 完成主任务
        task_record.complete_task(
            db=db,
            task_id=main_task_id,
            status="completed",
            execution_details=final_details
        )
        
        logger.info(f"🎉 完整任务流程完成: 抽取{extracted}条，成功分析{total_successful}条，剩余pending{final_pending_count}条")
        
        return final_details
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"❌ 异步完整任务流程失败: {error_msg}")
        
        # 标记主任务失败
        task_record.complete_task(
            db=db,
            task_id=main_task_id,
            status="failed",
            error_message=error_msg
        )
        
        return {"error": error_msg}


@router.post("/stop/{task_id}", summary="停止正在运行的任务")
async def stop_task(
    task_id: str,
    reason: str = Query("手动停止", description="停止原因"),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """停止正在运行的任务"""
    try:
        username = current_user.get("username", "unknown")
        
        # 检查任务是否存在
        task = task_record.get_task_by_id(db, task_id)
        if not task:
            raise HTTPException(status_code=404, detail=f"任务不存在: {task_id}")
        
        # 检查任务状态
        if task.get("status") not in ["running", "pending"]:
            return {
                "success": False,
                "message": f"任务当前状态为 {task.get('status')}，无法停止"
            }
        
        # 更新任务状态为已停止
        success = task_record.complete_task(
            db=db,
            task_id=task_id,
            status="cancelled",
            error_message=f"用户 {username} 手动停止: {reason}"
        )
        
        if success:
            logger.info(f"✅ 任务 {task_id} 已被用户 {username} 停止: {reason}")
            return {
                "success": True,
                "message": f"任务已停止: {reason}",
                "task_id": task_id,
                "stopped_by": username
            }
        else:
            raise HTTPException(status_code=500, detail="停止任务失败")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"停止任务失败: {task_id}, {e}")
        raise HTTPException(status_code=500, detail=f"停止任务失败: {str(e)}")


@router.post("/stop/batch", summary="批量停止任务")
async def stop_tasks_batch(
    task_ids: list[str],
    reason: str = Query("批量手动停止", description="停止原因"),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """批量停止多个正在运行的任务"""
    try:
        username = current_user.get("username", "unknown")
        results = []
        
        for task_id in task_ids:
            try:
                # 检查任务是否存在
                task = task_record.get_task_by_id(db, task_id)
                if not task:
                    results.append({
                        "task_id": task_id,
                        "success": False,
                        "message": f"任务不存在: {task_id}"
                    })
                    continue
                
                # 检查任务状态
                if task.get("status") not in ["running", "pending"]:
                    results.append({
                        "task_id": task_id,
                        "success": False,
                        "message": f"任务状态为 {task.get('status')}，无法停止"
                    })
                    continue
                
                # 更新任务状态为已停止
                success = task_record.complete_task(
                    db=db,
                    task_id=task_id,
                    status="cancelled",
                    error_message=f"用户 {username} 批量停止: {reason}"
                )
                
                if success:
                    logger.info(f"✅ 任务 {task_id} 已被用户 {username} 批量停止: {reason}")
                    results.append({
                        "task_id": task_id,
                        "success": True,
                        "message": f"任务已停止: {reason}"
                    })
                else:
                    results.append({
                        "task_id": task_id,
                        "success": False,
                        "message": "停止任务失败"
                    })
                    
            except Exception as e:
                logger.error(f"停止任务失败: {task_id}, {e}")
                results.append({
                    "task_id": task_id,
                    "success": False,
                    "message": f"停止失败: {str(e)}"
                })
        
        successful_count = sum(1 for r in results if r["success"])
        
        return {
            "success": successful_count > 0,
            "message": f"批量停止完成：成功 {successful_count}/{len(task_ids)} 个任务",
            "results": results,
            "summary": {
                "total": len(task_ids),
                "successful": successful_count,
                "failed": len(task_ids) - successful_count,
                "stopped_by": username
            }
        }
        
    except Exception as e:
        logger.error(f"批量停止任务失败: {e}")
        raise HTTPException(status_code=500, detail=f"批量停止任务失败: {str(e)}")


@router.get("/types", summary="获取任务类型列表")
async def get_task_types(
    current_user: dict = Depends(get_current_user)
):
    """获取支持的任务类型列表"""
    try:
        task_types = [
            {
                "type": "data_extraction",
                "name": "数据抽取",
                "description": "从数据库抽取工单数据"
            },
            {
                "type": "batch_analysis", 
                "name": "批量分析",
                "description": "批量分析工单评论"
            },
            {
                "type": "cleanup",
                "name": "数据清理", 
                "description": "清理旧的数据记录"
            },
            {
                "type": "scheduled",
                "name": "定时任务",
                "description": "系统定时执行的任务"
            }
        ]
        
        return {
            "success": True,
            "task_types": task_types,
            "total": len(task_types)
        }
        
    except Exception as e:
        logger.error(f"获取任务类型失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取任务类型失败: {str(e)}")


@router.post("/validate-cron", summary="验证CRON表达式")
async def validate_cron_expression_api(
    cron_data: dict,
    current_user: dict = Depends(get_current_user)
):
    """验证CRON表达式格式和有效性"""
    try:
        cron_expr = cron_data.get("cron_expression", "").strip()
        
        if not cron_expr:
            return {
                "success": False,
                "message": "cron表达式不能为空"
            }
        
        # 使用验证函数
        from app.services.apscheduler_service import validate_cron_expression
        validation_result = validate_cron_expression(cron_expr)
        
        return {
            "success": validation_result["valid"],
            "message": validation_result["message"],
            "description": validation_result.get("description", ""),
            "next_runs": validation_result.get("next_runs", []),
            "cron_expression": cron_expr
        }
        
    except Exception as e:
        logger.error(f"验证cron表达式失败: {e}")
        return {
            "success": False,
            "message": f"验证失败: {str(e)}"
        }


@router.get("/configs", summary="获取任务配置列表")
async def get_task_configs(
    current_user: dict = Depends(get_current_user)
):
    """获取所有任务配置"""
    try:
        from app.models.task_config import task_config
        from app.db.database import get_db
        
        db = next(get_db())
        try:
            configs = task_config.get_all_tasks(db, enabled_only=False)
            
            # 获取当前运行中的任务
            running_tasks = task_record.get_task_records(
                db=db,
                limit=10,
                status="running"
            )
            
            # 创建运行任务的快速查找映射
            running_task_keys = set()
            for running_task in running_tasks:
                task_config_key = running_task.get("task_config_key")
                if task_config_key:
                    running_task_keys.add(task_config_key)
            
            # 优化每个配置项
            optimized_configs = []
            for config in configs:
                task_key = config.get("task_key")
                optimized_config = config.copy()
                
                # 添加运行状态信息
                optimized_config["is_running"] = task_key in running_task_keys
                
                # 格式化时间和调度显示
                optimized_config["last_execution_display"] = _format_datetime_display(config.get("last_execution_time"))
                optimized_config["next_execution_display"] = _format_datetime_display(config.get("next_execution_time"))
                optimized_config["schedule_display"] = _format_schedule_display(config)
                
                optimized_configs.append(optimized_config)
            
        finally:
            db.close()
        
        return {
            "success": True,
            "data": optimized_configs,
            "summary": {
                "total_configs": len(configs),
                "enabled_configs": sum(1 for cfg in configs if cfg.get("is_enabled", False)),
                "running_tasks": len(running_task_keys)
            }
        }
        
    except Exception as e:
        logger.error(f"获取任务配置失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取任务配置失败: {str(e)}")


@router.post("/configs/{task_key}/toggle", summary="切换任务启用状态")
async def toggle_task_enabled(
    task_key: str,
    enabled: bool = Query(..., description="是否启用"),
    current_user: dict = Depends(get_current_user)
):
    """切换任务的启用/禁用状态"""
    try:
        username = current_user.get("username", "unknown")
        
        # 使用APScheduler处理任务切换
        success = await apscheduler_service.toggle_task_enabled(task_key, enabled)
        
        if success:
            status_text = "启用" if enabled else "禁用"
            logger.info(f"🔧 用户 {username} {status_text}了任务: {task_key}")
            
            return {
                "success": True,
                "message": f"任务 {task_key} 已{status_text}",
                "task_key": task_key,
                "enabled": enabled,
                "operator": username
            }
        else:
            raise HTTPException(status_code=404, detail=f"任务不存在: {task_key}")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"切换任务状态失败: {task_key}, {e}")
        raise HTTPException(status_code=500, detail=f"切换任务状态失败: {str(e)}")


@router.put("/configs/{task_key}", summary="更新任务配置")
async def update_task_config(
    task_key: str,
    updates: dict,
    current_user: dict = Depends(get_current_user)
):
    """更新任务配置"""
    try:
        username = current_user.get("username", "unknown")
        
        # 验证更新字段
        allowed_fields = {
            'task_name', 'task_description', 'schedule_interval', 'schedule_cron',
            'default_batch_size', 'priority', 'timeout_seconds', 'retry_times'
        }
        
        filtered_updates = {k: v for k, v in updates.items() if k in allowed_fields}
        
        if not filtered_updates:
            raise HTTPException(status_code=400, detail="没有有效的更新字段")
        
        # 验证cron表达式（如果提供了）
        if 'schedule_cron' in filtered_updates:
            cron_expr = filtered_updates['schedule_cron']
            if cron_expr and cron_expr.strip():
                from app.services.apscheduler_service import validate_cron_expression
                validation = validate_cron_expression(cron_expr)
                if not validation['valid']:
                    raise HTTPException(
                        status_code=400,
                        detail=f"cron表达式无效: {validation['message']}"
                    )
                logger.info(f"✅ cron表达式验证通过: {cron_expr} - {validation.get('description', '')}")
        
        # 使用APScheduler更新任务配置
        success = await apscheduler_service.update_task_config(task_key, filtered_updates)
        
        if success:
            logger.info(f"🔧 用户 {username} 更新了任务配置: {task_key}")
            
            return {
                "success": True,
                "message": f"任务配置 {task_key} 已更新",
                "task_key": task_key,
                "updates": filtered_updates,
                "operator": username
            }
        else:
            raise HTTPException(status_code=404, detail=f"任务不存在: {task_key}")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新任务配置失败: {task_key}, {e}")
        raise HTTPException(status_code=500, detail=f"更新任务配置失败: {str(e)}")


@router.post("/manual-execution/{task_key}", summary="手动执行指定任务")
async def manual_execute_task(
    task_key: str,
    current_user: dict = Depends(get_current_user)
):
    """手动执行指定的任务配置"""
    try:
        from app.models.task_config import task_config as tc
        from app.db.database import get_db
        
        db = next(get_db())
        try:
            task_config = tc.get_task_by_key(db, task_key)
        finally:
            db.close()
        
        if not task_config:
            raise HTTPException(status_code=404, detail=f"任务配置不存在: {task_key}")
        
        username = current_user.get("username", "unknown")
        task_handler = task_config.get("task_handler")
        
        logger.info(f"🔧 用户 {username} 手动执行任务: {task_key} ({task_handler})")
        
        # 根据任务处理器类型调用相应的执行方法
        if task_handler == "batch_analysis":
            from app.services.stage2_analysis_service import stage2_service
            
            result = await stage2_service.process_pending_analysis_queue(
                db=next(get_db()),
                batch_size=task_config.get("default_batch_size", 50),
                max_concurrent=5
            )
            
            # 格式化返回结果
            if result.get("success"):
                analysis_stats = result.get("analysis_statistics", {})
                result["task_id"] = f"manual_{task_key}_{int(datetime.now().timestamp())}"
                
        elif task_handler == "cleanup":
            from app.db.connection_manager import get_db_session
            from sqlalchemy import text
            
            with get_db_session() as db:
                task_id = task_record.create_task_record(
                    db=db,
                    task_name=f"手动执行 - {task_config.get('task_name')}",
                    task_type="cleanup",
                    trigger_type="manual",
                    trigger_user=username,
                    task_config_key=task_key,
                    execution_details={
                        "task_key": task_key,
                        "description": "手动触发的清理任务",
                        "requested_by": username
                    }
                )
                
                # 执行清理逻辑
                cutoff_date = datetime.now() - timedelta(days=30)
                
                cleanup_sql = """
                DELETE FROM ai_work_pending_analysis 
                WHERE updated_at < :cutoff_date 
                AND ai_status IN ('COMPLETED', 'FAILED')
                """
                
                result = db.execute(text(cleanup_sql), {"cutoff_date": cutoff_date})
                deleted_count = result.rowcount
                
                if deleted_count > 0:
                    db.commit()
                    logger.info(f"🗑️ 清理任务完成: 删除了{deleted_count}条过期待分析记录")
                else:
                    logger.info("✨ 清理任务完成: 没有过期记录需要清理")
                
                # 完成任务
                task_record.complete_task(
                    db=db,
                    task_id=task_id,
                    status="completed",
                    execution_details={
                        "completed_at": datetime.now().isoformat(),
                        "task_key": task_key,
                        "deleted_count": deleted_count
                    }
                )
                
                result = {
                    "success": True,
                    "task_id": task_id,
                    "message": f"清理任务执行完成，删除了{deleted_count}条记录"
                }
        else:
            raise HTTPException(status_code=400, detail=f"不支持的任务处理器: {task_handler}")
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"手动执行任务失败: {task_key}, {e}")
        raise HTTPException(status_code=500, detail=f"手动执行任务失败: {str(e)}")


@router.get("/configs-and-records", summary="获取任务配置和执行记录的联合视图")
async def get_tasks_combined_view(
    limit: int = Query(50, description="记录数限制", ge=1, le=200),
    current_user: dict = Depends(get_current_user)
):
    """获取任务配置和最近执行记录的联合视图"""
    try:
        from app.models.task_config import task_config as tc
        from app.db.database import get_db
        
        db = next(get_db())
        
        configs = tc.get_all_tasks(db, enabled_only=False)
        
        try:
            # 获取最近执行记录
            recent_records = task_record.get_task_records(
                db=db,
                limit=limit,
                offset=0
            )
            
            # 获取当前运行中的任务
            running_tasks = task_record.get_task_records(
                db=db,
                limit=10,
                status="running"
            )
            
        finally:
            db.close()
        
        # 合并数据
        combined_data = []
        
        # 创建运行任务的快速查找映射
        running_task_keys = set()
        for running_task in running_tasks:
            task_config_key = running_task.get("task_config_key")
            if task_config_key:
                running_task_keys.add(task_config_key)
        
        # 处理所有配置的任务
        for config in configs:
            task_key = config.get("task_key")
            
            # 查找该任务的最近执行记录
            latest_record = None
            execution_stats = {
                "total_executions": 0,
                "successful_executions": 0,
                "failed_executions": 0
            }
            
            for record in recent_records:
                if record.get("task_config_key") == task_key:
                    if not latest_record:
                        latest_record = record
                    
                    # 统计执行情况
                    execution_stats["total_executions"] += 1
                    if record.get("status") == "completed":
                        execution_stats["successful_executions"] += 1
                    elif record.get("status") == "failed":
                        execution_stats["failed_executions"] += 1
            
            # 优化后的任务项目数据结构
            combined_item = {
                "type": "configured_task",
                "task_key": task_key,
                "task_name": config.get("task_name"),
                "task_description": config.get("task_description"),
                "task_type": config.get("task_handler"),
                "status": "生效" if config.get("is_enabled", False) else "不生效",
                "is_enabled": config.get("is_enabled", False),
                "is_running": task_key in running_task_keys,
                "schedule_interval": config.get("schedule_interval"),
                "schedule_display": _format_schedule_display(config),
                "priority": config.get("priority", 5),
                
                # 时间信息
                "last_execution_time": config.get("last_execution_time"),
                "last_execution_display": _format_datetime_display(config.get("last_execution_time")),
                "next_execution_time": config.get("next_execution_time"),
                "next_execution_display": _format_datetime_display(config.get("next_execution_time")),
                
                # 执行统计
                "execution_stats": {
                    "config_count": config.get("execution_count", 0),
                    "config_success": config.get("success_count", 0),
                    "config_failure": config.get("failure_count", 0),
                    "actual_total": execution_stats["total_executions"],
                    "actual_success": execution_stats["successful_executions"],
                    "actual_failed": execution_stats["failed_executions"],
                    "display_total": max(config.get("execution_count", 0), execution_stats["total_executions"]),
                    "display_success": max(config.get("success_count", 0), execution_stats["successful_executions"]),
                    "display_failed": max(config.get("failure_count", 0), execution_stats["failed_executions"])
                },
                
                # 最近执行记录 - 🔥 添加终止状态标识
                "latest_record": latest_record,
                "has_recent_execution": latest_record is not None,
                
                # 🔥 为最近执行记录添加终止能力
                "latest_record_can_terminate": (
                    latest_record.get("status") in ["running", "pending"] 
                    if latest_record else False
                )
            }
            
            combined_data.append(combined_item)
        
        # 统计信息
        total_running = len(running_task_keys)
        enabled_count = sum(1 for cfg in configs if cfg.get("is_enabled", False))
        
        return {
            "success": True,
            "data": {
                "combined_tasks": combined_data,
                "summary": {
                    "total_configured": len(configs),
                    "enabled_tasks": enabled_count,
                    "disabled_tasks": len(configs) - enabled_count,
                    "running_tasks": total_running,
                    "recent_executions": len(recent_records)
                }
            }
        }
        
    except Exception as e:
        logger.error(f"获取任务联合视图失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取任务联合视图失败: {str(e)}")


def _format_schedule_display(config: dict) -> str:
    """格式化调度显示"""
    try:
        schedule_cron = config.get("schedule_cron")
        schedule_interval = config.get("schedule_interval", 0)
        
        # 优先显示cron表达式
        if schedule_cron and schedule_cron.strip():
            try:
                from app.services.apscheduler_service import _describe_cron_expression
                description = _describe_cron_expression(schedule_cron.strip())
                return f"CRON: {description} ({schedule_cron})"
            except:
                return f"CRON: {schedule_cron}"
        
        # 降级显示interval
        elif schedule_interval:
            return f"间隔: {_format_interval(schedule_interval)}"
        
        else:
            return "未设置"
            
    except Exception as e:
        return f"格式错误: {str(e)[:20]}"


def _format_interval(seconds: int) -> str:
    """格式化时间间隔显示"""
    if not seconds:
        return "未设置"
    
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


def _format_datetime_display(datetime_str: Optional[str]) -> str:
    """格式化日期时间显示"""
    if not datetime_str:
        return "无数据"
    
    try:
        if isinstance(datetime_str, str):
            clean_str = datetime_str.replace('Z', '').replace('+00:00', '')
            dt = datetime.fromisoformat(clean_str)
        else:
            dt = datetime_str
            
        now = datetime.now()
        diff = dt - now
        
        # 未来时间（下次执行）
        if diff.total_seconds() > 0:
            if diff.days > 0:
                return f"{diff.days}天后"
            elif diff.seconds > 3600:
                hours = diff.seconds // 3600
                return f"{hours}小时后"
            elif diff.seconds > 60:
                minutes = diff.seconds // 60
                return f"{minutes}分钟后"
            else:
                return "即将执行"
        
        # 过去时间（上次执行）
        else:
            diff = now - dt
            if diff.days > 0:
                return f"{diff.days}天前"
            elif diff.seconds > 3600:
                hours = diff.seconds // 3600
                return f"{hours}小时前"
            elif diff.seconds > 60:
                minutes = diff.seconds // 60
                return f"{minutes}分钟前"
            else:
                return "刚刚"
                
    except Exception as e:
        return f"格式错误: {str(e)[:20]}"


# ==================== 后台异步执行函数 ====================

async def run_manual_extraction_async(db: Session, target_datetime: datetime, username: str, main_task_id: str):
    """异步执行手动数据抽取任务"""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"🚀 开始异步手动数据抽取任务: {main_task_id}, 用户: {username}, 目标日期: {target_datetime.date()}")
        
        # 更新进度
        task_record.update_task_progress(
            db=db,
            task_id=main_task_id,
            process_stage="数据抽取中"
        )
        
        from app.services.stage1_work_extraction import stage1_service
        
        # 执行数据抽取
        extraction_result = stage1_service.extract_work_data_by_time_range(
            db=db,
            target_date=target_datetime
        )
        
        if extraction_result.get("success"):
            stats = extraction_result.get("statistics", {})
            extracted = stats.get("extracted", 0)
            inserted = stats.get("inserted", 0)
            skipped = stats.get("skipped", 0)
            
            # 完成任务
            final_details = {
                "extraction_result": extraction_result,
                "statistics": stats,
                "target_date": target_datetime.strftime("%Y-%m-%d"),
                "completion_message": f"抽取{extracted}条，插入{inserted}条，跳过{skipped}条"
            }
            
            task_record.complete_task(
                db=db,
                task_id=main_task_id,
                status="completed",
                execution_details=final_details
            )
            
            # 更新统计
            task_record.update_task_progress(
                db=db,
                task_id=main_task_id,
                total_records=extracted,
                extracted_records=extracted,
                success_records=inserted,
                skipped_records=skipped
            )
            
            logger.info(f"✅ 手动数据抽取完成: 抽取{extracted}条，插入{inserted}条，跳过{skipped}条")
            return final_details
        else:
            raise Exception(f"数据抽取失败: {extraction_result.get('message', '未知错误')}")
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"❌ 异步手动数据抽取任务失败: {error_msg}")
        
        # 标记任务失败
        task_record.complete_task(
            db=db,
            task_id=main_task_id,
            status="failed",
            error_message=error_msg
        )
        
        return {"error": error_msg}


async def run_manual_analysis_async(db: Session, limit: int, username: str, main_task_id: str):
    """异步执行手动分析任务"""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"🚀 开始异步手动分析任务: {main_task_id}, 用户: {username}, 限制: {limit}")
        
        # 更新任务状态为运行中
        task_record.update_task_progress(
            db=db,
            task_id=main_task_id,
            process_stage="批量分析中"
        )
        
        # 执行分析
        from app.services.stage2_analysis_service import stage2_service
        
        result = await stage2_service.process_pending_analysis_queue(
            db=db,
            batch_size=limit,
            max_concurrent=5
        )
        
        if result.get("success"):
            analysis_stats = result.get("analysis_statistics", {})
            successful = analysis_stats.get("successful_analyses", 0)
            failed = analysis_stats.get("failed_analyses", 0)
            total_analyzed = analysis_stats.get("analyzed_orders", 0)
            skipped_orders = analysis_stats.get("skipped_orders", 0)
            
            # 完成任务
            final_details = {
                "analysis_summary": {
                    "analyzed": total_analyzed,
                    "successful": successful,
                    "failed": failed,
                    "skipped": skipped_orders,
                    "limit": limit
                },
                "completion_message": f"成功分析{successful}条，失败{failed}条，跳过{skipped_orders}条"
            }
            
            task_record.complete_task(
                db=db,
                task_id=main_task_id,
                status="completed",
                execution_details=final_details
            )
            
            logger.info(f"✅ 手动分析任务完成: 成功{successful}条，失败{failed}条")
            return final_details
        else:
            raise Exception(f"分析失败: {result.get('message', '未知错误')}")
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"❌ 异步手动分析任务失败: {error_msg}")
        
        # 标记任务失败
        task_record.complete_task(
            db=db,
            task_id=main_task_id,
            status="failed",
            error_message=error_msg
        )
        
        return {"error": error_msg}


async def run_full_task_async(db: Session, target_datetime: datetime, analysis_limit: int, username: str, main_task_id: str):
    """异步执行完整任务流程"""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"🚀 开始异步完整任务流程: {main_task_id}, 用户: {username}")
        
        # 阶段1：数据抽取
        task_record.update_task_progress(
            db=db,
            task_id=main_task_id,
            process_stage="数据抽取中"
        )
        
        from app.services.stage1_work_extraction import stage1_service
        
        extraction_result = stage1_service.extract_work_data_by_time_range(
            db=db,
            target_date=target_datetime
        )
        
        if not extraction_result.get("success"):
            raise Exception(f"数据抽取失败: {extraction_result.get('message', '未知错误')}")
        
        stats = extraction_result.get("statistics", {})
        extracted = stats.get("extracted", 0)
        inserted = stats.get("inserted", 0)
        skipped = stats.get("skipped", 0)
        
        # 修复进度显示：抽取阶段只更新阶段信息和统计数据，不设置进度相关字段
        task_record.update_task_progress(
            db=db,
            task_id=main_task_id,
            process_stage="数据抽取完成，开始分析",
            extracted_records=extracted,
            skipped_records=skipped,
            duplicate_records=skipped
        )
        
        logger.info(f"✅ 阶段1完成: 抽取{extracted}条，插入{inserted}条，跳过重复{skipped}条")
        
        # 阶段2：批量分析
        from app.services.stage2_analysis_service import stage2_service
        
        # 在分析阶段开始时，查询实际的待分析工单数量，设置正确的进度基准
        pending_count_sql = f"""
        SELECT COUNT(*) as count 
        FROM {stage1_service.pending_table_name} 
        WHERE ai_status = 'PENDING'
        """
        pending_result = db.execute(text(pending_count_sql))
        total_pending_orders = pending_result.fetchone()[0]
        
        # 现在设置分析阶段的进度基准：基于实际待分析的工单数量
        task_record.update_task_progress(
            db=db,
            task_id=main_task_id,
            process_stage="批量分析中",
            total_records=total_pending_orders,
            processed_records=0
        )
        
        logger.info(f"📊 分析阶段开始: 待分析工单数={total_pending_orders}")
        
        analysis_result = await stage2_service.process_pending_analysis_queue(
            db=db,
            batch_size=analysis_limit,
            max_concurrent=5
        )
        
        if not analysis_result.get("success"):
            raise Exception(f"批量分析失败: {analysis_result.get('message', '未知错误')}")
        
        analysis_stats = analysis_result.get("analysis_statistics", {})
        successful = analysis_stats.get("successful_analyses", 0)
        failed = analysis_stats.get("failed_analyses", 0)
        total_analyzed = analysis_stats.get("analyzed_orders", 0)
        skipped_orders = analysis_stats.get("skipped_orders", 0)
        denoised_orders = analysis_stats.get("denoised_orders", 0)
        
        # 完成主任务
        final_details = {
            "extraction_summary": {
                "extracted": extracted,
                "inserted": inserted,
                "skipped": skipped
            },
            "analysis_summary": {
                "analyzed": total_analyzed,
                "successful": successful,
                "failed": failed,
                "skipped": skipped_orders,
                "denoised": denoised_orders
            },
            "completion_message": f"抽取{extracted}条，成功分析{successful}条"
        }
        
        task_record.complete_task(
            db=db,
            task_id=main_task_id,
            status="completed",
            execution_details=final_details
        )
        
        logger.info(f"🎉 异步完整任务流程完成: 抽取{extracted}条，成功分析{successful}条")
        return final_details
        
    except Exception as e:
        error_msg = str(e)
        logger.error(f"❌ 异步完整任务流程失败: {error_msg}")
        
        # 标记主任务失败
        task_record.complete_task(
            db=db,
            task_id=main_task_id,
            status="failed",
            error_message=error_msg
        )
        
        return {"error": error_msg}
