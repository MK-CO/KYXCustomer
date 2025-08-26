"""
分析相关API端点 - 简化版
"""
from datetime import datetime, timedelta
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from app.db.database import get_db

from app.core.auth import get_current_user

from app.services.stage2_analysis_service import stage2_service
from app.services.stage1_work_extraction import stage1_service
from app.services.content_denoiser import content_denoiser
from app.models.denoise import denoise_record_manager
from config.settings import settings

router = APIRouter(prefix="/analysis", tags=["analysis"])


class AnalysisRequest(BaseModel):
    """分析请求模型"""
    mode: str = Field("batch", description="分析模式: batch(批量), time_range(时间段)")
    start_date: Optional[datetime] = Field(None, description="开始日期")
    end_date: Optional[datetime] = Field(None, description="结束日期")
    days_back: int = Field(1, description="向前查找天数", ge=1, le=30)
    limit: int = Field(100, description="处理限制数量", ge=1, le=1000)
    ai_status: str = Field("PENDING", description="待分析数据状态")


class ExtractionRequest(BaseModel):
    """数据抽取请求模型"""
    mode: str = Field("daily", description="抽取模式: daily(按天), historical(历史全量), date_range(日期范围), time_range(精确时间范围)")
    
    # 精确时间范围参数（优先级最高）
    start_time: Optional[datetime] = Field(None, description="精确开始时间(time_range模式用)")
    end_time: Optional[datetime] = Field(None, description="精确结束时间(time_range模式用)")
    
    # 日期范围参数（兼容历史功能）
    start_date: Optional[datetime] = Field(None, description="开始日期(历史抽取和date_range模式用)")
    end_date: Optional[datetime] = Field(None, description="结束日期(历史抽取和date_range模式用)")
    
    # 简单参数（兼容日常抽取）
    days_back: int = Field(1, description="向前查找天数(daily模式用)", ge=1, le=365)
    target_date: Optional[datetime] = Field(None, description="指定抽取日期(daily模式用)")


class DenoiseQueryRequest(BaseModel):
    """去噪查询请求模型"""
    batch_id: Optional[str] = Field(None, description="批次ID")
    work_id: Optional[int] = Field(None, description="工单ID")
    days: int = Field(7, description="查询天数", ge=1, le=30)
    limit: int = Field(50, description="查询限制", ge=1, le=500)


@router.post("/analyze")
async def run_analysis(
    request: AnalysisRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    统一分析接口 - 后台异步执行，立即返回任务ID
    支持两种模式：
    1. batch: 批量分析待处理数据（默认）
    2. time_range: 按时间段分析数据
    """
    import asyncio
    import uuid
    from app.core.concurrency import async_task_manager
    
    try:
        # 生成唯一任务ID
        task_id = f"analysis_{uuid.uuid4().hex[:16]}"
        username = current_user.get("username", "unknown")
        
        # 提交到后台异步执行，不等待完成
        if request.mode == "time_range":
            # 时间段分析
            success = await async_task_manager.submit_task(
                task_id,
                analyze_by_time_range_async(db, request, username, task_id)
            )
        else:
            # 批量分析（默认）
            success = await async_task_manager.submit_task(
                task_id,
                analyze_batch_data_async(db, request, username, task_id)
            )
        
        if not success:
            raise HTTPException(status_code=500, detail="任务提交失败")
        
        # 立即返回任务ID，不等待任务完成
        return {
            "success": True,
            "task_id": task_id,
            "mode": request.mode,
            "status": "submitted",
            "message": f"分析任务已提交到后台执行",
            "check_status_url": f"/api/v1/tasks/status/{task_id}"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"分析任务提交失败: {str(e)}")


async def analyze_batch_data(db: Session, request: AnalysisRequest):
    """批量分析待处理数据"""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"开始批量分析，状态: {request.ai_status}, 限制: {request.limit}")
        
        # 如果是FAILED状态，先重置为PENDING以便重新分析
        reset_count = 0
        if request.ai_status == "FAILED":
            logger.info("🔄 检测到FAILED状态分析请求，先重置工单状态...")
            reset_count = stage1_service.reset_failed_work_orders_for_retry(
                db=db,
                limit=request.limit
            )
            if reset_count > 0:
                logger.info(f"✅ 成功重置 {reset_count} 个FAILED工单为PENDING状态")
                # 重置后，改为查询PENDING状态的工单
                actual_status = "PENDING"
            else:
                logger.warning("⚠️ 没有找到需要重置的FAILED工单")
                actual_status = "FAILED"  # 保持原状态查询
        else:
            actual_status = request.ai_status
        
        # 获取待处理的工单
        pending_orders = stage1_service.get_pending_work_orders(
            db=db,
            ai_status=actual_status,
            limit=request.limit
        )
        
        if not pending_orders:
            message = f"没有找到状态为 {request.ai_status} 的待处理工单"
            if request.ai_status == "FAILED" and reset_count > 0:
                message = f"重置了 {reset_count} 个FAILED工单，但没有找到可分析的工单"
            return {
                "success": True,
                "mode": "batch",
                "message": message,
                "reset_count": reset_count if request.ai_status == "FAILED" else 0,
                "statistics": {
                    "total_found": 0,
                    "processed": 0,
                    "successful": 0,
                    "failed": 0
                }
            }
        
        # 执行批量分析
        result = await stage2_service.process_pending_analysis_queue(
            db, 
            batch_size=request.limit
        )
        
        message = f"批量分析完成，处理了 {len(pending_orders)} 个工单"
        if request.ai_status == "FAILED" and reset_count > 0:
            message = f"重置了 {reset_count} 个FAILED工单并完成分析，处理了 {len(pending_orders)} 个工单"
        
        # 尝试获取去噪统计信息
        denoise_info = None
        if result.get("success") and "batch_id" in result.get("statistics", {}):
            try:
                batch_id = result["statistics"].get("batch_id")
                if batch_id:
                    denoise_records = denoise_record_manager.get_batch_statistics(db, batch_id, 1)
                    if denoise_records:
                        denoise_info = denoise_records[0]
                        logger.info(f"📊 获取到去噪统计: 批次 {batch_id}")
            except Exception as e:
                logger.warning(f"获取去噪统计失败: {e}")
        
        return {
            "success": True,
            "mode": "batch",
            "ai_status": request.ai_status,
            "limit": request.limit,
            "message": message,
            "reset_count": reset_count if request.ai_status == "FAILED" else 0,
            "pending_count": len(pending_orders),
            "result": result,
            "denoise_statistics": denoise_info
        }
        
    except Exception as e:
        logger.error(f"批量分析失败: {e}")
        return {
            "success": False,
            "mode": "batch",
            "error": str(e),
            "message": "批量分析失败"
        }


async def analyze_by_time_range(db: Session, request: AnalysisRequest):
    """按时间段分析数据"""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        # 计算时间范围
        if not request.start_date and not request.end_date:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=request.days_back)
        else:
            start_date = request.start_date
            end_date = request.end_date or datetime.now()
        
        logger.info(f"开始时间段分析: {start_date.date()} 到 {end_date.date()}")
        
        # 如果是FAILED状态，先重置为PENDING以便重新分析
        reset_count = 0
        if request.ai_status == "FAILED":
            logger.info("🔄 检测到FAILED状态分析请求，先重置工单状态...")
            reset_count = stage1_service.reset_failed_work_orders_for_retry(
                db=db,
                limit=request.limit
            )
            if reset_count > 0:
                logger.info(f"✅ 成功重置 {reset_count} 个FAILED工单为PENDING状态")
                actual_status = "PENDING"
            else:
                logger.warning("⚠️ 没有找到需要重置的FAILED工单")
                actual_status = "FAILED"
        else:
            actual_status = request.ai_status
        
        # 🔥 修复：直接在数据库层面按时间范围获取待处理工单，提高效率
        pending_orders = stage1_service.get_pending_work_orders(
            db=db,
            ai_status=actual_status,
            limit=request.limit,
            start_date=start_date,
            end_date=end_date
        )
        
        if not pending_orders:
            return {
                "success": True,
                "mode": "time_range",
                "time_range": {
                    "start_date": start_date.strftime("%Y-%m-%d"),
                    "end_date": end_date.strftime("%Y-%m-%d")
                },
                "message": f"在指定时间段内没有找到待处理的工单",
                "statistics": {
                    "total_found": len(pending_orders),
                    "time_range_filtered": len(pending_orders),
                    "processed": 0,
                    "successful": 0,
                    "failed": 0
                }
            }
        
        # 执行分析（处理时间范围内的工单数量）
        result = await stage2_service.process_pending_analysis_queue(
            db, 
            batch_size=len(pending_orders),
            start_date=start_date,
            end_date=end_date
        )
        
        return {
            "success": True,
            "mode": "time_range",
            "time_range": {
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d")
            },
            "ai_status": request.ai_status,
            "message": f"时间段分析完成，处理了 {len(pending_orders)} 个工单",
            "statistics": {
                "total_found": len(pending_orders),
                "time_range_filtered": len(pending_orders),
            },
            "result": result
        }
        
    except Exception as e:
        logger.error(f"时间段分析失败: {e}")
        return {
            "success": False,
            "mode": "time_range",
            "error": str(e),
            "message": "时间段分析失败"
        }


# ===== 数据抽取相关接口 =====

@router.post("/extraction/extract")
async def extract_work_data(
    request: ExtractionRequest,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    统一数据抽取接口 - 后台异步执行，立即返回任务ID
    支持四种模式：
    1. time_range: 精确时间范围抽取（推荐）
    2. daily: 按天抽取（默认）
    3. historical: 历史全量抽取
    4. date_range: 指定日期范围抽取
    """
    import uuid
    from app.core.concurrency import async_task_manager
    
    try:
        # 参数验证
        if request.mode == "time_range":
            if not request.start_time or not request.end_time:
                raise HTTPException(status_code=400, detail="时间范围模式需要指定start_time和end_time")
            if request.start_time >= request.end_time:
                raise HTTPException(status_code=400, detail="开始时间必须早于结束时间")
        elif request.mode == "date_range":
            if not request.start_date or not request.end_date:
                raise HTTPException(status_code=400, detail="日期范围模式需要指定start_date和end_date")
        
        # 生成唯一任务ID
        task_id = f"extraction_{uuid.uuid4().hex[:16]}"
        username = current_user.get("username", "unknown")
        
        # 提交到后台异步执行，不等待完成
        success = await async_task_manager.submit_task(
            task_id,
            extract_work_data_async(db, request, username, task_id)
        )
        
        if not success:
            raise HTTPException(status_code=500, detail="任务提交失败")
        
        # 立即返回任务ID，不等待任务完成
        return {
            "success": True,
            "task_id": task_id,
            "mode": request.mode,
            "status": "submitted",
            "message": f"数据抽取任务已提交到后台执行",
            "check_status_url": f"/api/v1/tasks/status/{task_id}"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"数据抽取任务提交失败: {str(e)}")


async def extract_historical_data(db: Session, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None):
    """抽取历史全量数据"""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        # 如果没有指定日期范围，默认抽取近30天的数据
        if not end_date:
            end_date = datetime.now()
        if not start_date:
            start_date = end_date - timedelta(days=30)
        
        logger.info(f"开始历史数据抽取: {start_date.date()} 到 {end_date.date()}")
        
        total_extracted = 0
        total_inserted = 0
        current_date = start_date.date()
        end_date_only = end_date.date()
        
        # 按天循环抽取
        while current_date <= end_date_only:
            try:
                day_result = stage1_service.extract_work_data_by_time_range(
                    db=db,
                    target_date=datetime.combine(current_date, datetime.min.time()),
                    days_back=1
                )
                
                if day_result.get("success"):
                    stats = day_result.get("statistics", {})
                    total_extracted += stats.get("extracted", 0)
                    total_inserted += stats.get("inserted", 0)
                    logger.info(f"日期 {current_date}: 抽取 {stats.get('extracted', 0)} 个工单，插入 {stats.get('inserted', 0)} 条记录")
                
            except Exception as e:
                logger.error(f"日期 {current_date} 抽取失败: {e}")
            
            current_date += timedelta(days=1)
        
        return {
            "success": True,
            "mode": "historical",
            "date_range": {
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d")
            },
            "statistics": {
                "total_extracted": total_extracted,
                "total_inserted": total_inserted,
                "days_processed": (end_date.date() - start_date.date()).days + 1
            },
            "message": f"历史数据抽取完成，共处理 {(end_date.date() - start_date.date()).days + 1} 天的数据"
        }
        
    except Exception as e:
        logger.error(f"历史数据抽取失败: {e}")
        return {
            "success": False,
            "mode": "historical",
            "error": str(e),
            "message": "历史数据抽取失败"
        }


async def extract_date_range_data(db: Session, start_date: datetime, end_date: datetime):
    """抽取指定日期范围的数据"""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"开始日期范围数据抽取: {start_date.date()} 到 {end_date.date()}")
        
        total_extracted = 0
        total_inserted = 0
        current_date = start_date.date()
        end_date_only = end_date.date()
        
        # 按天循环抽取
        while current_date <= end_date_only:
            try:
                day_result = stage1_service.extract_work_data_by_time_range(
                    db=db,
                    target_date=datetime.combine(current_date, datetime.min.time()),
                    days_back=1
                )
                
                if day_result.get("success"):
                    stats = day_result.get("statistics", {})
                    total_extracted += stats.get("extracted", 0)
                    total_inserted += stats.get("inserted", 0)
                    logger.info(f"日期 {current_date}: 抽取 {stats.get('extracted', 0)} 个工单，插入 {stats.get('inserted', 0)} 条记录")
                
            except Exception as e:
                logger.error(f"日期 {current_date} 抽取失败: {e}")
            
            current_date += timedelta(days=1)
        
        return {
            "success": True,
            "mode": "date_range",
            "date_range": {
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d")
            },
            "statistics": {
                "total_extracted": total_extracted,
                "total_inserted": total_inserted,
                "days_processed": (end_date.date() - start_date.date()).days + 1
            },
            "message": f"日期范围数据抽取完成，共处理 {(end_date.date() - start_date.date()).days + 1} 天的数据"
        }
        
    except Exception as e:
        logger.error(f"日期范围数据抽取失败: {e}")
        return {
            "success": False,
            "mode": "date_range",
            "error": str(e),
            "message": "日期范围数据抽取失败"
        }


@router.get("/extraction/statistics")
async def get_extraction_statistics(
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    获取数据抽取统计信息
    """
    try:
        stats = stage1_service.get_extraction_statistics(db)
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取统计信息失败: {str(e)}")


# ===== 去噪相关接口 =====

@router.get("/denoise/summary")
async def get_denoise_summary(
    days: int = Query(7, description="统计天数", ge=1, le=30),
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    获取去噪统计摘要
    返回指定天数内的去噪处理统计信息
    """
    try:
        summary = denoise_record_manager.get_denoise_summary(db, days)
        return {
            "success": True,
            "data": summary,
            "message": f"成功获取 {days} 天内的去噪统计摘要"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取去噪统计摘要失败: {str(e)}")


@router.get("/denoise/batches")
async def get_denoise_batches(
    batch_id: Optional[str] = Query(None, description="批次ID"),
    limit: int = Query(50, description="查询限制", ge=1, le=500),
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    获取去噪批次列表
    支持按批次ID查询或获取最近的批次列表
    """
    try:
        batches = denoise_record_manager.get_batch_statistics(db, batch_id, limit)
        return {
            "success": True,
            "data": batches,
            "total": len(batches),
            "message": f"成功获取 {len(batches)} 个批次记录"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取批次列表失败: {str(e)}")


@router.get("/denoise/records")
async def get_denoise_records(
    work_id: Optional[int] = Query(None, description="工单ID"),
    batch_id: Optional[str] = Query(None, description="批次ID"),
    limit: int = Query(100, description="查询限制", ge=1, le=500),
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    获取工单去噪记录
    支持按工单ID或批次ID查询
    """
    try:
        records = denoise_record_manager.get_work_order_denoise_records(
            db, work_id, batch_id, limit
        )
        return {
            "success": True,
            "data": records,
            "total": len(records),
            "message": f"成功获取 {len(records)} 条去噪记录"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取去噪记录失败: {str(e)}")


@router.post("/denoise/test")
async def test_denoise_single_work_order(
    work_id: int,
    current_user: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    测试单个工单的去噪效果
    用于调试和验证去噪规则
    """
    try:
        # 获取工单评论数据
        pending_orders = stage1_service.get_pending_work_orders(db, limit=1000)
        target_order = None
        
        for order in pending_orders:
            if order["work_id"] == work_id:
                target_order = order
                break
        
        if not target_order:
            raise HTTPException(status_code=404, detail=f"工单 {work_id} 不在待处理列表中")
        
        # 获取评论
        comments = stage1_service.get_work_comments(
            db, work_id, target_order["comment_table_name"]
        )
        
        if not comments:
            return {
                "success": True,
                "work_id": work_id,
                "message": "工单无评论数据",
                "original_comments": [],
                "denoise_result": {
                    "filtered_comments": [],
                    "original_count": 0,
                    "filtered_count": 0,
                    "removed_count": 0,
                    "filter_statistics": {}
                }
            }
        
        # 过滤空评论
        valid_comments = [c for c in comments if c.get("content") and str(c.get("content", "")).strip()]
        
        # 应用去噪
        denoise_result = content_denoiser.filter_comments(valid_comments)
        
        # 返回详细的对比结果
        return {
            "success": True,
            "work_id": work_id,
            "message": f"去噪测试完成：{denoise_result['original_count']} -> {denoise_result['filtered_count']} 条评论",
            "original_comments": valid_comments,
            "denoise_result": denoise_result,
            "comparison": {
                "removed_comments": [
                    {
                        "index": detail["index"],
                        "content": detail["comment"].get("content", ""),
                        "user_type": detail["comment"].get("user_type", ""),
                        "name": detail["comment"].get("name", ""),
                        "reason": detail["reason"]
                    }
                    for detail in denoise_result["filter_statistics"].get("removed_details", [])
                ]
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"去噪测试失败: {str(e)}")


# ==================== 后台异步执行函数 ====================

async def analyze_batch_data_async(db: Session, request: AnalysisRequest, username: str, task_id: str):
    """异步批量分析待处理数据"""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"🚀 开始异步批量分析任务: {task_id}, 用户: {username}")
        
        from app.services.stage2_analysis_service import stage2_service
        
        result = await stage2_service.process_pending_analysis_queue(
            db=db,
            batch_size=request.limit or 50,
            max_concurrent=8
        )
        
        logger.info(f"✅ 异步批量分析任务完成: {task_id}")
        return result
        
    except Exception as e:
        logger.error(f"❌ 异步批量分析任务失败: {task_id}, {e}")
        raise


async def analyze_by_time_range_async(db: Session, request: AnalysisRequest, username: str, task_id: str):
    """异步时间段分析"""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"🚀 开始异步时间段分析任务: {task_id}, 用户: {username}")
        
        # 调用原来的时间段分析逻辑
        result = await analyze_by_time_range(db, request)
        
        logger.info(f"✅ 异步时间段分析任务完成: {task_id}")
        return result
        
    except Exception as e:
        logger.error(f"❌ 异步时间段分析任务失败: {task_id}, {e}")
        raise


async def extract_work_data_async(db: Session, request: ExtractionRequest, username: str, task_id: str):
    """异步数据抽取"""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"🚀 开始异步数据抽取任务: {task_id}, 用户: {username}, 模式: {request.mode}")
        
        from app.services.stage1_work_extraction import stage1_service
        
        if request.mode == "time_range":
            # 精确时间范围抽取
            result = stage1_service.extract_work_data_by_time_range(
                db=db,
                start_time=request.start_time,
                end_time=request.end_time
            )
        elif request.mode == "historical":
            # 历史全量抽取
            result = await extract_historical_data(db, request.start_date, request.end_date)
        elif request.mode == "date_range":
            # 日期范围抽取
            result = await extract_date_range_data(db, request.start_date, request.end_date)
        else:
            # 日常按天抽取（默认）
            result = stage1_service.extract_work_data_by_time_range(
                db=db,
                target_date=request.target_date,
                days_back=request.days_back
            )
        
        logger.info(f"✅ 异步数据抽取任务完成: {task_id}")
        return result
        
    except Exception as e:
        logger.error(f"❌ 异步数据抽取任务失败: {task_id}, {e}")
        raise


