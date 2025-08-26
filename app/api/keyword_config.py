"""
关键词配置管理API
用于管理数据库中的关键词和正则规则配置
"""
import logging
from typing import Dict, List, Any, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from pydantic import BaseModel

from app.db.database import get_db
from app.services.keyword_config_manager import keyword_config_manager
from app.core.auth import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/keyword-config", tags=["关键词配置管理"])


# ==================== 请求/响应模型 ====================

class KeywordConfigCreate(BaseModel):
    """创建关键词配置的请求模型"""
    category_key: str
    keyword_type: str  # keyword, pattern, exclusion
    keyword_value: str
    weight: float = 1.0
    risk_level: str = "medium"
    description: Optional[str] = None


class KeywordConfigUpdate(BaseModel):
    """更新关键词配置的请求模型"""
    keyword_value: Optional[str] = None
    weight: Optional[float] = None
    risk_level: Optional[str] = None
    description: Optional[str] = None
    is_enabled: Optional[bool] = None


class DenoisePatternCreate(BaseModel):
    """创建去噪模式的请求模型"""
    pattern_name: str
    pattern_type: str  # normal_operation, invalid_data, system_keyword
    pattern_value: str
    description: Optional[str] = None
    action: str = "filter_out"


class ConfigResponse(BaseModel):
    """配置响应模型"""
    success: bool
    message: str
    data: Optional[Any] = None


# ==================== 分析关键词配置API ====================

@router.get("/analysis/categories", response_model=ConfigResponse)
async def get_analysis_categories(
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """获取分析关键词配置分类"""
    try:
        config = keyword_config_manager.get_analysis_keywords_config(db, use_cache=False)
        
        return ConfigResponse(
            success=True,
            message="获取分析关键词配置成功",
            data={
                "categories": list(config.keys()),
                "config": config
            }
        )
    except Exception as e:
        logger.error(f"获取分析关键词配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analysis/reload", response_model=ConfigResponse)
async def reload_analysis_config(
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """重新加载分析关键词配置"""
    try:
        result = keyword_config_manager.reload_config(db)
        
        return ConfigResponse(
            success=result["success"],
            message=result["message"],
            data=result.get("statistics")
        )
    except Exception as e:
        logger.error(f"重新加载配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analysis/keywords", response_model=ConfigResponse)
async def add_keyword_config(
    config: KeywordConfigCreate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """添加关键词配置"""
    try:
        success = keyword_config_manager.add_keyword_config(
            db=db,
            category_key=config.category_key,
            keyword_type=config.keyword_type,
            keyword_value=config.keyword_value,
            weight=config.weight,
            risk_level=config.risk_level,
            description=config.description
        )
        
        if success:
            return ConfigResponse(
                success=True,
                message="添加关键词配置成功"
            )
        else:
            raise HTTPException(status_code=400, detail="添加关键词配置失败")
            
    except Exception as e:
        logger.error(f"添加关键词配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/analysis/keywords/{config_id}", response_model=ConfigResponse)
async def update_keyword_config(
    config_id: int,
    config: KeywordConfigUpdate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """更新关键词配置"""
    try:
        # 过滤出非空字段
        update_data = {k: v for k, v in config.dict().items() if v is not None}
        
        success = keyword_config_manager.update_keyword_config(
            db=db,
            config_id=config_id,
            **update_data
        )
        
        if success:
            return ConfigResponse(
                success=True,
                message="更新关键词配置成功"
            )
        else:
            raise HTTPException(status_code=404, detail="关键词配置不存在")
            
    except Exception as e:
        logger.error(f"更新关键词配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/analysis/keywords/{config_id}", response_model=ConfigResponse)
async def delete_keyword_config(
    config_id: int,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """删除关键词配置"""
    try:
        success = keyword_config_manager.delete_keyword_config(db, config_id)
        
        if success:
            return ConfigResponse(
                success=True,
                message="删除关键词配置成功"
            )
        else:
            raise HTTPException(status_code=404, detail="关键词配置不存在")
            
    except Exception as e:
        logger.error(f"删除关键词配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/analysis/keywords/{config_id}/toggle", response_model=ConfigResponse)
async def toggle_keyword_config(
    config_id: int,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """切换关键词配置的启用状态"""
    try:
        from sqlalchemy import text
        
        # 查询当前状态
        check_sql = "SELECT is_enabled FROM ai_keyword_configs WHERE id = :config_id"
        result = db.execute(text(check_sql), {"config_id": config_id}).fetchone()
        
        if not result:
            raise HTTPException(status_code=404, detail="关键词配置不存在")
        
        # 切换状态
        new_status = not result.is_enabled
        success = keyword_config_manager.update_keyword_config(
            db=db,
            config_id=config_id,
            is_enabled=new_status
        )
        
        if success:
            status_text = "启用" if new_status else "禁用"
            return ConfigResponse(
                success=True,
                message=f"关键词配置已{status_text}",
                data={"config_id": config_id, "is_enabled": new_status}
            )
        else:
            raise HTTPException(status_code=400, detail="切换状态失败")
            
    except Exception as e:
        logger.error(f"切换关键词配置状态失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 去噪配置API ====================

@router.get("/denoise/patterns", response_model=ConfigResponse)
async def get_denoise_patterns(
    pattern_type: Optional[str] = Query(None, description="模式类型过滤"),
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """获取去噪模式配置"""
    try:
        patterns = keyword_config_manager.get_denoise_patterns(db, pattern_type, use_cache=False)
        
        return ConfigResponse(
            success=True,
            message="获取去噪模式配置成功",
            data={
                "pattern_type": pattern_type,
                "patterns": patterns,
                "count": len(patterns)
            }
        )
    except Exception as e:
        logger.error(f"获取去噪模式配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/denoise/system-keywords", response_model=ConfigResponse)
async def get_system_keywords(
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """获取系统关键词"""
    try:
        keywords = keyword_config_manager.get_system_keywords(db, use_cache=False)
        
        return ConfigResponse(
            success=True,
            message="获取系统关键词成功",
            data={
                "keywords": keywords,
                "count": len(keywords)
            }
        )
    except Exception as e:
        logger.error(f"获取系统关键词失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 配置导入导出API ====================

@router.get("/export", response_model=ConfigResponse)
async def export_config(
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """导出所有配置数据"""
    try:
        result = keyword_config_manager.export_config(db)
        
        return ConfigResponse(
            success=result["success"],
            message="导出配置成功" if result["success"] else "导出配置失败",
            data=result.get("data")
        )
    except Exception as e:
        logger.error(f"导出配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/clear-cache", response_model=ConfigResponse)
async def clear_config_cache(
    current_user: dict = Depends(get_current_user)
):
    """清空配置缓存"""
    try:
        keyword_config_manager.clear_cache()
        
        return ConfigResponse(
            success=True,
            message="配置缓存已清空"
        )
    except Exception as e:
        logger.error(f"清空配置缓存失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 配置测试API ====================

@router.post("/test/keyword-screening", response_model=ConfigResponse)
async def test_keyword_screening(
    text: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """测试关键词筛选"""
    try:
        from app.services.stage2_analysis_service import stage2_service
        
        result = stage2_service.keyword_screening(text, db)
        
        return ConfigResponse(
            success=True,
            message="关键词筛选测试完成",
            data=result
        )
    except Exception as e:
        logger.error(f"关键词筛选测试失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/test/denoise", response_model=ConfigResponse)
async def test_denoise_filter(
    comments: List[Dict[str, Any]],
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """测试去噪过滤"""
    try:
        from app.services.content_denoiser import content_denoiser
        
        result = content_denoiser.filter_comments(comments, db)
        
        return ConfigResponse(
            success=True,
            message="去噪过滤测试完成",
            data=result
        )
    except Exception as e:
        logger.error(f"去噪过滤测试失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 配置统计API ====================

@router.get("/statistics", response_model=ConfigResponse)
async def get_config_statistics(
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """获取配置统计信息（仅启用的配置）"""
    try:
        from sqlalchemy import text
        
        # 统计各类配置数量
        stats_sql = """
        SELECT 
            'analysis_categories' as type,
            COUNT(*) as count
        FROM ai_keyword_categories 
        WHERE category_type = 'analysis' AND is_enabled = 1
        
        UNION ALL
        
        SELECT 
            'analysis_keywords' as type,
            COUNT(*) as count
        FROM ai_keyword_configs kc
        JOIN ai_keyword_categories cat ON kc.category_id = cat.id
        WHERE cat.category_type = 'analysis' AND kc.is_enabled = 1
        
        UNION ALL
        
        SELECT 
            pattern_type as type,
            COUNT(*) as count
        FROM ai_denoise_patterns 
        WHERE is_enabled = 1
        GROUP BY pattern_type
        """
        
        results = db.execute(text(stats_sql)).fetchall()
        
        statistics = {}
        for result in results:
            statistics[result.type] = result.count
        
        return ConfigResponse(
            success=True,
            message="获取配置统计成功",
            data=statistics
        )
    except Exception as e:
        logger.error(f"获取配置统计失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics/detailed", response_model=ConfigResponse)
async def get_detailed_config_statistics(
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user)
):
    """获取详细的配置统计信息（包括启用/禁用状态）"""
    try:
        result = keyword_config_manager.get_config_statistics(db)
        
        return ConfigResponse(
            success=result["success"],
            message="获取详细配置统计成功" if result["success"] else "获取详细配置统计失败",
            data=result.get("data")
        )
    except Exception as e:
        logger.error(f"获取详细配置统计失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))
