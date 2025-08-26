"""
服务模块
"""
from .stage1_work_extraction import stage1_service
from .stage2_analysis_service import stage2_service

__all__ = ["stage1_service", "stage2_service"]
