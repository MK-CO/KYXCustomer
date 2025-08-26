"""
会话分析结果数据模型
"""
from datetime import datetime
from sqlalchemy import Column, Integer, String, Text, DateTime, Float, DECIMAL, Index, ForeignKey, BigInteger, Boolean
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

from app.db.database import Base



class WorkComment(Base):
    """工单回复记录表 - 对应t_work_comment"""
    __tablename__ = "t_work_comment"
    
    id = Column(BigInteger, primary_key=True, comment="主键ID")
    work_id = Column(BigInteger, nullable=False, comment="工单id")
    oper = Column(Boolean, default=False, comment="是否处理人")
    content = Column(String(500), nullable=True, comment="回复内容")
    image = Column(Text, nullable=True, comment="回复图片")
    user_type = Column(String(20), nullable=False, comment="回复人类型")
    user_id = Column(BigInteger, nullable=False, comment="回复人id")
    name = Column(String(64), nullable=True, comment="回复人名称")
    reissue = Column(Integer, default=0, comment="补发标记")
    ip = Column(String(30), nullable=True, comment="ip地址")
    create_by = Column(String(64), nullable=True, comment="回复人")
    create_time = Column(DateTime, nullable=True, comment="回复时间")
    deleted = Column(Boolean, default=False, comment="逻辑删除")
    coze_status = Column(String(20), default='UNUSED', comment="扣子访问状态")
    
    # 创建索引
    __table_args__ = (
        Index('idx_work_id', 'work_id'),
        Index('idx_user_id', 'user_id'),
        Index('idx_user_type', 'user_type'),
        Index('idx_create_time', 'create_time'),
        Index('idx_coze_status', 'coze_status'),
        Index('idx_deleted', 'deleted'),
    )


class WorkCommentAnalysisResult(Base):
    """工单回复分析结果表 - 专门针对t_work_comment的分析结果"""
    __tablename__ = "ai_work_comment_analysis_results"
    
    id = Column(Integer, primary_key=True, index=True, comment="主键ID")
    work_id = Column(BigInteger, nullable=False, index=True, comment="工单ID")
    order_id = Column(BigInteger, nullable=True, index=True, comment="订单ID（从工单表关联获取）")
    order_no = Column(String(100), nullable=True, index=True, comment="订单编号（从工单表关联获取）")
    session_id = Column(String(128), nullable=True, comment="会话ID（同work_id）")
    
    # 会话基本信息
    session_start_time = Column(DateTime, nullable=True, comment="会话开始时间")
    session_end_time = Column(DateTime, nullable=True, comment="会话结束时间")
    total_comments = Column(Integer, default=0, comment="总回复数")
    customer_comments = Column(Integer, default=0, comment="客户回复数")
    service_comments = Column(Integer, default=0, comment="客服回复数")
    
    # 规避责任检测结果
    has_evasion = Column(Integer, default=0, comment="是否规避责任 0/1")
    risk_level = Column(String(16), default='low', comment="风险级别: low/medium/high")
    confidence_score = Column(DECIMAL(4, 3), default=0.000, comment="置信度评分")
    evasion_types = Column(Text, nullable=True, comment="规避类型列表(JSON)")
    evidence_sentences = Column(Text, nullable=True, comment="证据句子列表(JSON)")
    improvement_suggestions = Column(Text, nullable=True, comment="改进建议列表(JSON)")
    
    # 关键词粗筛结果
    keyword_screening_score = Column(DECIMAL(4, 3), default=0.000, comment="关键词筛选评分")
    matched_categories = Column(String(255), nullable=True, comment="匹配的关键词类别")
    matched_keywords = Column(Text, nullable=True, comment="匹配的关键词详情(JSON)")
    is_suspicious = Column(Integer, default=0, comment="关键词粗筛是否疑似")
    
    # 情感分析结果
    sentiment = Column(String(16), default='neutral', comment="情感: positive/negative/neutral")
    sentiment_intensity = Column(DECIMAL(4, 3), default=0.000, comment="情感强度")
    
    # 原始数据
    conversation_text = Column(Text, nullable=True, comment="完整对话文本")
    llm_raw_response = Column(Text, nullable=True, comment="LLM原始响应")
    analysis_details = Column(Text, nullable=True, comment="完整分析结果(JSON)")
    analysis_note = Column(String(255), nullable=True, comment="分析备注（如：评论为空、LLM分析失败等）")
    
    # LLM调用信息
    llm_provider = Column(String(32), nullable=True, comment="LLM提供商")
    llm_model = Column(String(64), nullable=True, comment="LLM模型名称")
    llm_tokens_used = Column(Integer, default=0, comment="LLM消耗token数")
    
    # 时间戳
    analysis_time = Column(DateTime, default=func.now(), comment="分析执行时间")
    created_at = Column(DateTime, default=func.now(), comment="创建时间")
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now(), comment="更新时间")
    
    # 创建索引
    __table_args__ = (
        Index('idx_work_id_analysis', 'work_id'),
        Index('idx_order_id_analysis', 'order_id'),
        Index('idx_order_no_analysis', 'order_no'),
        Index('idx_risk_level_analysis', 'risk_level'),
        Index('idx_has_evasion_analysis', 'has_evasion'),
        Index('idx_analysis_time_analysis', 'analysis_time'),
        Index('idx_session_start_time_analysis', 'session_start_time'),
        Index('idx_confidence_score_analysis', 'confidence_score'),
    )
