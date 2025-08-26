"""
第二阶段：工单评论分析服务
获取待处理工单，分析评论内容，执行AI分析任务，包含批量分析和检测引擎功能
"""
import json
import re
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.database import get_db
from app.services.stage1_work_extraction import stage1_service
from app.services.llm.llm_factory import get_llm_provider
from app.services.content_denoiser import content_denoiser
from app.services.keyword_config_manager import keyword_config_manager
from app.models.denoise import safe_json_dumps
from config.settings import settings

logger = logging.getLogger(__name__)


class Stage2AnalysisService:
    """第二阶段：工单评论分析服务"""
    
    def __init__(self):
        """初始化第二阶段服务"""
        self.stage1 = stage1_service
        self.pending_table_name = "ai_work_pending_analysis"
        self.results_table_name = "ai_work_comment_analysis_results"
        self.llm_provider = get_llm_provider()
        self.keywords_config = {}  # 改为从数据库动态加载
        self.few_shot_examples = self._init_few_shot_examples()
    
    # ==================== 待处理工单获取方法 ====================
    
    def get_pending_work_orders_with_comments(
        self,
        db: Session,
        limit: int = 50,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """获取待处理工单及其评论数据
        
        Args:
            db: 数据库会话
            limit: 限制数量
            start_date: 开始时间（按create_time过滤）
            end_date: 结束时间（按create_time过滤）
        """
        time_range_info = ""
        if start_date or end_date:
            time_parts = []
            if start_date:
                time_parts.append(f"从{start_date}")
            if end_date:
                time_parts.append(f"到{end_date}")
            time_range_info = f" ({' '.join(time_parts)})"
        
        logger.info(f"📋 开始拉取pending工单数据，限制 {limit} 条{time_range_info}")
        
        try:
            # 1. 获取待处理工单列表（🔥 修复：分析阶段不使用时间过滤，处理所有PENDING）
            logger.info("📥 正在查询数据库中的PENDING状态工单...")
            pending_orders = self.stage1.get_pending_work_orders(
                db, ai_status='PENDING', limit=limit,
                start_date=start_date, end_date=end_date
            )
            logger.info(f"📊 从数据库查询到 {len(pending_orders) if pending_orders else 0} 个PENDING工单")
            
            if not pending_orders:
                return {
                    "success": True,
                    "stage": "第二阶段：获取待处理工单",
                    "message": "没有待处理的工单",
                    "work_orders": [],
                    "statistics": {
                        "total_pending": 0,
                        "with_comments": 0,
                        "without_comments": 0
                    }
                }
            
            # 2. 批量获取评论数据
            logger.info(f"💬 开始处理 {len(pending_orders)} 个工单的评论数据...")
            work_orders_with_comments = []
            with_comments_count = 0
            without_comments_count = 0
            denoised_count = 0  # 🔥 新增：去噪处理的工单数
            
            for i, order in enumerate(pending_orders, 1):
                work_id = order["work_id"]
                comment_table_name = order["comment_table_name"]
                
                logger.info(f"📋 处理工单 {work_id} ({i}/{len(pending_orders)}) - 评论表: {comment_table_name}")
                
                # 获取评论数据
                comments = self.stage1.get_work_comments(db, work_id, comment_table_name)
                logger.info(f"💭 工单 {work_id} 获取到 {len(comments) if comments else 0} 条原始评论")
                
                # 过滤有效评论 - 防止NoneType错误
                valid_comments = [c for c in comments if c.get("content") and str(c.get("content", "")).strip()]
                
                # 应用去噪过滤并保存记录
                if valid_comments:
                    logger.info(f"🔍 工单 {work_id} 开始去噪处理 {len(valid_comments)} 条有效评论...")
                    denoise_result = content_denoiser.filter_comments_with_record(
                        valid_comments, work_id, db, save_record=True
                    )
                    valid_comments = denoise_result["filtered_comments"]
                    logger.info(f"✅ 工单 {work_id} 去噪完成: {denoise_result['original_count']} -> {denoise_result['filtered_count']} 条评论")
                    if denoise_result["removed_count"] > 0:
                        denoised_count += 1  # 🔥 统计去噪处理的工单数
                        logger.info(f"🗑️ 工单 {work_id} 去噪移除了 {denoise_result['removed_count']} 条评论")
                else:
                    logger.info(f"⚠️ 工单 {work_id} 无有效评论，跳过去噪处理")
                    denoise_result = None
                
                if valid_comments:
                    with_comments_count += 1
                    comment_data = self._build_conversation_json(valid_comments)
                    logger.info(f"✅ 工单 {work_id} 有 {len(valid_comments)} 条有效评论，构建完成对话数据")
                    
                    # 更新工单评论统计
                    self.stage1.update_work_order_ai_status(
                        db, work_id, 'PENDING',
                        comment_count=len(valid_comments),
                        has_comments=True
                    )
                else:
                    without_comments_count += 1
                    comment_data = None
                    
                    # 🔥 优化：空评论工单直接标记为完成状态，不保存低风险分析结果
                    logger.info(f"🚫 工单 {work_id} 没有评论，直接标记为完成（不保存分析结果）")
                    self.stage1.update_work_order_ai_status(
                        db, work_id, 'COMPLETED',
                        comment_count=0,
                        has_comments=False,
                        error_message="评论为空，低风险不保存分析结果"
                    )
                    
                    # 🔥 不再保存空评论工单的分析结果，因为都是低风险
                
                # 构建完整的工单数据
                work_order_data = {
                    "pending_id": order["id"],
                    "work_id": work_id,
                    "work_table_name": order["work_table_name"],
                    "comment_table_name": order["comment_table_name"],
                    "extract_date": order["extract_date"],
                    "create_time": order["create_time"],
                    "work_type": order["work_type"],
                    "work_state": order["work_state"],
                    "create_by": order["create_by"],
                    "create_name": order["create_name"],
                    "ai_status": order["ai_status"],
                    "has_comments": comment_data is not None,
                    "comment_count": len(valid_comments) if valid_comments else 0,
                    "comments_data": comment_data  # 包含完整的评论数据
                }
                
                work_orders_with_comments.append(work_order_data)
            
            result = {
                "success": True,
                "stage": "第二阶段：获取待处理工单评论",
                "message": f"获取到 {len(pending_orders)} 个待处理工单，其中 {with_comments_count} 个有评论",
                "work_orders": work_orders_with_comments,
                "statistics": {
                    "total_pending": len(pending_orders),
                    "with_comments": with_comments_count,
                    "without_comments": without_comments_count,
                    "denoised_count": denoised_count  # 🔥 新增：去噪处理的工单数
                }
            }
            
            logger.info("=" * 60)
            logger.info(f"📋 pending工单数据拉取完成总结:")
            logger.info(f"  📥 查询到工单总数: {len(pending_orders)}")
            logger.info(f"  💬 有评论可分析: {with_comments_count}")
            logger.info(f"  💭 无评论已完成: {without_comments_count}")
            logger.info(f"  🔍 执行去噪处理: {denoised_count}")
            logger.info("=" * 60)
            return result
            
        except Exception as e:
            logger.error(f"第二阶段获取待处理工单评论失败: {e}")
            return {
                "success": False,
                "stage": "第二阶段：获取待处理工单评论",
                "error": str(e),
                "message": "获取评论数据失败"
            }
    
    def _build_conversation_json(self, comments: List[Dict[str, Any]]) -> Dict[str, Any]:
        """构建工单对话JSON结构"""
        if not comments:
            return {
                "work_id": None,
                "total_messages": 0,
                "customer_messages": 0,
                "service_messages": 0,
                "system_messages": 0,
                "conversation_text": "",
                "messages": [],
                "session_info": {
                    "start_time": None,
                    "end_time": None,
                    "duration_minutes": 0
                }
            }
        
        work_id = comments[0]["work_id"]
        
        # 统计消息类型
        customer_count = 0
        service_count = 0
        system_count = 0
        
        messages = []
        
        for comment in comments:
            user_type = comment.get("user_type", "")
            oper = comment.get("oper", False)
            
            # 统计消息数量
            if user_type == "customer":
                customer_count += 1
            elif user_type == "service" or oper:
                service_count += 1
            elif user_type == "system":
                system_count += 1
            
            # 构建消息对象
            messages.append({
                "id": comment["id"],
                "user_type": user_type,
                "user_id": comment.get("user_id"),
                "name": comment.get("name"),
                "content": str(comment.get("content") or ""),  # 防止NoneType错误
                "create_time": comment["create_time"].isoformat() if isinstance(comment["create_time"], datetime) else str(comment["create_time"]),
                "oper": oper,
                "image": comment.get("image"),
                "reissue": comment.get("reissue", 0)
            })
        
        # 计算会话时长
        start_time = comments[0]["create_time"]
        end_time = comments[-1]["create_time"]
        duration_minutes = 0
        
        if isinstance(start_time, datetime) and isinstance(end_time, datetime):
            duration = end_time - start_time
            duration_minutes = duration.total_seconds() / 60
        
        # 构建对话文本
        conversation_text = self.stage1.build_conversation_text(comments)
        
        return {
            "work_id": work_id,
            "total_messages": len(comments),
            "customer_messages": customer_count,
            "service_messages": service_count,
            "system_messages": system_count,
            "conversation_text": conversation_text,
            "messages": messages,
            "session_info": {
                "start_time": start_time.isoformat() if isinstance(start_time, datetime) else str(start_time),
                "end_time": end_time.isoformat() if isinstance(end_time, datetime) else str(end_time),
                "duration_minutes": round(duration_minutes, 2)
            },
            "metadata": {
                "extracted_at": datetime.now().isoformat(),
                "source_table": comments[0].get("source_table", ""),
                "comment_ids": [c["id"] for c in comments]
            }
        }
    
    # ==================== 去噪处理方法 ====================
    
    def apply_denoise_to_work_orders(
        self,
        work_orders: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """对工单列表应用去噪处理"""
        logger.info(f"🔍 开始对 {len(work_orders)} 个工单应用去噪处理")
        
        try:
            # 🔥 修复：使用去噪器进行批量处理，传递数据库会话以保存记录
            from app.db.connection_manager import get_db_session
            
            with get_db_session() as db_session:
                denoise_result = content_denoiser.batch_filter_work_orders(
                    work_orders, db=db_session, save_records=True
                )
            
            logger.info("🎉 批量去噪处理完成:")
            logger.info(f"  📋 处理工单数: {denoise_result['total_work_orders']}")
            logger.info(f"  📥 原始评论总数: {denoise_result['statistics']['total_original_comments']}")
            logger.info(f"  📤 过滤后评论总数: {denoise_result['statistics']['total_filtered_comments']}")
            logger.info(f"  🗑️ 移除评论总数: {denoise_result['statistics']['total_removed_comments']}")
            logger.info(f"  📊 整体过滤率: {denoise_result['statistics']['overall_filter_rate']:.1f}%")
            
            if denoise_result['statistics']['filter_reasons']:
                logger.info("🔍 过滤原因统计:")
                for reason, count in denoise_result['statistics']['filter_reasons'].items():
                    logger.info(f"  - {reason}: {count} 条")
            
            return {
                "success": True,
                "processed_orders": denoise_result["processed_orders"],
                "denoise_statistics": denoise_result["statistics"],
                "message": f"成功对 {denoise_result['total_work_orders']} 个工单应用去噪，总体过滤率 {denoise_result['statistics']['overall_filter_rate']:.1f}%"
            }
            
        except Exception as e:
            logger.error(f"❌ 批量去噪处理失败: {e}")
            return {
                "success": False,
                "error": str(e),
                "message": "去噪处理失败"
            }
    
    # ==================== 单个工单处理方法 ====================
    
    def process_single_work_order(
        self,
        db: Session,
        work_id: int
    ) -> Dict[str, Any]:
        """处理单个工单的完整流程"""
        logger.info(f"开始处理单个工单: {work_id}")
        
        try:
            # 1. 查找工单在待处理表中的记录
            pending_orders = self.stage1.get_pending_work_orders(db, limit=1000)
            target_order = None
            
            for order in pending_orders:
                if order["work_id"] == work_id:
                    target_order = order
                    break
            
            if not target_order:
                return {
                    "success": False,
                    "work_id": work_id,
                    "message": f"工单 {work_id} 不在待处理列表中"
                }
            
            # 2. 更新状态为处理中
            self.stage1.update_work_order_ai_status(
                db, work_id, 'PROCESSING'
            )
            
            # 3. 获取评论数据
            comments = self.stage1.get_work_comments(
                db, work_id, target_order["comment_table_name"]
            )
            
            # 过滤有效评论 - 防止NoneType错误
            valid_comments = [c for c in comments if c.get("content") and str(c.get("content", "")).strip()]
            
            # 应用去噪过滤并保存记录
            if valid_comments:
                denoise_result = content_denoiser.filter_comments_with_record(
                    valid_comments, work_id, db, save_record=True
                )
                valid_comments = denoise_result["filtered_comments"]
                logger.info(f"🔍 工单 {work_id} 去噪结果: {denoise_result['original_count']} -> {denoise_result['filtered_count']} 条评论")
                if denoise_result["removed_count"] > 0:
                    logger.debug(f"去噪移除: {denoise_result['filter_statistics']['filter_reasons']}")
                # 记录去噪保存状态
                if denoise_result.get("denoise_record", {}).get("saved"):
                    logger.debug(f"💾 工单 {work_id} 去噪记录已保存，批次: {denoise_result['denoise_record']['batch_id']}")
            
            comment_data = self._build_conversation_json(valid_comments) if valid_comments else None
            
            # 4. 构建结果
            result = {
                "success": True,
                "work_id": work_id,
                "pending_id": target_order["id"],
                "work_table_name": target_order["work_table_name"],
                "comment_table_name": target_order["comment_table_name"],
                "work_info": {
                    "extract_date": target_order["extract_date"],
                    "create_time": target_order["create_time"],
                    "work_type": target_order["work_type"],
                    "work_state": target_order["work_state"],
                    "create_by": target_order["create_by"],
                    "create_name": target_order["create_name"]
                },
                "has_comments": comment_data is not None,
                "comment_count": len(valid_comments) if valid_comments else 0,
                "comments_data": comment_data,
                "ai_status": "PROCESSING"
            }
            
            # 5. 更新评论统计
            self.stage1.update_work_order_ai_status(
                db, work_id, 'PROCESSING',
                comment_count=result["comment_count"],
                has_comments=result["has_comments"]
            )
            
            logger.info(f"工单 {work_id} 处理完成，有评论: {result['has_comments']}")
            return result
            
        except Exception as e:
            logger.error(f"处理工单 {work_id} 失败: {e}")
            
            # 更新状态为失败
            try:
                self.stage1.update_work_order_ai_status(
                    db, work_id, 'FAILED', error_message=str(e)
                )
            except:
                pass
            
            return {
                "success": False,
                "work_id": work_id,
                "error": str(e),
                "message": "处理失败"
            }
    
    # ==================== AI分析结果保存方法 ====================
    
    def _get_order_info_by_work_id(self, db: Session, work_id: int) -> tuple[Optional[int], Optional[str]]:
        """根据工单ID查询订单ID和订单编号"""
        try:
            # 获取当前年份，构造工单表名
            current_year = datetime.now().year
            work_order_table = f"t_work_order_{current_year}"
            
            sql = f"""
            SELECT order_id, order_no FROM {work_order_table} 
            WHERE id = :work_id 
            LIMIT 1
            """
            
            logger.debug(f"🔍 查询工单 {work_id} 的订单信息，使用表: {work_order_table}")
            
            result = db.execute(text(sql), {"work_id": work_id}).fetchone()
            
            if result:
                order_id = result[0]
                order_no = result[1]
                logger.debug(f"✅ 工单 {work_id} 对应的订单信息: order_id={order_id}, order_no={order_no}")
                return order_id, order_no
            else:
                logger.warning(f"⚠️ 在表 {work_order_table} 中未找到工单 {work_id} 对应的订单信息")
                return None, None
                
        except Exception as e:
            logger.error(f"❌ 查询工单 {work_id} 的订单信息失败，表: {work_order_table}, 错误: {e}")
            return None, None

    def save_analysis_result(
        self,
        db: Session,
        work_id: int,
        analysis_result: Dict[str, Any]
    ) -> bool:
        """保存AI分析结果到结果表"""
        logger.info(f"💾 保存工单 {work_id} 分析结果: 风险级别={analysis_result.get('risk_level', 'low')}, 规避责任={analysis_result.get('has_evasion', False)}")
        
        try:
            # 查询订单ID和订单编号
            order_id, order_no = self._get_order_info_by_work_id(db, work_id)
            
            # 🔥 修复：使用 INSERT ... ON DUPLICATE KEY UPDATE 语法避免重复插入
            # 这里使用 MySQL 的 UPSERT 语法，可以原子性地处理插入或更新
            upsert_sql = f"""
            INSERT INTO {self.results_table_name} (
                work_id, order_id, order_no, session_start_time, session_end_time,
                total_comments, customer_comments, service_comments,
                has_evasion, risk_level, confidence_score,
                evasion_types, evidence_sentences, improvement_suggestions,
                keyword_screening_score, matched_categories, matched_keywords, is_suspicious,
                sentiment, sentiment_intensity, conversation_text,
                llm_raw_response, analysis_details, analysis_note,
                llm_provider, llm_model, llm_tokens_used,
                analysis_time, created_at, updated_at
            ) VALUES (
                :work_id, :order_id, :order_no, :session_start_time, :session_end_time,
                :total_comments, :customer_comments, :service_comments,
                :has_evasion, :risk_level, :confidence_score,
                :evasion_types, :evidence_sentences, :improvement_suggestions,
                :keyword_screening_score, :matched_categories, :matched_keywords, :is_suspicious,
                :sentiment, :sentiment_intensity, :conversation_text,
                :llm_raw_response, :analysis_details, :analysis_note,
                :llm_provider, :llm_model, :llm_tokens_used,
                :analysis_time, :created_at, :updated_at
            ) ON DUPLICATE KEY UPDATE
                order_id = VALUES(order_id),
                order_no = VALUES(order_no),
                session_start_time = VALUES(session_start_time),
                session_end_time = VALUES(session_end_time),
                total_comments = VALUES(total_comments),
                customer_comments = VALUES(customer_comments),
                service_comments = VALUES(service_comments),
                has_evasion = VALUES(has_evasion),
                risk_level = VALUES(risk_level),
                confidence_score = VALUES(confidence_score),
                evasion_types = VALUES(evasion_types),
                evidence_sentences = VALUES(evidence_sentences),
                improvement_suggestions = VALUES(improvement_suggestions),
                keyword_screening_score = VALUES(keyword_screening_score),
                matched_categories = VALUES(matched_categories),
                matched_keywords = VALUES(matched_keywords),
                is_suspicious = VALUES(is_suspicious),
                sentiment = VALUES(sentiment),
                sentiment_intensity = VALUES(sentiment_intensity),
                conversation_text = VALUES(conversation_text),
                llm_raw_response = VALUES(llm_raw_response),
                analysis_details = VALUES(analysis_details),
                analysis_note = VALUES(analysis_note),
                llm_provider = VALUES(llm_provider),
                llm_model = VALUES(llm_model),
                llm_tokens_used = VALUES(llm_tokens_used),
                analysis_time = VALUES(analysis_time),
                updated_at = VALUES(updated_at)
            """
            
            params = self._build_analysis_params(work_id, analysis_result, order_id, order_no)
            params["created_at"] = datetime.now()
            params["updated_at"] = datetime.now()
            
            result = db.execute(text(upsert_sql), params)
            
            # 检查是插入还是更新
            if result.rowcount == 1:
                logger.info(f"✅ 成功插入工单 {work_id} 的分析结果")
            elif result.rowcount == 2:
                logger.info(f"✅ 成功更新工单 {work_id} 的分析结果")
            else:
                logger.warning(f"⚠️ 工单 {work_id} 保存结果异常: rowcount={result.rowcount}")
            
            db.commit()
            return True
            
        except Exception as e:
            logger.error(f"保存工单 {work_id} 分析结果失败: {e}")
            # 🔥 修复：如果是重复键错误，可能是并发导致的，不算真正失败
            if "Duplicate entry" in str(e) or "UNIQUE constraint failed" in str(e):
                logger.warning(f"⚠️ 工单 {work_id} 检测到重复键，可能是并发插入，忽略此错误")
                db.rollback()
                return True  # 重复键不算失败，因为数据已经存在
            db.rollback()
            return False
    
    def _safe_truncate_text(self, text: str, max_length: int, suffix: str = "...") -> str:
        """安全截断文本，确保不超出指定长度"""
        if not text or len(text) <= max_length:
            return text
        
        actual_max = max_length - len(suffix)
        if actual_max <= 0:
            return suffix[:max_length]
        
        return text[:actual_max] + suffix
    
    def _safe_truncate_json(self, data: Any, max_length: int) -> str:
        """安全截断JSON数据，确保不超出指定长度"""
        try:
            json_str = safe_json_dumps(data, ensure_ascii=False)
            if len(json_str) <= max_length:
                return json_str
            
            # 如果是列表，尝试减少元素数量
            if isinstance(data, list) and len(data) > 1:
                reduced_count = max(1, len(data) // 2)
                truncated_data = data[:reduced_count]
                # 添加截断标记
                if isinstance(truncated_data[0], str):
                    truncated_data.append(f"... (已截断，原始共{len(data)}项)")
                json_str = safe_json_dumps(truncated_data, ensure_ascii=False)
                
                # 如果还是太长，直接截断字符串
                if len(json_str) > max_length:
                    return self._safe_truncate_text(json_str, max_length)
                return json_str
            
            # 对于其他类型，直接截断字符串
            return self._safe_truncate_text(json_str, max_length)
            
        except Exception as e:
            logger.warning(f"JSON截断失败: {e}")
            return f'{{"error": "数据过长已截断", "original_type": "{type(data).__name__}"}}'

    def _build_analysis_params(self, work_id: int, analysis_result: Dict[str, Any], order_id: Optional[int] = None, order_no: Optional[str] = None) -> Dict[str, Any]:
        """构建分析结果参数，确保所有字段不超出数据库限制"""
        import json
        
        # 定义字段长度限制（根据数据库表结构设置）
        FIELD_LIMITS = {
            "conversation_text": 8000,      # TEXT字段通常8KB左右
            "llm_raw_response": 4000,       # JSON字段
            "analysis_details": 4000,       # JSON字段
            "evidence_sentences": 3000,     # JSON字段
            "improvement_suggestions": 2000, # JSON字段
            "evasion_types": 1000,          # JSON字段
            "matched_keywords": 2000,       # JSON字段
            "analysis_note": 1500,          # 已在_build_enhanced_analysis_note中处理
            "matched_categories": 500       # VARCHAR字段
        }
        
        # 获取关键词筛选结果
        keyword_screening = analysis_result.get("keyword_screening", {})
        
        # 获取LLM原始响应
        llm_raw_response = analysis_result.get("llm_raw_response", {})
        
        # 确定LLM提供商和模型信息
        llm_provider = None
        llm_model = None
        llm_tokens_used = 0
        
        if isinstance(llm_raw_response, dict):
            llm_provider = llm_raw_response.get("provider") or getattr(settings, "llm_provider", "unknown")
            # 根据提供商获取模型信息
            if settings.llm_provider == "volcengine":
                default_model = getattr(settings, "volcengine_model", "unknown")
            elif settings.llm_provider == "siliconflow":
                default_model = getattr(settings, "siliconflow_model", "unknown")
            else:
                default_model = "unknown"
            
            # 获取模型信息 - 支持多种数据结构
            llm_model = llm_raw_response.get("model")
            if not llm_model and "raw_response" in llm_raw_response:
                raw_resp = llm_raw_response["raw_response"]
                if isinstance(raw_resp, dict):
                    llm_model = raw_resp.get("model")
            if not llm_model:
                llm_model = default_model
            
            # 解析token消耗 - 支持多种数据结构
            llm_tokens_used = 0
            if "tokens_used" in llm_raw_response:
                llm_tokens_used = llm_raw_response["tokens_used"]
            elif "usage" in llm_raw_response:
                usage = llm_raw_response["usage"]
                if isinstance(usage, dict):
                    llm_tokens_used = usage.get("total_tokens", 0)
            elif "raw_response" in llm_raw_response:
                raw_resp = llm_raw_response["raw_response"]
                if isinstance(raw_resp, dict) and "usage" in raw_resp:
                    usage = raw_resp["usage"]
                    if isinstance(usage, dict):
                        llm_tokens_used = usage.get("total_tokens", 0)
        
        # 安全处理匹配类别字段
        matched_categories_str = None
        if keyword_screening.get("matched_categories"):
            categories_list = keyword_screening["matched_categories"][:10]  # 最多10个类别
            categories_str = ",".join(categories_list)
            matched_categories_str = self._safe_truncate_text(categories_str, FIELD_LIMITS["matched_categories"])
        
        # 构建保存参数字典，应用长度限制
        save_params = {
            "work_id": work_id,
            "order_id": order_id,
            "order_no": order_no,
            "session_start_time": analysis_result.get("session_start_time"),
            "session_end_time": analysis_result.get("session_end_time"),
            "total_comments": analysis_result.get("total_comments", 0),
            "customer_comments": analysis_result.get("customer_messages", 0),
            "service_comments": analysis_result.get("service_messages", 0),
            "has_evasion": 1 if analysis_result.get("has_evasion", False) else 0,
            "risk_level": analysis_result.get("risk_level", "low"),
            "confidence_score": analysis_result.get("confidence_score", 0.0),
            # JSON字段 - 应用长度限制
            "evasion_types": self._safe_truncate_json(analysis_result.get("evasion_types", []), FIELD_LIMITS["evasion_types"]),
            "evidence_sentences": self._safe_truncate_json(analysis_result.get("evidence_sentences", []), FIELD_LIMITS["evidence_sentences"]),
            "improvement_suggestions": self._safe_truncate_json(analysis_result.get("improvement_suggestions", []), FIELD_LIMITS["improvement_suggestions"]),
            # 关键词筛选结果
            "keyword_screening_score": keyword_screening.get("confidence_score", 0.0),
            "matched_categories": matched_categories_str,
            "matched_keywords": self._safe_truncate_json(keyword_screening.get("matched_details", {}), FIELD_LIMITS["matched_keywords"]) if keyword_screening.get("matched_details") else None,
            "is_suspicious": 1 if keyword_screening.get("is_suspicious", False) else 0,
            # 情感分析结果
            "sentiment": analysis_result.get("sentiment", "neutral"),
            "sentiment_intensity": analysis_result.get("sentiment_intensity", 0.0),
            # 原始数据 - 应用长度限制
            "conversation_text": self._safe_truncate_text(analysis_result.get("conversation_text", ""), FIELD_LIMITS["conversation_text"]),
            "llm_raw_response": self._safe_truncate_json(llm_raw_response, FIELD_LIMITS["llm_raw_response"]) if llm_raw_response else None,
            "analysis_details": self._safe_truncate_json(analysis_result, FIELD_LIMITS["analysis_details"]),
            "analysis_note": self._build_enhanced_analysis_note(analysis_result),  # 内部已处理长度限制
            # LLM调用信息
            "llm_provider": llm_provider,
            "llm_model": llm_model,
            "llm_tokens_used": llm_tokens_used,
            # 时间戳
            "analysis_time": datetime.now()
        }
        
        return save_params
    
    def mark_work_order_completed(
        self,
        db: Session,
        work_id: int,
        analysis_result: Optional[Dict[str, Any]] = None
    ) -> bool:
        """标记工单为已完成"""
        try:
            # 保存分析结果（如果有）
            if analysis_result:
                self.save_analysis_result(db, work_id, analysis_result)
            
            # 更新待处理表状态
            success = self.stage1.update_work_order_ai_status(
                db, work_id, 'COMPLETED'
            )
            
            if success:
                logger.info(f"工单 {work_id} 标记为已完成")
            
            return success
            
        except Exception as e:
            logger.error(f"标记工单 {work_id} 为已完成失败: {e}")
            return False
    
    def mark_work_order_failed(
        self,
        db: Session,
        work_id: int,
        error_message: str
    ) -> bool:
        """标记工单处理失败"""
        try:
            success = self.stage1.update_work_order_ai_status(
                db, work_id, 'FAILED', error_message=error_message
            )
            
            if success:
                logger.info(f"工单 {work_id} 标记为处理失败: {error_message}")
            
            return success
            
        except Exception as e:
            logger.error(f"标记工单 {work_id} 为失败失败: {e}")
            return False
    
    def _atomic_mark_processing(self, db: Session, work_id: int) -> bool:
        """原子性地标记工单为处理中状态
        
        使用数据库原子操作，只有当工单状态为PENDING时才更新为PROCESSING
        这样可以防止多个进程同时处理同一个工单
        
        Returns:
            bool: True表示成功标记为处理中，False表示工单已在处理中或不存在
        """
        try:
            # 🔥 原子性更新：只有当状态为PENDING时才更新为PROCESSING
            update_sql = f"""
            UPDATE {self.pending_table_name}
            SET 
                ai_status = 'PROCESSING',
                ai_process_start_time = :start_time,
                updated_at = :updated_at
            WHERE work_id = :work_id 
            AND ai_status = 'PENDING'
            """
            
            params = {
                "work_id": work_id,
                "start_time": datetime.now(),
                "updated_at": datetime.now()
            }
            
            result = db.execute(text(update_sql), params)
            db.commit()
            
            # 检查是否成功更新（affected_rows > 0 表示成功）
            success = result.rowcount > 0
            
            if success:
                logger.debug(f"🔒 工单 {work_id} 成功标记为处理中状态")
            else:
                logger.debug(f"⚠️ 工单 {work_id} 未能标记为处理中（可能已在处理中或不存在）")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ 原子性标记工单 {work_id} 为处理中失败: {e}")
            db.rollback()
            return False
    
    # ==================== 分析任务管理方法 ====================
    
    def get_analysis_queue_status(self, db: Session) -> Dict[str, Any]:
        """获取分析队列状态"""
        try:
            sql = f"""
            SELECT 
                ai_status,
                COUNT(*) as count,
                AVG(comment_count) as avg_comments
            FROM {self.pending_table_name}
            WHERE has_comments = 1
            GROUP BY ai_status
            """
            
            result = db.execute(text(sql))
            
            queue_status = {}
            for row in result:
                queue_status[row.ai_status] = {
                    "count": row.count,
                    "avg_comments": float(row.avg_comments or 0)
                }
            
            return {
                "success": True,
                "queue_status": queue_status,
                "summary": {
                    "pending": queue_status.get("PENDING", {}).get("count", 0),
                    "processing": queue_status.get("PROCESSING", {}).get("count", 0),
                    "completed": queue_status.get("COMPLETED", {}).get("count", 0),
                    "failed": queue_status.get("FAILED", {}).get("count", 0)
                }
            }
            
        except Exception as e:
            logger.error(f"获取分析队列状态失败: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def cleanup_old_results(
        self,
        db: Session,
        days_to_keep: int = 90
    ) -> Dict[str, Any]:
        """清理旧的分析结果"""
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            # 清理分析结果表
            results_sql = f"""
            DELETE FROM {self.results_table_name}
            WHERE created_at < :cutoff_date
            """
            
            result = db.execute(text(results_sql), {"cutoff_date": cutoff_date})
            results_deleted = result.rowcount
            
            # 清理已完成的待处理记录
            pending_sql = f"""
            DELETE FROM {self.pending_table_name}
            WHERE created_at < :cutoff_date
            AND ai_status IN ('COMPLETED', 'FAILED')
            """
            
            result = db.execute(text(pending_sql), {"cutoff_date": cutoff_date})
            pending_deleted = result.rowcount
            
            db.commit()
            
            logger.info(f"清理了 {results_deleted} 条分析结果，{pending_deleted} 条待处理记录")
            
            return {
                "success": True,
                "results_deleted": results_deleted,
                "pending_deleted": pending_deleted,
                "cutoff_date": cutoff_date.strftime("%Y-%m-%d %H:%M:%S"),
                "message": f"成功清理 {results_deleted + pending_deleted} 条旧记录"
            }
            
        except Exception as e:
            logger.error(f"清理旧记录失败: {e}")
            db.rollback()
            return {
                "success": False,
                "error": str(e),
                "message": "清理失败"
            }
    
    # ==================== 检测引擎方法 ====================
    
    def _load_keywords_config(self, db: Session) -> Dict[str, Dict[str, Any]]:
        """从数据库加载关键词配置"""
        try:
            logger.debug("从数据库加载关键词配置")
            config = keyword_config_manager.get_analysis_keywords_config(db)
            if config:
                logger.info(f"成功从数据库加载 {len(config)} 个关键词配置分类")
                return config
            else:
                logger.warning("数据库中未找到关键词配置，使用默认配置")
                return self._get_fallback_keywords_config()
        except Exception as e:
            logger.error(f"从数据库加载关键词配置失败: {e}，使用默认配置")
            return self._get_fallback_keywords_config()

    def _get_fallback_keywords_config(self) -> Dict[str, Dict[str, Any]]:
        """获取备用的默认关键词配置（原硬编码配置作为备用）"""
        logger.info("使用备用的默认关键词配置")
        return {
            "紧急催促": {
                "keywords": [
                    "撕", "催", "紧急", "加急联系", "速度", "又来了", "怎么样了", "有进展了吗"
                ],
                "patterns": [
                    r"(催|撕).{0,5}(催|撕)",  # 连续催促
                    r"(又|一直).*(催|撕|来了)",
                    r"(怎么样|进展).{0,10}(了|啊|呢|吗)",
                    r"(紧急|加急).*(联系|处理|解决)",
                    r"(速度|快点).*(处理|解决|搞定)",
                    r"(有|没有).*(进展|结果|消息).*(了|吗|呢)"
                ],
                "weight": 0.9,  # 提高权重
                "risk_level": "high"
            },
            "投诉纠纷": {
                "keywords": [
                    "纠纷单", "投诉", "退款了", "结果", "12315", "客诉", "翘单"
                ],
                "patterns": [
                    r"(纠纷|投诉).*(单|了|啊|呢)",
                    r"(退款|退钱).*(了|啊|呢)",
                    r"(客诉|投诉).*12315",
                    r"(翘单|逃单).{0,10}(了|呢)",
                    r"(结果|进展).*(不知道|不清楚|没消息|怎么样)",
                    r"12315.*(投诉|举报|客诉)"
                ],
                "weight": 1.2,  # 最高权重
                "risk_level": "high"
            },
            "推卸责任": {
                "keywords": [
                    "不是我们的问题", "不是我们负责", "不关我们事", "找其他部门", "联系供应商", 
                    "厂家问题", "配件问题", "找师傅", "师傅负责", "找安装师傅", "不是门店责任",
                    "这是厂家的", "原厂保修", "找4S店", "不归我们管", "系统问题", "总部决定",
                    "没办法", "无能为力", "爱莫能助", "无可奈何", "我们也很无奈"
                ],
                "patterns": [
                    r"(不是|不属于).*(我们|门店|本店).*(问题|责任|负责)",
                    r"(这是|属于).*(厂家|师傅|供应商|原厂).*(问题|责任)",
                    r"(找|联系|去问).*(师傅|厂家|供应商|4S店|原厂)",
                    r"(师傅|安装师傅).*(自己|负责|承担).*(责任|问题)",
                    r"(配件|产品).*(质量|问题).*找.*(厂家|供应商)",
                    r"(贴膜|安装|维修).*(问题|效果).*找.*(师傅|技师)",
                    r"(保修|售后).*找.*(原厂|4S店|厂家)",
                    r"(没办法|无能为力|爱莫能助|无可奈何).*解决",
                    r"这个.*不归.*(我们|门店).*管"
                ],
                "weight": 1.0,
                "risk_level": "high"
            },
            "拖延处理": {
                "keywords": [
                    "翘单", "逃单", "一直拖", "故意拖", "拖着不处理", "不想处理"
                ],
                "patterns": [
                    r"(翘单|逃单).{0,10}(了|呢)(?![^，。！？；]*[处理解决完成])",
                    r"(拖着|一直拖|故意拖).*(不处理|不解决)",
                    r"(不想|不愿意).*(处理|解决|管)",
                    r"(能拖|继续拖).*(就拖|一天)"
                ],
                "weight": 1.1,  # 高权重，拖延很严重
                "risk_level": "high"
            },
            "不当用词表达": {
                "keywords": [
                    "搞快点", "快点搞", "急死了", "催死了", "烦死了", "撕", 
                    "赶紧搞", "搞定", "又来催", "车主烦人", "师傅拖拉"
                ],
                "patterns": [
                    r"(搞|弄).*(快|定|好)",
                    r"(急|催|烦|撕).*(死了|要命)",
                    r"(又|一直).*(催|撕|来了)",
                    r"(车主|客户).*(烦人|烦死|麻烦死)",
                    r"(师傅|技师).*(拖拉|磨叽|慢吞吞|烦人)",
                    r"(赶紧|快点).*(搞|弄|处理)"
                ],
                "weight": 0.8,  # 提高权重，不当用词需要重视
                "risk_level": "medium"
            },
            "模糊回应": {
                "keywords": [
                    "需要时间", "耐心等待", "已经在处理", "尽快联系", "正在处理中", 
                    "会尽快", "稍等一下", "马上处理"
                ],
                "patterns": [
                    r"(这个|这种).*(需要时间|要等)(?![^，。！？；]*[具体时间|明确|预计])",
                    r"(已经在|正在).*(处理|跟进)(?![^，。！？；]*[具体|详细|明确])",
                    r"(会|将).*(尽快|马上)(?![^，。！？；]*[今天|明天|\d+小时])",
                    r"(请|您).*(耐心|稍等)(?![^，。！？；]*[\d+分钟|\d+小时])"
                ],
                "exclusions": [
                    r"(预计|大概|估计).*(时间|小时|分钟|天)",
                    r"(具体|详细).*(时间|进度)",
                    r"(\d+).*(小时|分钟|天).*内",
                    r"(今天|明天|本周).*(完成|处理)"
                ],
                "weight": 0.6,  # 适度提高，但仍低于其他类别
                "risk_level": "medium"
            }
        }
    
    def _init_few_shot_examples(self) -> List[Dict[str, Any]]:
        """初始化少样本示例"""
        return [
            {
                "conversation": "门店: 车主一直催贴膜进度，又来了，怎么样了？\n客服: 这个需要时间处理，让车主耐心等待。",
                "analysis": {
                    "has_evasion": False,  # 🔥 修复：只是模糊回应，不是推卸责任
                    "risk_level": "medium",
                    "confidence_score": 0.75,
                    "evasion_types": ["紧急催促", "模糊回应"],
                    "evidence_sentences": ["车主一直催贴膜进度，又来了，怎么样了", "这个需要时间处理，让车主耐心等待"],
                    "improvement_suggestions": ["应具体回应车主的催促，提供明确的完成时间，如'师傅今天下午3点完成贴膜'"]
                }
            },
            {
                "conversation": "门店: 车主投诉配件质量，要退款了\n客服: 这不是我们的问题，是厂家的配件质量问题，让车主直接找供应商。",
                "analysis": {
                    "has_evasion": True,
                    "risk_level": "high",
                    "confidence_score": 0.95,
                    "evasion_types": ["投诉纠纷", "推卸责任"],
                    "evidence_sentences": ["车主投诉配件质量，要退款了", "这不是我们的问题，是厂家的配件质量问题"],
                    "improvement_suggestions": ["面对投诉和退款要求，门店应承担售后责任，协助处理而不是推卸给厂家"]
                }
            },
            {
                "conversation": "师傅: 又来催了，撕心裂肺的，搞快点弄完\n门店: 知道了，赶紧搞定",
                "analysis": {
                    "has_evasion": False,  # 🔥 修复：只是不当用词，不是推卸责任
                    "risk_level": "medium",
                    "confidence_score": 0.8,
                    "evasion_types": ["不当用词表达"],
                    "evidence_sentences": ["又来催了，撕心裂肺的，搞快点弄完", "赶紧搞定"],
                    "improvement_suggestions": ["应使用专业用语，如'车主比较着急，请加快处理速度'，避免'撕'、'搞'等不当表达"]
                }
            },
            {
                "conversation": "门店: 有纠纷单，客诉12315了\n客服: 翘单吧，能拖就拖一天是一天。",
                "analysis": {
                    "has_evasion": False,  # 🔥 修复：虽然是严重问题，但分类不包含推卸责任
                    "risk_level": "high",
                    "confidence_score": 0.98,
                    "evasion_types": ["投诉纠纷", "拖延处理"],
                    "evidence_sentences": ["有纠纷单，客诉12315了", "翘单吧，能拖就拖一天是一天"],
                    "improvement_suggestions": ["严禁故意拖延处理客诉和12315投诉，应立即响应和解决"]
                }
            },
            {
                "conversation": "门店: 车主加急联系，速度催结果，有进展了吗？\n客服: 已经在跟进了，会尽快给答复。",
                "analysis": {
                    "has_evasion": False,  # 🔥 修复：只是模糊回应，不是推卸责任
                    "risk_level": "medium",
                    "confidence_score": 0.75,
                    "evasion_types": ["紧急催促", "模糊回应"],
                    "evidence_sentences": ["车主加急联系，速度催结果，有进展了吗", "已经在跟进了，会尽快给答复"],
                    "improvement_suggestions": ["面对加急催促，应提供具体的进展情况和预计完成时间"]
                }
            },
            {
                "conversation": "门店: 车主咨询全车贴膜价格和质保期\n客服: 全车贴膜1800元，质保2年，包括材料和人工，预计明天上午完成安装。",
                "analysis": {
                    "has_evasion": False,
                    "risk_level": "low",
                    "confidence_score": 0.1,
                    "evasion_types": [],
                    "evidence_sentences": [],
                    "improvement_suggestions": []
                }
            },
            {
                "conversation": "门店: 车主说贴膜有气泡要求重新处理\n客服: 这不是我们门店的问题，是师傅技术问题，你直接找安装师傅负责。",
                "analysis": {
                    "has_evasion": True,  # 🔥 正确示例：明显的推卸责任行为
                    "risk_level": "high", 
                    "confidence_score": 0.95,
                    "evasion_types": ["推卸责任"],
                    "evidence_sentences": ["这不是我们门店的问题，是师傅技术问题", "你直接找安装师傅负责"],
                    "improvement_suggestions": ["门店应承担服务责任，协调师傅重新处理，而不是直接推卸给师傅"]
                }
            }
        ]
    
    def _extract_evidence_sentences(self, conversation_text: str, keyword: str, category: str) -> List[str]:
        """提取包含关键词的具体句子和上下文"""
        evidence_list = []
        
        # 将对话文本按句子分割（支持多种标点符号）
        import re
        sentences = re.split(r'[。！？；\n]', conversation_text)
        
        for i, sentence in enumerate(sentences):
            sentence = sentence.strip()
            if not sentence:
                continue
                
            # 如果句子包含关键词
            if keyword in sentence:
                # 构建上下文（前一句 + 当前句 + 后一句）
                context_parts = []
                
                # 前一句
                if i > 0 and sentences[i-1].strip():
                    context_parts.append(f"上文: {sentences[i-1].strip()}")
                
                # 当前句（高亮关键词）
                highlighted_sentence = sentence.replace(keyword, f"【{keyword}】")
                context_parts.append(f"匹配句: {highlighted_sentence}")
                
                # 后一句
                if i < len(sentences) - 1 and sentences[i+1].strip():
                    context_parts.append(f"下文: {sentences[i+1].strip()}")
                
                evidence_entry = f"[{category}] " + " | ".join(context_parts)
                evidence_list.append(evidence_entry)
        
        return evidence_list
    
    def _extract_pattern_evidence(self, conversation_text: str, patterns: List[str], category: str) -> List[str]:
        """提取匹配正则模式的具体内容"""
        evidence_list = []
        import re
        
        for pattern in patterns:
            try:
                matches = re.finditer(pattern, conversation_text, re.DOTALL)
                for match in matches:
                    matched_text = match.group()
                    start_pos = max(0, match.start() - 20)  # 前20个字符作为上下文
                    end_pos = min(len(conversation_text), match.end() + 20)  # 后20个字符作为上下文
                    
                    context = conversation_text[start_pos:end_pos]
                    highlighted_context = context.replace(matched_text, f"【{matched_text}】")
                    
                    evidence_entry = f"[{category}-正则] 匹配内容: {highlighted_context}"
                    evidence_list.append(evidence_entry)
                    
            except re.error as e:
                logger.warning(f"正则表达式 {pattern} 执行失败: {e}")
                continue
        
        return evidence_list
    
    def _build_enhanced_analysis_note(self, analysis_result: Dict[str, Any]) -> str:
        """构建增强的分析备注，包含详细证据信息，确保长度不超出数据库限制"""
        notes = []
        max_length = 1500  # 设置最大长度限制，为数据库字段预留缓冲空间
        
        # 基本分析信息（必要信息，优先级最高）
        risk_level = analysis_result.get("risk_level", "unknown")
        confidence = analysis_result.get("confidence_score", 0.0)
        categories = analysis_result.get("evasion_types", [])
        
        notes.append(f"风险级别: {risk_level}, 置信度: {confidence:.3f}")
        
        if categories:
            categories_str = ', '.join(categories[:5])  # 最多显示5个类别
            if len(categories) > 5:
                categories_str += f" 等{len(categories)}个类别"
            notes.append(f"匹配类别: {categories_str}")
        
        # 检查当前长度
        current_length = len(" | ".join(notes))
        remaining_length = max_length - current_length - 100  # 为后续内容预留100字符
        
        # 详细证据信息（如果有剩余空间）
        if remaining_length > 50:
            detailed_evidence = analysis_result.get("detailed_evidence", [])
            if detailed_evidence:
                notes.append(f"证据条数: {len(detailed_evidence)}")
                
                # 动态调整证据预览数量和长度
                available_space = remaining_length - 50  # 为后续内容预留
                evidence_preview = []
                
                for i, evidence in enumerate(detailed_evidence[:2]):  # 最多显示2条证据
                    evidence_length = min(50, available_space // 2)  # 每条证据最多50字符
                    if len(evidence) > evidence_length:
                        evidence = evidence[:evidence_length] + "..."
                    evidence_preview.append(f"{i+1}. {evidence}")
                    available_space -= len(evidence_preview[-1]) + 3  # 3个字符用于分隔符
                    
                    if available_space < 20:  # 空间不足时停止
                        break
                
                if evidence_preview:
                    notes.append("主要证据: " + " | ".join(evidence_preview))
                
                if len(detailed_evidence) > len(evidence_preview):
                    notes.append(f"... 还有{len(detailed_evidence) - len(evidence_preview)}条证据")
        
        # 更新剩余长度
        current_length = len(" | ".join(notes))
        remaining_length = max_length - current_length - 50  # 为最后的内容预留
        
        # 匹配的关键词（如果有剩余空间）
        if remaining_length > 30:
            matched_keywords = analysis_result.get("matched_keywords", [])
            if matched_keywords:
                keyword_space = min(remaining_length - 20, 150)  # 关键词最多占用150字符
                keywords_str = ""
                keyword_count = 0
                
                for keyword in matched_keywords[:8]:  # 最多8个关键词
                    test_str = keywords_str + (", " if keywords_str else "") + keyword
                    if len(test_str) <= keyword_space:
                        keywords_str = test_str
                        keyword_count += 1
                    else:
                        break
                
                if len(matched_keywords) > keyword_count:
                    keywords_str += f" 等{len(matched_keywords)}个关键词"
                
                notes.append(f"匹配关键词: {keywords_str}")
        
        # 对话统计（简化版）
        current_length = len(" | ".join(notes))
        if current_length < max_length - 50:
            total_comments = analysis_result.get("total_comments", 0)
            customer_comments = analysis_result.get("customer_comments", 0)
            service_comments = analysis_result.get("service_comments", 0)
            
            if total_comments > 0:
                notes.append(f"对话统计: 总{total_comments}条(客户{customer_comments}条,服务{service_comments}条)")
        
        # 分析方式标记（简化版）
        current_length = len(" | ".join(notes))
        if current_length < max_length - 30:
            llm_analysis = analysis_result.get("llm_analysis", True)
            if not llm_analysis:
                notes.append("基于关键词规则直接判定")
        
        # 最终安全截断
        final_note = " | ".join(notes)
        if len(final_note) > max_length:
            final_note = final_note[:max_length-3] + "..."
            logger.warning(f"分析备注超出长度限制，已截断至{max_length}字符")
        
        return final_note

    def keyword_screening(self, conversation_text: str, db: Session = None) -> Dict[str, Any]:
        """关键词粗筛"""
        matched_categories = []
        total_score = 0.0
        matched_details = {}
        
        # 动态加载关键词配置
        if db is not None:
            keywords_config = self._load_keywords_config(db)
        else:
            # 如果没有提供数据库会话，使用默认配置
            keywords_config = self._get_fallback_keywords_config()
        
        for category, config in keywords_config.items():
            category_score = 0.0
            matched_keywords = []
            matched_patterns = []
            excluded = False
            
            # 首先检查排除条件（如果配置了的话）
            if "exclusions" in config:
                for exclusion_pattern in config["exclusions"]:
                    if re.search(exclusion_pattern, conversation_text, re.DOTALL):
                        excluded = True
                        break
            
            if not excluded:
                # 检查关键词
                for keyword in config["keywords"]:
                    if keyword in conversation_text:
                        matched_keywords.append(keyword)
                        category_score += 0.1
                
                # 检查正则模式
                for pattern in config["patterns"]:
                    if re.search(pattern, conversation_text, re.DOTALL):
                        matched_patterns.append(pattern)
                        category_score += 0.2
            
            if (matched_keywords or matched_patterns) and not excluded:
                weighted_score = category_score * config["weight"]
                total_score += weighted_score
                matched_categories.append(category)
                
                matched_details[category] = {
                    "keywords": matched_keywords,
                    "patterns": matched_patterns,
                    "score": weighted_score,
                    "risk_level": config["risk_level"],
                    "excluded": False
                }
            elif excluded and (matched_keywords or matched_patterns):
                # 记录被排除的匹配，用于调试
                matched_details[f"{category}(已排除)"] = {
                    "keywords": matched_keywords,
                    "patterns": matched_patterns,
                    "score": 0.0,
                    "risk_level": config["risk_level"],
                    "excluded": True
                }
        
        # 优化判定逻辑：提高阈值，减少误检
        is_suspicious = total_score > 0.3 and len(matched_categories) > 0
        
        return {
            "is_suspicious": is_suspicious,
            "confidence_score": min(total_score, 1.0),
            "matched_categories": matched_categories,
            "matched_details": matched_details,
            "total_score": total_score
        }
    
    def build_analysis_prompt(self, conversation_text: str) -> str:
        """构建分析提示词"""
        few_shot_text = "\n\n".join([
            f"对话示例{i+1}:\n{example['conversation']}\n分析结果:\n{safe_json_dumps(example['analysis'], ensure_ascii=False)}"
            for i, example in enumerate(self.few_shot_examples)
        ])
        
        prompt = f"""
你是一个专业的汽车服务行业质量分析专家，请分析以下师傅、门店、客服之间的对话中是否存在规避责任的行为。

在汽车服务行业（配件销售、贴膜、维修、上门服务）中，规避责任的表现包括：
1. 推卸责任：将问题完全推给师傅、厂家、供应商或4S店，拒绝承担售后服务责任
2. 模糊回应：给出"需要时间"、"正在处理"等模糊答复，不提供具体的维修时间、师傅安排
3. 拖延处理：故意延长处理时间，希望车主放弃投诉或自行解决
4. 不当用词：在内部沟通中使用"车主烦人"、"师傅磨叽"等非专业表达，贬低客户或合作伙伴
5. 敷衍态度：随意应付车主咨询，对质量问题、安装效果等不负责任

⚠️ 汽车服务行业重点关注：
- 配件质量问题推给"厂家"、"供应商"，不协助处理
- 贴膜、安装问题推给"师傅自己负责"，门店不承担责任
- 维修服务问题推给"原厂保修"、"4S店"，拒绝售后支持
- 师傅服务问题（迟到、操作不当）不主动协调解决
- 对于推卸责任行为，置信度应给予0.8以上的高评分

⚠️ 模糊回应识别标准（汽车服务特点）：
- 只有明显缺乏具体时间安排、师傅调度信息的回应才算模糊回应
- 如果提到了"预计明天"、"联系师傅确认时间"等具体安排，则不算模糊回应
- 对于模糊回应，置信度应在0.6-0.8之间

分析要求：
1. 重点关注汽车服务行业的推卸责任行为
2. 识别师傅、门店、客服三方责任边界问题
3. 严格区分模糊回应和正常的服务流程说明
4. 评估风险级别：low（无风险）、medium（中等风险）、high（高风险）
5. 提供准确的置信度评分（0-1之间）
6. 列出具体的证据句子
7. 给出符合汽车服务行业特点的改进建议

{few_shot_text}

现在请分析以下对话：
{conversation_text}

请严格按照以下JSON格式返回分析结果：
{{
    "has_evasion": boolean,
    "risk_level": "low|medium|high",
    "confidence_score": float,
    "evasion_types": [string],
    "evidence_sentences": [string],
    "improvement_suggestions": [string],
    "sentiment": "positive|negative|neutral",
    "sentiment_intensity": float
}}
"""
        return prompt
    
    # ==================== LLM分析方法 ====================
    
    async def analyze_single_conversation(self, conversation_data: Dict[str, Any], db: Session = None) -> Dict[str, Any]:
        """分析单个对话"""
        work_id = conversation_data.get("work_id", "未知")
        logger.info(f"🔍 开始分析工单 {work_id} 的对话")
        
        try:
            conversation_text = str(conversation_data.get("conversation_text") or "")
            
            if not conversation_text.strip():
                logger.warning(f"⚠️ 工单 {work_id} 对话内容为空")
                return {
                    "success": False,
                    "error": "对话内容为空"
                }
            
            logger.debug(f"📝 工单 {work_id} 对话文本长度: {len(conversation_text)} 字符")
            
            # 1. 关键词粗筛
            logger.debug(f"🔍 工单 {work_id} 开始关键词粗筛...")
            keyword_result = self.keyword_screening(conversation_text, db)
            logger.info(f"📊 工单 {work_id} 关键词筛选结果: 可疑={keyword_result['is_suspicious']}, 置信度={keyword_result['confidence_score']:.3f}")
            
            # 2. 🔥 优化：关键词和正则命中的直接判定为中风险以上，LLM为辅助分析
            if keyword_result["is_suspicious"] and keyword_result["confidence_score"] >= 0.3:
                logger.info(f"🎯 工单 {work_id} 命中关键词类别: {keyword_result['matched_categories']}，置信度: {keyword_result['confidence_score']:.3f}")
                
                # 🔥 新优化逻辑：关键词命中直接判定为中风险以上，不依赖LLM
                # 🔥 优化：构建详细证据信息，包含具体聊天内容和上下文
                matched_risk_levels = []
                evidence_sentences = []
                matched_keywords = []
                detailed_evidence = []
                
                for category, details in keyword_result["matched_details"].items():
                    if not details.get("excluded", False):
                        matched_risk_levels.append(details.get("risk_level", "medium"))
                        
                        # 🔥 新增：收集匹配关键词的具体句子和上下文
                        if details.get("keywords"):
                            matched_keywords.extend(details["keywords"])
                            for keyword in details["keywords"]:
                                # 在对话文本中找到包含该关键词的句子
                                sentences = self._extract_evidence_sentences(conversation_text, keyword, category)
                                evidence_sentences.extend(sentences)
                                detailed_evidence.extend(sentences)
                        
                        # 🔥 新增：收集正则模式匹配的具体内容
                        if details.get("patterns"):
                            pattern_matches = self._extract_pattern_evidence(conversation_text, details["patterns"], category)
                            evidence_sentences.extend(pattern_matches)
                            detailed_evidence.extend(pattern_matches)
                
                # 确定最终风险级别（取最高风险级别）
                if "high" in matched_risk_levels:
                    final_risk_level = "high"
                elif "medium" in matched_risk_levels:
                    final_risk_level = "medium"
                else:
                    final_risk_level = "medium"  # 默认中风险
                
                # 🔥 修复逻辑：只有推卸责任分类命中时才算规避责任
                has_evasion_behavior = any(
                    "推卸责任" in category for category in keyword_result["matched_categories"]
                )
                
                # 🎯 关键词命中直接构建分析结果，无需LLM分析
                keyword_based_result = {
                    "has_evasion": has_evasion_behavior,  # 🔥 修复：只有推卸责任分类命中才算规避责任
                    "risk_level": final_risk_level,
                    "confidence_score": min(keyword_result["confidence_score"], 1.0),
                    "evasion_types": keyword_result["matched_categories"],
                    "evidence_sentences": evidence_sentences,
                    "detailed_evidence": detailed_evidence,  # 🔥 新增：详细证据信息
                    "improvement_suggestions": [f"检测到 {', '.join(keyword_result['matched_categories'])} 相关行为，建议加强服务质量管控和人员培训" + 
                                             (f"。特别关注推卸责任行为的改进" if has_evasion_behavior else "")],
                    "sentiment": "negative",  # 关键词命中通常表示负面情况
                    "sentiment_intensity": 0.7,
                    "keyword_screening": keyword_result,
                    "llm_analysis": False,  # 标记未使用LLM
                    "analysis_note": f"基于关键词和正则匹配直接判定为{final_risk_level}风险，匹配类别: {', '.join(keyword_result['matched_categories'])}，详细证据: {len(detailed_evidence)}条" + 
                                   (f"，存在推卸责任行为" if has_evasion_behavior else f"，未发现推卸责任行为"),
                    # 🔥 优化：补充完整的会话信息和详细证据
                    "session_start_time": conversation_data.get("session_info", {}).get("start_time"),
                    "session_end_time": conversation_data.get("session_info", {}).get("end_time"),
                    "total_comments": conversation_data.get("total_messages", 0),
                    "customer_comments": conversation_data.get("customer_messages", 0),
                    "service_comments": conversation_data.get("service_messages", 0),
                    "conversation_text": conversation_text,
                    "conversation_messages": conversation_data.get("messages", []),  # 🔥 新增：完整消息列表
                    "matched_keywords": matched_keywords,  # 🔥 新增：匹配的关键词列表
                    "evidence_count": len(detailed_evidence)  # 🔥 新增：证据条数
                }
                
                logger.info(f"✅ 工单 {work_id} 基于关键词直接判定完成: 风险级别={final_risk_level}, 类别={keyword_result['matched_categories']}, 推卸责任={has_evasion_behavior}")
                
                return {
                    "success": True,
                    "work_id": work_id,
                    "analysis_result": keyword_based_result
                }
            else:
                logger.info(f"⏭️ 工单 {work_id} 未命中关键词阈值（置信度: {keyword_result['confidence_score']:.3f}），判定为低风险，不保存")
                
                # 🔥 新优化：低风险直接返回，不保存到数据库
                low_risk_result = {
                    "has_evasion": False,
                    "risk_level": "low",
                    "confidence_score": keyword_result["confidence_score"],
                    "evasion_types": [],
                    "evidence_sentences": [],
                    "improvement_suggestions": [],
                    "sentiment": "neutral",
                    "sentiment_intensity": 0.0,
                    "keyword_screening": keyword_result,
                    "llm_analysis": False,
                    "analysis_note": "未命中关键词阈值，判定为正常对话，不保存到数据库",
                    "skip_save": True,  # 🔥 标记跳过保存
                    # 补充会话信息
                    "session_start_time": conversation_data.get("session_info", {}).get("start_time"),
                    "session_end_time": conversation_data.get("session_info", {}).get("end_time"),
                    "total_comments": conversation_data.get("total_messages", 0),
                    "customer_comments": conversation_data.get("customer_messages", 0),
                    "service_comments": conversation_data.get("service_messages", 0),
                    "conversation_text": conversation_text
                }
                
                return {
                    "success": True,
                    "work_id": work_id,
                    "analysis_result": low_risk_result
                }
            
            # 🔥 注意：由于现在采用关键词优先的策略，LLM分析部分已被移除
            # 所有分析决策都基于关键词和正则匹配结果
            # 这里不应该被执行到，因为上面的逻辑已经处理了所有情况
            
        except Exception as e:
            logger.error(f"❌ 工单 {work_id} 分析对话失败: {e}")
            logger.error(f"错误详情: {str(e)}")
            import traceback
            logger.error(f"错误堆栈: {traceback.format_exc()}")
            return {
                "success": False,
                "work_id": work_id,
                "error": str(e)
            }
    
    async def batch_analyze_conversations(
        self,
        db: Session,
        work_orders: List[Dict[str, Any]],
        max_concurrent: int = None
    ) -> Dict[str, Any]:
        """批量分析对话"""
        import asyncio
        
        logger.info("=" * 80)
        logger.info(f"🧠 开始批量分析处理 {len(work_orders)} 个工单")
        
        # 🔥 修复：先去重工单ID，防止重复处理
        unique_work_orders = {}
        for order in work_orders:
            work_id = order.get("work_id")
            if work_id and work_id not in unique_work_orders:
                unique_work_orders[work_id] = order
        
        deduplicated_orders = list(unique_work_orders.values())
        if len(deduplicated_orders) < len(work_orders):
            logger.warning(f"⚠️ 发现 {len(work_orders) - len(deduplicated_orders)} 个重复工单ID，已去重")
        
        # 过滤出有评论的工单
        orders_with_comments = [
            order for order in deduplicated_orders 
            if order.get("has_comments") and order.get("comments_data")
        ]
        
        logger.info(f"📊 批量分析前预处理统计:")
        logger.info(f"  📥 输入工单总数: {len(work_orders)}")
        logger.info(f"  🔄 去重后工单数: {len(deduplicated_orders)}")
        logger.info(f"  💬 有评论可分析: {len(orders_with_comments)}")
        logger.info(f"  💭 无评论跳过: {len(deduplicated_orders) - len(orders_with_comments)}")
        
        if not orders_with_comments:
            logger.warning("⚠️ 没有需要分析的工单（所有工单都没有评论）")
            return {
                "success": True,
                "message": "没有需要分析的工单",
                "total_orders": len(deduplicated_orders),
                "analyzed_orders": 0,
                "successful_analyses": 0,
                "failed_analyses": 0
            }
        
        # 🔥 修复：批量标记工单为处理中状态，防止并发重复处理
        logger.info(f"🔒 开始原子性标记 {len(orders_with_comments)} 个工单为处理中状态...")
        processing_work_ids = []
        for i, order in enumerate(orders_with_comments, 1):
            work_id = order["work_id"]
            try:
                # 原子性地检查并更新状态
                update_success = self._atomic_mark_processing(db, work_id)
                if update_success:
                    processing_work_ids.append(work_id)
                    logger.info(f"✅ 工单 {work_id} 成功标记为处理中 ({i}/{len(orders_with_comments)})")
                else:
                    logger.warning(f"⚠️ 工单 {work_id} 可能正在被其他进程处理，跳过 ({i}/{len(orders_with_comments)})")
            except Exception as e:
                logger.error(f"❌ 标记工单 {work_id} 为处理中失败: {e}")
        
        # 过滤出成功标记为处理中的工单
        final_orders_to_process = [
            order for order in orders_with_comments 
            if order["work_id"] in processing_work_ids
        ]
        
        logger.info(f"🔒 批量状态标记完成: {len(final_orders_to_process)}/{len(orders_with_comments)} 个工单成功标记为处理中状态")
        
        if not final_orders_to_process:
            logger.warning("⚠️ 没有工单可以进行分析（可能都在处理中）")
            return {
                "success": True,
                "message": "没有工单可以进行分析",
                "total_orders": len(deduplicated_orders),
                "analyzed_orders": 0,
                "successful_analyses": 0,
                "failed_analyses": 0
            }
        
        # 显示前几个工单的基本信息
        for i, order in enumerate(final_orders_to_process[:3], 1):
            logger.info(f"📋 工单 #{i}: ID={order['work_id']}, 评论数={order.get('comment_count', 0)}")
        
        # 从配置中获取并发参数
        if max_concurrent is None:
            max_concurrent = settings.concurrency_analysis_max_concurrent
            
        # 创建分析任务
        logger.info(f"🔄 准备创建 {len(final_orders_to_process)} 个分析任务，并发数: {max_concurrent}")
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def analyze_with_semaphore(order):
            work_id = order["work_id"]
            async with semaphore:
                try:
                    result = await self.analyze_single_conversation(order["comments_data"], db)
                    return result
                except Exception as e:
                    logger.error(f"❌ 工单 {work_id} 分析异常: {e}")
                    raise e
        
        # 执行批量分析
        logger.info(f"⚡ 开始执行批量分析任务 - 目标工单数: {len(final_orders_to_process)}, 并发数: {max_concurrent}")
        tasks = [analyze_with_semaphore(order) for order in final_orders_to_process]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("⚡ 批量分析任务执行完成，开始处理和保存结果...")
        
        # 处理结果
        successful_count = 0
        failed_count = 0
        logger.info(f"📊 开始处理 {len(results)} 个分析结果...")
        
        for i, result in enumerate(results):
            order = final_orders_to_process[i]
            work_id = order["work_id"]
            
            if isinstance(result, Exception):
                logger.error(f"❌ 工单 {work_id} 分析异常: {result}")
                self.mark_work_order_failed(db, work_id, str(result))
                failed_count += 1
                continue
            
            if result.get("success"):
                analysis_result = result["analysis_result"]
                
                # 🔥 新优化：检查是否需要跳过保存（低风险结果）
                if analysis_result.get("skip_save", False):
                    # 低风险结果不保存到数据库，但标记工单为已完成
                    self.stage1.update_work_order_ai_status(db, work_id, 'COMPLETED',
                                                            error_message="低风险，未保存分析结果")
                    successful_count += 1
                else:
                    # 中风险以上才保存分析结果
                    if self.save_analysis_result(db, work_id, analysis_result):
                        # 🔥 修复：标记为已完成，但不再重复保存分析结果
                        self.mark_work_order_completed(db, work_id, None)  # 传入None避免重复保存
                        successful_count += 1
                    else:
                        self.mark_work_order_failed(db, work_id, "保存分析结果失败")
                        failed_count += 1
            else:
                error_msg = result.get("error", "未知错误")
                logger.error(f"❌ 工单 {work_id} 分析失败: {error_msg}")
                self.mark_work_order_failed(db, work_id, error_msg)
                failed_count += 1
        
        logger.info("=" * 40)
        logger.info(f"🎉 批量分析完成统计:")
        logger.info(f"  ✅ 成功: {successful_count}")
        logger.info(f"  ❌ 失败: {failed_count}")
        logger.info(f"  📊 成功率: {successful_count / len(final_orders_to_process) * 100:.1f}%" if final_orders_to_process else "0%")
        logger.info("=" * 40)
        
        return {
            "success": True,
            "message": f"批量分析完成",
            "total_orders": len(deduplicated_orders),
            "analyzed_orders": len(final_orders_to_process),
            "successful_analyses": successful_count,
            "failed_analyses": failed_count
        }
    
    async def process_pending_analysis_queue(
        self,
        db: Session,
        batch_size: int = None,
        max_concurrent: int = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """处理待分析队列
        
        Args:
            db: 数据库会话
            batch_size: 批次大小
            max_concurrent: 最大并发数
            start_date: 开始时间（按create_time过滤）
            end_date: 结束时间（按create_time过滤）
        """
        # 从配置中获取默认参数
        if batch_size is None:
            batch_size = settings.concurrency_analysis_batch_size
        if max_concurrent is None:
            max_concurrent = settings.concurrency_analysis_max_concurrent
        
        time_range_info = ""
        if start_date or end_date:
            time_parts = []
            if start_date:
                time_parts.append(f"从{start_date}")
            if end_date:
                time_parts.append(f"到{end_date}")
            time_range_info = f" ({' '.join(time_parts)})"
            
        logger.info("=" * 80)
        logger.info(f"🚀 开始处理pending分析队列{time_range_info}")
        logger.info(f"⚙️ 配置参数: batch_size={batch_size}, max_concurrent={max_concurrent}")
        
        try:
            # 步骤1: 获取待处理工单（🔥 修复：分析阶段不使用时间过滤）
            logger.info("🔄 步骤1: 拉取pending工单数据开始...")
            pending_result = self.get_pending_work_orders_with_comments(
                db, batch_size, start_date=start_date, end_date=end_date
            )
            logger.info(f"📊 步骤1: pending数据拉取结果 - success: {pending_result['success']}")
            
            if not pending_result["success"]:
                logger.error(f"❌ 获取待处理工单失败: {pending_result}")
                return pending_result
            
            work_orders = pending_result["work_orders"]
            logger.info(f"✅ 步骤1完成: 拉取pending数据成功，获取到 {len(work_orders)} 个工单")
            
            if not work_orders:
                logger.warning("⚠️ 没有待处理的pending工单")
                return {
                    "success": True,
                    "message": "没有待处理的工单",
                    "statistics": pending_result["statistics"]
                }
            
            # 打印工单详情
            logger.info("📊 pending工单统计详情:")
            logger.info(f"  📥 拉取工单总数: {len(work_orders)}")
            logger.info(f"  💬 有评论待分析: {pending_result['statistics']['with_comments']}")
            logger.info(f"  💭 无评论已处理: {pending_result['statistics']['without_comments']}")
            logger.info(f"  🔍 去噪处理数量: {pending_result['statistics'].get('denoised_count', 0)}")
            
            # 步骤2: 批量分析
            logger.info("🔄 步骤2: 开始批量AI分析处理...")
            analysis_result = await self.batch_analyze_conversations(
                db, work_orders, max_concurrent
            )
            logger.info(f"📊 步骤2: 批量分析结果 - success: {analysis_result.get('success', False)}, 成功: {analysis_result.get('successful_analyses', 0)}, 失败: {analysis_result.get('failed_analyses', 0)}")
            
            # 计算跳过的记录数（没有评论的工单）
            skipped_orders = analysis_result["total_orders"] - analysis_result["analyzed_orders"]
            
            # 合并统计信息
            final_result = {
                "success": True,
                "stage": "完整的分析流程",
                "extraction_statistics": pending_result["statistics"],
                "analysis_statistics": {
                    "total_orders": analysis_result["total_orders"],
                    "analyzed_orders": analysis_result["analyzed_orders"],
                    "successful_analyses": analysis_result["successful_analyses"],
                    "failed_analyses": analysis_result["failed_analyses"],
                    "skipped_orders": skipped_orders,  # 🔥 新增：跳过的工单数（无评论）
                    "denoised_orders": pending_result["statistics"].get("denoised_count", 0)  # 🔥 新增：去噪的工单数
                },
                "message": f"处理完成: 提取 {len(work_orders)} 个工单，成功分析 {analysis_result['successful_analyses']} 个，跳过 {skipped_orders} 个"
            }
            
            # 打印最终统计
            logger.info("=" * 80)
            logger.info("🎉 pending分析队列处理完成 - 最终统计:")
            logger.info(f"  📥 拉取pending工单总数: {len(work_orders)}")
            logger.info(f"  💬 有评论需分析数量: {pending_result['statistics']['with_comments']}")
            logger.info(f"  🔍 实际分析处理数量: {analysis_result['analyzed_orders']}")
            logger.info(f"  ✅ 成功分析完成数量: {analysis_result['successful_analyses']}")
            logger.info(f"  ❌ 分析失败数量: {analysis_result['failed_analyses']}")
            logger.info(f"  ⏭️ 跳过处理数量: {skipped_orders}")
            logger.info(f"  📊 分析成功率: {analysis_result['successful_analyses'] / analysis_result['analyzed_orders'] * 100:.1f}%" if analysis_result['analyzed_orders'] > 0 else "0%")
            logger.info("=" * 80)
            
            return final_result
            
        except Exception as e:
            logger.error(f"❌ 处理待分析队列失败: {e}")
            logger.error(f"错误详情: {str(e)}")
            import traceback
            logger.error(f"错误堆栈: {traceback.format_exc()}")
            return {
                "success": False,
                "error": str(e),
                "message": "处理失败"
            }


# ==================== 批量分析工作流函数 ====================

async def execute_batch_analysis_workflow(db: Session, task_id: str) -> Dict[str, Any]:
    """
    执行批量分析工作流 - 供APScheduler调用
    这是一个独立的工作流函数，用于处理完整的批量分析流程
    """
    logger.info(f"🚀 开始执行批量分析工作流: task_id={task_id}")
    
    try:
        from app.models.task import task_record
        
        # 更新任务状态为运行中
        task_record.update_task_progress(
            db=db,
            task_id=task_id,
            status="running",
            process_stage="批量分析工作流"
        )
        
        # 使用全局服务实例执行批量分析
        result = await stage2_service.process_pending_analysis_queue(db)
        
        if result["success"]:
            # 更新任务状态为完成
            task_record.complete_task(
                db=db,
                task_id=task_id,
                status="completed",
                execution_details={
                    "workflow_result": result,
                    "completion_message": "批量分析工作流执行成功"
                }
            )
            logger.info(f"✅ 批量分析工作流完成: task_id={task_id}")
        else:
            # 更新任务状态为失败
            task_record.complete_task(
                db=db,
                task_id=task_id,
                status="failed",
                execution_details={
                    "workflow_result": result,
                    "error_message": result.get("error", "未知错误")
                }
            )
            logger.error(f"❌ 批量分析工作流失败: task_id={task_id}, error={result.get('error')}")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ 批量分析工作流异常: task_id={task_id}, error={e}")
        
        # 更新任务状态为失败
        try:
            from app.models.task import task_record
            task_record.complete_task(
                db=db,
                task_id=task_id,
                status="failed",
                execution_details={
                    "error_message": str(e),
                    "exception_type": type(e).__name__
                }
            )
        except:
            pass
        
        return {
            "success": False,
            "error": str(e),
            "message": "批量分析工作流执行异常"
        }


# 全局第二阶段服务实例
stage2_service = Stage2AnalysisService()
