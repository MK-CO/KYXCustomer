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
        self.few_shot_examples_by_category = self._init_category_few_shot_examples()  # 按分类组织的few-shot示例
    
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
            
            # 统计消息数量 - 仅根据oper字段判断
            if user_type == "system":
                system_count += 1
            elif oper:  # oper为1，客服回复
                service_count += 1
            else:  # oper为0，客户回复
                customer_count += 1
            
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
    
    def _get_real_comment_stats_for_save(
        self,
        db: Session,
        work_id: int
    ) -> Dict[str, int]:
        """专门用于保存结果时的统计查询：从t_work_comment表统计真实的客户和客服回复数量（基于oper字段）"""
        try:
            # 根据work_id确定对应的评论表名，默认使用当前年份
            comment_table_name = self.stage1.get_comment_table_name()
            
            # 统计所有评论的客户和客服数量
            sql = f"""
            SELECT 
                COUNT(*) as total_count,
                SUM(CASE WHEN oper = 0 THEN 1 ELSE 0 END) as customer_count,
                SUM(CASE WHEN oper = 1 THEN 1 ELSE 0 END) as service_count
            FROM {comment_table_name}
            WHERE work_id = :work_id 
            AND deleted = 0
            """
            
            result = db.execute(text(sql), {"work_id": work_id}).fetchone()
            
            # 🔥 确保返回整数类型，避免Decimal序列化错误
            total_count = int(result.total_count) if result and result.total_count else 0
            customer_count = int(result.customer_count) if result and result.customer_count else 0
            service_count = int(result.service_count) if result and result.service_count else 0
            
            logger.info(f"📊 保存时统计工单 {work_id}: 总{total_count}条，客户{customer_count}条，客服{service_count}条")
            
            return {
                "total_comments": total_count,
                "customer_messages": customer_count,  # 使用customer_messages保持与现有代码一致
                "service_messages": service_count     # 使用service_messages保持与现有代码一致
            }
            
        except Exception as e:
            logger.error(f"❌ 保存时统计工单 {work_id} 评论数量失败: {e}")
            return {
                "total_comments": 0,
                "customer_messages": 0,
                "service_messages": 0
            }

    def save_analysis_result(
        self,
        db: Session,
        work_id: int,
        analysis_result: Dict[str, Any]
    ) -> bool:
        """保存AI分析结果到结果表"""
        
        # 🔥 修复：强制检查skip_save标记，确保低风险记录不被保存
        if analysis_result.get("skip_save", False):
            logger.info(f"⏭️ 工单 {work_id} 标记为跳过保存，不保存到数据库")
            return True  # 返回True表示"成功处理"，但实际没有保存
        
        # 🔥 修复：检查风险级别，如果是low且无规避行为，也不保存
        risk_level = analysis_result.get('risk_level', 'low')
        has_evasion = analysis_result.get('has_evasion', False)
        
        if risk_level == 'low' and not has_evasion:
            logger.info(f"⏭️ 工单 {work_id} 风险级别为低且无规避行为，不保存到数据库")
            return True  # 返回True表示"成功处理"，但实际没有保存
        
        logger.info(f"💾 保存工单 {work_id} 分析结果: 风险级别={risk_level}, 规避责任={has_evasion}")
        
        try:
            # 查询订单ID和订单编号
            order_id, order_no = self._get_order_info_by_work_id(db, work_id)
            
            # 🔥 获取正确的统计数据用于保存（基于oper字段）
            correct_stats = self._get_real_comment_stats_for_save(db, work_id)
            
            # 将正确的统计数据覆盖到analysis_result中，仅用于数据库保存
            analysis_result_for_save = analysis_result.copy()
            analysis_result_for_save.update(correct_stats)
            
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
            
            params = self._build_analysis_params(work_id, analysis_result_for_save, order_id, order_no)
            params["created_at"] = datetime.now()
            params["updated_at"] = datetime.now()
            
            # 🔥 添加SQL执行的错误处理和日志
            try:
                result = db.execute(text(upsert_sql), params)
            except Exception as sql_error:
                logger.error(f"❌ SQL执行失败，工单 {work_id}，错误: {sql_error}")
                logger.error(f"📊 参数类型统计: evidence_sentences={type(params.get('evidence_sentences'))}, "
                           f"matched_keywords={type(params.get('matched_keywords'))}, "
                           f"analysis_details={type(params.get('analysis_details'))}")
                logger.error(f"🔍 SQL语句片段: {upsert_sql[:200]}...")
                
                # 🔥 修复：检查并修复可能的参数类型问题
                # 确保所有JSON字段都是字符串类型
                json_fields = ['evidence_sentences', 'matched_keywords', 'analysis_details', 
                              'llm_raw_response', 'evasion_types', 'improvement_suggestions']
                
                for field in json_fields:
                    if field in params and params[field] is not None:
                        if not isinstance(params[field], str):
                            # 如果不是字符串，使用safe_json_dumps转换
                            params[field] = safe_json_dumps(params[field])
                            logger.info(f"🔄 修复字段 {field} 的数据类型为字符串")
                
                logger.info(f"🔄 修复参数类型后重试保存工单 {work_id}")
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
                # 更激进的截断，直接保留前几个元素
                reduced_count = min(3, len(data))  # 最多保留3个元素
                truncated_data = data[:reduced_count]
                # 添加截断标记
                if truncated_data and isinstance(truncated_data[0], (str, dict)):
                    truncated_data.append({"truncated": True, "original_count": len(data), "note": "已截断避免保存错误"})
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
        """构建分析结果参数，保存完整原始数据"""
        import json
        
        # 🔥 移除长度限制 - 数据库字段都是TEXT/LONGTEXT类型，可以存储完整数据
        # 只对VARCHAR字段保留必要限制
        VARCHAR_LIMITS = {
            "matched_categories": 500       # VARCHAR(255)字段
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
        
        # 安全处理匹配类别字段（VARCHAR字段需要限制长度）
        matched_categories_str = None
        if keyword_screening.get("matched_categories"):
            categories_list = keyword_screening["matched_categories"][:10]  # 最多10个类别
            categories_str = ",".join(categories_list)
            matched_categories_str = self._safe_truncate_text(categories_str, VARCHAR_LIMITS["matched_categories"])
        
        # 🔥 构建保存参数字典，保存完整原始数据（TEXT/LONGTEXT字段无长度限制）
        save_params = {
            "work_id": work_id,
            "order_id": order_id,
            "order_no": order_no,
            "session_start_time": analysis_result.get("session_start_time"),
            "session_end_time": analysis_result.get("session_end_time"),
            "total_comments": int(analysis_result.get("total_comments", 0)),  # 确保是int类型
            "customer_comments": int(analysis_result.get("customer_messages", 0)),  # 确保是int类型  
            "service_comments": int(analysis_result.get("service_messages", 0)),  # 确保是int类型
            "has_evasion": 1 if analysis_result.get("has_evasion", False) else 0,
            "risk_level": analysis_result.get("risk_level", "low"),
            "confidence_score": float(analysis_result.get("confidence_score", 0.0)),  # 确保是float类型
            # JSON字段 - 保存完整数据
            "evasion_types": safe_json_dumps(analysis_result.get("evasion_types", [])) if analysis_result.get("evasion_types") else None,
            "evidence_sentences": safe_json_dumps(analysis_result.get("evidence_sentences", [])) if analysis_result.get("evidence_sentences") else None,
            "improvement_suggestions": safe_json_dumps(analysis_result.get("improvement_suggestions", [])) if analysis_result.get("improvement_suggestions") else None,
            # 关键词筛选结果
            "keyword_screening_score": float(keyword_screening.get("confidence_score", 0.0)),  # 确保是float类型
            "matched_categories": matched_categories_str,  # VARCHAR字段，已处理长度限制
            "matched_keywords": safe_json_dumps(keyword_screening.get("matched_details", {})) if keyword_screening.get("matched_details") else None,
            "is_suspicious": 1 if keyword_screening.get("is_suspicious", False) else 0,
            # 情感分析结果
            "sentiment": analysis_result.get("sentiment", "neutral"),
            "sentiment_intensity": float(analysis_result.get("sentiment_intensity", 0.0)),  # 确保是float类型
            # 原始数据 - 保存完整数据（LONGTEXT字段）
            "conversation_text": analysis_result.get("conversation_text", ""),
            "llm_raw_response": safe_json_dumps(llm_raw_response) if llm_raw_response else None,
            "analysis_details": safe_json_dumps(analysis_result),
            "analysis_note": self._build_enhanced_analysis_note(analysis_result),
            # LLM调用信息
            "llm_provider": llm_provider,
            "llm_model": llm_model,
            "llm_tokens_used": int(llm_tokens_used) if llm_tokens_used else 0,  # 确保是int类型
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
        """获取备用的默认关键词配置（只保留推卸责任类别）"""
        logger.warning("数据库关键词配置加载失败，使用备用配置（仅推卸责任类别）")
        return {
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
    
    def _init_category_few_shot_examples(self) -> Dict[str, List[Dict[str, Any]]]:
        """初始化按分类组织的few-shot示例"""
        return {
            "urgent_urging": [  # 紧急催促
                {
                    "conversation": "门店: 车主一直催贴膜进度，又来了，怎么样了？\n客服: 这个需要时间处理，让车主耐心等待。",
                    "analysis": {
                        "has_evasion": False,
                        "risk_level": "medium",
                        "confidence_score": 0.75,
                        "evasion_types": "紧急催促",
                        "evidence_sentences": ["车主一直催贴膜进度，又来了，怎么样了", "这个需要时间处理，让车主耐心等待"],
                        "improvement_suggestions": ["应具体回应车主的催促，提供明确的完成时间，如'师傅今天下午3点完成贴膜'"]
                    }
                },
                {
                    "conversation": "门店: 车主加急联系，速度催结果，有进展了吗？\n客服: 已经在跟进了，会尽快给答复。",
                    "analysis": {
                        "has_evasion": False,
                        "risk_level": "medium",
                        "confidence_score": 0.75,
                        "evasion_types": "紧急催促",
                        "evidence_sentences": ["车主加急联系，速度催结果，有进展了吗", "已经在跟进了，会尽快给答复"],
                        "improvement_suggestions": ["面对加急催促，应提供具体的进展情况和预计完成时间"]
                    }
                }
            ],
            "complaint_dispute": [  # 投诉纠纷
                {
                    "conversation": "门店: 车主投诉配件质量，要退款了\n客服: 这不是我们的问题，是厂家的配件质量问题，让车主直接找供应商。",
                    "analysis": {
                        "has_evasion": True,
                        "risk_level": "high",
                        "confidence_score": 0.95,
                        "evasion_types": "投诉纠纷",
                        "evidence_sentences": ["车主投诉配件质量，要退款了", "这不是我们的问题，是厂家的配件质量问题"],
                        "improvement_suggestions": ["面对投诉和退款要求，门店应承担售后责任，协助处理而不是推卸给厂家"]
                    }
                },
                {
                    "conversation": "门店: 有纠纷单，客诉12315了\n客服: 翘单吧，能拖就拖一天是一天。",
                    "analysis": {
                        "has_evasion": False,
                        "risk_level": "high",
                        "confidence_score": 0.98,
                        "evasion_types": "投诉纠纷",
                        "evidence_sentences": ["有纠纷单，客诉12315了", "翘单吧，能拖就拖一天是一天"],
                        "improvement_suggestions": ["严禁故意拖延处理客诉和12315投诉，应立即响应和解决"]
                    }
                }
            ],
            "responsibility_evasion": [  # 推卸责任（增加更多示例）
                {
                    "conversation": "门店: 车主说贴膜有气泡要求重新处理\n客服: 这不是我们门店的问题，是师傅技术问题，你直接找安装师傅负责。",
                    "analysis": {
                        "has_evasion": True,
                        "risk_level": "high", 
                        "confidence_score": 0.95,
                        "evasion_types": "推卸责任",
                        "evidence_sentences": ["这不是我们门店的问题，是师傅技术问题", "你直接找安装师傅负责"],
                        "improvement_suggestions": ["门店应承担服务责任，协调师傅重新处理，而不是直接推卸给师傅"]
                    }
                },
                {
                    "conversation": "车主: 我的订单到现在都没有发货，什么时候能处理？\n客服: 这个需要仓库那边处理，我们客服管不了发货的事情，仓库的事情不归我管，你自己想办法联系吧。",
                    "analysis": {
                        "has_evasion": True,
                        "risk_level": "high",
                        "confidence_score": 0.88,
                        "evasion_types": "推卸责任",
                        "evidence_sentences": ["这个需要仓库那边处理，我们客服管不了发货的事情", "仓库的事情不归我管，你自己想办法联系吧"],
                        "improvement_suggestions": ["主动协调各部门解决客户问题", "提供具体的解决方案和时间节点"]
                    }
                },
                {
                    "conversation": "车主: 产品质量有问题，我要退换货\n客服: 这个是产品部门的问题，我们售后处理不了，我们只负责接电话，具体处理不是我们的职责范围。",
                    "analysis": {
                        "has_evasion": True,
                        "risk_level": "high", 
                        "confidence_score": 0.90,
                        "evasion_types": "推卸责任",
                        "evidence_sentences": ["这个是产品部门的问题，我们售后处理不了", "我们只负责接电话，具体处理不是我们的职责范围"],
                        "improvement_suggestions": ["主动承担责任协调解决", "为客户提供明确的处理流程"]
                    }
                },
                {
                    "conversation": "车主: 你们网站登录不了，一直报错\n客服: 网站问题我们解决不了，这是IT部门负责的，我们只管接咨询，技术问题不归我们处理。",
                    "analysis": {
                        "has_evasion": True,
                        "risk_level": "high",
                        "confidence_score": 0.85,
                        "evasion_types": "推卸责任",
                        "evidence_sentences": ["网站问题我们解决不了，这是IT部门负责的", "我们只管接咨询，技术问题不归我们处理"],
                        "improvement_suggestions": ["主动协助客户解决技术问题", "建立跨部门协作机制"]
                    }
                },
                {
                    "conversation": "车主: 我付款了但是没收到确认短信\n客服: 短信是技术部门发的，我们管不了这个，你去联系技术部门吧，或者等系统自己恢复。",
                    "analysis": {
                        "has_evasion": True,
                        "risk_level": "medium",
                        "confidence_score": 0.78,
                        "evasion_types": "推卸责任", 
                        "evidence_sentences": ["短信是技术部门发的，我们管不了这个", "你去联系技术部门吧，或者等系统自己恢复"],
                        "improvement_suggestions": ["主动帮助客户联系相关部门", "提供替代解决方案"]
                    }
                }
            ],
            "delay_handling": [  # 拖延处理
                {
                    "conversation": "客户: 订单什么时候能处理完？\n客服: 这个...具体时间不好说，你再等等看吧。",
                    "analysis": {
                        "has_evasion": False,
                        "risk_level": "medium",
                        "confidence_score": 0.7,
                        "evasion_types": "拖延处理",
                        "evidence_sentences": ["具体时间不好说，你再等等看吧"],
                        "improvement_suggestions": ["应提供具体的处理时间节点，避免模糊回应"]
                    }
                }
            ],
            "inappropriate_wording": [  # 不当用词
                {
                    "conversation": "师傅: 又来催了，撕心裂肺的，搞快点弄完\n门店: 知道了，赶紧搞定",
                    "analysis": {
                        "has_evasion": False,
                        "risk_level": "medium",
                        "confidence_score": 0.8,
                        "evasion_types": "不当用词",
                        "evidence_sentences": ["又来催了，撕心裂肺的，搞快点弄完", "赶紧搞定"],
                        "improvement_suggestions": ["应使用专业用语，如'车主比较着急，请加快处理速度'，避免'撕'、'搞'等不当表达"]
                    }
                }
            ],
            "normal_service": [  # 正常服务（对照组）
                {
                    "conversation": "门店: 车主咨询全车贴膜价格和质保期\n客服: 全车贴膜1800元，质保2年，包括材料和人工，预计明天上午完成安装。",
                    "analysis": {
                        "has_evasion": False,
                        "risk_level": "low",
                        "confidence_score": 0.1,
                        "evasion_types": "",
                        "evidence_sentences": [],
                        "improvement_suggestions": []
                    }
                }
            ]
        }
    
    def _get_category_few_shot_examples(self, db: Session, target_categories: List[str]) -> List[Dict[str, Any]]:
        """根据目标分类获取对应的few-shot示例"""
        try:
            if not target_categories:
                logger.warning("未提供目标分类，返回空示例")
                return []
            
            # 查询启用的分析分类（过滤出目标分类中启用的）
            category_keys_str = "', '".join(target_categories)
            sql = f"""
            SELECT category_key, category_name 
            FROM ai_keyword_categories 
            WHERE category_type = 'analysis' 
            AND is_enabled = 1 
            AND category_key IN ('{category_keys_str}')
            ORDER BY sort_order
            """
            
            enabled_categories = db.execute(text(sql)).fetchall()
            
            if not enabled_categories:
                logger.warning(f"目标分类 {target_categories} 中没有启用的分类")
                return []
            
            # 收集对应分类的few-shot示例
            category_examples = []
            enabled_category_keys = [cat.category_key for cat in enabled_categories]
            
            logger.info(f"为目标分类 {target_categories} 找到启用的分类: {enabled_category_keys}")
            
            for category_key in enabled_category_keys:
                if category_key in self.few_shot_examples_by_category:
                    examples = self.few_shot_examples_by_category[category_key]
                    category_examples.extend(examples)
                    logger.debug(f"分类 {category_key} 添加了 {len(examples)} 个专门示例")
            
            # 总是添加正常服务的对照组示例
            if "normal_service" in self.few_shot_examples_by_category:
                normal_examples = self.few_shot_examples_by_category["normal_service"]
                category_examples.extend(normal_examples)
                logger.debug(f"添加了 {len(normal_examples)} 个正常服务对照示例")
            
            logger.info(f"为分类 {enabled_category_keys} 生成了 {len(category_examples)} 个专门few-shot示例")
            return category_examples
            
        except Exception as e:
            logger.error(f"获取分类few-shot示例失败: {e}")
            # 降级：返回正常服务示例
            if "normal_service" in self.few_shot_examples_by_category:
                fallback_examples = self.few_shot_examples_by_category["normal_service"]
                logger.warning(f"降级使用正常服务示例，共 {len(fallback_examples)} 个")
                return fallback_examples
            return []
    
    def _extract_evidence_sentences(self, messages: List[Dict[str, Any]], keyword: str, category: str, config_id: int = None) -> List[Dict[str, Any]]:
        """从消息列表中提取包含关键词的具体消息，返回结构化JSON格式"""
        evidence_list = []
        
        for i, message in enumerate(messages):
            content = str(message.get("content", "")).strip()
            if not content:
                continue
                
            # 如果这条消息包含关键词
            if keyword in content:
                # 构建显示用的消息格式
                user_type = message.get("user_type", "")
                name = message.get("name", "")
                create_time = message.get("create_time", "")
                oper = message.get("oper", False)
                
                # 确定角色显示名称 - 仅根据oper字段判断
                if user_type == "system":
                    role = "系统"
                elif oper:  # oper为1，客服
                    role = "客服"
                else:  # oper为0，客户
                    role = "客户"
                
                # 如果有名称，添加到角色后面
                if name:
                    role_display = f"{role}({name})"
                else:
                    role_display = role
                
                # 构建完整的消息显示
                if create_time:
                    message_display = f"[{create_time}] {role_display}: {content}"
                else:
                    message_display = f"{role_display}: {content}"
                
                # 高亮关键词
                highlighted_content = content.replace(keyword, f"【{keyword}】")
                if create_time:
                    highlighted_display = f"[{create_time}] {role_display}: {highlighted_content}"
                else:
                    highlighted_display = f"{role_display}: {highlighted_content}"
                
                # 计算匹配位置（关键词匹配）
                match_start_pos = content.find(keyword)
                match_end_pos = match_start_pos + len(keyword) if match_start_pos >= 0 else 0
                
                # 构建结构化的证据条目（标准格式）
                evidence_entry = {
                    "rule_type": "keyword",  # 规则类型：keyword 或 pattern
                    "rule_name": category,  # 规则名称/类别
                    "category": category,   # 分类名称
                    "matched_keyword": keyword,  # 匹配的关键词
                    "matched_pattern": None,    # 正则表达式（关键词匹配时为空）
                    "matched_text": keyword,    # 实际匹配的文本
                    "message_content": content,  # 原始消息内容
                    "conversation_context": message_display,  # 完整消息显示格式
                    "highlighted_context": highlighted_display,   # 高亮后的消息显示
                    "config_id": config_id,    # 配置ID
                    "message_index": i,       # 消息在列表中的索引
                    "message_id": message.get("id"),  # 消息ID
                    "user_type": user_type,   # 用户类型
                    "user_name": name,        # 用户姓名
                    "create_time": create_time, # 消息创建时间
                    "match_start_pos": match_start_pos,  # 匹配开始位置
                    "match_end_pos": match_end_pos,      # 匹配结束位置
                    "evidence_timestamp": datetime.now().isoformat(),  # 证据提取时间戳
                    # 初始LLM分析信息（后续会被_enhance_evidence_with_llm_analysis方法更新）
                    "llm_analysis": {
                        "llm_confirmed": False,
                        "llm_risk_assessment": "unknown",
                        "llm_analysis_reason": "待LLM分析",
                        "llm_match_score": 0.0,
                        "llm_evidence_match": None,
                        "llm_suggestion": "",
                        "regex_matched": True,  # 关键词匹配成功
                        "llm_overridden": False,  # 初始状态
                        "confidence_explanation": f"关键词 '{keyword}' 在类别 '{category}' 中匹配成功"
                    },
                    "analysis_timestamp": datetime.now().isoformat(),  # 分析时间戳
                    "evidence_status": "regex_matched"  # 证据状态：正则匹配
                }
                
                evidence_list.append(evidence_entry)
        
        return evidence_list
    
    def _extract_pattern_evidence(self, messages: List[Dict[str, Any]], patterns: List[str], category: str, config_id: int = None) -> List[Dict[str, Any]]:
        """从消息列表中提取匹配正则模式的具体内容，返回结构化JSON格式"""
        evidence_list = []
        import re
        
        for pattern in patterns:
            try:
                # 对每条消息单独进行正则匹配
                for i, message in enumerate(messages):
                    content = str(message.get("content", "")).strip()
                    if not content:
                        continue
                    
                    matches = re.finditer(pattern, content, re.IGNORECASE)
                    for match in matches:
                        matched_text = match.group()
                        
                        # 构建显示用的消息格式
                        user_type = message.get("user_type", "")
                        name = message.get("name", "")
                        create_time = message.get("create_time", "")
                        oper = message.get("oper", False)
                        
                        # 确定角色显示名称 - 仅根据oper字段判断
                        if user_type == "system":
                            role = "系统"
                        elif oper:  # oper为1，客服
                            role = "客服"
                        else:  # oper为0，客户
                            role = "客户"
                        
                        # 如果有名称，添加到角色后面
                        if name:
                            role_display = f"{role}({name})"
                        else:
                            role_display = role
                        
                        # 构建完整的消息显示
                        if create_time:
                            message_display = f"[{create_time}] {role_display}: {content}"
                        else:
                            message_display = f"{role_display}: {content}"
                        
                        # 高亮匹配的内容
                        highlighted_content = content.replace(matched_text, f"【{matched_text}】")
                        if create_time:
                            highlighted_display = f"[{create_time}] {role_display}: {highlighted_content}"
                        else:
                            highlighted_display = f"{role_display}: {highlighted_content}"
                        
                        # 构建结构化的证据条目（标准格式）
                        evidence_entry = {
                            "rule_type": "pattern",  # 规则类型：keyword 或 pattern
                            "rule_name": category,   # 规则名称/类别
                            "category": category,    # 分类名称
                            "matched_keyword": None, # 匹配的关键词（正则匹配时为空）
                            "matched_pattern": pattern,  # 正则表达式
                            "matched_text": matched_text,  # 实际匹配的文本
                            "message_content": content,  # 原始消息内容
                            "conversation_context": message_display,  # 完整消息显示格式
                            "highlighted_context": highlighted_display,    # 高亮后的消息显示
                            "config_id": config_id,  # 配置ID
                            "message_index": i,      # 消息在列表中的索引
                            "message_id": message.get("id"),  # 消息ID
                            "user_type": user_type,  # 用户类型
                            "user_name": name,       # 用户姓名
                            "create_time": create_time, # 消息创建时间
                            "match_start_pos": match.start(),  # 匹配开始位置（在消息内容中）
                            "match_end_pos": match.end(),      # 匹配结束位置（在消息内容中）
                            "evidence_timestamp": datetime.now().isoformat(),  # 证据提取时间戳
                            # 初始LLM分析信息（后续会被_enhance_evidence_with_llm_analysis方法更新）
                            "llm_analysis": {
                                "llm_confirmed": False,
                                "llm_risk_assessment": "unknown", 
                                "llm_analysis_reason": "待LLM分析",
                                "llm_match_score": 0.0,
                                "llm_evidence_match": None,
                                "llm_suggestion": "",
                                "regex_matched": True,  # 正则匹配成功
                                "llm_overridden": False,  # 初始状态
                                "confidence_explanation": f"正则模式 '{pattern}' 在类别 '{category}' 中匹配成功"
                            },
                            "analysis_timestamp": datetime.now().isoformat(),  # 分析时间戳
                            "evidence_status": "pattern_matched"  # 证据状态：正则模式匹配
                        }
                        
                        evidence_list.append(evidence_entry)
                        
            except re.error as e:
                logger.warning(f"正则表达式 {pattern} 执行失败: {e}")
                continue
        
        return evidence_list
    
    def _build_evidence_context(self, detailed_evidence: List[Dict[str, Any]], keyword_result: Dict[str, Any]) -> str:
        """构建证据上下文，传递给LLM进行深度分析"""
        if not detailed_evidence:
            return ""
        
        context_parts = [
            "=== 正则匹配发现的关键证据 ===",
            f"总计发现 {len(detailed_evidence)} 条证据，涉及类别: {', '.join(keyword_result.get('matched_categories', []))}",
            ""
        ]
        
        # 按类别组织证据
        evidence_by_category = {}
        for evidence in detailed_evidence:
            category = evidence.get("category", "未分类")
            if category not in evidence_by_category:
                evidence_by_category[category] = []
            evidence_by_category[category].append(evidence)
        
        for category, evidences in evidence_by_category.items():
            context_parts.append(f"📂 {category} ({len(evidences)}条):")
            
            for i, evidence in enumerate(evidences[:3], 1):  # 每个类别最多显示3条证据
                rule_type = evidence.get("rule_type", "未知")
                matched_text = evidence.get("matched_text", "")
                highlighted_context = evidence.get("highlighted_context", "")
                
                if rule_type == "keyword":
                    context_parts.append(f"  {i}. [关键词匹配] \"{matched_text}\"")
                elif rule_type == "pattern":
                    pattern = evidence.get("matched_pattern", "")
                    context_parts.append(f"  {i}. [正则匹配] 模式: {pattern} -> \"{matched_text}\"")
                
                context_parts.append(f"     对话: {highlighted_context}")
                context_parts.append("")
            
            if len(evidences) > 3:
                context_parts.append(f"     ... 还有 {len(evidences) - 3} 条证据")
                context_parts.append("")
        
        context_parts.extend([
            "=== 分析要求 ===",
            "请基于以上证据，结合完整对话内容，进行深度分析：",
            "1. 确认这些证据是否真的表明存在问题行为",
            "2. 评估问题的严重程度和风险级别",
            "3. 判断是否存在规避责任行为",
            "4. 给出具体的改进建议",
            ""
        ])
        
        return "\n".join(context_parts)
    
    def _merge_regex_and_llm_results(
        self, 
        keyword_result: Dict[str, Any], 
        detailed_evidence: List[Dict[str, Any]], 
        llm_analysis: Dict[str, Any],
        conversation_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """融合正则匹配和LLM分析结果"""
        
        # 基础信息从LLM分析结果获取
        merged_result = {
            "has_evasion": llm_analysis.get("has_evasion", False),
            "risk_level": llm_analysis.get("risk_level", "low"),
            "confidence_score": llm_analysis.get("confidence_score", 0.0),
            "evasion_types": llm_analysis.get("evasion_types", ""),
            "improvement_suggestions": llm_analysis.get("improvement_suggestions", []),
            "sentiment": llm_analysis.get("sentiment", "neutral"),
            "sentiment_intensity": llm_analysis.get("sentiment_intensity", 0.0),
        }
        
        # 🔥 关键修复：处理LLM找到证据但正则匹配为空的情况
        enhanced_evidence = self._enhance_evidence_with_llm_analysis(detailed_evidence, llm_analysis)
        
        # 🔥 如果正则匹配没有找到证据，但LLM分析找到了evidence_sentences，需要创建结构化证据
        if len(enhanced_evidence) == 0 and llm_analysis.get("evidence_sentences"):
            logger.debug("正则匹配无证据，但LLM分析找到证据，创建结构化证据对象")
            enhanced_evidence = self._create_llm_evidence_objects(
                llm_analysis, conversation_data.get("messages", [])
            )
        
        # 🔥 修复：evidence_sentences始终使用标准结构化格式
        # evidence_sentences使用完整的结构化证据对象数组（与用户要求的标准格式一致）
        merged_result.update({
            "evidence_sentences": enhanced_evidence,  # 使用完整的结构化证据对象数组
            "detailed_evidence": enhanced_evidence,   # 保持兼容性，用于详细分析  
            "matched_keywords": [e.get("matched_keyword") for e in enhanced_evidence if e.get("matched_keyword")],
            "evidence_count": len(enhanced_evidence),
        })
        
        # 会话信息
        merged_result.update({
            "session_start_time": conversation_data.get("session_info", {}).get("start_time"),
            "session_end_time": conversation_data.get("session_info", {}).get("end_time"),
            "total_comments": conversation_data.get("total_messages", 0),
            "customer_comments": conversation_data.get("customer_messages", 0),
            "service_comments": conversation_data.get("service_messages", 0),
            "conversation_text": conversation_data.get("conversation_text", ""),
            "conversation_messages": conversation_data.get("messages", []),
        })
        
        # 如果LLM的置信度过低，调整为基于正则匹配的置信度
        if merged_result["confidence_score"] < 0.5 and keyword_result.get("confidence_score", 0) > 0.5:
            logger.debug("LLM置信度较低，使用正则匹配的置信度")
            merged_result["confidence_score"] = min(keyword_result["confidence_score"], 0.8)
        
        # 如果LLM没有识别出规避责任，但正则匹配到了推卸责任类别，进行二次确认
        if not merged_result["has_evasion"] and any("推卸责任" in cat for cat in keyword_result.get("matched_categories", [])):
            logger.debug("LLM未识别规避责任，但正则匹配到推卸责任，进行二次确认")
            if merged_result["confidence_score"] > 0.7:  # 高置信度时认为可能存在推卸责任
                merged_result["has_evasion"] = True
                if merged_result["evasion_types"] != "推卸责任":
                    merged_result["evasion_types"] = "推卸责任"
        
        return merged_result
    
    def _enhance_evidence_with_llm_analysis(
        self, 
        detailed_evidence: List[Dict[str, Any]], 
        llm_analysis: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """将LLM分析结果关联到每条证据中"""
        
        enhanced_evidence = []
        llm_evidence_sentences = llm_analysis.get("evidence_sentences", [])
        
        for evidence in detailed_evidence:
            # 复制原始证据
            enhanced_evidence_item = evidence.copy()
            
            # 获取证据的原始消息内容
            message_content = evidence.get("message_content", "")
            conversation_context = evidence.get("conversation_context", "")
            
            # 🔥 更新LLM分析相关字段
            # 如果LLM成功分析了（有risk_level结果），则认为LLM确实进行了分析
            llm_has_analysis = bool(llm_analysis.get("risk_level")) and llm_analysis.get("risk_level") != "unknown"
            
            # 获取现有的llm_analysis字段（如果存在）并进行更新
            current_llm_analysis = enhanced_evidence_item.get("llm_analysis", {})
            
            # 更新LLM分析信息
            llm_analysis_info = {
                "llm_confirmed": False,  # LLM是否确认此证据有问题（默认为False，只有匹配到才设为True）
                "llm_risk_assessment": llm_analysis.get("risk_level", "low") if llm_has_analysis else "unknown",  # LLM对此证据的风险评估
                "llm_analysis_reason": "LLM分析后认为此内容正常，未发现问题行为" if llm_has_analysis else "未进行LLM分析",  # LLM分析此证据的原因
                "llm_match_score": 0.0,           # 与LLM证据的匹配度
                "llm_evidence_match": None,       # 匹配到的LLM证据句子
                "llm_suggestion": "此内容经LLM分析认为是正常业务对话" if llm_has_analysis else "",  # LLM针对此证据的建议
                "regex_matched": current_llm_analysis.get("regex_matched", True),  # 保留正则匹配状态
                "llm_overridden": False,  # 初始状态，后续可能更新
                "confidence_explanation": current_llm_analysis.get("confidence_explanation", ""),  # 保留置信度解释
            }
            
            # 尝试将此证据与LLM识别的证据句子进行匹配
            best_match_score = 0.0
            best_match_sentence = None
            
            for llm_sentence in llm_evidence_sentences:
                if not llm_sentence or not isinstance(llm_sentence, str):
                    continue
                
                # 计算匹配度
                match_score = self._calculate_evidence_similarity(
                    message_content, conversation_context, llm_sentence
                )
                
                if match_score > best_match_score:
                    best_match_score = match_score
                    best_match_sentence = llm_sentence
            
            # 如果找到较好的匹配（匹配度 > 0.3）
            if best_match_score > 0.3:
                llm_analysis_info.update({
                    "llm_confirmed": True,
                    "llm_risk_assessment": llm_analysis.get("risk_level", "unknown"),
                    "llm_analysis_reason": f"LLM识别此内容属于{llm_analysis.get('evasion_types', '')}行为",
                    "llm_match_score": round(best_match_score, 3),
                    "llm_evidence_match": best_match_sentence,
                    "llm_suggestion": self._extract_relevant_suggestion(
                        llm_analysis.get("improvement_suggestions", []), 
                        evidence.get("category", "")
                    ),
                    "llm_overridden": False,  # LLM确认了正则匹配
                    "confidence_explanation": f"正则匹配命中 '{evidence.get('category')}' 分类，LLM分析确认存在问题行为"
                })
                enhanced_evidence_item["evidence_status"] = "regex_hit_llm_confirmed"
            else:
                # 未匹配到LLM证据，但可能是相关类别
                category = evidence.get("category", "")
                if category == llm_analysis.get("evasion_types", ""):
                    llm_analysis_info.update({
                        "llm_confirmed": True,
                        "llm_risk_assessment": llm_analysis.get("risk_level", "unknown"),
                        "llm_analysis_reason": f"LLM确认存在{category}行为，虽未具体匹配到此证据",
                        "llm_match_score": 0.2,  # 类别匹配给予较低分数
                        "llm_suggestion": self._extract_relevant_suggestion(
                            llm_analysis.get("improvement_suggestions", []), 
                            category
                        ),
                        "llm_overridden": False,  # LLM确认了正则匹配
                        "confidence_explanation": f"正则匹配命中 '{category}' 分类，LLM分析确认存在相关问题行为"
                    })
                    enhanced_evidence_item["evidence_status"] = "regex_hit_llm_category_match"
                else:
                    # LLM未确认此证据，可能是误报
                    llm_analysis_info.update({
                        "llm_overridden": True,  # LLM覆盖了正则匹配结果
                        "confidence_explanation": f"正则匹配命中 '{category}' 分类，但LLM分析认为是正常对话"
                    })
                    enhanced_evidence_item["evidence_status"] = "regex_hit_llm_normal"
            
            # 更新证据中的LLM分析信息
            enhanced_evidence_item["llm_analysis"] = llm_analysis_info
            enhanced_evidence_item["analysis_timestamp"] = datetime.now().isoformat()
            
            enhanced_evidence.append(enhanced_evidence_item)
        
        return enhanced_evidence
    
    def _create_llm_evidence_objects(
        self, 
        llm_analysis: Dict[str, Any], 
        messages: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """从LLM分析结果创建结构化证据对象"""
        
        evidence_objects = []
        llm_evidence_sentences = llm_analysis.get("evidence_sentences", [])
        
        if not llm_evidence_sentences:
            return []
        
        # 确保llm_evidence_sentences是列表
        if isinstance(llm_evidence_sentences, str):
            llm_evidence_sentences = [llm_evidence_sentences]
        
        logger.debug(f"LLM分析找到 {len(llm_evidence_sentences)} 条证据句子，尝试在 {len(messages)} 条消息中匹配")
        
        for idx, evidence_sentence in enumerate(llm_evidence_sentences):
            if not evidence_sentence or not isinstance(evidence_sentence, str):
                continue
            
            # 在消息列表中查找包含该证据的消息
            matched_message = None
            matched_message_idx = -1
            best_similarity = 0.0
            
            for msg_idx, message in enumerate(messages):
                content = str(message.get("content", "")).strip()
                if not content:
                    continue
                
                # 计算相似度
                similarity = self._calculate_text_similarity(evidence_sentence, content)
                if similarity > best_similarity and similarity > 0.3:  # 需要一定的相似度阈值
                    best_similarity = similarity
                    matched_message = message
                    matched_message_idx = msg_idx
            
            # 构建证据对象
            if matched_message:
                # 找到匹配的消息，使用实际消息信息
                user_type = matched_message.get("user_type", "")
                name = matched_message.get("name", "")
                create_time = matched_message.get("create_time", "")
                oper = matched_message.get("oper", False)
                
                # 确定角色显示名称
                if user_type == "system":
                    role = "系统"
                elif oper:
                    role = "客服"
                else:
                    role = "客户"
                
                if name:
                    role_display = f"{role}({name})"
                else:
                    role_display = role
                
                # 构建消息显示格式
                message_content = matched_message.get("content", "")
                if create_time:
                    conversation_context = f"[{create_time}] {role_display}: {message_content}"
                else:
                    conversation_context = f"{role_display}: {message_content}"
                
                # 高亮证据内容
                highlighted_context = conversation_context.replace(evidence_sentence, f"【{evidence_sentence}】")
                
                # 计算匹配位置
                match_start_pos = message_content.find(evidence_sentence)
                match_end_pos = match_start_pos + len(evidence_sentence) if match_start_pos >= 0 else 0
            else:
                # 没有找到匹配消息，创建虚拟证据对象
                logger.warning(f"LLM证据句子未在消息中找到匹配: {evidence_sentence}")
                user_type = "SYSTEM"
                name = "LLM分析"
                create_time = None
                role_display = "LLM分析"
                message_content = evidence_sentence
                conversation_context = f"LLM分析发现: {evidence_sentence}"
                highlighted_context = f"LLM分析发现: 【{evidence_sentence}】"
                matched_message_idx = -1
                match_start_pos = 0
                match_end_pos = len(evidence_sentence)
            
            # 处理evasion_types可能是数组或字符串的情况
            evasion_types = llm_analysis.get("evasion_types", "LLM识别")
            if isinstance(evasion_types, list):
                evasion_type_str = evasion_types[0] if evasion_types else "LLM识别"
            else:
                evasion_type_str = str(evasion_types) if evasion_types else "LLM识别"
            
            # 创建标准格式的证据对象
            evidence_entry = {
                "rule_type": "llm_analysis",  # LLM分析类型
                "rule_name": evasion_type_str,  # LLM识别的类别
                "category": evasion_type_str,
                "matched_keyword": None,  # LLM分析不是基于关键词匹配
                "matched_pattern": None,  # LLM分析不是基于正则模式
                "matched_text": evidence_sentence,  # LLM识别的证据文本
                "message_content": message_content,  # 原始消息内容
                "conversation_context": conversation_context,  # 完整消息显示格式
                "highlighted_context": highlighted_context,  # 高亮后的消息显示
                "config_id": None,  # LLM分析没有配置ID
                "message_index": matched_message_idx,  # 消息索引
                "message_id": matched_message.get("id") if matched_message else None,
                "user_type": user_type,
                "user_name": name,
                "create_time": create_time,
                "match_start_pos": match_start_pos,
                "match_end_pos": match_end_pos,
                "evidence_timestamp": datetime.now().isoformat(),
                # LLM分析信息
                "llm_analysis": {
                    "llm_confirmed": True,  # LLM分析直接确认
                    "llm_risk_assessment": llm_analysis.get("risk_level", "unknown"),
                    "llm_analysis_reason": f"LLM直接识别此内容属于{evasion_type_str}行为",
                    "llm_match_score": round(best_similarity, 3) if matched_message else 1.0,
                    "llm_evidence_match": evidence_sentence,
                    "llm_suggestion": self._extract_relevant_suggestion(
                        llm_analysis.get("improvement_suggestions", []), 
                        evasion_type_str
                    ),
                    "regex_matched": False,  # 不是正则匹配
                    "llm_overridden": False,  # 不是覆盖结果
                    "confidence_explanation": f"LLM直接分析识别出 '{evasion_type_str}' 类型的证据"
                },
                "analysis_timestamp": datetime.now().isoformat(),
                "evidence_status": "llm_identified"  # LLM直接识别的证据
            }
            
            evidence_objects.append(evidence_entry)
        
        logger.info(f"从LLM分析结果创建了 {len(evidence_objects)} 个结构化证据对象")
        return evidence_objects
    
    def _calculate_text_similarity(self, text1: str, text2: str) -> float:
        """计算两个文本的相似度（简单实现）"""
        if not text1 or not text2:
            return 0.0
        
        # 检查text1是否是text2的子字符串
        if text1 in text2:
            return 1.0
        
        # 检查text2是否是text1的子字符串  
        if text2 in text1:
            return len(text2) / len(text1)
        
        # 计算词级别的重叠度
        words1 = set(text1.split())
        words2 = set(text2.split())
        
        if not words1 or not words2:
            return 0.0
        
        intersection = len(words1.intersection(words2))
        union = len(words1.union(words2))
        
        return intersection / union if union > 0 else 0.0
    
    def _calculate_evidence_similarity(self, message_content: str, conversation_context: str, llm_sentence: str) -> float:
        """计算证据与LLM识别句子的相似度"""
        if not message_content or not llm_sentence:
            return 0.0
        
        # 清理文本
        message_clean = message_content.strip().lower()
        context_clean = conversation_context.strip().lower()
        llm_clean = llm_sentence.strip().lower()
        
        # 1. 完全包含关系
        if message_clean in llm_clean or llm_clean in message_clean:
            return 1.0
        
        if context_clean in llm_clean or llm_clean in context_clean:
            return 0.9
        
        # 2. 关键词重叠度计算
        message_words = set(message_clean.split())
        llm_words = set(llm_clean.split())
        
        if not message_words or not llm_words:
            return 0.0
        
        intersection = message_words.intersection(llm_words)
        union = message_words.union(llm_words)
        
        jaccard_score = len(intersection) / len(union) if union else 0.0
        
        # 3. 长度相似度调整
        length_ratio = min(len(message_clean), len(llm_clean)) / max(len(message_clean), len(llm_clean))
        
        # 综合评分
        final_score = jaccard_score * 0.7 + length_ratio * 0.3
        
        return min(final_score, 1.0)
    
    def _extract_relevant_suggestion(self, suggestions: List[str], category: str) -> str:
        """从LLM建议中提取与特定类别相关的建议"""
        if not suggestions or not category:
            return ""
        
        # 查找包含该类别关键词的建议
        for suggestion in suggestions:
            if category in suggestion:
                return suggestion
        
        # 如果没有找到特定建议，返回第一个通用建议
        return suggestions[0] if suggestions else ""
    
    def _enhance_low_risk_evidence(
        self, 
        detailed_evidence: List[Dict[str, Any]], 
        llm_analysis: Dict[str, Any],
        keyword_result: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """增强低风险证据信息，记录LLM的低风险评估"""
        
        enhanced_evidence = []
        
        for evidence in detailed_evidence:
            enhanced_evidence_item = evidence.copy()
            
            # 获取现有的llm_analysis字段并更新
            current_llm_analysis = enhanced_evidence_item.get("llm_analysis", {})
            
            # 🔥 低风险情况的特殊LLM分析信息
            low_risk_analysis = {
                "llm_confirmed": False,  # LLM不确认此证据有问题
                "llm_risk_assessment": llm_analysis.get("risk_level", "low"),  # LLM评估的实际风险级别
                "llm_analysis_reason": llm_analysis.get("low_risk_reason", "LLM判定此内容为正常对话，不构成问题行为"),
                "llm_match_score": 0.0,  # 匹配度设为0
                "llm_evidence_match": None,  # 无匹配的LLM证据
                "llm_suggestion": "此内容经LLM分析认为是正常业务对话，无需改进",
                "regex_matched": current_llm_analysis.get("regex_matched", True),  # 标记正则匹配成功
                "llm_overridden": True,  # 标记LLM覆盖了正则判定
                "confidence_explanation": f"正则匹配命中 '{evidence.get('category')}' 分类，但LLM分析认为是误报或正常情况"
            }
            
            # 更新证据中的LLM分析信息
            enhanced_evidence_item["llm_analysis"] = low_risk_analysis
            enhanced_evidence_item["analysis_timestamp"] = datetime.now().isoformat()
            
            # 更新证据状态标记
            enhanced_evidence_item["evidence_status"] = "regex_hit_llm_low_risk"  # 正则命中但LLM低风险
            
            enhanced_evidence.append(enhanced_evidence_item)
        
        return enhanced_evidence
    
    def _build_enhanced_analysis_note(self, analysis_result: Dict[str, Any]) -> str:
        """构建增强的分析备注，包含详细证据信息，确保长度不超出数据库限制"""
        notes = []
        max_length = 1500  # 设置最大长度限制，为数据库字段预留缓冲空间
        
        # 基本分析信息（必要信息，优先级最高）
        risk_level = analysis_result.get("risk_level", "unknown")
        confidence = analysis_result.get("confidence_score", 0.0)
        evasion_type = analysis_result.get("evasion_types", "")
        
        notes.append(f"风险级别: {risk_level}, 置信度: {confidence:.3f}")
        
        if evasion_type:
            notes.append(f"问题类型: {evasion_type}")
        
        # 显示匹配的关键词类别信息
        matched_categories = analysis_result.get("matched_categories", [])
        if matched_categories and isinstance(matched_categories, list):
            categories_str = ', '.join(matched_categories[:5])  # 最多显示5个类别
            if len(matched_categories) > 5:
                categories_str += f" 等{len(matched_categories)}个类别"
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
                    # 处理新的结构化证据格式
                    if isinstance(evidence, dict):
                        # 构建证据摘要：规则类型 + 匹配内容
                        rule_type = evidence.get("rule_type", "未知")
                        matched_text = evidence.get("matched_text", "")
                        category = evidence.get("category", "")
                        
                        if rule_type == "keyword":
                            evidence_summary = f"[{category}-关键词] {matched_text}"
                        elif rule_type == "pattern":
                            evidence_summary = f"[{category}-正则] {matched_text}"
                        else:
                            evidence_summary = f"[{category}] {matched_text}"
                    else:
                        # 兼容旧格式（字符串）
                        evidence_summary = str(evidence)
                    
                    evidence_length = min(50, available_space // 2)  # 每条证据最多50字符
                    if len(evidence_summary) > evidence_length:
                        evidence_summary = evidence_summary[:evidence_length] + "..."
                    evidence_preview.append(f"{i+1}. {evidence_summary}")
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
    "evasion_types": string,
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
            messages = conversation_data.get("messages", [])
            
            if not conversation_text.strip() or not messages:
                logger.warning(f"⚠️ 工单 {work_id} 对话内容或消息列表为空")
                return {
                    "success": False,
                    "error": "对话内容或消息列表为空"
                }
            
            logger.debug(f"📝 工单 {work_id} 对话文本长度: {len(conversation_text)} 字符，消息数量: {len(messages)} 条")
            
            # 1. 关键词粗筛
            logger.debug(f"🔍 工单 {work_id} 开始关键词粗筛...")
            keyword_result = self.keyword_screening(conversation_text, db)
            logger.info(f"📊 工单 {work_id} 关键词筛选结果: 可疑={keyword_result['is_suspicious']}, 置信度={keyword_result['confidence_score']:.3f}")
            
            # 2. 🔥 新流程：正则匹配 + LLM深度分析
            if keyword_result["is_suspicious"] and keyword_result["confidence_score"] >= 0.3:
                logger.info(f"🎯 工单 {work_id} 命中关键词类别: {keyword_result['matched_categories']}，置信度: {keyword_result['confidence_score']:.3f}")
                
                # 🔥 第一步：收集正则匹配的证据
                logger.debug(f"📋 工单 {work_id} 开始收集正则匹配证据...")
                matched_risk_levels = []
                matched_keywords = []
                detailed_evidence = []
                
                for category, details in keyword_result["matched_details"].items():
                    if not details.get("excluded", False):
                        matched_risk_levels.append(details.get("risk_level", "medium"))
                        
                        # 收集匹配关键词的具体句子和上下文
                        if details.get("keywords"):
                            matched_keywords.extend(details["keywords"])
                            for keyword in details["keywords"]:
                                sentences = self._extract_evidence_sentences(messages, keyword, category)
                                detailed_evidence.extend(sentences)
                        
                        # 收集正则模式匹配的具体内容
                        if details.get("patterns"):
                            pattern_matches = self._extract_pattern_evidence(messages, details["patterns"], category)
                            detailed_evidence.extend(pattern_matches)
                
                logger.info(f"📊 工单 {work_id} 正则匹配结果: 收集到 {len(detailed_evidence)} 条证据")
                
                # 🔥 第二步：调用LLM进行深度分析
                logger.debug(f"🤖 工单 {work_id} 开始LLM深度分析...")
                
                # 构建证据上下文
                evidence_context = self._build_evidence_context(detailed_evidence, keyword_result)
                
                # 🔥 新增：根据当前匹配的分类获取专门的few-shot示例
                matched_categories = keyword_result.get("matched_categories", [])
                # 将中文分类名映射为category_key
                category_key_mapping = {
                    "紧急催促": "urgent_urging",
                    "投诉纠纷": "complaint_dispute", 
                    "推卸责任": "responsibility_evasion",
                    "拖延处理": "delay_handling",
                    "不当用词": "inappropriate_wording"
                }
                
                target_category_keys = []
                for category in matched_categories:
                    if category in category_key_mapping:
                        target_category_keys.append(category_key_mapping[category])
                
                logger.info(f"工单 {work_id} 匹配分类: {matched_categories} -> category_keys: {target_category_keys}")
                
                few_shot_examples = self._get_category_few_shot_examples(db, target_category_keys)
                
                # 调用LLM进行分析（传入针对性few-shot示例）
                llm_result = await self.llm_provider.analyze_responsibility_evasion(
                    conversation_text, 
                    context=evidence_context,
                    few_shot_examples=few_shot_examples
                )
                
                if llm_result["success"]:
                    logger.info(f"✅ 工单 {work_id} LLM分析成功")
                    llm_analysis = llm_result["analysis"]
                    
                    # 🔥 第三步：融合正则匹配和LLM分析结果
                    final_result = self._merge_regex_and_llm_results(
                        keyword_result, detailed_evidence, llm_analysis, conversation_data
                    )
                    
                    # 🔥 新增：处理低风险但关键词命中的情况
                    if final_result.get("risk_level", "low") == "low" and len(detailed_evidence) > 0:
                        logger.info(f"⚠️ 工单 {work_id} 关键词命中但LLM判定为低风险，记录详细评估")
                        
                        # 增强证据信息，记录低风险原因
                        enhanced_evidence = self._enhance_low_risk_evidence(
                            detailed_evidence, llm_analysis, keyword_result
                        )
                        final_result["evidence_sentences"] = enhanced_evidence
                        final_result["detailed_evidence"] = enhanced_evidence
                        
                        final_result["analysis_note"] = f"正则匹配发现 {len(detailed_evidence)} 条证据，但LLM评估为低风险。原因: {llm_analysis.get('low_risk_reason', 'LLM判定为正常对话')}"
                    else:
                        final_result["analysis_note"] = f"正则匹配发现 {len(detailed_evidence)} 条证据，LLM确认风险级别: {final_result['risk_level']}，置信度: {final_result['confidence_score']:.3f}"
                    
                    # 添加分析元信息
                    final_result.update({
                        "llm_analysis": True,
                        "keyword_screening": keyword_result,
                        "llm_raw_response": llm_result.get("raw_response"),
                        "matched_categories": matched_categories,
                        "few_shot_categories": target_category_keys
                    })
                    
                    logger.info(f"🎯 工单 {work_id} 最终分析结果: 风险级别={final_result['risk_level']}, 推卸责任={final_result.get('has_evasion', False)}, LLM置信度={final_result['confidence_score']:.3f}")
                    
                    return {
                        "success": True,
                        "work_id": work_id,
                        "analysis_result": final_result
                    }
                else:
                    # LLM分析失败，降级到基于正则的判定
                    logger.warning(f"⚠️ 工单 {work_id} LLM分析失败: {llm_result.get('error', '未知错误')}，降级到正则判定")
                    
                    # 确定风险级别（基于正则匹配）
                    if "high" in matched_risk_levels:
                        final_risk_level = "high"
                    elif "medium" in matched_risk_levels:
                        final_risk_level = "medium"
                    else:
                        final_risk_level = "medium"
                    
                    has_evasion_behavior = any(
                        "推卸责任" in category for category in keyword_result["matched_categories"]
                    )
                    
                    fallback_result = {
                        "has_evasion": has_evasion_behavior,
                        "risk_level": final_risk_level,
                        "confidence_score": min(keyword_result["confidence_score"], 1.0),
                        "evasion_types": keyword_result["matched_categories"][0] if keyword_result["matched_categories"] else "",
                        "evidence_sentences": detailed_evidence,
                        "detailed_evidence": detailed_evidence,
                        "improvement_suggestions": [f"检测到 {', '.join(keyword_result['matched_categories'])} 相关行为，建议加强服务质量管控和人员培训"],
                        "sentiment": "negative",
                        "sentiment_intensity": 0.7,
                        "keyword_screening": keyword_result,
                        "llm_analysis": False,
                        "analysis_note": f"LLM分析失败，基于正则匹配判定为{final_risk_level}风险，匹配类别: {', '.join(keyword_result['matched_categories'])}",
                        # 补充会话信息
                        "session_start_time": conversation_data.get("session_info", {}).get("start_time"),
                        "session_end_time": conversation_data.get("session_info", {}).get("end_time"),
                        "total_comments": conversation_data.get("total_messages", 0),
                        "customer_comments": conversation_data.get("customer_messages", 0),
                        "service_comments": conversation_data.get("service_messages", 0),
                        "conversation_text": conversation_text,
                        "matched_keywords": matched_keywords,
                        "evidence_count": len(detailed_evidence)
                    }
                    
                    return {
                        "success": True,
                        "work_id": work_id,
                        "analysis_result": fallback_result
                    }
            else:
                logger.info(f"⏭️ 工单 {work_id} 未命中关键词阈值（置信度: {keyword_result['confidence_score']:.3f}），判定为低风险，不保存")
                
                # 🔥 新优化：低风险直接返回，不保存到数据库
                low_risk_result = {
                    "has_evasion": False,
                    "risk_level": "low",
                    "confidence_score": keyword_result["confidence_score"],
                    "evasion_types": "",
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
            
            # 🔥 注意：当前采用正则匹配 + LLM分析的策略
            # 未命中正则匹配阈值的对话判定为低风险
            # 上面的逻辑已经处理了所有情况
            
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
