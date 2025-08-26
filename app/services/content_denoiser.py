"""
内容去噪服务
用于过滤工单评论中的无效数据和正常操作记录
"""
import re
import logging
import time
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class ContentDenoiser:
    """内容去噪器，用于过滤工单评论中的噪音数据"""
    
    def __init__(self):
        """初始化去噪器"""
        self.normal_operation_patterns = self._init_normal_operation_patterns()
        self.invalid_data_patterns = self._init_invalid_data_patterns()
        self.system_keywords = self._init_system_keywords()
    
    def _init_normal_operation_patterns(self) -> List[Dict[str, Any]]:
        """初始化正常操作模式"""
        return [
            {
                "name": "工单关闭操作",
                "pattern": r"【完结】.*?关闭工单",
                "description": "工单正常关闭操作",
                "action": "filter_out"
            },
            {
                "name": "自动完结工单",
                "pattern": r"【自动完结工单】.*?(已撤单|订单已派单|已完成|已关闭|自动关闭|系统关闭)",
                "description": "系统自动完结工单操作",
                "action": "filter_out"
            },
            {
                "name": "自动完结通知",
                "pattern": r"【自动完结工单】.*",
                "description": "系统自动完结通知",
                "action": "filter_out"
            },
            {
                "name": "系统状态更新",
                "pattern": r"【.*?】.*?(状态|更新|变更)",
                "description": "系统自动状态更新",
                "action": "filter_out"
            },
            {
                "name": "工单创建通知",
                "pattern": r"工单.*?(创建|提交|生成)",
                "description": "工单创建系统通知",
                "action": "filter_out"
            },
            {
                "name": "自动分配通知",
                "pattern": r"(自动分配|系统分配|已分配给)",
                "description": "系统自动分配通知",
                "action": "filter_out"
            },
            {
                "name": "催单提醒",
                "pattern": r"(催单|提醒|超时)",
                "description": "系统催单提醒",
                "action": "filter_out"
            },
            {
                "name": "订单状态变更",
                "pattern": r".*?(已撤单|订单已派单|已派单|订单状态|派单成功|撤单成功)",
                "description": "订单状态自动变更通知",
                "action": "filter_out"
            }
        ]
    
    def _init_invalid_data_patterns(self) -> List[Dict[str, Any]]:
        """初始化无效数据模式"""
        return [
            {
                "name": "重复数字",
                "pattern": r"^(\d)\1{2,}$",  # 匹配 111, 222, 1111 等
                "description": "重复的数字串",
                "action": "filter_out"
            },
            {
                "name": "单字符重复",
                "pattern": r"^(.)\1{2,}$",  # 匹配 aaa, BBB 等
                "description": "重复的单字符",
                "action": "filter_out"
            },
            {
                "name": "纯数字短内容",
                "pattern": r"^[\d\s]{1,5}$",  # 1-5位纯数字
                "description": "过短的纯数字内容",
                "action": "filter_out"
            },
            {
                "name": "测试内容",
                "pattern": r"^(test|测试|TEST|Test|\.\.\.|。。。)$",
                "description": "明显的测试内容",
                "action": "filter_out"
            },
            {
                "name": "空白或符号",
                "pattern": r"^[\s\-_=+\*\.]{1,10}$",
                "description": "只含空白字符或简单符号",
                "action": "filter_out"
            },
            {
                "name": "意义不明的短内容",
                "pattern": r"^[a-zA-Z]{1,3}$",  # 1-3个字母
                "description": "过短无意义字母",
                "action": "filter_out"
            }
        ]
    
    def _init_system_keywords(self) -> List[str]:
        """初始化系统关键词"""
        return [
            "系统", "自动", "通知", "提醒", "分配", "转派",
            "【完结】", "【处理中】", "【待处理】", "【已分配】", "【自动完结工单】",
            "工单创建", "工单关闭", "状态变更", "优先级调整",
            "已撤单", "订单已派单", "派单成功", "撤单成功", "订单状态"
        ]
    
    def is_normal_operation(self, content: str, user_type: str = None, name: str = None) -> Tuple[bool, str]:
        """
        判断是否为正常操作记录
        
        Args:
            content: 评论内容
            user_type: 用户类型 (system, service, customer)
            name: 用户名称
            
        Returns:
            (是否为正常操作, 匹配的规则描述)
        """
        if not content or not isinstance(content, str):
            return False, ""
        
        content = content.strip()
        
        # 1. 检查是否为系统用户的操作
        if user_type == "system":
            return True, "系统用户操作"
        
        # 2. 检查是否包含系统关键词
        for keyword in self.system_keywords:
            if keyword in content:
                return True, f"包含系统关键词: {keyword}"
        
        # 3. 检查正常操作模式
        for pattern_config in self.normal_operation_patterns:
            if re.search(pattern_config["pattern"], content, re.IGNORECASE):
                return True, pattern_config["description"]
        
        # 4. 检查特定格式的正常操作
        # 工单客服的标准操作格式
        if name and "工单客服" in name:
            if re.match(r"^\d+$", content) and len(content) <= 10:
                return True, "工单客服数字标记"
        
        return False, ""
    
    def is_invalid_data(self, content: str) -> Tuple[bool, str]:
        """
        判断是否为无效数据
        
        Args:
            content: 评论内容
            
        Returns:
            (是否为无效数据, 匹配的规则描述)
        """
        if not content or not isinstance(content, str):
            return True, "空内容"
        
        content = content.strip()
        
        # 检查是否为空或只有空白字符
        if not content:
            return True, "空白内容"
        
        # 检查无效数据模式
        for pattern_config in self.invalid_data_patterns:
            if re.match(pattern_config["pattern"], content, re.IGNORECASE):
                return True, pattern_config["description"]
        
        # 检查内容长度（太短可能无意义）
        if len(content) <= 2 and not re.match(r'^[\u4e00-\u9fff]+$', content):  # 非中文且过短
            return True, "内容过短且非中文"
        
        return False, ""
    
    def should_filter_comment(self, comment: Dict[str, Any]) -> Tuple[bool, str]:
        """
        判断评论是否应该被过滤
        
        Args:
            comment: 评论数据字典，包含content, user_type, name等字段
            
        Returns:
            (是否应该过滤, 过滤原因)
        """
        content = str(comment.get("content", "")).strip()
        user_type = comment.get("user_type", "")
        name = comment.get("name", "")
        
        # 1. 检查是否为正常操作
        is_normal, normal_reason = self.is_normal_operation(content, user_type, name)
        if is_normal:
            return True, f"正常操作: {normal_reason}"
        
        # 2. 检查是否为无效数据
        is_invalid, invalid_reason = self.is_invalid_data(content)
        if is_invalid:
            return True, f"无效数据: {invalid_reason}"
        
        return False, ""
    
    def filter_comments(self, comments: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        过滤评论列表，去除噪音数据
        
        Args:
            comments: 评论列表
            
        Returns:
            {
                "filtered_comments": 过滤后的评论列表,
                "original_count": 原始评论数量,
                "filtered_count": 过滤后评论数量,
                "removed_count": 被移除的评论数量,
                "filter_statistics": 过滤统计信息
            }
        """
        if not comments:
            return {
                "filtered_comments": [],
                "original_count": 0,
                "filtered_count": 0,
                "removed_count": 0,
                "filter_statistics": {}
            }
        
        logger.info(f"🔍 开始过滤评论，原始数量: {len(comments)}")
        
        filtered_comments = []
        removed_comments = []
        filter_reasons = {}
        
        for i, comment in enumerate(comments):
            should_filter, reason = self.should_filter_comment(comment)
            
            if should_filter:
                removed_comments.append({
                    "index": i,
                    "comment": comment,
                    "reason": reason
                })
                
                # 统计过滤原因
                filter_reasons[reason] = filter_reasons.get(reason, 0) + 1
                
                logger.debug(f"⚠️ 过滤评论 #{i}: {comment.get('content', '')[:50]}... - 原因: {reason}")
            else:
                filtered_comments.append(comment)
        
        result = {
            "filtered_comments": filtered_comments,
            "original_count": len(comments),
            "filtered_count": len(filtered_comments),
            "removed_count": len(removed_comments),
            "filter_statistics": {
                "filter_reasons": filter_reasons,
                "removed_details": removed_comments[:10]  # 只保留前10个被移除的详情
            }
        }
        
        logger.info(f"✅ 评论过滤完成:")
        logger.info(f"  📥 原始数量: {result['original_count']}")
        logger.info(f"  📤 过滤后数量: {result['filtered_count']}")
        logger.info(f"  🗑️ 移除数量: {result['removed_count']}")
        logger.info(f"  📊 过滤率: {(result['removed_count'] / result['original_count'] * 100):.1f}%" if result['original_count'] > 0 else "0%")
        
        if filter_reasons:
            logger.info("🔍 过滤原因统计:")
            for reason, count in filter_reasons.items():
                logger.info(f"  - {reason}: {count} 条")
        
        return result
    
    def filter_comments_with_record(
        self, 
        comments: List[Dict[str, Any]], 
        work_id: int,
        db: Optional[Session] = None,
        save_record: bool = True
    ) -> Dict[str, Any]:
        """
        过滤评论并保存去噪记录
        
        Args:
            comments: 评论列表
            work_id: 工单ID
            db: 数据库会话（可选）
            save_record: 是否保存去噪记录
            
        Returns:
            过滤结果和记录信息
        """
        # 执行正常的过滤
        filter_result = self.filter_comments(comments)
        
        # 如果需要保存记录且有数据库连接
        if save_record and db and work_id:
            try:
                from app.models.denoise import denoise_record_manager
                import time
                
                # 生成单独的批次ID
                batch_id = denoise_record_manager.generate_batch_id()
                
                # 创建批次记录
                denoise_record_manager.create_batch_record(db, batch_id, 1)
                
                # 保存工单去噪记录
                start_time = time.time()
                success = denoise_record_manager.save_work_order_denoise_record(
                    db, work_id, batch_id, filter_result
                )
                processing_time_ms = int((time.time() - start_time) * 1000)
                
                if success:
                    # 更新批次统计
                    batch_statistics = {
                        "total_work_orders": 1,
                        "total_original_comments": filter_result["original_count"],
                        "total_filtered_comments": filter_result["filtered_count"],
                        "total_removed_comments": filter_result["removed_count"],
                        "overall_filter_rate": (filter_result["removed_count"] / filter_result["original_count"] * 100) if filter_result["original_count"] > 0 else 0.0,
                        "filter_reasons": filter_result["filter_statistics"]["filter_reasons"],
                        "total_processing_time_ms": processing_time_ms
                    }
                    
                    denoise_record_manager.update_batch_statistics(
                        db, batch_id, batch_statistics, "COMPLETED"
                    )
                    
                    logger.info(f"✅ 保存工单 {work_id} 去噪记录成功，批次: {batch_id}")
                    
                    # 添加记录信息到结果中
                    filter_result["denoise_record"] = {
                        "batch_id": batch_id,
                        "saved": True,
                        "processing_time_ms": processing_time_ms
                    }
                else:
                    logger.warning(f"⚠️ 保存工单 {work_id} 去噪记录失败")
                    filter_result["denoise_record"] = {"saved": False, "error": "保存失败"}
                    
            except Exception as e:
                logger.error(f"❌ 保存工单 {work_id} 去噪记录异常: {e}")
                filter_result["denoise_record"] = {"saved": False, "error": str(e)}
        else:
            filter_result["denoise_record"] = {"saved": False, "reason": "未满足保存条件"}
        
        return filter_result
    
    def check_comment_quality(self, comment: Dict[str, Any]) -> Dict[str, Any]:
        """
        检查单条评论的质量
        
        Args:
            comment: 评论数据
            
        Returns:
            质量评估结果
        """
        content = str(comment.get("content", "")).strip()
        user_type = comment.get("user_type", "")
        name = comment.get("name", "")
        
        result = {
            "content": content,
            "length": len(content),
            "user_type": user_type,
            "name": name,
            "is_valid": True,
            "quality_score": 1.0,
            "issues": []
        }
        
        # 检查是否为正常操作
        is_normal, normal_reason = self.is_normal_operation(content, user_type, name)
        if is_normal:
            result["is_valid"] = False
            result["quality_score"] = 0.0
            result["issues"].append(f"正常操作: {normal_reason}")
            return result
        
        # 检查是否为无效数据
        is_invalid, invalid_reason = self.is_invalid_data(content)
        if is_invalid:
            result["is_valid"] = False
            result["quality_score"] = 0.0
            result["issues"].append(f"无效数据: {invalid_reason}")
            return result
        
        # 计算质量评分
        quality_score = 1.0
        
        # 长度评分
        if len(content) < 5:
            quality_score -= 0.3
            result["issues"].append("内容较短")
        elif len(content) < 10:
            quality_score -= 0.1
            result["issues"].append("内容偏短")
        
        # 字符多样性评分
        unique_chars = len(set(content))
        if unique_chars < 3:
            quality_score -= 0.3
            result["issues"].append("字符重复度高")
        
        # 是否包含有意义的信息
        if re.match(r'^[\d\s\-_=+\*\.]+$', content):
            quality_score -= 0.4
            result["issues"].append("主要为数字或符号")
        
        result["quality_score"] = max(0.0, quality_score)
        
        return result
    
    def batch_filter_work_orders(
        self, 
        work_orders: List[Dict[str, Any]], 
        db: Optional[Session] = None,
        save_records: bool = True
    ) -> Dict[str, Any]:
        """
        批量过滤工单的评论数据
        
        Args:
            work_orders: 工单列表，每个工单包含comments_data
            db: 数据库会话（可选，用于保存记录）
            save_records: 是否保存去噪记录到数据库
            
        Returns:
            批量过滤结果
        """
        start_time = time.time()
        logger.info(f"🔍 开始批量过滤 {len(work_orders)} 个工单的评论")
        
        # 导入记录管理器
        batch_id = None
        if save_records and db:
            from app.models.denoise import denoise_record_manager
            batch_id = denoise_record_manager.generate_batch_id()
            denoise_record_manager.create_batch_record(db, batch_id, len(work_orders))
            logger.info(f"🏷️ 创建批次记录: {batch_id}")
        
        total_original = 0
        total_filtered = 0
        total_removed = 0
        global_filter_reasons = {}
        
        processed_orders = []
        
        for order in work_orders:
            work_id = order.get("work_id", "未知")
            comments_data = order.get("comments_data", {})
            
            if not comments_data or "messages" not in comments_data:
                logger.debug(f"⏭️ 工单 {work_id} 无评论数据，跳过")
                processed_orders.append(order)
                continue
            
            # 过滤评论
            filter_result = self.filter_comments(comments_data["messages"])
            
            # 保存单个工单的去噪记录
            if save_records and db and batch_id:
                work_start_time = time.time()
                denoise_record_manager.save_work_order_denoise_record(
                    db, work_id, batch_id, filter_result
                )
                work_processing_time = int((time.time() - work_start_time) * 1000)
                logger.debug(f"💾 保存工单 {work_id} 去噪记录，耗时: {work_processing_time}ms")
            
            # 更新统计
            total_original += filter_result["original_count"]
            total_filtered += filter_result["filtered_count"]
            total_removed += filter_result["removed_count"]
            
            # 合并过滤原因统计
            for reason, count in filter_result["filter_statistics"]["filter_reasons"].items():
                global_filter_reasons[reason] = global_filter_reasons.get(reason, 0) + count
            
            # 更新工单数据
            updated_order = order.copy()
            if filter_result["filtered_count"] > 0:
                # 重新构建comments_data
                updated_comments_data = comments_data.copy()
                updated_comments_data["messages"] = filter_result["filtered_comments"]
                updated_comments_data["total_messages"] = filter_result["filtered_count"]
                
                # 重新构建对话文本
                from app.services.stage1_work_extraction import stage1_service
                updated_comments_data["conversation_text"] = stage1_service.build_conversation_text(
                    filter_result["filtered_comments"]
                )
                
                updated_order["comments_data"] = updated_comments_data
                updated_order["comment_count"] = filter_result["filtered_count"]
                updated_order["has_comments"] = filter_result["filtered_count"] > 0
            else:
                # 如果所有评论都被过滤掉了，标记为无评论
                updated_order["comments_data"] = None
                updated_order["comment_count"] = 0
                updated_order["has_comments"] = False
            
            # 添加过滤信息
            updated_order["denoise_info"] = {
                "original_comment_count": filter_result["original_count"],
                "filtered_comment_count": filter_result["filtered_count"],
                "removed_comment_count": filter_result["removed_count"],
                "filter_applied": True
            }
            
            processed_orders.append(updated_order)
            
            logger.debug(f"📋 工单 {work_id}: {filter_result['original_count']} -> {filter_result['filtered_count']} 条评论")
        
        # 计算总处理时间
        total_processing_time_ms = int((time.time() - start_time) * 1000)
        
        # 构建结果
        result = {
            "processed_orders": processed_orders,
            "total_work_orders": len(work_orders),
            "batch_id": batch_id,
            "statistics": {
                "total_original_comments": total_original,
                "total_filtered_comments": total_filtered,
                "total_removed_comments": total_removed,
                "overall_filter_rate": (total_removed / total_original * 100) if total_original > 0 else 0,
                "filter_reasons": global_filter_reasons,
                "total_processing_time_ms": total_processing_time_ms
            }
        }
        
        # 保存批次统计
        if save_records and db and batch_id:
            try:
                denoise_record_manager.update_batch_statistics(
                    db, batch_id, result["statistics"], "COMPLETED"
                )
                logger.info(f"✅ 保存批次统计: {batch_id}")
            except Exception as e:
                logger.error(f"❌ 保存批次统计失败: {e}")
                denoise_record_manager.update_batch_statistics(
                    db, batch_id, result["statistics"], "FAILED", str(e)
                )
        
        logger.info("🎉 批量过滤完成:")
        logger.info(f"  📋 处理工单数: {result['total_work_orders']}")
        logger.info(f"  📥 原始评论总数: {total_original}")
        logger.info(f"  📤 过滤后评论总数: {total_filtered}")
        logger.info(f"  🗑️ 移除评论总数: {total_removed}")
        logger.info(f"  📊 整体过滤率: {result['statistics']['overall_filter_rate']:.1f}%")
        
        return result


# 全局去噪器实例
content_denoiser = ContentDenoiser()
