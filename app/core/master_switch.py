"""
调度器总开关独立管理模块
完全独立于调度器运行状态，支持调度器停止时也能切换开关状态
"""
import os
import logging
from datetime import datetime
from typing import Dict, Any
from pathlib import Path

logger = logging.getLogger(__name__)


class SchedulerMasterSwitch:
    """调度器总开关管理器 - 独立于调度器运行状态"""
    
    def __init__(self, switch_file_path: str = None):
        # 使用项目根目录下的config目录存储状态文件
        if switch_file_path is None:
            project_root = Path(__file__).parent.parent.parent
            config_dir = project_root / "config"
            config_dir.mkdir(exist_ok=True)
            self.switch_file = config_dir / "scheduler_master_switch.state"
        else:
            self.switch_file = Path(switch_file_path)
        
        self._enabled = True  # 默认开启
        self._load_state()
    
    def _load_state(self):
        """从持久化文件加载总开关状态"""
        try:
            if self.switch_file.exists():
                with open(self.switch_file, 'r', encoding='utf-8') as f:
                    state = f.read().strip().lower()
                    self._enabled = state == 'true'
                    logger.info(f"📂 加载调度器总开关状态: {'开启' if self._enabled else '关闭'}")
            else:
                # 首次运行，创建默认状态文件
                self._enabled = True
                self._save_state()
                logger.info("🔧 初始化调度器总开关为开启状态")
        except Exception as e:
            logger.warning(f"⚠️ 加载调度器总开关状态失败，使用默认开启状态: {e}")
            self._enabled = True
    
    def _save_state(self):
        """保存总开关状态到持久化文件"""
        try:
            # 确保目录存在
            self.switch_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(self.switch_file, 'w', encoding='utf-8') as f:
                f.write('true' if self._enabled else 'false')
            
            logger.debug(f"💾 保存调度器总开关状态: {'开启' if self._enabled else '关闭'}")
        except Exception as e:
            logger.error(f"❌ 保存调度器总开关状态失败: {e}")
            raise
    
    @property
    def enabled(self) -> bool:
        """获取总开关状态"""
        return self._enabled
    
    def set_enabled(self, enabled: bool) -> bool:
        """设置总开关状态"""
        try:
            old_state = self._enabled
            self._enabled = enabled
            self._save_state()
            
            state_text = "开启" if enabled else "关闭"
            old_state_text = "开启" if old_state else "关闭"
            
            if old_state != enabled:
                logger.info(f"🔄 调度器总开关状态变更: {old_state_text} → {state_text}")
            else:
                logger.debug(f"✅ 调度器总开关状态保持不变: {state_text}")
            
            return True
        except Exception as e:
            logger.error(f"❌ 设置调度器总开关失败: {e}")
            # 回滚状态
            self._enabled = old_state if 'old_state' in locals() else True
            return False
    
    def enable(self) -> bool:
        """开启总开关"""
        return self.set_enabled(True)
    
    def disable(self) -> bool:
        """关闭总开关"""
        return self.set_enabled(False)
    
    def toggle(self) -> Dict[str, Any]:
        """切换总开关状态"""
        old_state = self._enabled
        new_state = not old_state
        success = self.set_enabled(new_state)
        
        return {
            "success": success,
            "previous_state": old_state,
            "new_state": new_state if success else old_state,
            "action": "开启" if new_state else "关闭"
        }
    
    def get_status(self) -> Dict[str, Any]:
        """获取详细的总开关状态信息"""
        return {
            "enabled": self._enabled,
            "status_text": "开启" if self._enabled else "关闭",
            "description": "调度器总开关控制是否执行任务。关闭时，即使任务生效也不会执行。",
            "switch_file": str(self.switch_file),
            "last_modified": datetime.now().isoformat(),
            "independent": True,  # 标记这是独立的状态管理
            "supports_offline": True  # 支持调度器离线时操作
        }
    
    def reload(self):
        """重新加载状态（用于外部文件修改后刷新）"""
        logger.info("🔄 重新加载调度器总开关状态")
        self._load_state()


# 全局独立的总开关管理器实例
master_switch = SchedulerMasterSwitch()
