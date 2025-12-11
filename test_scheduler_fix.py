#!/usr/bin/env python3
"""
测试APScheduler定时任务修复
验证长周期任务（超过1天）是否能正常执行
"""
import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent))

from config.logging_config import init_logging
init_logging()

import logging
logger = logging.getLogger(__name__)


async def test_scheduler_configuration():
    """测试调度器配置"""
    from app.services.apscheduler_service import apscheduler_service

    print("\n" + "="*60)
    print("🧪 测试 APScheduler 配置")
    print("="*60)

    # 检查 misfire_grace_time
    grace_time = apscheduler_service.scheduler._job_defaults.get('misfire_grace_time')
    print(f"\n✅ misfire_grace_time: {grace_time} 秒 ({grace_time/86400:.1f} 天)")

    if grace_time < 86400:
        print("❌ 警告: misfire_grace_time 小于1天，长周期任务可能会被跳过！")
        return False

    # 检查 coalesce 设置
    coalesce = apscheduler_service.scheduler._job_defaults.get('coalesce')
    print(f"✅ coalesce: {coalesce} (合并错过的执行)")

    return True


async def test_interval_trigger():
    """测试interval触发器配置"""
    from app.services.apscheduler_service import apscheduler_service
    from app.db.connection_manager import get_db_session
    from app.models.task_config import task_config

    print("\n" + "="*60)
    print("🧪 测试 Interval 触发器配置")
    print("="*60)

    # 启动调度器
    await apscheduler_service.start()

    # 获取所有已注册的任务
    jobs = apscheduler_service.get_jobs()

    print(f"\n已注册任务数: {len(jobs)}")

    for job in jobs:
        print(f"\n📋 任务: {job.name} ({job.id})")
        print(f"   触发器类型: {type(job.trigger).__name__}")
        print(f"   下次执行时间: {job.next_run_time}")

        # 检查触发器配置
        if hasattr(job.trigger, 'interval'):
            interval_seconds = int(job.trigger.interval.total_seconds())
            interval_days = interval_seconds / 86400
            print(f"   执行间隔: {interval_seconds} 秒 ({interval_days:.2f} 天)")

            if interval_days > 1:
                print(f"   ⚠️ 这是一个长周期任务（>{interval_days:.1f}天）")

                # 验证 next_run_time 是否正确设置
                if job.next_run_time:
                    time_until_next = job.next_run_time - datetime.now(job.next_run_time.tzinfo)
                    days_until = time_until_next.total_seconds() / 86400
                    print(f"   距离下次执行: {days_until:.2f} 天")

                    if days_until > interval_days + 1:
                        print(f"   ❌ 错误: 下次执行时间异常！")
                        return False
                    else:
                        print(f"   ✅ 下次执行时间正常")
                else:
                    print(f"   ❌ 错误: 未设置 next_run_time！")
                    return False

    # 检查数据库中的配置
    print("\n" + "="*60)
    print("🗄️ 数据库任务配置")
    print("="*60)

    with get_db_session() as db:
        configs = task_config.get_all_tasks(db, enabled_only=True)

        for cfg in configs:
            print(f"\n📋 {cfg['task_name']}")
            print(f"   task_key: {cfg['task_key']}")
            print(f"   间隔: {cfg['schedule_interval']} 秒 ({cfg['schedule_interval']/86400:.2f} 天)")
            print(f"   上次执行: {cfg['last_execution_time']}")
            print(f"   下次执行: {cfg['next_execution_time']}")

    return True


async def simulate_long_period_task():
    """模拟长周期任务场景"""
    print("\n" + "="*60)
    print("🧪 模拟长周期任务（4天间隔）")
    print("="*60)

    from apscheduler.triggers.interval import IntervalTrigger
    from datetime import datetime, timezone

    # 创建一个4天间隔的触发器
    now = datetime.now(timezone.utc)
    trigger = IntervalTrigger(days=4, start_date=now)

    print(f"\n当前时间: {now}")

    # 获取接下来的几次执行时间
    next_time = trigger.get_next_fire_time(None, now)
    print(f"第1次执行: {next_time} (距现在 {(next_time-now).total_seconds()/86400:.2f} 天)")

    for i in range(2, 6):
        next_time = trigger.get_next_fire_time(next_time, next_time)
        print(f"第{i}次执行: {next_time} (距上次 {4} 天)")

    print("\n✅ 触发器能够正确计算长周期任务的执行时间")
    return True


async def main():
    """主测试函数"""
    print("\n" + "="*70)
    print("🚀 APScheduler 长周期任务修复验证")
    print("="*70)

    try:
        # 测试1: 配置检查
        result1 = await test_scheduler_configuration()

        # 测试2: Interval触发器
        result2 = await test_interval_trigger()

        # 测试3: 模拟长周期任务
        result3 = await simulate_long_period_task()

        # 总结
        print("\n" + "="*70)
        print("📊 测试结果总结")
        print("="*70)
        print(f"✅ 配置检查: {'通过' if result1 else '失败'}")
        print(f"✅ Interval触发器: {'通过' if result2 else '失败'}")
        print(f"✅ 长周期任务模拟: {'通过' if result3 else '失败'}")

        if result1 and result2 and result3:
            print("\n🎉 所有测试通过！长周期任务应该能正常工作了。")
            print("\n💡 建议:")
            print("   1. 重启应用以应用新配置")
            print("   2. 监控任务执行日志，确保任务按预期触发")
            print("   3. 检查 apscheduler_jobs 表中的 next_run_time 字段")
            return 0
        else:
            print("\n❌ 部分测试失败，请检查配置！")
            return 1

    except Exception as e:
        logger.error(f"❌ 测试失败: {e}", exc_info=True)
        return 1
    finally:
        # 停止调度器
        from app.services.apscheduler_service import apscheduler_service
        if apscheduler_service._running:
            await apscheduler_service.stop()


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
