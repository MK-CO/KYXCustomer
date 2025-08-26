"""
AI平台智能分析系统 - 主应用程序入口
"""
import os
import sys
import asyncio
import argparse
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import uvicorn

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 初始化日志配置（必须在其他导入之前）
from config.logging_config import init_logging
init_logging()

from config.settings import settings
from app.db.database import create_tables
from app.api.analysis import router as analysis_router
from app.api.system import router as system_router
from app.api.tasks import router as tasks_router
from app.api.auth import router as auth_router
from app.api.security import router as security_router
from app.api.keyword_config import router as keyword_config_router
from app.services.apscheduler_service import apscheduler_service
from app.core.security import security_middleware
from app.core.concurrency import concurrency_manager


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    
    def print_section(title: str, icon: str = ""):
        """打印格式化的区块标题"""
        print(f"\n{icon} {title}")
        print("─" * (len(title) + 3))
    
    def print_item(key: str, value: str, status: str = ""):
        """打印格式化的配置项"""
        status_icon = {"✅": "✅", "❌": "❌", "⚠️": "⚠️", "🔄": "🔄"}.get(status, "  ")
        print(f"  {status_icon} {key:<20} : {value}")
    
    # 启动标题
    print("\n" + "═" * 80)
    print("🚀 AI平台智能分析系统")
    print("═" * 80)
    
    # 系统信息
    print_section("系统配置", "⚙️")
    print_item("运行环境", settings.environment)
    print_item("应用版本", settings.app_version)
    print_item("API端口", str(settings.api_port))
    
    # 安全配置信息
    print_section("安全配置", "🔒")
    try:
        print_item("每分钟限制", f"{settings.security_rate_limit_requests_per_minute} 次", "✅")
        print_item("每小时限制", f"{settings.security_rate_limit_requests_per_hour} 次", "✅")
        print_item("每日限制", f"{settings.security_rate_limit_requests_per_day} 次", "✅")
        print_item("JWT过期时间", f"{settings.security_jwt_expire_hours} 小时", "✅")
        print_item("密钥状态", f"长度 {len(settings.security_jwt_secret_key)} 字符", "✅")
    except Exception as e:
        print_item("安全配置", f"加载失败: {str(e)[:30]}...", "❌")
    
    # LLM配置信息
    print_section("LLM配置", "🤖")
    print_item("服务提供商", settings.llm_provider)
    if settings.llm_provider == "volcengine":
        print_item("模型名称", settings.volcengine_model)
        print_item("备用模型", settings.volcengine_model_alternate)
        print_item("服务区域", settings.volcengine_region)
        print_item("API状态", "已配置" if settings.volcengine_api_key else "未配置", "✅" if settings.volcengine_api_key else "❌")
    elif settings.llm_provider == "siliconflow":
        print_item("模型名称", settings.siliconflow_model)
        print_item("API地址", settings.siliconflow_base_url)
        print_item("API状态", "已配置" if settings.siliconflow_api_key else "未配置", "✅" if settings.siliconflow_api_key else "❌")
    
    # 数据库初始化
    print_section("数据库连接", "🗄️")
    try:
        print_item("数据库地址", f"{settings.db_host}:{settings.db_port}")
        print_item("数据库名称", settings.db_name)
        print_item("数据库用户", settings.db_user)
        
        create_tables()
        print_item("连接状态", "正常，表结构检查完成", "✅")
    except Exception as e:
        print_item("连接状态", f"失败: {str(e)[:50]}...", "❌")
    
    # 规则引擎加载
    print_section("规则引擎配置", "🔧")
    try:
        from app.db.database import get_db
        from app.services.keyword_config_manager import keyword_config_manager
        
        # 获取数据库会话来加载配置
        db_gen = get_db()
        db = next(db_gen)
        
        try:
            # 获取详细的配置统计信息
            stats_result = keyword_config_manager.get_config_statistics(db)
            
            if stats_result["success"]:
                stats = stats_result["data"]
                
                # 显示分析关键词配置统计
                categories_stats = stats.get("analysis_categories", {})
                keywords_stats = stats.get("analysis_keywords", {})
                
                print_item("配置来源", "数据库配置", "✅")
                print_item("分析分类", f"启用 {categories_stats.get('enabled', 0)} 个, 禁用 {categories_stats.get('disabled', 0)} 个", "✅")
                print_item("分析关键词", f"启用 {keywords_stats.get('enabled', 0)} 个, 禁用 {keywords_stats.get('disabled', 0)} 个", "✅")
                
                # 显示各分类的详细配置
                analysis_config = keyword_config_manager.get_analysis_keywords_config(db, use_cache=False)
                if analysis_config:
                    for category, config in analysis_config.items():
                        keyword_count = len(config.get("keywords", []))
                        pattern_count = len(config.get("patterns", []))
                        exclusion_count = len(config.get("exclusions", []))
                        extra_info = ""
                        if exclusion_count > 0:
                            extra_info = f", {exclusion_count} 排除"
                        print_item(f"  └─ {category}", f"{keyword_count} 关键词, {pattern_count} 正则{extra_info}")
                
                # 显示去噪配置统计
                denoise_stats = stats.get("denoise_patterns", {})
                total_enabled = sum(pattern.get("enabled", 0) for pattern in denoise_stats.values())
                total_disabled = sum(pattern.get("disabled", 0) for pattern in denoise_stats.values())
                
                print_item("去噪规则", f"启用 {total_enabled} 个, 禁用 {total_disabled} 个", "✅")
                
                for pattern_type, pattern_stats in denoise_stats.items():
                    type_name_map = {
                        "normal_operation": "正常操作",
                        "invalid_data": "无效数据", 
                        "system_keyword": "系统关键词"
                    }
                    type_name = type_name_map.get(pattern_type, pattern_type)
                    enabled = pattern_stats.get("enabled", 0)
                    disabled = pattern_stats.get("disabled", 0)
                    if disabled > 0:
                        print_item(f"  └─ {type_name}", f"启用 {enabled} 个, 禁用 {disabled} 个")
                    else:
                        print_item(f"  └─ {type_name}", f"启用 {enabled} 个")
                
            else:
                print_item("配置来源", "数据库连接失败，使用默认配置", "⚠️")
                print_item("备用方案", "硬编码配置已激活", "⚠️")
                
        finally:
            db.close()
            
    except Exception as e:
        print_item("规则引擎", f"加载失败: {str(e)[:50]}...", "❌")
        print_item("备用方案", "将使用硬编码默认配置", "⚠️")
    
    # 并发管理器
    print_section("并发控制", "⚡")
    concurrency_manager.initialize()
    print_item("并发管理器", "已初始化", "✅")
    print_item("最大并发数", str(getattr(settings, 'concurrency_analysis_max_concurrent', 3)))
    print_item("批次大小", str(getattr(settings, 'concurrency_analysis_batch_size', 50)))
    
    # 调度器启动
    print_section("任务调度器", "🔄")
    try:
        await apscheduler_service.start()
        print_item("APScheduler", "启动成功", "✅")
        print_item("调度状态", "自动任务调度已启用", "✅")
    except Exception as e:
        print_item("APScheduler", f"启动失败: {str(e)[:50]}...", "❌")
        print_item("影响范围", "手动任务仍可正常执行", "⚠️")
    
    # 网络访问信息
    print_section("网络访问", "🌐")
    import socket
    def get_local_ip():
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(("8.8.8.8", 80))
                return s.getsockname()[0]
        except Exception:
            return "127.0.0.1"
    
    local_ip = get_local_ip()
    port = settings.api_port
    
    print_item("本地访问", f"http://localhost:{port}")
    print_item("局域网访问", f"http://{local_ip}:{port}")
    print_item("API文档", f"http://localhost:{port}/docs")
    print_item("关键词配置", f"http://localhost:{port}/api/keyword-config/statistics")
    
    # 启动完成
    print("\n" + "═" * 80)
    print("🎉 系统启动完成！准备接收请求...")
    print("═" * 80)
    
    yield
    
    # 关闭时执行
    print("\n" + "═" * 80)
    print("🛑 系统关闭中...")
    print("═" * 80)
    
    # 停止APScheduler调度器
    if apscheduler_service._running:
        await apscheduler_service.stop()
        print_item("APScheduler", "已停止", "✅")
    
    # 并发管理器关闭
    concurrency_manager.shutdown()
    print_item("并发管理器", "已关闭", "✅")
    
    print("\n👋 系统已安全关闭")


# 创建FastAPI应用
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="AI平台智能分析系统 - 用于分析客服对话并检测规避责任行为",
    lifespan=lifespan
)


# 安全中间件函数
@app.middleware("http")
async def security_middleware_handler(request: Request, call_next):
    # 应用安全防护
    await security_middleware(request)
    
    # 继续处理请求
    response = await call_next(request)
    return response

# 添加CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产环境应该限制具体域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
app.include_router(auth_router, prefix=settings.api_prefix)
app.include_router(security_router, prefix=settings.api_prefix)
app.include_router(analysis_router, prefix=settings.api_prefix)
app.include_router(system_router, prefix=settings.api_prefix)
app.include_router(tasks_router, prefix=settings.api_prefix)
app.include_router(keyword_config_router, prefix=settings.api_prefix)

# 静态文件处理
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    # 挂载静态文件目录
    app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/")
async def root():
    """根路径 - 重定向到登录页面"""
    login_file = os.path.join(os.path.dirname(__file__), "static", "login.html")
    if os.path.exists(login_file):
        return FileResponse(login_file)
    else:
        return {
            "app_name": settings.app_name,
            "version": settings.app_version,
            "environment": settings.environment,
            "message": "AI平台智能分析系统运行中",
            "docs_url": "/docs",
            "debug_url": "/debug",
            "api_prefix": settings.api_prefix,
            "auth_required": True,
            "auth_header": "Authorization: Bearer YOUR_API_KEY"
        }


@app.get("/debug")
async def debug_page():
    """调试页面"""
    debug_file = os.path.join(os.path.dirname(__file__), "static", "debug.html")
    if os.path.exists(debug_file):
        return FileResponse(debug_file)
    else:
        return {"error": "调试页面未找到", "path": debug_file}


@app.get("/health")
async def health():
    """简单健康检查"""
    return {
        "status": "healthy",
        "app_name": settings.app_name,
        "version": settings.app_version,
        "environment": settings.environment
    }


async def run_scheduler():
    """运行APScheduler调度器（用于独立进程）"""
    print("启动独立的APScheduler调度器...")
    await apscheduler_service.start()
    # 保持运行
    try:
        while True:
            await asyncio.sleep(60)
    except KeyboardInterrupt:
        print("接收到停止信号，关闭调度器...")
        await apscheduler_service.stop()


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="AI平台智能分析系统")
    parser.add_argument("--mode", choices=["api", "scheduler", "both"], default="api", 
                       help="运行模式: api(仅API服务), scheduler(仅调度器), both(两者)")
    parser.add_argument("--host", default=settings.api_host, help="API服务主机")
    parser.add_argument("--port", type=int, default=settings.api_port, help="API服务端口")
    parser.add_argument("--env", choices=["local", "prod"], default=settings.environment,
                       help="运行环境")
    
    args = parser.parse_args()
    
    # 设置环境
    os.environ["ENVIRONMENT"] = args.env
    settings.environment = args.env
    
    if args.mode == "scheduler":
        # 仅运行调度器
        asyncio.run(run_scheduler())
    elif args.mode == "both":
        # 同时运行API和调度器
        print("同时启动API服务和调度器...")
        
        # 在后台启动调度器
        import threading
        scheduler_thread = threading.Thread(target=lambda: asyncio.run(run_scheduler()), daemon=True)
        scheduler_thread.start()
        
        # 启动API服务
        uvicorn.run(
            "main:app",
            host=args.host,
            port=args.port,
            reload=args.env == "local",
            log_level="info"
        )
    else:
        # 仅运行API服务
        print(f"🚀 启动API服务... 环境: {args.env}")
        print(f"🌐 监听地址: {args.host}:{args.port}")
        print(f"🔄 热重载: {'开启' if args.env == 'local' else '关闭'}")
        uvicorn.run(
            "main:app",
            host=args.host,
            port=args.port,
            reload=args.env == "local",
            log_level="info"
        )


if __name__ == "__main__":
    main()
