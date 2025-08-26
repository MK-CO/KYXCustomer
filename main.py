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
from app.services.apscheduler_service import apscheduler_service
from app.core.security import security_middleware
from app.core.concurrency import concurrency_manager


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动时执行
    print(f"🚀 AI平台智能分析系统启动中...")
    print(f"🌍 运行环境: {settings.environment}")
    print(f"🤖 LLM提供商: {settings.llm_provider}")
    
    # 创建数据库表
    try:
        create_tables()
        print("✅ 数据库表检查完成")
    except Exception as e:
        print(f"❌ 数据库表创建失败: {e}")
    
    # 初始化并发管理器
    concurrency_manager.initialize()
    print("⚡ 并发管理器已初始化")
    
    # 🚀 自动启动APScheduler调度器
    try:
        await apscheduler_service.start()
        print("✅ APScheduler调度器已自动启动")
        print("📋 自动加载任务配置并开始执行")
        print("🔄 使用APScheduler替代传统调度器，更稳定更强大")
    except Exception as e:
        print(f"❌ APScheduler调度器启动失败: {e}")
        print("💡 系统将继续运行，但任务调度不可用")
    
    # 启动成功日志
    print("=" * 70)
    print("🎉 AI平台智能分析系统启动成功！")
    
    # 获取本机IP地址
    import socket
    def get_local_ip():
        try:
            # 连接到一个远程地址来获取本机IP
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(("8.8.8.8", 80))
                return s.getsockname()[0]
        except Exception:
            return "127.0.0.1"
    
    local_ip = get_local_ip()
    port = settings.api_port
    
    print(f"📍 本地访问: http://localhost:{port}")
    print(f"🌐 局域网访问: http://{local_ip}:{port}")
    print(f"📚 API文档: http://localhost:{port}/docs")
    print(f"🔍 调试页面: http://localhost:{port}/debug")
    print(f"💓 健康检查: http://localhost:{port}/health")
    print(f"🔑 认证方式: Bearer Token")
    print("=" * 70)
    
    yield
    
    # 关闭时执行
    print("🛑 AI平台智能分析系统关闭中...")
    
    # 🔥 停止APScheduler调度器
    if apscheduler_service._running:
        await apscheduler_service.stop()
        print("⏹️ APScheduler调度器已停止")
    
    # 并发管理器关闭
    concurrency_manager.shutdown()
    print("⚡ 并发管理器已关闭")
    print("👋 系统已安全关闭")


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
