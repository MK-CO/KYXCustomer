#!/usr/bin/env python3
"""
AI平台智能分析系统 - 一键启动脚本
支持local和prod环境切换，简化启动流程
"""
import os
import sys
import argparse
import subprocess
import time
from pathlib import Path


def print_banner():
    """打印启动横幅"""
    banner = """
╔══════════════════════════════════════════════╗
║         AI平台智能分析系统                   ║
║      AI Platform Smart Analysis System      ║
║                                              ║
║           🚀 一键启动服务 🚀                  ║
╚══════════════════════════════════════════════╝
    """
    print(banner)


def check_requirements():
    """检查环境依赖"""
    print("🔍 检查环境依赖...")
    
    # 检查Python版本
    if sys.version_info < (3, 8):
        print("❌ Python版本过低，需要Python 3.8+")
        sys.exit(1)
    
    # 检查关键文件
    required_files = [
        "main.py",
        "requirements.txt", 
        "application.properties",
        "config/settings.py"
    ]
    
    for file in required_files:
        if not Path(file).exists():
            print(f"❌ 缺少必需文件: {file}")
            sys.exit(1)
    
    print("✅ 环境检查通过")


def install_dependencies():
    """检查并按需安装依赖包"""
    print("📦 检查依赖包...")
    try:
        # 检查关键依赖是否存在
        result = subprocess.run([
            sys.executable, "-c", "import fastapi, uvicorn, sqlalchemy"
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ 依赖包检查通过")
            return
        
        # 如果缺少依赖，则安装
        print("⚠️ 发现缺少依赖包，正在安装...")
        result = subprocess.run([
            sys.executable, "-m", "pip", "install", "-r", "requirements.txt"
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ 依赖包安装完成")
        else:
            print(f"❌ 依赖安装失败: {result.stderr}")
            sys.exit(1)
    except Exception as e:
        print(f"❌ 依赖检查失败: {e}")
        sys.exit(1)


def set_environment_config(env):
    """设置环境配置"""
    print(f"⚙️ 设置环境配置: {env}")
    
    # 设置环境变量
    os.environ["ENVIRONMENT"] = env
    os.environ["APP_ENVIRONMENT"] = env
    
    # 根据环境设置特定配置
    if env == "local":
        print("📍 本地开发环境配置:")
        print("   - 数据库: localhost:3306")
        print("   - 调试模式: 开启")
        print("   - 热重载: 开启")
        print("   - 日志级别: INFO")
    elif env == "prod":
        print("🏭 生产环境配置:")
        print("   - 数据库: 生产服务器")
        print("   - 调试模式: 关闭")
        print("   - 热重载: 关闭")
        print("   - 日志级别: WARNING")


def check_database_connection():
    """检查数据库连接"""
    print("🔌 检查数据库连接...")
    try:
        # 简单的数据库连接测试
        from config.settings import settings
        print(f"   - 数据库地址: {settings.db_host}:{settings.db_port}")
        print(f"   - 数据库名称: {settings.db_name}")
        print("✅ 数据库配置加载成功")
        return True
    except Exception as e:
        print(f"⚠️ 数据库配置警告: {e}")
        return False


def show_security_config():
    """显示安全配置信息"""
    print("🔒 检查安全配置...")
    try:
        from config.settings import settings
        print("   🛡️ 速率限制配置:")
        print(f"      - 每分钟限制: {settings.security_rate_limit_requests_per_minute} 次")
        print(f"      - 每小时限制: {settings.security_rate_limit_requests_per_hour} 次")
        print(f"      - 每日限制: {settings.security_rate_limit_requests_per_day} 次")
        print(f"   🔐 认证配置:")
        print(f"      - JWT密钥长度: {len(settings.secret_key)} 字符")
        print(f"      - Token过期时间: {settings.access_token_expire_minutes} 分钟")
        print("✅ 安全配置加载成功")
        return True
    except Exception as e:
        print(f"⚠️ 安全配置警告: {e}")
        return False


def start_service(env, mode, host, port, install_deps=True, skip_deps=False):
    """启动服务"""
    print(f"\n🚀 启动AI平台智能分析系统...")
    print(f"   环境: {env}")
    print(f"   模式: {mode}")
    print(f"   地址: http://{host}:{port}")
    
    # 设置环境
    set_environment_config(env)
    
    # 检查依赖
    if not skip_deps and install_deps:
        install_dependencies()
    elif skip_deps:
        print("⏭️ 跳过依赖检查")
    
    # 检查数据库
    check_database_connection()
    
    # 显示安全配置
    show_security_config()
    
    print("\n" + "="*50)
    print("🎯 服务启动中...")
    print("="*50)
    
    # 构建启动命令
    cmd = [
        sys.executable, "main.py",
        "--env", env,
        "--mode", mode,
        "--host", host,
        "--port", str(port)
    ]
    
    try:
        # 启动服务
        subprocess.run(cmd)
    except KeyboardInterrupt:
        print("\n\n🛑 服务已停止")
    except Exception as e:
        print(f"\n❌ 服务启动失败: {e}")
        sys.exit(1)


def show_service_info(host, port):
    """显示服务信息"""
    print("\n" + "="*50)
    print("📋 服务信息")
    print("="*50)
    print(f"🌐 API文档: http://{host}:{port}/docs")
    print(f"❤️ 健康检查: http://{host}:{port}/health")
    print(f"🏠 主页: http://{host}:{port}/")
    print(f"📊 API接口: http://{host}:{port}/api/v1")
    print("\n🔧 常用命令:")
    print("   Ctrl+C : 停止服务")
    print("   访问 /docs : 查看API文档")
    print("   访问 /health : 检查服务状态")


def main():
    """主函数"""
    print_banner()
    
    parser = argparse.ArgumentParser(
        description="AI平台智能分析系统一键启动脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  python start.py                    # 默认local环境启动
  python start.py --env prod         # 生产环境启动
  python start.py --mode both        # 同时启动API和调度器
  python start.py --port 9000        # 指定端口启动
  python start.py --no-install       # 跳过依赖安装
        """
    )
    
    parser.add_argument(
        "--env", 
        choices=["local", "prod"], 
        default="local",
        help="运行环境 (默认: local)"
    )
    
    parser.add_argument(
        "--mode", 
        choices=["api", "scheduler", "both"], 
        default="api",
        help="运行模式 (默认: api)"
    )
    
    parser.add_argument(
        "--host", 
        default="0.0.0.0",
        help="服务主机地址 (默认: 0.0.0.0)"
    )
    
    parser.add_argument(
        "--port", 
        type=int, 
        default=8993,
        help="服务端口 (默认: 8993)"
    )
    
    parser.add_argument(
        "--no-install", 
        action="store_true",
        help="跳过依赖包安装"
    )
    
    parser.add_argument(
        "--skip-deps", 
        action="store_true",
        help="完全跳过依赖检查和安装"
    )
    
    parser.add_argument(
        "--check-only", 
        action="store_true",
        help="仅进行环境检查，不启动服务"
    )
    
    args = parser.parse_args()
    
    # 环境检查
    check_requirements()
    
    if args.check_only:
        print("✅ 环境检查完成，服务可以正常启动")
        return
    
    # 显示服务信息
    show_service_info(args.host, args.port)
    
    # 启动服务
    start_service(
        env=args.env,
        mode=args.mode, 
        host=args.host,
        port=args.port,
        install_deps=not args.no_install,
        skip_deps=args.skip_deps
    )


if __name__ == "__main__":
    main()
