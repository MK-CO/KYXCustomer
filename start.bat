@echo off
rem AI平台智能分析系统 - 一键启动脚本 (Windows版本)
chcp 65001 >nul

title AI平台智能分析系统

rem 默认参数
set ENV=local
set MODE=api
set PORT=8993
set HOST=0.0.0.0

rem 解析命令行参数
:parse_args
if "%~1"=="" goto start_banner
if "%~1"=="-e" (
    set ENV=%~2
    shift
    shift
    goto parse_args
)
if "%~1"=="--env" (
    set ENV=%~2
    shift
    shift
    goto parse_args
)
if "%~1"=="-m" (
    set MODE=%~2
    shift
    shift
    goto parse_args
)
if "%~1"=="--mode" (
    set MODE=%~2
    shift
    shift
    goto parse_args
)
if "%~1"=="-p" (
    set PORT=%~2
    shift
    shift
    goto parse_args
)
if "%~1"=="--port" (
    set PORT=%~2
    shift
    shift
    goto parse_args
)
if "%~1"=="-h" goto show_help
if "%~1"=="--help" goto show_help
echo 未知参数: %~1
goto show_help

:show_help
echo.
echo 用法: %~nx0 [选项]
echo.
echo 选项:
echo   -e, --env     设置环境 (local^|prod)，默认: local
echo   -m, --mode    运行模式 (api^|scheduler^|both)，默认: api
echo   -p, --port    指定端口，默认: 8993
echo   -h, --help    显示帮助信息
echo.
echo 示例:
echo   %~nx0                        # 默认local环境启动
echo   %~nx0 -e prod                # 生产环境启动
echo   %~nx0 -m both                # 启动API和调度器
echo   %~nx0 -e prod -p 9000        # 生产环境，端口9000
echo.
pause
exit /b 0

:start_banner
cls
echo.
echo ╔══════════════════════════════════════════════╗
echo ║         AI平台智能分析系统                   ║
echo ║      AI Platform Smart Analysis System      ║
echo ║                                              ║
echo ║           🚀 一键启动服务 🚀                  ║
echo ╚══════════════════════════════════════════════╝
echo.

rem 验证环境参数
if not "%ENV%"=="local" if not "%ENV%"=="prod" (
    echo ❌ 环境参数错误，必须是 local 或 prod
    pause
    exit /b 1
)

rem 验证模式参数
if not "%MODE%"=="api" if not "%MODE%"=="scheduler" if not "%MODE%"=="both" (
    echo ❌ 模式参数错误，必须是 api、scheduler 或 both
    pause
    exit /b 1
)

echo 🔍 检查环境...

rem 检查Python
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python 未安装或不在PATH中
    pause
    exit /b 1
)

rem 检查必需文件
if not exist "main.py" (
    echo ❌ 缺少必需文件: main.py
    pause
    exit /b 1
)
if not exist "requirements.txt" (
    echo ❌ 缺少必需文件: requirements.txt
    pause
    exit /b 1
)
if not exist "application.properties" (
    echo ❌ 缺少必需文件: application.properties
    pause
    exit /b 1
)

echo ✅ 环境检查通过

rem 安装依赖
echo 📦 安装依赖包...
python -m pip install -r requirements.txt >nul 2>&1
if errorlevel 1 (
    echo ⚠️ 依赖安装警告，继续启动...
) else (
    echo ✅ 依赖包安装完成
)

rem 设置环境变量
set ENVIRONMENT=%ENV%
set APP_ENVIRONMENT=%ENV%

echo ⚙️ 环境配置: %ENV%
echo 🎯 运行模式: %MODE%
echo 🌐 服务地址: http://%HOST%:%PORT%

echo.
echo ═══════════════════════════════════════════════
echo 📋 服务信息
echo ═══════════════════════════════════════════════
echo 🌐 API文档: http://%HOST%:%PORT%/docs
echo ❤️ 健康检查: http://%HOST%:%PORT%/health
echo 🏠 主页: http://%HOST%:%PORT%/
echo 📊 API接口: http://%HOST%:%PORT%/api/v1
echo.
echo 🔧 按 Ctrl+C 停止服务
echo ═══════════════════════════════════════════════
echo.

rem 启动服务
echo 🚀 启动服务中...
python main.py --env %ENV% --mode %MODE% --host %HOST% --port %PORT%

pause
