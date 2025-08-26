#!/bin/bash
# AI平台智能分析系统 - 一键启动脚本 (Shell版本)

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 打印横幅
print_banner() {
    echo -e "${BLUE}"
    echo "╔══════════════════════════════════════════════╗"
    echo "║         AI平台智能分析系统                   ║"
    echo "║      AI Platform Smart Analysis System      ║"
    echo "║                                              ║"
    echo "║           🚀 一键启动服务 🚀                  ║"
    echo "╚══════════════════════════════════════════════╝"
    echo -e "${NC}"
}

# 显示帮助信息
show_help() {
    echo "用法: $0 [选项]"
    echo ""
    echo "选项:"
    echo "  -e, --env     设置环境 (local|prod)，默认: local"
    echo "  -m, --mode    运行模式 (api|scheduler|both)，默认: api"
    echo "  -p, --port    指定端口，默认: 8993"
    echo "  -h, --help    显示帮助信息"
    echo ""
    echo "示例:"
    echo "  $0                        # 默认local环境启动"
    echo "  $0 -e prod                # 生产环境启动"
    echo "  $0 -m both                # 启动API和调度器"
    echo "  $0 -e prod -p 9000        # 生产环境，端口9000"
}

# 默认参数
ENV="local"
MODE="api"
PORT="8993"
HOST="0.0.0.0"

# 解析命令行参数
while [[ $# -gt 0 ]]; do
    case $1 in
        -e|--env)
            ENV="$2"
            shift 2
            ;;
        -m|--mode)
            MODE="$2"
            shift 2
            ;;
        -p|--port)
            PORT="$2"
            shift 2
            ;;
        -h|--help)
            show_help
            exit 0
            ;;
        *)
            echo -e "${RED}❌ 未知参数: $1${NC}"
            show_help
            exit 1
            ;;
    esac
done

# 验证环境参数
if [[ "$ENV" != "local" && "$ENV" != "prod" ]]; then
    echo -e "${RED}❌ 环境参数错误，必须是 local 或 prod${NC}"
    exit 1
fi

# 验证模式参数
if [[ "$MODE" != "api" && "$MODE" != "scheduler" && "$MODE" != "both" ]]; then
    echo -e "${RED}❌ 模式参数错误，必须是 api、scheduler 或 both${NC}"
    exit 1
fi

print_banner

echo -e "${BLUE}🔍 检查环境...${NC}"

# 检查Python
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}❌ Python3 未安装${NC}"
    exit 1
fi

# 检查必需文件
required_files=("main.py" "requirements.txt" "application.properties")
for file in "${required_files[@]}"; do
    if [[ ! -f "$file" ]]; then
        echo -e "${RED}❌ 缺少必需文件: $file${NC}"
        exit 1
    fi
done

# 检查并激活虚拟环境
VENV_DIR="ai-platform-venv"
if [[ -d "$VENV_DIR" ]]; then
    echo -e "${BLUE}🐍 检测到虚拟环境，正在激活...${NC}"
    source "$VENV_DIR/bin/activate"
    if [[ $? -eq 0 ]]; then
        echo -e "${GREEN}✅ 虚拟环境已激活: $VENV_DIR${NC}"
    else
        echo -e "${RED}❌ 虚拟环境激活失败${NC}"
        exit 1
    fi
else
    echo -e "${YELLOW}⚠️ 未找到虚拟环境 $VENV_DIR，将使用系统 Python${NC}"
fi

echo -e "${GREEN}✅ 环境检查通过${NC}"

# 检查依赖（可选安装）
echo -e "${BLUE}📦 检查依赖包...${NC}"
if ! python -c "import fastapi, uvicorn" > /dev/null 2>&1; then
    echo -e "${YELLOW}⚠️ 发现缺少依赖包，正在安装...${NC}"
    python -m pip install -r requirements.txt > /dev/null 2>&1
    if [[ $? -eq 0 ]]; then
        echo -e "${GREEN}✅ 依赖包安装完成${NC}"
    else
        echo -e "${RED}❌ 依赖安装失败${NC}"
        exit 1
    fi
else
    echo -e "${GREEN}✅ 依赖包检查通过${NC}"
fi

# 设置环境变量
export ENVIRONMENT="$ENV"
export APP_ENVIRONMENT="$ENV"

echo -e "${BLUE}⚙️ 环境配置: $ENV${NC}"
echo -e "${BLUE}🎯 运行模式: $MODE${NC}"
echo -e "${BLUE}🌐 服务地址: http://$HOST:$PORT${NC}"

# 显示服务信息
echo ""
echo "═══════════════════════════════════════════════"
echo -e "${GREEN}📋 服务信息${NC}"
echo "═══════════════════════════════════════════════"
echo -e "🌐 API文档: ${BLUE}http://$HOST:$PORT/docs${NC}"
echo -e "❤️ 健康检查: ${BLUE}http://$HOST:$PORT/health${NC}"
echo -e "🏠 主页: ${BLUE}http://$HOST:$PORT/${NC}"
echo -e "📊 API接口: ${BLUE}http://$HOST:$PORT/api/v1${NC}"
echo ""
echo -e "${YELLOW}🔧 按 Ctrl+C 停止服务${NC}"
echo "═══════════════════════════════════════════════"
echo ""

# 启动服务
echo -e "${GREEN}🚀 启动服务中...${NC}"
python main.py --env "$ENV" --mode "$MODE" --host "$HOST" --port "$PORT"
