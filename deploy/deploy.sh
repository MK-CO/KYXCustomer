#!/bin/bash

# AI平台智能分析系统 - 服务器部署脚本
# 用途: 在服务器上自动部署应用，包括依赖安装、服务配置等

set -e  # 遇到错误立即退出

# 配置变量
APP_NAME="ai-platform-smart"
APP_USER="root"
APP_HOME="/opt/ai-platform-smart"
SERVICE_NAME="ai-platform-smart"
PYTHON_CMD="python3"
PIP_CMD="pip3"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 打印函数
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_banner() {
    echo -e "${BLUE}"
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║                   AI平台智能分析系统                          ║"
    echo "║                   服务器自动部署脚本                          ║"
    echo "║                                                              ║"
    echo "║                  🚀 一键自动化部署 🚀                        ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

# 检查运行权限
check_permissions() {
    if [ "$EUID" -ne 0 ]; then
        print_error "此脚本需要root权限，请使用 sudo 运行"
        exit 1
    fi
}

# 检测操作系统
detect_os() {
    print_info "检测操作系统..."
    
    if [ -f /etc/os-release ]; then
        . /etc/os-release
        OS=$NAME
        VER=$VERSION_ID
    else
        print_error "无法检测操作系统版本"
        exit 1
    fi
    
    print_info "操作系统: $OS $VER"
    
    # 设置包管理器
    case "$OS" in
        *"Ubuntu"*|*"Debian"*)
            PKG_MANAGER="apt"
            PKG_UPDATE="apt update"
            PKG_INSTALL="apt install -y"
            ;;
        *"CentOS"*|*"Red Hat"*|*"Rocky"*)
            PKG_MANAGER="yum"
            PKG_UPDATE="yum update -y"
            PKG_INSTALL="yum install -y"
            ;;
        *)
            print_warning "未知的操作系统，将使用默认配置"
            PKG_MANAGER="apt"
            PKG_UPDATE="apt update"
            PKG_INSTALL="apt install -y"
            ;;
    esac
}

# 安装适合的Python版本
install_python() {
    print_info "安装Python环境..."
    
    # 更新包管理器
    $PKG_UPDATE
    
    if [ "$PKG_MANAGER" = "apt" ]; then
        # Ubuntu/Debian - 尝试安装最新可用版本
        print_info "检查可用的Python版本..."
        
        # 添加deadsnakes PPA获取更新版本的Python
        if ! grep -q "deadsnakes/ppa" /etc/apt/sources.list.d/* 2>/dev/null; then
            print_info "添加Python PPA源..."
            $PKG_INSTALL software-properties-common
            add-apt-repository ppa:deadsnakes/ppa -y
            $PKG_UPDATE
        fi
        
        # 尝试安装Python 3.12 或 3.11（更稳定且广泛支持）
        if apt-cache show python3.12 &> /dev/null; then
            print_info "安装Python 3.12..."
            $PKG_INSTALL python3.12 python3.12-venv python3.12-dev
            # 确保pip可用
            if ! command -v python3.12 -m pip &> /dev/null; then
                $PKG_INSTALL python3.12-pip 2>/dev/null || true
            fi
            PYTHON_CMD="python3.12"
            PIP_CMD="python3.12 -m pip"
        elif apt-cache show python3.11 &> /dev/null; then
            print_info "安装Python 3.11..."
            $PKG_INSTALL python3.11 python3.11-venv python3.11-dev
            # 确保pip可用
            if ! command -v python3.11 -m pip &> /dev/null; then
                $PKG_INSTALL python3.11-pip 2>/dev/null || true
            fi
            PYTHON_CMD="python3.11"
            PIP_CMD="python3.11 -m pip"
        else
            print_info "安装系统默认Python..."
            $PKG_INSTALL python3 python3-venv python3-dev python3-pip
            PYTHON_CMD="python3"
            PIP_CMD="python3 -m pip"
        fi
        
        # 安装pip（如果没有）
        if ! $PYTHON_CMD -m pip --version &> /dev/null; then
            print_info "安装pip..."
            curl -sS https://bootstrap.pypa.io/get-pip.py | $PYTHON_CMD
        fi
        
        # 安装编译依赖
        $PKG_INSTALL build-essential libmysqlclient-dev pkg-config
        
    else
        # CentOS/RHEL
        print_info "安装Python 3.11..."
        $PKG_INSTALL python3.11 python3.11-pip python3.11-devel gcc gcc-c++ make mysql-devel pkgconfig
        PYTHON_CMD="python3.11"
        PIP_CMD="python3.11 -m pip"
    fi
    
    # 验证安装
    PYTHON_VERSION=$($PYTHON_CMD --version 2>&1)
    print_success "Python安装完成: $PYTHON_VERSION"
}

# 检查系统依赖
check_system_dependencies() {
    print_info "检查系统依赖..."
    
    # 检查是否已安装合适的Python版本
    if command -v python3.12 &> /dev/null; then
        PYTHON_CMD="python3.12"
        PIP_CMD="python3.12 -m pip"
        print_success "发现Python 3.12"
    elif command -v python3.11 &> /dev/null; then
        PYTHON_CMD="python3.11"
        PIP_CMD="python3.11 -m pip"
        print_success "发现Python 3.11"
    elif command -v python3 &> /dev/null; then
        PYTHON_VERSION=$(python3 --version 2>&1 | cut -d' ' -f2)
        PYTHON_CMD="python3"
        PIP_CMD="python3 -m pip"
        print_info "发现系统Python: $PYTHON_VERSION"
    else
        print_info "需要安装Python环境"
        PYTHON_CMD=""
        PIP_CMD=""
    fi
    
    print_success "系统依赖检查完成"
}

# 安装系统依赖（必须）
install_system_dependencies() {
    print_info "安装Python环境和系统依赖..."
    
    # 安装Python
    install_python
    
    print_success "系统依赖安装完成"
}

# 创建应用用户
create_app_user() {
    print_info "使用root用户运行，跳过用户创建..."
    print_success "运行用户: $APP_USER"
}

# 创建应用目录
create_app_directory() {
    print_info "创建应用目录..."
    
    # 创建应用根目录
    mkdir -p "$APP_HOME"
    mkdir -p "$APP_HOME/logs"
    mkdir -p "$APP_HOME/data"
    mkdir -p "$APP_HOME/backups"
    
    # 设置目录权限
    chmod 755 "$APP_HOME"
    chmod 755 "$APP_HOME/logs"
    chmod 755 "$APP_HOME/data"
    chmod 755 "$APP_HOME/backups"
    
    print_success "应用目录创建完成: $APP_HOME"
}

# 复制应用文件
copy_application_files() {
    print_info "复制应用文件..."
    
    # 获取当前脚本所在目录（即应用根目录）
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
    
    # 检查是否已经在目标目录中运行
    if [ "$SCRIPT_DIR" = "$APP_HOME" ]; then
        print_info "应用文件已在目标位置，无需复制"
    else
        # 复制应用文件到目标目录
        cp -r "$SCRIPT_DIR"/* "$APP_HOME/"
        
        # 确保脚本可执行
        chmod +x "$APP_HOME/start.py"
        chmod +x "$APP_HOME/deploy/deploy.sh"
    fi
    
    print_success "应用文件准备完成"
}

# 创建Python虚拟环境
install_python_dependencies() {
    print_info "创建Python 3.13虚拟环境..."
    
    # 切换到应用目录
    cd "$APP_HOME"
    
    # 删除旧的虚拟环境（如果存在）
    if [ -d "ai-platform-venv" ]; then
        print_info "删除旧的虚拟环境..."
        rm -rf ai-platform-venv
    fi
    
    if [ -d "venv" ]; then
        print_info "删除旧的venv..."
        rm -rf venv
    fi
    
    # 创建新的虚拟环境
    print_info "使用Python 3.13创建虚拟环境..."
    $PYTHON_CMD -m venv ai-platform-venv
    
    # 安装依赖
    print_info "安装Python依赖包..."
    ai-platform-venv/bin/pip install --upgrade pip
    ai-platform-venv/bin/pip install -r requirements.txt
    
    # 验证安装
    VENV_PYTHON_VERSION=$(ai-platform-venv/bin/python --version)
    print_success "虚拟环境创建完成: $VENV_PYTHON_VERSION"
    
    # 测试导入
    print_info "测试项目导入..."
    if ai-platform-venv/bin/python -c "import sys; sys.path.append('$APP_HOME'); from app.services.stage2_analysis_service import stage2_service; print('✅ 项目导入成功')"; then
        print_success "项目依赖配置完成"
    else
        print_error "项目导入测试失败"
        exit 1
    fi
}

# 配置应用环境
configure_application() {
    print_info "使用现有的 application.properties 配置文件"
    
    if [ -f "$APP_HOME/application.properties" ]; then
        print_success "应用配置文件已就绪"
    else
        print_error "未找到 application.properties 配置文件"
        exit 1
    fi
    
    print_success "应用环境配置完成"
}

# 创建systemd服务
create_systemd_service() {
    print_info "开始创建systemd服务..."
    
    # 使用ai-platform-venv虚拟环境
    PYTHON_EXEC="$APP_HOME/ai-platform-venv/bin/python"
    
    cat > "/etc/systemd/system/$SERVICE_NAME.service" << EOF
[Unit]
Description=AI Platform Smart Analysis System
After=network.target
Wants=network.target

[Service]
Type=simple
User=$APP_USER
WorkingDirectory=$APP_HOME
ExecStart=$PYTHON_EXEC main.py
Restart=always
RestartSec=10

# 日志配置
StandardOutput=journal
StandardError=journal
SyslogIdentifier=$SERVICE_NAME

[Install]
WantedBy=multi-user.target
EOF

    # 重新加载systemd配置
    systemctl daemon-reload
    
    # 启用服务
    systemctl enable "$SERVICE_NAME"
    
    print_success "systemd服务创建完成 (Python: $PYTHON_EXEC)"
}

# 跳过Nginx配置（使用直接访问模式）
create_nginx_config() {
    print_info "使用直接访问模式，跳过Nginx配置..."
    print_info "应用将通过 IP:8993 端口直接访问"
}

# 配置防火墙
configure_firewall() {
    print_info "配置防火墙..."
    
    # 检查并配置ufw（Ubuntu/Debian）
    if command -v ufw &> /dev/null; then
        print_info "配置ufw防火墙..."
        ufw allow 8993/tcp comment "AI Platform Smart"
        print_success "ufw防火墙配置完成 - 已开放8993端口"
    
    # 检查并配置firewalld（CentOS/RHEL）
    elif command -v firewall-cmd &> /dev/null; then
        print_info "配置firewalld防火墙..."
        firewall-cmd --permanent --add-port=8993/tcp
        firewall-cmd --reload
        print_success "firewalld防火墙配置完成 - 已开放8993端口"
    
    else
        print_warning "未检测到防火墙管理工具，请手动开放8993端口"
    fi
}

# 启动服务
start_services() {
    print_info "启动应用服务..."
    
    # 启动应用服务
    systemctl start "$SERVICE_NAME"
    
    # 等待服务启动
    sleep 3
    
    # 检查服务状态
    if systemctl is-active --quiet "$SERVICE_NAME"; then
        print_success "🎉 AI平台智能分析系统服务启动成功!"
        
        # 获取服务器IP
        SERVER_IP=$(hostname -I | awk '{print $1}' 2>/dev/null || echo "localhost")
        
        print_info "服务信息:"
        echo "   ✅ 服务状态: 运行中"
        echo "   🌐 访问地址: http://$SERVER_IP:8993"
        echo "   📋 服务管理: systemctl {start|stop|restart|status} $SERVICE_NAME"
        echo "   📋 查看日志: journalctl -u $SERVICE_NAME -f"
        
    else
        print_error "❌ 服务启动失败"
        print_info "查看详细错误信息:"
        systemctl status "$SERVICE_NAME" --no-pager
        print_info "查看服务日志:"
        journalctl -u "$SERVICE_NAME" -n 20 --no-pager
        exit 1
    fi
}

# 运行健康检查
run_health_check() {
    print_info "运行健康检查..."
    
    # 等待服务完全启动
    sleep 5
    
    # 检查服务端口
    if netstat -tlnp 2>/dev/null | grep :8993 &> /dev/null || ss -tlnp 2>/dev/null | grep :8993 &> /dev/null; then
        print_success "✅ 服务端口8993监听正常"
    else
        print_warning "⚠️  服务端口8993未监听，可能还在启动中"
    fi
    
    # 检查HTTP响应
    local max_attempts=6
    local attempt=1
    
    print_info "测试应用响应..."
    while [ $attempt -le $max_attempts ]; do
        if curl -s -f http://localhost:8993/health &> /dev/null; then
            print_success "✅ 应用健康检查通过"
            print_success "🌟 系统已就绪，可以正常使用！"
            return 0
        else
            print_info "🔄 等待应用启动... ($attempt/$max_attempts)"
            sleep 5
            ((attempt++))
        fi
    done
    
    print_warning "⚠️  健康检查超时，但服务可能仍在启动中"
    print_info "请手动检查："
    echo "   - 服务状态: systemctl status ai-platform-smart"
    echo "   - 应用日志: journalctl -u ai-platform-smart -f"
    echo "   - 访问测试: curl http://localhost:8993/health"
}

# 显示部署结果
show_deployment_result() {
    print_success "🎉 部署完成!"
    echo ""
    echo -e "${GREEN}📋 服务信息:${NC}"
    echo -e "   应用名称: AI平台智能分析系统"
    echo -e "   安装路径: $APP_HOME"
    echo -e "   运行用户: $APP_USER"
    echo -e "   服务名称: $SERVICE_NAME"
    echo ""
    echo -e "${GREEN}🌐 访问地址:${NC}"
    
    # 获取服务器IP
    SERVER_IP=$(hostname -I | awk '{print $1}' 2>/dev/null || echo "localhost")
    
    echo -e "   主页: http://$SERVER_IP:8993/"
    echo -e "   API文档: http://$SERVER_IP:8993/docs"
    echo -e "   健康检查: http://$SERVER_IP:8993/health"
    echo -e "   调试页面: http://$SERVER_IP:8993/debug"
    
    echo ""
    echo -e "${YELLOW}🔧 服务管理命令:${NC}"
    echo -e "   启动服务: sudo systemctl start $SERVICE_NAME 或 $APP_HOME/start_service.sh"
    echo -e "   停止服务: sudo systemctl stop $SERVICE_NAME 或 $APP_HOME/stop_service.sh"
    echo -e "   重启服务: sudo systemctl restart $SERVICE_NAME 或 $APP_HOME/restart_service.sh"
    echo -e "   查看状态: sudo systemctl status $SERVICE_NAME"
    echo -e "   查看日志: $APP_HOME/view_logs.sh (多种日志选项)"
    echo -e "   错误排查: $APP_HOME/debug.sh (一键诊断)"
    echo ""
    echo -e "${YELLOW}⚠️  重要提醒:${NC}"
    echo -e "   1. 确保 $APP_HOME/application.properties 中的配置正确"
    echo -e "   2. 如需修改配置，修改后重启: sudo systemctl restart $SERVICE_NAME"
    echo -e "   3. 确保防火墙已开放8993端口"
    echo -e "   4. 定期备份数据库和配置文件"
    echo ""
    echo -e "${YELLOW}📋 日志文件位置:${NC}"
    echo -e "   应用日志: $APP_HOME/logs/app.log (所有应用日志)"
    echo -e "   错误日志: $APP_HOME/logs/error.log (错误和异常信息)"
    echo -e "   系统日志: journalctl -u $SERVICE_NAME (systemd服务日志)"
}

# 创建管理脚本
create_management_scripts() {
    print_info "创建管理脚本..."
    
    # 创建启动脚本
    cat > "$APP_HOME/start_service.sh" << EOF
#!/bin/bash
sudo systemctl start $SERVICE_NAME
sudo systemctl status $SERVICE_NAME
EOF
    
    # 创建停止脚本
    cat > "$APP_HOME/stop_service.sh" << EOF
#!/bin/bash
sudo systemctl stop $SERVICE_NAME
sudo systemctl status $SERVICE_NAME
EOF
    
    # 创建重启脚本
    cat > "$APP_HOME/restart_service.sh" << EOF
#!/bin/bash
sudo systemctl restart $SERVICE_NAME
sudo systemctl status $SERVICE_NAME
EOF
    
    # 创建日志查看脚本
    cat > "$APP_HOME/view_logs.sh" << EOF
#!/bin/bash
echo "=== 选择要查看的日志 ==="
echo "1) systemd服务日志 (实时)"
echo "2) Python应用日志 (实时)" 
echo "3) Python错误日志 (实时)"
echo "4) Python应用日志 (最近100行)"
echo "5) Python错误日志 (最近100行)"
echo "6) 查看所有日志文件"
echo ""
read -p "请选择 [1-6]: " choice

case \$choice in
    1)
        echo "查看systemd服务日志 (Ctrl+C退出):"
        sudo journalctl -u $SERVICE_NAME -f
        ;;
    2)
        echo "查看Python应用日志 (Ctrl+C退出):"
        sudo tail -f $APP_HOME/logs/app.log
        ;;
    3)
        echo "查看Python错误日志 (Ctrl+C退出):"
        sudo tail -f $APP_HOME/logs/error.log
        ;;
    4)
        echo "Python应用日志 (最近100行):"
        sudo tail -n 100 $APP_HOME/logs/app.log
        ;;
    5)
        echo "Python错误日志 (最近100行):"
        sudo tail -n 100 $APP_HOME/logs/error.log
        ;;
    6)
        echo "所有日志文件:"
        ls -la $APP_HOME/logs/
        echo ""
        echo "查看特定文件: sudo cat $APP_HOME/logs/文件名"
        ;;
    *)
        echo "无效选择"
        ;;
esac
EOF
    
    # 创建备份脚本
    cat > "$APP_HOME/backup_config.sh" << EOF
#!/bin/bash
BACKUP_DIR="$APP_HOME/backups"
TIMESTAMP=\$(date +"%Y%m%d_%H%M%S")
BACKUP_FILE="\$BACKUP_DIR/config_backup_\$TIMESTAMP.tar.gz"

mkdir -p "\$BACKUP_DIR"
tar -czf "\$BACKUP_FILE" -C "$APP_HOME" application.properties 2>/dev/null
echo "配置备份完成: \$BACKUP_FILE"
EOF

    # 创建错误排查脚本
    cat > "$APP_HOME/debug.sh" << EOF
#!/bin/bash
echo "=== AI平台智能分析系统 - 错误排查工具 ==="
echo ""

echo "🔍 服务状态检查:"
sudo systemctl status $SERVICE_NAME --no-pager
echo ""

echo "🔍 进程检查:"
ps aux | grep python | grep -v grep
echo ""

echo "🔍 端口检查:"
netstat -tlnp 2>/dev/null | grep :8993 || ss -tlnp 2>/dev/null | grep :8993
echo ""

echo "🔍 Python环境检查:"
$APP_HOME/ai-platform-venv/bin/python --version
echo ""

echo "🔍 依赖检查:"
$APP_HOME/ai-platform-venv/bin/pip list | grep -E "(fastapi|uvicorn|sqlalchemy|mysqlclient)"
echo ""

echo "🔍 配置文件检查:"
if [ -f "$APP_HOME/application.properties" ]; then
    echo "✅ application.properties 存在"
    echo "   数据库配置: \$(grep 'db.prod.host' $APP_HOME/application.properties)"
else
    echo "❌ application.properties 不存在"
fi
echo ""

echo "🔍 日志文件:"
ls -la $APP_HOME/logs/ 2>/dev/null || echo "logs目录不存在"
echo ""

echo "🔍 最新错误 (最近10行):"
if [ -f "$APP_HOME/logs/error.log" ]; then
    sudo tail -n 10 $APP_HOME/logs/error.log
else
    echo "暂无错误日志"
fi
echo ""

echo "🔍 最新应用日志 (最近20行):"
if [ -f "$APP_HOME/logs/app.log" ]; then
    sudo tail -n 20 $APP_HOME/logs/app.log
else
    echo "暂无应用日志"
fi
echo ""

echo "💡 常用排查命令:"
echo "   查看实时日志: $APP_HOME/view_logs.sh"
echo "   重启服务: sudo systemctl restart $SERVICE_NAME"
echo "   手动启动: cd $APP_HOME && ./ai-platform-venv/bin/python main.py"
echo "   查看配置: cat $APP_HOME/application.properties"
EOF
    
    # 设置权限
    chmod +x "$APP_HOME"/*.sh
    
    print_success "管理脚本创建完成"
}

# 主函数
main() {
    print_banner
    
    print_info "开始部署 AI平台智能分析系统..."
    
    # 检查运行权限
    check_permissions
    
    # 执行部署流程
    detect_os
    install_system_dependencies
    check_system_dependencies
    create_app_user
    create_app_directory
    copy_application_files
    install_python_dependencies
    configure_application
    create_systemd_service
    create_nginx_config
    configure_firewall
    create_management_scripts
    start_services
    run_health_check
    
    # 显示结果
    show_deployment_result
}

# 信号处理
trap 'print_error "部署过程被中断"' INT TERM

# 执行主函数
main "$@"
