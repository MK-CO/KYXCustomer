# AI平台智能分析系统 - 部署指南

## 📦 部署概述

本项目提供了完整的自动化部署解决方案，支持从本地打包到服务器一键部署的全流程。

## 🚀 快速开始

### 1. 本地打包

在项目根目录执行：

```bash
# 给打包脚本执行权限
chmod +x deploy/package.sh

# 执行打包
./deploy/package.sh
```

打包完成后会生成：
- `ai-platform-smart_YYYYMMDD_HHMMSS.zip` - 部署包
- `ai-platform-smart_YYYYMMDD_HHMMSS.zip.sha256` - 校验文件

### 2. 上传到服务器

将生成的zip文件上传到服务器：

```bash
# 使用scp上传
scp ai-platform-smart_*.zip root@your-server:/tmp/

# 或使用其他方式上传到服务器
```

### 3. 服务器部署

在服务器上执行：

```bash
# 解压部署包
cd /tmp
unzip ai-platform-smart_*.zip

# 进入应用目录
cd ai-platform-smart

# 执行一键部署
sudo chmod +x deploy/deploy.sh
sudo ./deploy/deploy.sh
```

### 4. 配置和启动

```bash
# 修改生产环境配置
sudo vi /opt/ai-platform-smart/application.properties

# 重启服务
sudo systemctl restart ai-platform-smart

# 检查服务状态
sudo systemctl status ai-platform-smart
```

## 📋 详细说明

### 打包脚本功能

`deploy/package.sh` 会自动：
- ✅ 检查系统依赖
- ✅ 清理构建目录
- ✅ 复制项目文件
- ✅ 创建生产环境配置模板
- ✅ 生成部署文档
- ✅ 创建版本信息和文件清单
- ✅ 打包成zip文件
- ✅ 计算文件校验和

### 部署脚本功能

`deploy/deploy.sh` 会自动：
- ✅ 检测操作系统类型
- ✅ 安装系统依赖 (Python3, pip, MySQL客户端等)
- ✅ 创建专用用户 `aiplatform`
- ✅ 创建应用目录 `/opt/ai-platform-smart`
- ✅ 安装Python虚拟环境和依赖
- ✅ 配置systemd服务
- ✅ 配置Nginx反向代理 (如果已安装)
- ✅ 配置防火墙规则
- ✅ 创建管理脚本
- ✅ 启动服务并进行健康检查

## 🔧 系统要求

### 服务器要求
- **操作系统**: Ubuntu 18.04+ / CentOS 7+ / Debian 9+
- **Python**: 3.8 或更高版本
- **内存**: 最低 2GB，推荐 4GB+
- **存储**: 最低 10GB 可用空间
- **数据库**: MySQL 5.7+ 或 8.0+

### 网络要求
- 服务器可访问互联网 (安装依赖)
- 开放端口 8993 (API服务)
- 开放端口 80/443 (如果使用Nginx)

## ⚙️ 配置说明

### 主配置文件

部署后需要修改 `/opt/ai-platform-smart/application.properties`:

```properties
# 数据库配置 - 必须修改
db.prod.host=your-database-host
db.prod.port=3306
db.prod.name=your-database-name
db.prod.user=your-database-user
db.prod.password=your-database-password

# API密钥配置 - 必须修改
volcengine.api.key=your-volcengine-api-key
siliconflow.api.key=your-siliconflow-api-key
```

### 环境变量配置

可以使用 `deploy/env.template` 作为参考创建 `.env` 文件。

## 🛠️ 服务管理

### 基本命令

```bash
# 启动服务
sudo systemctl start ai-platform-smart

# 停止服务
sudo systemctl stop ai-platform-smart

# 重启服务
sudo systemctl restart ai-platform-smart

# 查看状态
sudo systemctl status ai-platform-smart

# 查看日志
sudo journalctl -u ai-platform-smart -f
```

### 便捷脚本

部署后会在 `/opt/ai-platform-smart/scripts/` 目录生成管理脚本：

```bash
# 启动服务
/opt/ai-platform-smart/scripts/start.sh

# 停止服务
/opt/ai-platform-smart/scripts/stop.sh

# 重启服务
/opt/ai-platform-smart/scripts/restart.sh

# 查看日志
/opt/ai-platform-smart/scripts/logs.sh

# 备份配置
/opt/ai-platform-smart/scripts/backup.sh
```

## 🌐 访问地址

部署成功后可通过以下地址访问：

- **主页**: http://your-server:8993/ 或 http://your-server/ (使用Nginx)
- **API文档**: http://your-server:8993/docs
- **健康检查**: http://your-server:8993/health
- **调试页面**: http://your-server:8993/debug

## 🔒 安全配置

### Nginx配置 (推荐)

如果服务器安装了Nginx，部署脚本会自动配置反向代理：
- 隐藏内部端口8993
- 添加安全头
- 启用Gzip压缩
- 静态文件缓存
- 错误页面自定义

### 防火墙配置

部署脚本会自动配置防火墙：
- 开放端口8993 (API服务)
- 开放端口80/443 (HTTP/HTTPS)
- 其他端口保持关闭状态

### SSL/HTTPS配置

要启用HTTPS，需要：
1. 获取SSL证书
2. 修改 `/etc/nginx/sites-available/ai-platform-smart`
3. 取消注释HTTPS配置段
4. 重启Nginx

## 📊 监控和日志

### 系统日志

```bash
# 查看应用日志
sudo journalctl -u ai-platform-smart -f

# 查看Nginx日志
sudo tail -f /var/log/nginx/ai_platform_smart_access.log
sudo tail -f /var/log/nginx/ai_platform_smart_error.log

# 查看系统资源
htop
df -h
free -h
```

### 健康检查

```bash
# 检查服务状态
curl http://localhost:8993/health

# 检查API响应
curl -H "Authorization: Bearer YOUR_API_KEY" http://localhost:8993/api/v1/system/info
```

## 🔄 更新部署

### 在线更新

```bash
# 备份当前配置
sudo /opt/ai-platform-smart/scripts/backup.sh

# 停止服务
sudo systemctl stop ai-platform-smart

# 更新代码 (假设使用git)
cd /opt/ai-platform-smart
sudo -u aiplatform git pull

# 更新依赖
sudo -u aiplatform bash -c "source venv/bin/activate && pip install -r requirements.txt"

# 启动服务
sudo systemctl start ai-platform-smart
```

### 重新部署

对于重大更新，建议重新打包部署：
1. 在本地执行新的打包
2. 上传新的部署包
3. 备份现有配置
4. 重新执行部署脚本

## 🆘 故障排除

### 常见问题

#### 1. 服务无法启动
```bash
# 检查Python环境
/opt/ai-platform-smart/venv/bin/python --version

# 检查依赖
/opt/ai-platform-smart/venv/bin/pip list

# 检查配置文件
sudo -u aiplatform python /opt/ai-platform-smart/main.py --check-only
```

#### 2. 数据库连接失败
```bash
# 测试数据库连接
mysql -h your-db-host -u your-db-user -p

# 检查网络连通性
telnet your-db-host 3306
```

#### 3. API访问异常
```bash
# 检查端口监听
netstat -tlnp | grep 8993

# 检查防火墙
sudo ufw status
sudo firewall-cmd --list-all
```

#### 4. Nginx配置问题
```bash
# 测试Nginx配置
sudo nginx -t

# 重新加载配置
sudo systemctl reload nginx
```

### 获取帮助

如果遇到问题，可以：
1. 查看详细日志：`sudo journalctl -u ai-platform-smart -f`
2. 检查系统状态：`sudo systemctl status ai-platform-smart`
3. 查看错误信息：`sudo cat /var/log/nginx/ai_platform_smart_error.log`

## 📝 注意事项

1. **首次部署前务必修改数据库配置和API密钥**
2. **定期备份数据库和配置文件**
3. **监控系统资源使用情况**
4. **定期更新系统和依赖包**
5. **建议使用HTTPS保护API通信**
6. **生产环境建议删除或保护调试页面**
7. **配置日志轮转避免磁盘空间不足**

## 📞 技术支持

如需技术支持，请提供：
- 操作系统版本
- Python版本
- 错误日志
- 配置文件 (脱敏后)
- 部署步骤详情
