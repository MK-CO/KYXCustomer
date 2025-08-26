# Docker构建和部署指南

## 1. 本地构建amd64镜像

### 构建镜像
```bash
# 在项目根目录执行
docker build --platform linux/amd64 -t ai-platform-smart:latest .
```

### 验证镜像
```bash
# 查看镜像信息
docker images ai-platform-smart

# 检查镜像架构
docker inspect ai-platform-smart:latest | grep Architecture
```

## 2. 打包镜像为tar文件

### 导出镜像
```bash
# 导出为tar文件
docker save ai-platform-smart:latest -o ai-platform-smart-linux-amd64.tar

# 查看文件大小
ls -lh ai-platform-smart-linux-amd64.tar
```

## 3. 上传到服务器

### 使用scp上传
```bash
# 上传tar文件到服务器
scp ai-platform-smart-linux-amd64.tar root@your-server:/opt/
```

### 或使用其他方式上传
- 通过FTP/SFTP工具上传
- 通过云存储中转
- 通过wget下载（如果有公共下载链接）

## 4. 服务器端操作

### 4.1 准备配置文件
```bash
# 创建应用目录
mkdir -p /opt/ai-platform-smart/logs /opt/ai-platform-smart/data

# 复制配置文件到服务器（如果还没有的话）
# 可以从项目中复制 application.properties 到 /opt/ai-platform-smart/
# 或者手动创建并编辑配置文件
```

### 4.2 加载镜像
```bash
# 在服务器上加载镜像
docker load -i /opt/ai-platform-smart-linux-amd64.tar

# 验证镜像加载成功
docker images ai-platform-smart
```

### 4.3 停止并删除旧容器（如果存在）
```bash
# 停止旧容器
docker stop ai-platform-smart 2>/dev/null || true

# 删除旧容器
docker rm ai-platform-smart 2>/dev/null || true
```

### 4.4 运行新容器
```bash
# 确保配置文件存在
mkdir -p /opt/ai-platform-smart/logs /opt/ai-platform-smart/data

# 运行容器（挂载配置文件，添加 --privileged 解决线程创建问题）
docker run -d \
  --name ai-platform-smart \
  --restart unless-stopped \
  --privileged \
  -p 8993:8993 \
  -v /opt/ai-platform-smart/application.properties:/app/application.properties:ro \
  -v /opt/ai-platform-smart/logs:/app/logs \
  -v /opt/ai-platform-smart/data:/app/data \
  ai-platform-smart:latest
```

> **重要说明：** 添加了 `--privileged` 参数来解决HTML线程创建失败的问题。这给予容器更多的系统权限，通常可以解决线程和进程相关的权限问题。

### 4.5 查看容器状态
```bash
# 查看容器运行状态
docker ps | grep ai-platform-smart

# 查看容器日志
docker logs ai-platform-smart

# 实时查看日志
docker logs -f ai-platform-smart
```

## 5. 验证部署

### 5.1 健康检查
```bash
# 检查服务健康状态
curl http://localhost:8993/health

# 或从外部访问
curl http://your-server-ip:8993/health
```

### 5.2 访问应用
```bash
# 主页
http://your-server-ip:8993/

# API文档
http://your-server-ip:8993/docs

# 调试页面
http://your-server-ip:8993/debug
```

## 6. 启动调度器（重要！）

系统启动后调度器不会自动运行，需要手动启动：

### 方法1: 通过API启动
```bash
# 获取Bearer Token后，启动调度器
curl -X POST "http://your-server-ip:8993/api/v1/system/scheduler/start" \
  -H "Authorization: Bearer YOUR_API_KEY"

# 查看调度器状态
curl "http://your-server-ip:8993/api/v1/system/scheduler/status" \
  -H "Authorization: Bearer YOUR_API_KEY"
```

### 方法2: 通过Web界面启动
1. 访问 `http://your-server-ip:8993/docs`
2. 找到 `/system/scheduler/start` 接口
3. 输入API Key并执行

## 7. 常用管理命令

### 容器管理
```bash
# 启动容器
docker start ai-platform-smart

# 停止容器
docker stop ai-platform-smart

# 重启容器
docker restart ai-platform-smart

# 删除容器
docker rm ai-platform-smart

# 进入容器
docker exec -it ai-platform-smart bash
```

### 镜像管理
```bash
# 删除旧镜像
docker rmi ai-platform-smart:old-tag

# 清理未使用的镜像
docker image prune
```

### 日志管理
```bash
# 查看最近100行日志
docker logs --tail 100 ai-platform-smart

# 查看特定时间的日志
docker logs --since="2024-01-01T00:00:00" ai-platform-smart
```

## 8. 故障排查

### 如果容器无法启动
```bash
# 查看详细错误信息
docker logs ai-platform-smart

# 检查镜像是否正确
docker inspect ai-platform-smart:latest

# 尝试交互式运行
docker run -it --rm ai-platform-smart:latest bash
```

### 如果页面无法访问
```bash
# 检查端口是否开放
netstat -tlnp | grep 8993

# 检查防火墙设置
ufw status

# 检查容器端口映射
docker port ai-platform-smart
```

## 9. 优化建议

### 生产环境配置
```bash
# 使用更严格的资源限制（包含 --privileged 解决线程问题）
docker run -d \
  --name ai-platform-smart \
  --restart unless-stopped \
  --privileged \
  -p 8993:8993 \
  --memory="1g" \
  --cpus="1.0" \
  -v /opt/ai-platform-smart/application.properties:/app/application.properties:ro \
  -v /opt/ai-platform-smart/logs:/app/logs \
  -v /opt/ai-platform-smart/data:/app/data \
  ai-platform-smart:latest
```

### 备份重要数据
```bash
# 备份配置文件
cp /opt/ai-platform-smart/application.properties /opt/backup/

# 备份日志
tar -czf logs_backup_$(date +%Y%m%d).tar.gz /opt/ai-platform-smart/logs/
```

---

## 总结

1. **本地构建** → `docker build --platform linux/amd64 -t ai-platform-smart:latest .`
2. **打包镜像** → `docker save ai-platform-smart:latest -o ai-platform-smart-linux-amd64.tar`
3. **上传服务器** → `scp ai-platform-smart-linux-amd64.tar root@server:/opt/`
4. **加载镜像** → `docker load -i /opt/ai-platform-smart-linux-amd64.tar`
5. **运行容器** → `docker run -d --name ai-platform-smart -p 8993:8993 ai-platform-smart:latest`
6. **手动启动调度器** → 通过API或Web界面启动

**注意：** 系统启动后需要手动启动调度器，这样可以避免刚部署完就开始执行任务的问题。
