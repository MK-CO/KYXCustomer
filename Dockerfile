# AI Platform Smart - 优化版
FROM python:3.11-slim

# 设置工作目录
WORKDIR /app

# 设置环境变量
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV ENVIRONMENT=prod
ENV TZ=Asia/Shanghai

# 解决线程问题的关键环境变量
ENV UVICORN_WORKERS=1
ENV UVICORN_LIMIT_CONCURRENCY=100
ENV UVICORN_LIMIT_MAX_REQUESTS=1000
ENV UVICORN_TIMEOUT_KEEP_ALIVE=30

# 更换为国内apt源
RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/debian.sources

# 安装系统依赖
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    pkg-config \
    default-libmysqlclient-dev \
    curl \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# 设置时区
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 复制依赖文件并安装（使用阿里云pip源）
COPY requirements.txt .
RUN pip install --no-cache-dir -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com -r requirements.txt

# 复制应用代码
COPY . .

# 创建必要的目录
RUN mkdir -p /app/logs /app/data

# 暴露端口
EXPOSE 8993

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8993/health || exit 1

# 优化的启动命令 - 解决线程问题
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8993", "--workers", "1", "--limit-concurrency", "100", "--limit-max-requests", "1000", "--timeout-keep-alive", "30", "--access-log", "--log-level", "info"]
