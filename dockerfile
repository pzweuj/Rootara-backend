# Rootara后端 - 优化版本
# docker run -e ROOTARA_API_KEY="your-actual-secret-key" -p <your_port>:8000 -v <your_path>:/data rootara-backend:latest

# 第一阶段：Go构建环境
FROM golang:1.23-alpine AS go-builder
WORKDIR /build
COPY scripts/rootara_reader.go .
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o rootara_reader rootara_reader.go

# 第二阶段：Python运行环境（优化版本）
FROM python:3.11.10-slim-bookworm

# 设置环境变量
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive \
    ROOTARA_API_KEY="rootara_api_key_default_001" \
    LOG_LEVEL="INFO" \
    LOG_CONSOLE="true" \
    LOG_STRUCTURED="false" \
    REDIS_HOST="localhost" \
    REDIS_PORT="6379" \
    REDIS_DB="0" \
    DB_MAX_CONNECTIONS="10" \
    DB_TIMEOUT="30.0" \
    CACHE_TTL="3600"

# 创建非root用户
RUN groupadd -r rootara && useradd -r -g rootara rootara

# 安装构建和运行时依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    # 构建依赖（用于编译pysam等包）
    gcc \
    g++ \
    make \
    libc6-dev \
    zlib1g-dev \
    libbz2-dev \
    liblzma-dev \
    libcurl4-openssl-dev \
    libssl-dev \
    # 运行时必需的库
    zlib1g \
    libbz2-1.0 \
    liblzma5 \
    libcurl4 \
    libssl3 \
    libsqlite3-0 \
    # 仅在需要时安装git
    git \
    # 添加procfs支持用于系统监控
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 创建requirements.txt用于更好的依赖管理
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt \
    # 清理构建依赖以减小镜像大小
    && apt-get update && apt-get remove -y --purge \
    gcc \
    g++ \
    make \
    libc6-dev \
    zlib1g-dev \
    libbz2-dev \
    liblzma-dev \
    libcurl4-openssl-dev \
    libssl-dev \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /tmp/* \
    && rm -rf /var/tmp/*

# 从Go构建阶段复制编译好的二进制文件
COPY --from=go-builder /build/rootara_reader /app/scripts/
RUN chmod +x /app/scripts/rootara_reader

# 设置工作目录
WORKDIR /app

# 克隆haplogrouper（优化：使用浅克隆）
RUN git clone --depth 1 --single-branch https://gitlab.com/bio_anth_decode/haploGrouper.git \
    && cd haploGrouper \
    && rm -rf .git \
    && cd ..

# 复制应用代码
COPY . .

# 创建必要的目录并设置权限
RUN mkdir -p /data /app/logs \
    && chown -R rootara:rootara /app /data \
    && chmod +x /app/scripts/*.py 2>/dev/null || true

# 切换到非root用户
USER rootara

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8000/health/live', timeout=5)" || exit 1

# 暴露端口
EXPOSE 8000

# 启动命令（优化：使用更好的生产设置）
CMD ["uvicorn", "main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "1", \
     "--loop", "uvloop", \
     "--http", "httptools", \
     "--access-log", \
     "--log-config", "/dev/null"]
