# Rootara后端
# 第一阶段：Go构建环境
FROM golang:1.23-alpine AS go-builder
WORKDIR /build
COPY scripts/rootara_reader.go .
RUN go build -o rootara_reader rootara_reader.go

# 第二阶段：Python运行环境
FROM python:3.13.3-slim-bookworm

# 设置环境变量
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive

# 安装运行时依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    # 运行时必需的库
    zlib1g \
    libbz2-1.0 \
    liblzma5 \
    libcurl4 \
    libssl3 \
    libsqlite3-0 \
    git \
    # 清理
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 安装Python依赖
RUN pip install --no-cache-dir \
    pandas \
    pysam \
    fastapi \
    uvicorn \
    pydantic \
    git+https://github.com/stevenliuyi/admix

# 从Go构建阶段复制编译好的二进制文件
COPY --from=go-builder /build/rootara_reader /app/scripts/

# 设置工作目录
WORKDIR /app

# 克隆haplogrouper
RUN git clone --depth 1 https://gitlab.com/bio_anth_decode/haploGrouper.git

# 复制应用代码
COPY . .

# 暴露端口
EXPOSE 8000

# 启动命令
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
