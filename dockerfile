# Rootara后端
# docker build --build-arg PYTHON_VERSION=3.11 -t rootara_backend:v0.0.1 .

# 使用一个具体的 Python 版本，例如 bullseye 或 bookworm
ARG PYTHON_VERSION=3.13.3
FROM python:${PYTHON_VERSION}-slim-bookworm

# 设置环境变量，避免 Python 生成 .pyc 文件和缓冲输出
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
# 防止 apt-get 在构建时进行交互式提问
ENV DEBIAN_FRONTEND=noninteractive

# ---- Go 安装 ----
# Go 版本可以作为构建参数传入
ARG GO_VERSION=1.23.5
ENV GOROOT /usr/local/go
ENV GOPATH /go
ENV PATH $GOPATH/bin:$GOROOT/bin:$PATH

# ---- 系统依赖和 Python 库安装 ----
# 将所有 apt-get 和 pip 操作尽可能合并到一个 RUN 指令中以减少层数
RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir -p /etc/apt/sources.list.d && rm -f /etc/apt/sources.list.d/*.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    # Go 安装需要的工具
    curl \
    ca-certificates \
    # Pysam 和 Pandas (NumPy) 可能需要的编译依赖
    build-essential \
    gcc \
    pkg-config \
    # Pysam 依赖
    zlib1g-dev \
    libbz2-dev \
    liblzma-dev \
    # pysam 可能需要，用于htsfile 通过 https 读取
    libcurl4-openssl-dev \
    # 通用 SSL
    libssl-dev \
    # (可选) 如果需要编译 Python sqlite3 扩展或某些包需要它
    libsqlite3-dev \
    git \
    # 安装 Go
    && curl -fsSL "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz" -o go.tar.gz \
    && tar -C /usr/local -xzf go.tar.gz \
    && rm go.tar.gz \
    # 验证 Go 安装
    && go version \
    # 创建 GOPATH 目录
    && mkdir -p "$GOPATH/src" "$GOPATH/bin" && chmod -R 777 "$GOPATH" \
    # 安装 Python 库
    # argparse 和 tempfile 是标准库，不需要通过 pip 安装
    # sqlite3 Python 模块通常是内置的
    && pip install --no-cache-dir \
    pandas \
    pysam \
    # 清理 apt 缓存和不再需要的构建依赖
    && apt-get purge -y --auto-remove \
    build-essential \
    gcc \
    # 保留 pkg-config 如果某些运行时库可能还需要它，否则也可以移除
    # 保留 *-dev 包的运行时库 (例如 zlib1g, libbz2-1.0, liblzma5)
    # libsqlite3-dev 也可以在这里移除，因为 sqlite3 模块通常链接到 libsqlite3-0
    libsqlite3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# 设置工作目录
WORKDIR /app

# 安装 admix 和 haplogrouper
RUN pip install --no-cache-dir git+https://github.com/stevenliuyi/admix && \
    git clone https://gitlab.com/bio_anth_decode/haploGrouper.git

# 加入其他脚本
# COPY . /app/rootara_scripts

# 将你的应用代码复制到镜像中
# COPY . .

# 暴露你应用运行的端口 (根据你的后端应用修改)
# EXPOSE 8000

# 定义容器启动时运行的命令 (根据你的后端应用修改)
# CMD ["python", "your_main_script.py"]
# 或者如果你用 gunicorn/uvicorn:
# CMD ["gunicorn", "-b", "0.0.0.0:8000", "your_project.wsgi:application"]


