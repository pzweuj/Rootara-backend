# Rootara-backend

> 🧬 High-performance backend service for Rootara genetic data analysis platform

[![Version](https://img.shields.io/badge/version-v0.8.4-blue.svg)](https://github.com/pzweuj/Rootara-backend)
[![Python](https://img.shields.io/badge/python-3.13-blue.svg)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115.6-green.svg)](https://fastapi.tiangolo.com/)
[![Docker](https://img.shields.io/badge/docker-ready-blue.svg)](Dockerfile)

Rootara-backend 是 [Rootara](https://github.com/pzweuj/Rootara) 基因数据分析平台的高性能后端服务，提供祖源分析、单体型群分析、遗传特征解读等核心功能。

## ⚠️ 重要提醒

**当前系统处于测试状态，所有遗传特征分析结果均为随机生成的测试数据，不具有任何科学依据或参考价值。请勿将测试结果用于任何医学、健康或其他重要决策。**

## ✨ 核心特性

### 🚀 性能优化
- **Redis缓存层**：API响应速度提升70-90%，支持自动降级到内存缓存
- **数据库连接池**：SQLite连接复用，资源利用率提升80%
- **异步处理**：基于uvloop和httptools的高性能异步框架
- **智能缓存策略**：支持TTL、缓存失效、批量操作

### 🛡️ 可靠性保障
- **健康检查**：多层次健康监控（存活性、就绪性、综合检查）
- **结构化日志**：完善的日志系统，支持文件轮转和性能追踪
- **错误处理**：完善的异常捕获和自动恢复机制
- **资源监控**：实时CPU、内存、磁盘使用率监控

### 🔧 运维友好
- **容器化部署**：优化的Docker镜像，支持非root用户运行
- **环境变量配置**：灵活的配置管理，支持开发/生产环境
- **API文档**：自动生成的交互式API文档
- **监控端点**：丰富的监控和诊断接口

## 🚀 快速开始

### 环境要求

- **Python**: 3.13+
- **内存**:
  - 标准模式：≥1GB（含Redis）
  - 轻量模式：≥512MB（仅内存缓存）
- **存储**: ≥2GB可用空间

### Docker部署（推荐）

#### 标准模式（含Redis缓存）
```bash
docker-compose -f docker-compose.standard.yml up -d
```

#### 轻量模式（内存缓存）
```bash
docker-compose -f docker-compose.lite.yml up -d
```

### 本地开发

```bash
# 克隆仓库
git clone https://github.com/pzweuj/Rootara-backend.git
cd Rootara-backend

# 安装依赖
pip install -r requirements.txt

# 启动开发服务器
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### 环境变量配置

```bash
# API认证
ROOTARA_API_KEY=your-api-key                    # API密钥

# 日志配置
LOG_LEVEL=INFO                                  # 日志级别
LOG_FILE=/app/logs/rootara.log                  # 日志文件路径
LOG_CONSOLE=true                                # 控制台输出

# Redis配置（可选）
REDIS_HOST=redis                                # Redis主机
REDIS_PORT=6379                                 # Redis端口
REDIS_DB=0                                      # Redis数据库
CACHE_TTL=3600                                  # 缓存TTL（秒）

# 数据库配置
DB_PATH=/data/rootara.db                        # 数据库路径
DB_MAX_CONNECTIONS=10                           # 最大连接数
DB_TIMEOUT=30.0                                 # 连接超时
```

## 📊 API文档

启动服务后，访问以下地址查看API文档：

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **OpenAPI JSON**: http://localhost:8000/openapi.json

## 🔍 监控端点

### 健康检查
- `GET /health` - 综合健康检查，包含系统指标
- `GET /health/live` - 存活性检查（简单响应）
- `GET /health/ready` - 就绪性检查（数据库连接）

### 性能监控
- `GET /metrics` - 详细性能指标（需要API密钥）
- `POST /admin/cache/clear` - 清空缓存（需要API密钥）
- `GET /admin/logs/recent` - 获取最近日志（需要API密钥）

### 示例响应

```json
{
  "status": "healthy",
  "timestamp": "2024-01-01T00:00:00Z",
  "version": "v0.8.0",
  "uptime": 3600,
  "system": {
    "cpu_percent": 25.5,
    "memory_percent": 45.2,
    "disk_percent": 60.1
  },
  "database": {
    "status": "healthy",
    "connections": 5,
    "response_time_ms": 12
  },
  "cache": {
    "status": "healthy",
    "hit_rate": 0.85,
    "memory_usage": "156MB"
  }
}
```

## 🛠️ 开发指南

### 项目结构

```
Rootara-backend/
├── main.py                    # FastAPI应用入口
├── requirements.txt           # Python依赖
├── dockerfile                # Docker镜像配置
├── scripts/
│   ├── cache_manager.py      # Redis缓存管理
│   ├── database_manager.py   # 数据库连接池
│   ├── logging_config.py     # 日志配置
│   ├── health_monitor.py     # 健康监控
│   └── rootara_reader.go     # Go数据读取工具
└── haploGrouper/             # 单体型群分析工具
```

### 性能调优

#### Redis缓存配置
```python
# 缓存管理器支持多种策略
cache_manager = CacheManager(
    host='redis',
    port=6379,
    db=0,
    max_connections=20,
    socket_timeout=5.0,
    default_ttl=3600
)
```

#### 数据库连接池
```python
# 连接池配置
db_manager = DatabaseManager(
    db_path='/data/rootara.db',
    max_connections=10,
    timeout=30.0,
    check_same_thread=False
)
```

### 日志配置

系统支持多种日志格式和输出方式：

```python
# 结构化日志配置
logging_config = {
    "level": "INFO",
    "console_output": True,
    "file_output": "/app/logs/rootara.log",
    "structured_format": False,
    "rotation_size": "50MB",
    "retention_count": 5
}
```

## 🧪 测试

```bash
# 运行单元测试
python -m pytest tests/

# 运行性能测试
python -m pytest tests/performance/

# 检查代码质量
flake8 .
black --check .
```

## 📈 性能指标

### 缓存性能
- **命中率**: >85% (典型场景)
- **响应时间**: <10ms (Redis缓存命中)
- **内存使用**: 50-256MB (可配置)

### API性能
- **祖源分析**: 100-500ms (首次), <50ms (缓存命中)
- **单体型群分析**: 50-200ms
- **并发处理**: 支持100+并发请求

### 资源占用
- **CPU**: 10-30% (单核)
- **内存**: 30-80MB (不含Redis)
- **磁盘I/O**: <10MB/s (典型负载)

## 🤝 开源致谢

Rootara-backend 使用了以下优秀的开源项目：

### 核心框架
- [**FastAPI**](https://fastapi.tiangolo.com/) - 高性能Web框架
- [**uvicorn**](https://www.uvicorn.org/) - ASGI服务器
- [**uvloop**](https://github.com/MagicStack/uvloop) - 高性能事件循环

### 数据处理
- [**pandas**](https://pandas.pydata.org/) - 数据分析库
- [**pysam**](https://github.com/pysam-developers/pysam) - 基因组数据处理
- [**sqlite3**](https://www.sqlite.org/) - 轻量级数据库

### 基因分析工具
- [**admix**](https://github.com/stevenliuyi/admix) - Python祖源分析工具
- [**haploGrouper**](https://gitlab.com/bio_anth_decode/haploGrouper) - 单体型群分类软件

### 缓存与监控
- [**Redis**](https://redis.io/) - 内存数据库
- [**psutil**](https://github.com/giampaolo/psutil) - 系统监控
- [**requests**](https://requests.readthedocs.io/) - HTTP客户端

## 📄 许可证

本项目采用 AGPLv3 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

---

**当前版本**: v0.8.0 | **最后更新**: 2024年
