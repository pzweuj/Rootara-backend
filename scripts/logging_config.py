# coding=utf-8
"""
日志配置模块
提供统一的日志配置和管理
"""

import logging
import logging.handlers
import os
import sys
import json
from datetime import datetime
from typing import Optional, Dict, Any

class StructuredFormatter(logging.Formatter):
    """结构化日志格式器"""

    def format(self, record: logging.LogRecord) -> str:
        # 基础日志信息
        log_data = {
            'timestamp': datetime.fromtimestamp(record.created).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line': record.lineno
        }

        # 添加异常信息
        if record.exc_info:
            log_data['exception'] = self.formatException(record.exc_info)

        # 添加自定义字段
        if hasattr(record, 'extra_data'):
            log_data.update(record.extra_data)

        # 添加请求相关信息（如果有）
        for attr in ['request_id', 'user_id', 'report_id', 'api_endpoint', 'duration']:
            if hasattr(record, attr):
                log_data[attr] = getattr(record, attr)

        return json.dumps(log_data, ensure_ascii=False, separators=(',', ':'))

class ColoredConsoleFormatter(logging.Formatter):
    """彩色控制台格式器"""

    COLORS = {
        'DEBUG': '\033[36m',     # 青色
        'INFO': '\033[32m',      # 绿色
        'WARNING': '\033[33m',   # 黄色
        'ERROR': '\033[31m',     # 红色
        'CRITICAL': '\033[35m',  # 紫色
        'RESET': '\033[0m'       # 重置
    }

    def format(self, record: logging.LogRecord) -> str:
        if sys.stdout.isatty():  # 只在终端中使用颜色
            color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
            record.levelname = f"{color}{record.levelname}{self.COLORS['RESET']}"

        return super().format(record)

def setup_logging(
    log_level: str = None,
    log_file: str = None,
    enable_console: bool = True,
    enable_structured: bool = False,
    max_file_size: int = 50 * 1024 * 1024,  # 50MB
    backup_count: int = 5
) -> None:
    """
    设置日志配置

    Args:
        log_level: 日志级别 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: 日志文件路径
        enable_console: 是否启用控制台输出
        enable_structured: 是否使用结构化日志格式
        max_file_size: 日志文件最大大小（字节）
        backup_count: 日志文件备份数量
    """

    # 从环境变量获取配置
    log_level = log_level or os.environ.get('LOG_LEVEL', 'INFO').upper()
    log_file = log_file or os.environ.get('LOG_FILE')
    enable_console = enable_console and os.environ.get('LOG_CONSOLE', 'true').lower() == 'true'
    enable_structured = enable_structured or os.environ.get('LOG_STRUCTURED', 'false').lower() == 'true'

    # 验证日志级别
    numeric_level = getattr(logging, log_level, None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'无效的日志级别: {log_level}')

    # 获取根日志器
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)

    # 清除现有处理器
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # 控制台处理器
    if enable_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(numeric_level)

        if enable_structured:
            console_formatter = StructuredFormatter()
        else:
            console_formatter = ColoredConsoleFormatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )

        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)

    # 文件处理器
    if log_file:
        # 确保日志目录存在
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)

        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=max_file_size,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(numeric_level)

        if enable_structured:
            file_formatter = StructuredFormatter()
        else:
            file_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s'
            )

        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)

    # 设置第三方库的日志级别
    logging.getLogger('uvicorn').setLevel(logging.INFO)
    logging.getLogger('uvicorn.access').setLevel(logging.WARNING)
    logging.getLogger('fastapi').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)

    # 记录日志配置信息
    logger = logging.getLogger(__name__)
    logger.info(f"日志系统已初始化 - 级别: {log_level}, 控制台: {enable_console}, 文件: {log_file}")

class RequestLogger:
    """请求日志记录器"""

    def __init__(self, logger_name: str = 'rootara.requests'):
        self.logger = logging.getLogger(logger_name)

    def log_request(
        self,
        method: str,
        endpoint: str,
        request_id: str,
        user_id: Optional[str] = None,
        start_time: Optional[float] = None,
        duration: Optional[float] = None,
        status_code: Optional[int] = None,
        error: Optional[str] = None,
        extra_data: Optional[Dict[str, Any]] = None
    ):
        """记录API请求日志"""

        log_data = {
            'request_id': request_id,
            'method': method,
            'endpoint': endpoint,
            'user_id': user_id,
            'status_code': status_code,
            'duration': duration,
            'extra_data': extra_data or {}
        }

        # 创建带有额外信息的日志记录
        extra = {}
        for key, value in log_data.items():
            if value is not None:
                extra[key] = value

        if error:
            self.logger.error(f"请求失败: {method} {endpoint} - {error}", extra=extra)
        elif status_code and status_code >= 400:
            self.logger.warning(f"请求错误: {method} {endpoint}", extra=extra)
        else:
            self.logger.info(f"请求成功: {method} {endpoint}", extra=extra)

class PerformanceLogger:
    """性能日志记录器"""

    def __init__(self, logger_name: str = 'rootara.performance'):
        self.logger = logging.getLogger(logger_name)

    def log_database_query(
        self,
        query_type: str,
        table: str,
        duration: float,
        cache_hit: bool = False,
        record_count: Optional[int] = None
    ):
        """记录数据库查询性能"""

        extra = {
            'query_type': query_type,
            'table': table,
            'duration': duration,
            'cache_hit': cache_hit,
            'record_count': record_count
        }

        if cache_hit:
            self.logger.debug(f"缓存命中: {query_type} {table}", extra=extra)
        elif duration > 1.0:  # 超过1秒的查询
            self.logger.warning(f"慢查询: {query_type} {table} ({duration:.2f}s)", extra=extra)
        else:
            self.logger.debug(f"数据库查询: {query_type} {table} ({duration:.3f}s)", extra=extra)

    def log_cache_operation(
        self,
        operation: str,
        category: str,
        key: str,
        hit: bool = None,
        duration: Optional[float] = None
    ):
        """记录缓存操作"""

        extra = {
            'operation': operation,
            'category': category,
            'key': key,
            'hit': hit,
            'duration': duration
        }

        message = f"缓存{operation}: {category}:{key}"
        if hit is not None:
            message += f" ({'命中' if hit else '未命中'})"

        self.logger.debug(message, extra=extra)

# 全局日志器实例
request_logger = RequestLogger()
performance_logger = PerformanceLogger()

# 初始化日志系统
def init_logging():
    """初始化日志系统"""
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("Rootara 后端服务启动")