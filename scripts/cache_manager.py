# coding=utf-8
"""
Redis缓存管理器
提供统一的缓存接口，支持数据缓存和失效管理
"""

import json
import redis
import os
import logging
from typing import Any, Optional, Union
from datetime import timedelta

# 配置日志
logger = logging.getLogger(__name__)

class CacheManager:
    """Redis缓存管理器"""

    def __init__(self):
        """初始化Redis连接"""
        self.redis_host = os.environ.get("REDIS_HOST", "localhost")
        self.redis_port = int(os.environ.get("REDIS_PORT", "6379"))
        self.redis_db = int(os.environ.get("REDIS_DB", "0"))
        self.redis_password = os.environ.get("REDIS_PASSWORD", None)

        # 默认缓存过期时间 (1小时)
        self.default_ttl = int(os.environ.get("CACHE_TTL", "3600"))

        # 连接池配置
        self.connection_pool = redis.ConnectionPool(
            host=self.redis_host,
            port=self.redis_port,
            db=self.redis_db,
            password=self.redis_password,
            decode_responses=True,
            max_connections=20,
            retry_on_timeout=True,
            socket_keepalive=True,
            socket_keepalive_options={},
            health_check_interval=30
        )

        try:
            self.redis_client = redis.Redis(connection_pool=self.connection_pool)
            # 测试连接
            self.redis_client.ping()
            self.is_available = True
            logger.info(f"Redis连接成功: {self.redis_host}:{self.redis_port}/{self.redis_db}")
        except Exception as e:
            logger.warning(f"Redis连接失败: {e}，将使用内存缓存作为后备")
            self.redis_client = None
            self.is_available = False
            # 使用简单的内存缓存作为后备
            self._memory_cache = {}

    def _get_key(self, category: str, identifier: str) -> str:
        """生成缓存键名"""
        return f"rootara:{category}:{identifier}"

    def get(self, category: str, identifier: str) -> Optional[Any]:
        """获取缓存数据"""
        cache_key = self._get_key(category, identifier)

        try:
            if self.is_available:
                # 使用Redis
                cached_data = self.redis_client.get(cache_key)
                if cached_data:
                    return json.loads(cached_data)
            else:
                # 使用内存缓存
                return self._memory_cache.get(cache_key)
        except Exception as e:
            logger.error(f"获取缓存失败 {cache_key}: {e}")

        return None

    def set(self, category: str, identifier: str, data: Any, ttl: Optional[int] = None) -> bool:
        """设置缓存数据"""
        cache_key = self._get_key(category, identifier)
        ttl = ttl or self.default_ttl

        try:
            if self.is_available:
                # 使用Redis
                serialized_data = json.dumps(data, ensure_ascii=False)
                return self.redis_client.setex(cache_key, ttl, serialized_data)
            else:
                # 使用内存缓存（简单实现，不支持TTL）
                self._memory_cache[cache_key] = data
                return True
        except Exception as e:
            logger.error(f"设置缓存失败 {cache_key}: {e}")
            return False

    def delete(self, category: str, identifier: str) -> bool:
        """删除缓存数据"""
        cache_key = self._get_key(category, identifier)

        try:
            if self.is_available:
                return bool(self.redis_client.delete(cache_key))
            else:
                return bool(self._memory_cache.pop(cache_key, None))
        except Exception as e:
            logger.error(f"删除缓存失败 {cache_key}: {e}")
            return False

    def delete_pattern(self, pattern: str) -> int:
        """删除匹配模式的缓存数据"""
        try:
            if self.is_available:
                keys = self.redis_client.keys(f"rootara:{pattern}")
                if keys:
                    return self.redis_client.delete(*keys)
                return 0
            else:
                # 内存缓存简单实现
                keys_to_delete = [k for k in self._memory_cache.keys() if pattern in k]
                for key in keys_to_delete:
                    del self._memory_cache[key]
                return len(keys_to_delete)
        except Exception as e:
            logger.error(f"删除模式缓存失败 {pattern}: {e}")
            return 0

    def clear_all(self) -> bool:
        """清空所有Rootara相关缓存"""
        try:
            if self.is_available:
                keys = self.redis_client.keys("rootara:*")
                if keys:
                    return bool(self.redis_client.delete(*keys))
                return True
            else:
                self._memory_cache.clear()
                return True
        except Exception as e:
            logger.error(f"清空缓存失败: {e}")
            return False

    def get_stats(self) -> dict:
        """获取缓存统计信息"""
        try:
            if self.is_available:
                info = self.redis_client.info()
                return {
                    "type": "redis",
                    "connected_clients": info.get("connected_clients", 0),
                    "used_memory": info.get("used_memory_human", "unknown"),
                    "total_commands_processed": info.get("total_commands_processed", 0),
                    "keyspace_hits": info.get("keyspace_hits", 0),
                    "keyspace_misses": info.get("keyspace_misses", 0),
                    "uptime_in_seconds": info.get("uptime_in_seconds", 0)
                }
            else:
                return {
                    "type": "memory",
                    "cached_items": len(self._memory_cache)
                }
        except Exception as e:
            logger.error(f"获取缓存统计失败: {e}")
            return {"type": "error", "message": str(e)}

# 全局缓存实例
cache_manager = CacheManager()

# 缓存装饰器
def cache_result(category: str, ttl: Optional[int] = None):
    """缓存函数结果的装饰器"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            # 生成缓存标识符
            cache_id = f"{func.__name__}:{hash(str(args) + str(sorted(kwargs.items())))}"

            # 尝试从缓存获取
            cached_result = cache_manager.get(category, cache_id)
            if cached_result is not None:
                logger.debug(f"缓存命中: {category}:{cache_id}")
                return cached_result

            # 执行函数并缓存结果
            result = func(*args, **kwargs)
            if result is not None:
                cache_manager.set(category, cache_id, result, ttl)
                logger.debug(f"缓存设置: {category}:{cache_id}")

            return result
        return wrapper
    return decorator

# 常用缓存类别
class CacheCategories:
    ADMIXTURE = "admixture"           # 祖源分析结果
    HAPLOGROUP = "haplogroup"         # 单倍群分析结果
    TRAITS = "traits"                 # 特征分析结果
    CLINVAR = "clinvar"               # ClinVar数据
    REPORTS = "reports"               # 报告信息
    USERS = "users"                   # 用户信息
    SNP_INFO = "snp_info"            # SNP位点信息