# coding=utf-8
"""
健康检查和系统监控模块
提供系统健康状态检查和资源监控功能
"""

import os
import time
import psutil
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)

@dataclass
class HealthStatus:
    """健康状态数据类"""
    status: str  # healthy, degraded, unhealthy
    timestamp: str
    uptime_seconds: float
    checks: Dict[str, Any]
    details: Optional[Dict[str, Any]] = None

class SystemMonitor:
    """系统资源监控器"""

    def __init__(self):
        self.start_time = time.time()
        self.process = psutil.Process()

    def get_cpu_info(self) -> Dict[str, Any]:
        """获取CPU信息"""
        try:
            cpu_percent = psutil.cpu_percent(interval=1)
            cpu_count = psutil.cpu_count()
            load_avg = os.getloadavg() if hasattr(os, 'getloadavg') else [0, 0, 0]

            return {
                'cpu_percent': cpu_percent,
                'cpu_count': cpu_count,
                'load_average': {
                    '1min': load_avg[0],
                    '5min': load_avg[1],
                    '15min': load_avg[2]
                }
            }
        except Exception as e:
            logger.error(f"获取CPU信息失败: {e}")
            return {'error': str(e)}

    def get_memory_info(self) -> Dict[str, Any]:
        """获取内存信息"""
        try:
            memory = psutil.virtual_memory()
            swap = psutil.swap_memory()

            return {
                'virtual_memory': {
                    'total': memory.total,
                    'available': memory.available,
                    'used': memory.used,
                    'percent': memory.percent
                },
                'swap_memory': {
                    'total': swap.total,
                    'used': swap.used,
                    'percent': swap.percent
                }
            }
        except Exception as e:
            logger.error(f"获取内存信息失败: {e}")
            return {'error': str(e)}

    def get_disk_info(self) -> Dict[str, Any]:
        """获取磁盘信息"""
        try:
            disk_usage = psutil.disk_usage('/')

            # 获取数据目录磁盘使用情况
            data_path = '/data'
            data_disk_usage = None
            if os.path.exists(data_path):
                data_disk_usage = psutil.disk_usage(data_path)

            result = {
                'root_partition': {
                    'total': disk_usage.total,
                    'used': disk_usage.used,
                    'free': disk_usage.free,
                    'percent': (disk_usage.used / disk_usage.total) * 100
                }
            }

            if data_disk_usage:
                result['data_partition'] = {
                    'total': data_disk_usage.total,
                    'used': data_disk_usage.used,
                    'free': data_disk_usage.free,
                    'percent': (data_disk_usage.used / data_disk_usage.total) * 100
                }

            return result
        except Exception as e:
            logger.error(f"获取磁盘信息失败: {e}")
            return {'error': str(e)}

    def get_process_info(self) -> Dict[str, Any]:
        """获取进程信息"""
        try:
            with self.process.oneshot():
                memory_info = self.process.memory_info()
                cpu_percent = self.process.cpu_percent()

                return {
                    'pid': self.process.pid,
                    'memory_rss': memory_info.rss,
                    'memory_vms': memory_info.vms,
                    'cpu_percent': cpu_percent,
                    'num_threads': self.process.num_threads(),
                    'create_time': self.process.create_time(),
                    'uptime_seconds': time.time() - self.start_time
                }
        except Exception as e:
            logger.error(f"获取进程信息失败: {e}")
            return {'error': str(e)}

    def get_network_info(self) -> Dict[str, Any]:
        """获取网络信息"""
        try:
            net_io = psutil.net_io_counters()
            connections = len(psutil.net_connections())

            return {
                'bytes_sent': net_io.bytes_sent,
                'bytes_recv': net_io.bytes_recv,
                'packets_sent': net_io.packets_sent,
                'packets_recv': net_io.packets_recv,
                'connections_count': connections
            }
        except Exception as e:
            logger.error(f"获取网络信息失败: {e}")
            return {'error': str(e)}

class HealthChecker:
    """健康检查器"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.monitor = SystemMonitor()

    def check_database_health(self) -> Dict[str, Any]:
        """检查数据库健康状态"""
        try:
            from .database_manager import get_db_pool

            start_time = time.time()
            db_pool = get_db_pool(self.db_path)

            # 测试数据库连接
            with db_pool.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()

            response_time = time.time() - start_time
            pool_stats = db_pool.get_stats()

            return {
                'status': 'healthy',
                'response_time_ms': round(response_time * 1000, 2),
                'pool_stats': pool_stats,
                'database_file_exists': os.path.exists(self.db_path),
                'database_size_bytes': os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
            }

        except Exception as e:
            logger.error(f"数据库健康检查失败: {e}")
            return {
                'status': 'unhealthy',
                'error': str(e),
                'database_file_exists': os.path.exists(self.db_path)
            }

    def check_cache_health(self) -> Dict[str, Any]:
        """检查缓存健康状态"""
        try:
            from .cache_manager import cache_manager

            start_time = time.time()

            # 测试缓存连接
            test_key = f"health_check_{int(time.time())}"
            test_value = {'test': True, 'timestamp': time.time()}

            # 测试设置和获取
            cache_manager.set('health', test_key, test_value, ttl=10)
            retrieved_value = cache_manager.get('health', test_key)

            response_time = time.time() - start_time
            cache_stats = cache_manager.get_stats()

            # 清理测试数据
            cache_manager.delete('health', test_key)

            is_working = retrieved_value is not None and retrieved_value.get('test') is True

            return {
                'status': 'healthy' if is_working else 'degraded',
                'type': cache_stats.get('type', 'unknown'),
                'available': cache_manager.is_available,
                'response_time_ms': round(response_time * 1000, 2),
                'stats': cache_stats
            }

        except Exception as e:
            logger.error(f"缓存健康检查失败: {e}")
            return {
                'status': 'unhealthy',
                'error': str(e),
                'available': False
            }

    def check_disk_space(self) -> Dict[str, Any]:
        """检查磁盘空间"""
        try:
            disk_info = self.monitor.get_disk_info()

            # 检查根分区
            root_status = 'healthy'
            root_percent = disk_info.get('root_partition', {}).get('percent', 0)
            if root_percent > 90:
                root_status = 'critical'
            elif root_percent > 80:
                root_status = 'warning'

            # 检查数据分区
            data_status = 'healthy'
            data_percent = disk_info.get('data_partition', {}).get('percent', 0)
            if data_percent > 90:
                data_status = 'critical'
            elif data_percent > 80:
                data_status = 'warning'

            overall_status = 'healthy'
            if root_status == 'critical' or data_status == 'critical':
                overall_status = 'critical'
            elif root_status == 'warning' or data_status == 'warning':
                overall_status = 'warning'

            return {
                'status': overall_status,
                'root_partition': root_status,
                'data_partition': data_status,
                'details': disk_info
            }

        except Exception as e:
            logger.error(f"磁盘空间检查失败: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }

    def check_memory_usage(self) -> Dict[str, Any]:
        """检查内存使用情况"""
        try:
            memory_info = self.monitor.get_memory_info()

            memory_percent = memory_info.get('virtual_memory', {}).get('percent', 0)
            swap_percent = memory_info.get('swap_memory', {}).get('percent', 0)

            status = 'healthy'
            if memory_percent > 90 or swap_percent > 50:
                status = 'critical'
            elif memory_percent > 80 or swap_percent > 25:
                status = 'warning'

            return {
                'status': status,
                'memory_percent': memory_percent,
                'swap_percent': swap_percent,
                'details': memory_info
            }

        except Exception as e:
            logger.error(f"内存检查失败: {e}")
            return {
                'status': 'error',
                'error': str(e)
            }

    def perform_health_check(self) -> HealthStatus:
        """执行完整的健康检查"""
        start_time = time.time()

        checks = {
            'database': self.check_database_health(),
            'cache': self.check_cache_health(),
            'disk_space': self.check_disk_space(),
            'memory': self.check_memory_usage(),
        }

        # 确定整体健康状态
        overall_status = 'healthy'
        critical_issues = []
        warnings = []

        for check_name, check_result in checks.items():
            status = check_result.get('status', 'unknown')

            if status in ['unhealthy', 'critical', 'error']:
                overall_status = 'unhealthy'
                critical_issues.append(check_name)
            elif status in ['degraded', 'warning'] and overall_status == 'healthy':
                overall_status = 'degraded'
                warnings.append(check_name)

        # 系统资源信息
        details = {
            'system': {
                'cpu': self.monitor.get_cpu_info(),
                'memory': self.monitor.get_memory_info(),
                'disk': self.monitor.get_disk_info(),
                'process': self.monitor.get_process_info(),
                'network': self.monitor.get_network_info()
            },
            'check_duration_ms': round((time.time() - start_time) * 1000, 2)
        }

        if critical_issues:
            details['critical_issues'] = critical_issues
        if warnings:
            details['warnings'] = warnings

        return HealthStatus(
            status=overall_status,
            timestamp=datetime.now().isoformat(),
            uptime_seconds=time.time() - self.monitor.start_time,
            checks=checks,
            details=details
        )

# 全局健康检查器
_health_checker: Optional[HealthChecker] = None

def get_health_checker(db_path: str = None) -> HealthChecker:
    """获取健康检查器实例"""
    global _health_checker

    if _health_checker is None:
        if db_path is None:
            db_path = os.environ.get('DB_PATH', '/app/database/rootara.db')
        _health_checker = HealthChecker(db_path)

    return _health_checker