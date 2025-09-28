# coding=utf-8
"""
数据库连接池管理器
提供SQLite连接池，提升数据库访问性能和稳定性
"""

import sqlite3
import threading
import time
import os
import logging
from contextlib import contextmanager
from typing import Optional, Generator
from queue import Queue, Empty, Full

logger = logging.getLogger(__name__)

class DatabaseConnectionPool:
    """SQLite连接池管理器"""

    def __init__(self, db_path: str, max_connections: int = 10, timeout: float = 30.0):
        """
        初始化数据库连接池

        Args:
            db_path: 数据库文件路径
            max_connections: 最大连接数
            timeout: 连接超时时间（秒）
        """
        self.db_path = db_path
        self.max_connections = max_connections
        self.timeout = timeout
        self.pool = Queue(maxsize=max_connections)
        self.created_connections = 0
        self.lock = threading.Lock()

        # 统计信息
        self.stats = {
            'total_connections_created': 0,
            'active_connections': 0,
            'pool_hits': 0,
            'pool_misses': 0,
            'connection_errors': 0
        }

        # 预创建一些连接
        self._warm_up_pool(min(3, max_connections))

    def _create_connection(self) -> sqlite3.Connection:
        """创建新的数据库连接"""
        try:
            conn = sqlite3.connect(
                self.db_path,
                timeout=self.timeout,
                check_same_thread=False
            )

            # 优化SQLite配置
            conn.execute("PRAGMA journal_mode=WAL")  # 启用WAL模式
            conn.execute("PRAGMA synchronous=NORMAL")  # 平衡性能和安全性
            conn.execute("PRAGMA cache_size=10000")  # 增加缓存大小
            conn.execute("PRAGMA temp_store=MEMORY")  # 临时表存储在内存中
            conn.execute("PRAGMA foreign_keys=ON")  # 启用外键约束

            with self.lock:
                self.created_connections += 1
                self.stats['total_connections_created'] += 1
                self.stats['active_connections'] += 1

            logger.debug(f"创建新的数据库连接 #{self.created_connections}")
            return conn

        except Exception as e:
            with self.lock:
                self.stats['connection_errors'] += 1
            logger.error(f"创建数据库连接失败: {e}")
            raise

    def _warm_up_pool(self, count: int):
        """预热连接池"""
        for _ in range(count):
            try:
                conn = self._create_connection()
                self.pool.put_nowait(conn)
            except (Full, Exception) as e:
                logger.warning(f"预热连接池失败: {e}")
                break

    def get_connection(self) -> sqlite3.Connection:
        """从连接池获取连接"""
        try:
            # 尝试从池中获取连接
            conn = self.pool.get_nowait()
            with self.lock:
                self.stats['pool_hits'] += 1

            # 检查连接是否有效
            try:
                conn.execute("SELECT 1")
                return conn
            except Exception:
                # 连接无效，关闭并创建新连接
                self._close_connection(conn)
                return self._create_new_connection()

        except Empty:
            # 池中无可用连接
            with self.lock:
                self.stats['pool_misses'] += 1
            return self._create_new_connection()

    def _create_new_connection(self) -> sqlite3.Connection:
        """创建新连接（当池满时）"""
        with self.lock:
            if self.created_connections < self.max_connections:
                return self._create_connection()
            else:
                # 达到最大连接数，等待可用连接
                logger.warning("达到最大连接数，等待可用连接")

        try:
            conn = self.pool.get(timeout=self.timeout)
            with self.lock:
                self.stats['pool_hits'] += 1
            return conn
        except Empty:
            raise Exception(f"获取数据库连接超时（{self.timeout}秒）")

    def return_connection(self, conn: sqlite3.Connection):
        """归还连接到池中"""
        if conn is None:
            return

        try:
            # 检查连接是否正常
            conn.execute("SELECT 1")
            conn.rollback()  # 回滚任何未提交的事务

            try:
                self.pool.put_nowait(conn)
            except Full:
                # 池已满，关闭连接
                self._close_connection(conn)

        except Exception as e:
            logger.warning(f"归还连接时检查失败: {e}")
            self._close_connection(conn)

    def _close_connection(self, conn: sqlite3.Connection):
        """关闭数据库连接"""
        try:
            conn.close()
            with self.lock:
                self.created_connections -= 1
                self.stats['active_connections'] -= 1
        except Exception as e:
            logger.error(f"关闭数据库连接失败: {e}")

    @contextmanager
    def get_connection_context(self) -> Generator[sqlite3.Connection, None, None]:
        """获取数据库连接的上下文管理器"""
        conn = None
        try:
            conn = self.get_connection()
            yield conn
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            logger.error(f"数据库操作失败: {e}")
            raise
        finally:
            if conn:
                self.return_connection(conn)

    def execute_query(self, query: str, params: tuple = (), fetch_one: bool = False):
        """执行查询（便捷方法）"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)

            if query.strip().upper().startswith(('INSERT', 'UPDATE', 'DELETE')):
                conn.commit()
                return cursor.rowcount
            elif fetch_one:
                return cursor.fetchone()
            else:
                return cursor.fetchall()

    def close_all(self):
        """关闭所有连接"""
        logger.info("关闭数据库连接池")

        # 关闭池中的所有连接
        while not self.pool.empty():
            try:
                conn = self.pool.get_nowait()
                self._close_connection(conn)
            except Empty:
                break

        logger.info(f"连接池已关闭，共关闭 {self.stats['total_connections_created']} 个连接")

    def get_stats(self) -> dict:
        """获取连接池统计信息"""
        with self.lock:
            return {
                **self.stats,
                'pool_size': self.pool.qsize(),
                'max_connections': self.max_connections,
                'created_connections': self.created_connections
            }

# 全局数据库连接池
_db_pool: Optional[DatabaseConnectionPool] = None
_pool_lock = threading.Lock()

def get_db_pool(db_path: str = None) -> DatabaseConnectionPool:
    """获取全局数据库连接池"""
    global _db_pool

    if _db_pool is None:
        with _pool_lock:
            if _db_pool is None:
                if db_path is None:
                    db_path = os.environ.get('DB_PATH', '/app/database/rootara.db')

                max_connections = int(os.environ.get('DB_MAX_CONNECTIONS', '10'))
                timeout = float(os.environ.get('DB_TIMEOUT', '30.0'))

                _db_pool = DatabaseConnectionPool(
                    db_path=db_path,
                    max_connections=max_connections,
                    timeout=timeout
                )
                logger.info(f"数据库连接池已初始化: {db_path}, 最大连接数: {max_connections}")

    return _db_pool

def close_db_pool():
    """关闭全局数据库连接池"""
    global _db_pool
    if _db_pool:
        _db_pool.close_all()
        _db_pool = None