# coding=utf-8
# 祖源分析结果查询

import sqlite3
import logging
from .cache_manager import cache_manager, CacheCategories

logger = logging.getLogger(__name__)

def get_admixture_info(report_id, db_path):
    """获取祖源分析信息，支持缓存"""

    # 尝试从缓存获取
    cached_result = cache_manager.get(CacheCategories.ADMIXTURE, report_id)
    if cached_result is not None:
        logger.debug(f"祖源分析缓存命中: {report_id}")
        return cached_result

    logger.debug(f"查询祖源分析数据: {report_id}")

    # 连接到数据库
    conn = None
    try:
        conn = sqlite3.connect(db_path, timeout=30.0)
        cursor = conn.cursor()

        # 检查report_id是否存在于admixture表中
        cursor.execute("SELECT COUNT(*) FROM admixture WHERE report_id=?", (report_id,))
        if cursor.fetchone()[0] == 0:
            # report_id不存在时返回空结果
            empty_result = {}
            # 缓存空结果，避免重复查询（较短的TTL）
            cache_manager.set(CacheCategories.ADMIXTURE, report_id, empty_result, ttl=300)
            return empty_result

        # 查询admixture表中的数据
        cursor.execute("""
            SELECT * FROM admixture WHERE report_id=?""",
            (report_id,)
        )
        row = cursor.fetchone()

        if not row:
            empty_result = {}
            cache_manager.set(CacheCategories.ADMIXTURE, report_id, empty_result, ttl=300)
            return empty_result

        # 获取列名
        column_names = [description[0] for description in cursor.description]

        # 将查询结果转换为字典，排除report_id列
        result = {}
        for i, column_name in enumerate(column_names):
            if column_name != 'report_id':
                result[column_name] = row[i]

        # 缓存结果 (1小时)
        cache_manager.set(CacheCategories.ADMIXTURE, report_id, result, ttl=3600)
        logger.debug(f"祖源分析数据已缓存: {report_id}")

        return result

    except Exception as e:
        logger.error(f"查询祖源分析数据失败 {report_id}: {e}")
        return {}
    finally:
        if conn:
            conn.close()
