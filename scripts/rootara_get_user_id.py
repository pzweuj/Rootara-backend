# coding=utf-8
# pzw
# 查询用户ID
# 因为现在只允许一个用户，所以用户ID直接get第一个就好

import sqlite3

def get_user_id(db_path):
    # 连接到数据库
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 检查表是否存在
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
    if not cursor.fetchone():
        # 表不存在时返回空结果
        return None

    # 查询用户ID
    cursor.execute("SELECT user_id FROM users LIMIT 1")
    user_id = cursor.fetchone()
    if user_id:
        return user_id[0]
    else:
        return None
