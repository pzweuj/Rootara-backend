# coding=utf-8
# pzw
# 查询单倍群信息

import sqlite3

def get_haplogroup_info(report_id, db_path):
    # 连接到数据库
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 检查report_id是否存在于haplogroup表中
    cursor.execute("SELECT COUNT(*) FROM haplogroup WHERE report_id=?", (report_id,))
    if cursor.fetchone()[0] == 0:
        # report_id不存在时返回空结果
        empty_result = {}
        return empty_result

    # 查询haplogroup表中的数据
    cursor.execute("""
        SELECT * FROM haplogroup WHERE report_id=?""", 
        (report_id,)
    )
    row = cursor.fetchone()
    
    # 获取列名
    column_names = [description[0] for description in cursor.description]
    
    # 将查询结果转换为字典，排除report_id列
    result = {}
    for i, column_name in enumerate(column_names):
        if column_name != 'report_id':
            result[column_name] = row[i]
    
    # 关闭数据库连接
    conn.close()
    return result
