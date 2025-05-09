# coding=utf-8
# 将一份报告设置为默认报告

import sqlite3

def set_default_report(report_id, db_path):
    # 连接到数据库
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 检查report_id是否存在于reports表中
    cursor.execute("SELECT COUNT(*) FROM reports WHERE report_id=?", (report_id,))
    if cursor.fetchone()[0] == 0:
        print("报告不存在！")
        return

    # 将指定报告的is_default字段设置为1
    cursor.execute("UPDATE reports SET select_default=1 WHERE report_id=?", (report_id,))
    conn.commit()

    # 将其他报告的is_default字段设置为0
    cursor.execute("UPDATE reports SET select_default=0 WHERE report_id<>?", (report_id,))
    conn.commit()

    # 关闭数据库连接
    conn.close()
    print("报告设置成功！")
