# coding=utf-8
# pzw
# 主要用于调整和查询报告的信息

import sqlite3

# 调整报告的自定义名称
def update_report_name(report_id, new_name, db_file):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("UPDATE reports SET name = ? WHERE report_id = ?", (new_name, report_id))
    conn.commit()
    conn.close()

# 查询报告的信息
def get_report_info(report_id, db_file):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM reports WHERE report_id =?", (report_id,))
    report_info = cursor.fetchone()
    conn.close()
    return report_info

# 列出所有报告的ID
def list_all_report_ids(db_file):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    cursor.execute("SELECT report_id FROM reports")
    report_ids = cursor.fetchall()
    conn.close()
    return [report_id[0] for report_id in report_ids]
