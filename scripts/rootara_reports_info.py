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

# 所有的报告信息 || 现在没有区分用户，所以不需要用户ID
def get_all_report_info(db_file):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # 当报告的数目≥2，不需要显示默认报告RPT_TEMPLATE01
    cursor.execute("SELECT COUNT(*) FROM reports")
    report_count = cursor.fetchone()[0]
    if report_count >= 2:
        cursor.execute("SELECT * FROM reports WHERE report_id != 'RPT_TEMPLATE01'")
    else:
        cursor.execute("SELECT * FROM reports")
    report_info = cursor.fetchall()
    conn.close()

    sample_info_json = []
    for i in report_info:
        report_dict = {}
        report_dict['id'] = i[0]
        report_dict['user_id'] = i[1]
        report_dict['extend'] = i[2]
        report_dict['source'] = i[3]
        report_dict['name'] = i[4]
        report_dict['nameZh'] = i[4]
        report_dict['isDefault'] = i[5]
        report_dict['snpCount'] = i[6]
        report_dict['uploadDate'] = i[7]
        sample_info_json.append(report_dict)
    return sample_info_json
