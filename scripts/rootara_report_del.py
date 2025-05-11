# coding=utf-8
# pzw
# 删除报告
# 会从sqlite数据库中删除对应的报告
# 并会重新设定默认报告

import sqlite3
import argparse
import sys

def delete_report(report_id, db_file):
    print("删除报告：{report_id}".format(report_id=report_id))

    if report_id == 'RPT_TEMPLATE01':
        print("模板报告不能删除")
        return

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # 删除报告表 - 使用引用标识符语法
    cursor.execute("DROP TABLE IF EXISTS [{}]".format(report_id))

    # 删除admixture表记录
    cursor.execute("DELETE FROM admixture WHERE report_id = ?", (report_id,))

    # 删除单倍群表记录
    cursor.execute("DELETE FROM haplogroup WHERE report_id =?", (report_id,))

    # 删除报告记录
    # 首先先查看这个报告是不是默认报告
    cursor.execute("SELECT select_default FROM reports WHERE report_id =?", (report_id,))
    result = cursor.fetchone()
    
    # 如果是，则先需要将其他报告设置为默认报告
    if result:
        # 按上传时间排序选择最新的报告作为默认报告，排除当前要删除的报告
        cursor.execute("""
            UPDATE reports 
            SET select_default = True 
            WHERE report_id = (
                SELECT report_id 
                FROM reports 
                WHERE report_id != ? 
                ORDER BY upload_date DESC 
                LIMIT 1
            )
        """, (report_id,))
        conn.commit()

    cursor.execute("DELETE FROM reports WHERE report_id = ?", (report_id,))
    conn.commit()

def main():
    parser = argparse.ArgumentParser(description='删除报告')
    parser.add_argument('--db', type=str, help='数据库文件')
    parser.add_argument('--report', type=str, help='报告ID')
    args = parser.parse_args()

    # 检查是否提供了所有必需参数
    if not all([args.db, args.report]):
        parser.print_help()
        sys.exit(1)

    delete_report(args.db, args.report)

if __name__ == '__main__':
    main()


