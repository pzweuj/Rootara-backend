# coding=utf-8
# 模板表格的建立
# 初始化模式下，自动生成TEMPLATE表
# 前端页面需要设计，当用户上传的表格≥1时，不要显示TEMPLATE表，而是显示用户上传的表格
# 当用户未上传表格，或者将自己上传的表格都删掉时，显示TEMPLATE表

import os
import sqlite3
import random
import tempfile
import shutil
import sys
from datetime import datetime
import argparse

# 根据脚本运行方式选择合适的导入路径
if __name__ == "__main__":
    # 将项目根目录添加到模块搜索路径
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from scripts.rootara_admixture import data_to_sqlite as admix_data_to_sqlite
    from scripts.rootara_snp_2_db import csv_to_sqlite
    from scripts.rootara_2_vcf import trans_rootara_to_vcf
    from scripts.rootara_haplogroup import insert_haplogroup_to_db
else:
    # 作为模块导入时使用相对导入
    from scripts.rootara_admixture import data_to_sqlite as admix_data_to_sqlite
    from scripts.rootara_snp_2_db import csv_to_sqlite
    from scripts.rootara_2_vcf import trans_rootara_to_vcf
    from scripts.rootara_haplogroup import insert_haplogroup_to_db

# 已测试1000000次，没有重复
def generate_random_id():
    """
    生成10位随机编号，由大写字母和数字组成
    :return: 10位随机编号字符串
    """
    # 定义字符池：26个大写字母和10个数字
    chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    # 生成10位随机编号
    random_id = ''.join(random.choice(chars) for _ in range(10))
    return random_id

# 使用GO脚本进行格式转换
def format_covert(input_data, source_from):
    rootara_core_path = '/app/database/Rootara.core.202404.txt.gz'
    go_binary = '/app/scripts/rootara_reader'
    temp_dir = tempfile.mkdtemp()
    output_file = os.path.join(temp_dir, 'output.rootara.csv')
    cmd = f'{go_binary} -input {input_data} -output {output_file} -method {source_from} -rootara {rootara_core_path}'
    os.system(cmd)
    return output_file

def create_new_report(user_id, input_data, source_from, report_name, db_path, default_report=False, initail=False):
    # 当在初始化模式下，创建新的报告时，需要将default_report设置为True
    if initail:
        # 连接到数据库
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        default_report = True
        # 初始化模式下，只需要创建出SNP表即可
        report_id = 'RPT_TEMPLATE01'
        rawdata_id = 'RDT_TEMPLATE01'
        rootara_csv = format_covert(input_data, source_from)
        csv_to_sqlite(rootara_csv, db_path, report_id, force=True)
        shutil.rmtree(os.path.dirname(rootara_csv))

        # 查看当前的report_id表的总行数
        cursor.execute('SELECT COUNT(*) FROM ' + report_id)
        total_snp = cursor.fetchone()[0]

        # 将报告信息插入到reports表中
        cursor.execute('''
            INSERT INTO reports (report_id, user_id, file_format, data_source, name, select_default, total_snps, upload_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (report_id, user_id, 'txt', source_from, report_name, True, total_snp, datetime.now().isoformat()))

        # 提交更改并关闭连接
        conn.commit()
        conn.close()
        return 201
    
    # 连接到数据库，如果这是用户上传的第一个报告，则自动设置为默认报告
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 检查报告的数目
    cursor.execute('SELECT COUNT(*) FROM reports')
    report_count = cursor.fetchone()[0]

    # 如果这是用户上传的第一个报告，则自动设置为默认报告，因为会存在一个模板报告
    if initail is False and report_count == 1:
        default_report = True

    # 生成随机ID
    random_id = generate_random_id()
    report_id = 'RPT_' + random_id
    rawdata_id = 'RDT_' + random_id

    # 进行格式转换
    rootara_csv = format_covert(input_data, source_from)
    csv_to_sqlite(rootara_csv, db_path, report_id, force=True)

    # 查看当前的report_id表的总行数
    cursor.execute('SELECT COUNT(*) FROM ' + report_id)
    total_snp = cursor.fetchone()[0]

    # 祖源分析
    admix_data_to_sqlite(input_data, report_id, source_from, db_path, force=True)

    # 单倍群分析
    temp_dir = os.path.dirname(rootara_csv)
    vcf_file = os.path.join(temp_dir, 'output.vcf.gz')
    trans_rootara_to_vcf(rootara_csv, vcf_file)
    insert_haplogroup_to_db(report_id, vcf_file, db_path, force=True)

    # 原始数据拓展名
    extend_name = os.path.splitext(input_data)[1]

    # 将报告信息插入到reports表中
    cursor.execute('''
        INSERT INTO reports (report_id, user_id, file_format, data_source, name, select_default, total_snps, upload_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (report_id, user_id, extend_name, source_from, report_name, default_report, total_snp, datetime.now().isoformat()))

    # 提交更改并关闭连接
    conn.commit()
    conn.close()

    # 将原始数据保存到固定目录中
    rawdata_dir = '/data/rawdata'
    if not os.path.exists(rawdata_dir):
        os.makedirs(rawdata_dir, exist_ok=True)
    shutil.copy2(input_data, os.path.join(rawdata_dir, rawdata_id + '.' + extend_name))
    shutil.rmtree(temp_dir)

def main():
    parser = argparse.ArgumentParser(description='创建新的报告')
    parser.add_argument('--user_id', type=str, help='用户ID')
    parser.add_argument('--input_data', type=str, help='输入数据路径')
    parser.add_argument('--source_from', type=str, help='数据来源')
    parser.add_argument('--report_name', type=str, help='报告名称')
    parser.add_argument('--db_path', type=str, help='数据库路径')
    parser.add_argument('--default_report', type=bool, help='是否设置为默认报告')
    parser.add_argument('--initail', type=bool, help='是否是初始化模式')
    args = parser.parse_args()

    create_new_report(args.user_id, args.input_data, args.source_from, args.report_name, args.db_path, args.default_report, args.initail)

if __name__ == '__main__':
    main()

