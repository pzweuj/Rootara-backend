# coding=utf-8
# pzw
# 20250423
# 用于从vcf分析单倍型
# 使用 https://gitlab.com/bio_anth_decode/haploGrouper

import os
import sys
import argparse
import sqlite3
import tempfile
import pandas as pd
import shutil

def y_haplogroup(vcf_file, output_dir, rpt_id):
    base_dir = '/home/clinic/clinic_backup/software/haploGrouper'
    data_dir = f'{base_dir}/data'
    tool = f'{base_dir}/haploGrouper.py'

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    cmd = f"""
        python3 {tool} \\
            -v {vcf_file} \\
            -t {data_dir}/chrY_isogg2019_tree.txt \\
            -l {data_dir}/chrY_isogg2019-decode1_loci_b37.txt \\
            -o {output_dir}/{rpt_id}.YHap.txt
    """

    os.system(cmd)

def mt_haplogroup(vcf_file, output_dir, rpt_id):
    base_dir = '/home/clinic/clinic_backup/software/haploGrouper'
    data_dir = f'{base_dir}/data'
    tool = f'{base_dir}/haploGrouper.py'

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    cmd = f"""
        python3 {tool} \\
            -v {vcf_file} \\
            -t {data_dir}/chrMT_phylotree17_tree.txt \\
            -l {data_dir}/chrMT_phylotree17_loci.txt \\
            -o {output_dir}/{rpt_id}.MTHap.txt
    """

    os.system(cmd)

# 查询数据库中这个编号的报告是否已存在结果，如已存在，不重新分析
def insert_haplogroup_to_db(rpt_id, vcf_file, db_file, force=False):
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # 检查是否已存在结果
    cursor.execute("SELECT COUNT(*) FROM haplogroup WHERE report_id = ?", (rpt_id,))
    count = cursor.fetchone()[0]

    if count > 0 and not force:
        print(f"报告 {rpt_id} 已存在结果，跳过分析。")
        return

    running_dir = tempfile.gettempdir()
    y_haplogroup(vcf_file, running_dir, rpt_id)
    mt_haplogroup(vcf_file, running_dir, rpt_id)
    y_haplogroup_result = f"{running_dir}/{rpt_id}.YHap.txt"
    mt_haplogroup_result = f"{running_dir}/{rpt_id}.MTHap.txt"

    # 对结果进行解析
    df_y = pd.read_csv(y_haplogroup_result, sep='\t', header=0)
    df_mt = pd.read_csv(mt_haplogroup_result, sep='\t', header=0)

    y_hap = df_y['Haplogroup'].values[0]
    mt_hap = df_mt['Haplogroup'].values[0]

    # 插入结果到数据库
    cursor.execute("INSERT INTO haplogroup (report_id, y_hap, mt_hap) VALUES (?, ?, ?)", (rpt_id, y_hap, mt_hap))
    conn.commit()
    shutil.rmtree(running_dir)

def main():
    parser = argparse.ArgumentParser(description='分析单倍型')
    parser.add_argument('--input', type=str, help='输入VCF文件')
    parser.add_argument('--id', type=str, help='输入报告编号')
    parser.add_argument('--db', type=str, help='数据数据库文件')
    parser.add_argument('--force', type=bool, help='是否强制执行，默认False', default=False)
    args = parser.parse_args()

    # 检查是否提供了所有必需参数
    if not all([args.input, args.id, args.db]):
        parser.print_help()
        sys.exit(1)

    try:
        insert_haplogroup_to_db(args.id, args.input, args.db, args.force)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()

