# coding=utf-8
# pzw
# 20250507
# 用于从原始输入数据中进行祖源分析
# 使用 https://github.com/stevenliuyi/admix
# 模型参考 https://dnagenics.com/products/admixturecalculators

import os
import tempfile
import sqlite3
import argparse
import shutil

# admix运行，可能会占用较高的计算资源
def admix_cli(input_file, rpt_id, method):
    input_file = os.path.abspath(input_file)
    
    # 创建一个固定的临时目录
    temp_base_dir = '/data/temp'
    if not os.path.exists(temp_base_dir):
        os.makedirs(temp_base_dir, exist_ok=True)
    
    # 使用固定目录创建临时目录
    temp_dir = tempfile.mkdtemp(dir=temp_base_dir)
    
    cmd = f'admix -f {input_file} -v {method} -m K47 > {temp_dir}/{rpt_id}.admix.txt'
    os.system(cmd)
    return f'{temp_dir}/{rpt_id}.admix.txt'

# 解析admix结果
def parse_admix_result(admix_file):
    admix_file = os.path.abspath(admix_file)
    with open(admix_file, 'r') as f:
        lines = f.readlines()

    ances_dict = {}
    begin = False
    for line in lines:
        if line == "\n":
            continue
        if 'K47' in line:
            begin = True
            continue
        if line.startswith('Calcuation'):
            continue
        if begin:
            line = line.strip()
            line = line.split(': ')
            ances_dict[line[0].replace('-', '_')] = float(line[1].replace('%', ''))

    return ances_dict

# 结果导入数据库
def import_result_to_db(ances_dict, rpt_id, db_path):
    db_path = os.path.abspath(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 获取表结构信息
    cursor.execute("PRAGMA table_info(admixture)")
    columns = [column[1] for column in cursor.fetchall()]
    
    # 准备SQL语句的列名和占位符部分
    valid_columns = ['report_id']
    values = [rpt_id]
    
    # 检查每个祖源成分是否在表结构中存在
    for ances, ratio in ances_dict.items():
        if ances in columns:
            valid_columns.append(ances)
            values.append(ratio)
        else:
            print(f"警告: 祖源成分 '{ances}' 在数据表结构中不存在，已忽略")
    
    # 构建SQL语句
    columns_str = ", ".join(valid_columns)
    placeholders = ", ".join(["?" for _ in valid_columns])
    
    # 执行插入
    sql = f"INSERT INTO admixture ({columns_str}) VALUES ({placeholders})"
    cursor.execute(sql, values)
    
    # 提交并关闭连接
    conn.commit()
    conn.close()

# 完整流程
def data_to_sqlite(input_file, rpt_id, method, db_path, force=False):
    # 检查数据库中是否已经存在该报告的祖源分析结果
    db_path = os.path.abspath(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM admixture WHERE report_id=?", (rpt_id,))
    result_count = cursor.fetchone()[0]
    conn.close()

    if result_count > 0 and not force:
        print(f"报告 {rpt_id} 的祖源分析结果已存在于数据库中，跳过该报告")
        return
    
    admix_file = admix_cli(input_file, rpt_id, method)
    ances_dict = parse_admix_result(admix_file)
    import_result_to_db(ances_dict, rpt_id, db_path)
    print(f"报告 {rpt_id} 的祖源分析结果已导入数据库")
    shutil.rmtree(os.path.dirname(admix_file))

def main():
    parser = argparse.ArgumentParser(description='Process input file and report ID.')
    parser.add_argument('--input', type=str, required=True, help='Path to the input file')
    parser.add_argument('--rpt_id', type=str, required=True, help='Report ID')
    parser.add_argument('--method', type=str, required=True, help='Source of data')
    parser.add_argument('--db_path', type=str, required=True, help='Path to the SQLite database')
    parser.add_argument('--force', help='Force overwrite existing results, default=False', default=False)
    args = parser.parse_args()

    data_to_sqlite(args.input, args.rpt_id, args.method, args.db_path, args.force)

if __name__ == "__main__":
    main()

