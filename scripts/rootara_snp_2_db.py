# coding=utf-8
# 将数据进行转换，并写入数据库

import pandas as pd
import sqlite3


def convert_data_to_df(file_path):
    df = pd.read_csv(file_path, sep=',', header=0, low_memory=False)
    return df

def dataframe_to_sqlite(df, db_path, table_name, if_exists='replace'):
    """
    将Pandas DataFrame转换为SQLite表
    
    :param df: Pandas DataFrame对象
    :param db_path: SQLite数据库文件路径
    :param table_name: 要创建的表名
    :param if_exists: 如果表已存在，执行的操作：'replace'(替换)、'append'(追加)或'fail'(报错)
    :return: 成功返回True，失败返回False
    """
    try:
        # 连接到数据库
        conn = sqlite3.connect(db_path)
        
        # 重命名DataFrame的列以匹配数据库表结构
        df_renamed = df.rename(columns={
            'Chrom': 'chromosome',
            'Start': 'position',
            'Ref': 'ref',
            'Alt': 'alt',
            'RSID': 'rsid',
            'gnomAD_AF': 'gnomAD_AF',
            'Gene': 'gene',
            'CLNSIG': 'clnsig',
            'CLNDN': 'clndn',
            'Genotype': 'genotype',
            'Check': 'gt'
        })
        
        # 使用pandas的to_sql方法直接将DataFrame写入数据库
        # 设置index=False避免写入DataFrame的索引列
        df_renamed.to_sql(
            name=table_name,
            con=conn,
            if_exists=if_exists,
            index=False,
            dtype={
                'chromosome': 'TEXT',
                'position': 'INTEGER',
                'ref': 'TEXT',
                'alt': 'TEXT',
                'rsid': 'TEXT',
                'gnomAD_AF': 'FLOAT',
                'gene': 'TEXT',
                'clnsig': 'TEXT',
                'clndn': 'TEXT',
                'genotype': 'TEXT',
                'gt': 'TEXT'
            }
        )
        
        conn.close()
        
        print(f"成功将DataFrame转换为SQLite表 '{table_name}'，共 {len(df)} 行")
        return True
    
    except Exception as e:
        print(f"将DataFrame转换为SQLite表失败: {e}")
        return False

# 流程
def csv_to_sqlite(file_path, db_path, table_name, force=False):
    df = convert_data_to_df(file_path)
    if_exists = 'fail'
    if force:
        if_exists = 'replace'
    dataframe_to_sqlite(df, db_path, table_name, if_exists)

def main():
    parser = argparse.ArgumentParser(description='将Rootara CSV转换为SQLite数据库')
    parser.add_argument('--input', type=str, help='输入CSV文件路径')
    parser.add_argument('--db', type=str, help='输出数据库文件路径')
    parser.add_argument('--id', type=str, help='数据表名称')
    parser.add_argument('--force', type=bool, help='是否强制覆盖已存在的数据表，默认False', default=False)
    args = parser.parse_args()

    # 检查是否提供了所有必需参数
    if not all([args.input, args.db, args.id]):
        parser.print_help()
        sys.exit(1)

    csv_to_sqlite(file_path=args.input, db_path=args.db, table_name=args.id, force=args.force)

if __name__ == '__main__':
    main()
