# coding=utf-8
# sqlite3 初始化

import argparse
import sqlite3
import os
import random
import sys
from datetime import datetime

# 根据脚本运行方式选择合适的导入路径
if __name__ == "__main__":
    # 将项目根目录添加到模块搜索路径
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from scripts.rootara_report_create import create_new_report
    from scripts.rootara_traits import json_to_trait_table
else:
    # 作为模块导入时使用相对导入
    from scripts.rootara_report_create import create_new_report
    from scripts.rootara_traits import json_to_trait_table

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

def init_sqlite_db(db_path):
    """
    初始化SQLite数据库，创建必要的表结构
    :param db_path: 数据库文件路径
    :return: 成功返回True，失败返回False
    """
    try:
        # 确保目录存在
        db_dir = os.path.dirname(db_path)
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)

        # 连接到数据库（如果不存在则创建）
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 创建用户表 || 这个表暂时是摆设，没有用
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            user_id TEXT UNIQUE NOT NULL,
            password_hash TEXT DEFAULT NULL,
            name TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')

        # 创建报告表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS reports (
            report_id TEXT PRIMARY KEY,
            user_id INTEGER,
            file_format TEXT,
            data_source TEXT,
            name TEXT,
            select_default boolean,
            total_snps INTEGER,
            upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
        ''')

        # 创建SNP数据表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS RPT_TEMPLATE01 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chromosome TEXT,
            position INTEGER,
            ref TEXT,
            alt TEXT,
            rsid TEXT DEFAULT null,
            gnomAD_AF FLOAT DEFAULT null,
            gene TEXT,
            clnsig TEXT DEFAULT null,
            clndn TEXT DEFAULT null,
            genotype TEXT,
            gt TEXT
        )
        ''')

        # 创建单倍群记录
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS haplogroup (
            report_id TEXT PRIMARY KEY,
            y_hap TEXT DEFAULT null,
            mt_hap TEXT DEFAULT null
        )
        ''')

        # 创建祖源分析记录
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS admixture (
            report_id TEXT PRIMARY KEY,
            Kushitic REAL DEFAULT 0.00,
            North_Iberian REAL DEFAULT 0.00,
            East_Iberian REAL DEFAULT 0.00,
            Tibeto_Burman REAL DEFAULT 0.00,
            North_African REAL DEFAULT 0.00,
            South_Caucasian REAL DEFAULT 0.00,
            North_Caucasian REAL DEFAULT 0.00,
            Paleo_Balkan REAL DEFAULT 0.00,
            Turkic_Altai REAL DEFAULT 0.00,
            Proto_Austronesian REAL DEFAULT 0.00,
            Nilotic REAL DEFAULT 0.00,
            East_Med REAL DEFAULT 0.00,
            Omotic REAL DEFAULT 0.00,
            Munda REAL DEFAULT 0.00,
            North_Amerind REAL DEFAULT 0.00,
            Arabic REAL DEFAULT 0.00,
            East_Euro REAL DEFAULT 0.00,
            Central_African REAL DEFAULT 0.00,
            Andean REAL DEFAULT 0.00,
            Indo_Chinese REAL DEFAULT 0.00,
            South_Indian REAL DEFAULT 0.00,
            NE_Asian REAL DEFAULT 0.00,
            Volgan REAL DEFAULT 0.00,
            Mongolian REAL DEFAULT 0.00,
            Siberian REAL DEFAULT 0.00,
            North_Sea_Germanic REAL DEFAULT 0.00,
            Celtic REAL DEFAULT 0.00,
            West_African REAL DEFAULT 0.00,
            West_Finnic REAL DEFAULT 0.00,
            Uralic REAL DEFAULT 0.00,
            Sahelian REAL DEFAULT 0.00,
            NW_Indian REAL DEFAULT 0.00,
            East_African REAL DEFAULT 0.00,
            East_Asian REAL DEFAULT 0.00,
            Amuro_Manchurian REAL DEFAULT 0.00,
            Scando_Germanic REAL DEFAULT 0.00,
            Iranian REAL DEFAULT 0.00,
            South_African REAL DEFAULT 0.00,
            Amazonian REAL DEFAULT 0.00,
            Baltic REAL DEFAULT 0.00,
            Malay REAL DEFAULT 0.00,
            Meso_Amerind REAL DEFAULT 0.00,
            South_Chinese REAL DEFAULT 0.00,
            Papuan REAL DEFAULT 0.00,
            West_Med REAL DEFAULT 0.00,
            Pamirian REAL DEFAULT 0.00,
            Central_Med REAL DEFAULT 0.00
        )
        ''')

        # 提交事务
        conn.commit()

        # 关闭连接
        conn.close()

        return True
    except Exception as e:
        print(f"初始化数据库失败: {e}")
        return False

# 生成模板数据
def generate_template_data(name, email, db_path, force=False):
    # 用户的唯一ID
    user_id = 'ID_' + generate_random_id()
    template_id = 'RPT_TEMPLATE01'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    if force:
        # 强制模式：更新或插入用户数据
        cursor.execute('SELECT user_id FROM users WHERE email = ?', (email,))
        existing_user = cursor.fetchone()

        if existing_user:
            # 用户已存在，更新用户信息
            user_id = existing_user[0]
            cursor.execute('''
            UPDATE users SET name = ?, created_at = ? WHERE email = ?
            ''', (name, datetime.now().isoformat(), email))
            print(f"强制更新用户信息: {user_id}")
        else:
            # 用户不存在，创建新用户
            cursor.execute('''
            INSERT INTO users (email, user_id, name, created_at)
            VALUES (?, ?, ?, ?)
            ''', (email, user_id, name, datetime.now().isoformat()))
            print(f"强制创建新用户: {user_id}")

        # 强制更新或插入祖源分析记录
        cursor.execute('SELECT report_id FROM admixture WHERE report_id = ?', (template_id,))
        if cursor.fetchone():
            cursor.execute('''
            UPDATE admixture SET Omotic = ?, North_Sea_Germanic = ?, West_African = ?, East_Asian = ?
            WHERE report_id = ?
            ''', (0.01, 0.02, 13.28, 86.69, template_id))
            print("强制更新祖源分析记录")
        else:
            cursor.execute('''
            INSERT INTO admixture (report_id, Omotic, North_Sea_Germanic, West_African, East_Asian)
            VALUES (?,?,?,?,?)
            ''', (template_id, 0.01, 0.02, 13.28, 86.69))
            print("强制创建祖源分析记录")

        # 强制更新或插入单倍群信息
        cursor.execute('SELECT report_id FROM haplogroup WHERE report_id = ?', (template_id,))
        if cursor.fetchone():
            cursor.execute('''
            UPDATE haplogroup SET y_hap = ?, mt_hap = ? WHERE report_id = ?
            ''', ('O2a2a1a1a', 'F1a1', template_id))
            print("强制更新单倍群信息记录")
        else:
            cursor.execute('''
            INSERT INTO haplogroup (report_id, y_hap, mt_hap)
            VALUES (?,?,?)
            ''', (template_id, 'O2a2a1a1a', 'F1a1'))
            print("强制创建单倍群信息记录")

        # 提交事务关闭连接
        conn.commit()
        conn.close()

        # 强制重新创建SNP表
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (template_id,))
        if cursor.fetchone():
            cursor.execute(f'DROP TABLE {template_id}')
            conn.commit()
            print("强制删除现有SNP表")
        conn.close()

        create_new_report(user_id, '/app/database/TEMPLATE01.txt', '23andme', "EXAMPLE", db_path, initail=True)
        print("强制重新创建SNP表")

        # 强制重新创建特征表
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='traits'")
        if cursor.fetchone():
            cursor.execute('DELETE FROM traits')
            conn.commit()
            print("强制清空特征表")
        conn.close()

        json_to_trait_table('/app/database/default-traits.json', db_path)
        print("强制重新创建特征表")

    else:
        # 非强制模式：只在数据不存在时创建
        cursor.execute('SELECT user_id FROM users WHERE email = ?', (email,))
        existing_user = cursor.fetchone()

        if existing_user:
            user_id = existing_user[0]
            print(f"用户已存在，使用现有ID: {user_id}")
        else:
            cursor.execute('''
            INSERT INTO users (email, user_id, name, created_at)
            VALUES (?, ?, ?, ?)
            ''', (email, user_id, name, datetime.now().isoformat()))
            print(f"创建新用户: {user_id}")

        # 检查并创建祖源分析记录
        cursor.execute('SELECT report_id FROM admixture WHERE report_id = ?', (template_id,))
        if not cursor.fetchone():
            cursor.execute('''
            INSERT INTO admixture (report_id, Omotic, North_Sea_Germanic, West_African, East_Asian)
            VALUES (?,?,?,?,?)
            ''', (template_id, 0.01, 0.02, 13.28, 86.69))
            print("创建祖源分析记录")

        # 检查并创建单倍群信息
        cursor.execute('SELECT report_id FROM haplogroup WHERE report_id = ?', (template_id,))
        if not cursor.fetchone():
            cursor.execute('''
            INSERT INTO haplogroup (report_id, y_hap, mt_hap)
            VALUES (?,?,?)
            ''', (template_id, 'O2a2a1a1a', 'F1a1'))
            print("创建单倍群信息记录")

        # 提交事务关闭连接
        conn.commit()
        conn.close()

        # 检查并创建SNP表
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (template_id,))
        if not cursor.fetchone():
            conn.close()
            create_new_report(user_id, '/app/database/TEMPLATE01.txt', '23andme', "EXAMPLE", db_path, initail=True)
            print("创建SNP表")
        else:
            conn.close()

        # 检查并创建特征表
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='traits'")
        if not cursor.fetchone():
            conn.close()
            json_to_trait_table('/app/database/default-traits.json', db_path)
            print("创建特征表")
        else:
            cursor.execute('SELECT COUNT(*) FROM traits')
            trait_count = cursor.fetchone()[0]
            conn.close()
            if trait_count == 0:
                json_to_trait_table('/app/database/default-traits.json', db_path)
                print("特征表为空，插入默认数据")

def init_db(name, email, db_file, force=False):
    if os.path.exists(db_file) and force is False:
        print(f"数据库文件已存在: {db_file}")
        print("跳过初始化，使用现有数据库")
        return
    elif os.path.exists(db_file) and force is True:
        print(f"数据库文件已存在，但强制重新初始化: {db_file}")
        if init_sqlite_db(db_file):
            generate_template_data(name, email, db_file, force=True)
            print(f"数据库强制重新初始化成功: {db_file}")
        else:
            print("数据库强制重新初始化失败")
    elif init_sqlite_db(db_file):
        generate_template_data(name, email, db_file, force=False)
        print(f"数据库初始化成功: {db_file}")
    else:
        print("数据库初始化失败")

def str_to_bool(v):
    """将字符串转换为布尔值"""
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def main():
    parser = argparse.ArgumentParser(description='初始化SQLite数据库')
    parser.add_argument('--name', type=str, help='用户姓名')
    parser.add_argument('--email', type=str, help='用户邮箱')
    parser.add_argument('--db', type=str, help='数据库路径')
    parser.add_argument('--force', type=str_to_bool, help='是否强制覆盖已存在的数据库，默认False', default=False)
    args = parser.parse_args()

    if not args.name or not args.email or not args.db:
        parser.print_help()
        return

    init_db(args.name, args.email, args.db, args.force)

if __name__ == "__main__":
    main()
