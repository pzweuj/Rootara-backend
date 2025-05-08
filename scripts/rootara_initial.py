# coding=utf-8
# sqlite3 初始化

import sqlite3
import os
import random
from datetime import datetime
from scripts.rootara_report_create import create_new_report

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
            check TEXT
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
def generate_template_data(name, email, db_path):
    # 用户的唯一ID
    user_id = 'ID_' + generate_random_id()
    template_id = 'RPT_TEMPLATE01'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 记录信息到用户表
    cursor.execute('''
    INSERT INTO users (email, user_id, name, created_at)
    VALUES (?, ?, ?, ?)
    ''', (email, user_id, name, datetime.now()))

    # 创建祖源分析记录
    cursor.execute('''
    INSERT INTO admixture (report_id, Omotic, North_Sea_Germanic, West_African, East_Asian)
    VALUES (?,?,?,?,?)
    ''', (template_id, 0.01, 0.02, 13.28, 86.69))
    
    # 创建单倍群信息表
    cursor.execute('''
    INSERT INTO haplogroup (report_id, y_hap, mt_hap)
    VALUES (?,?,?)
    ''', (template_id, 'O2a2a1a1a', 'F1a1'))

    # 先提交一次事务关闭连接
    conn.commit()
    conn.close()

    # 创建SNP表
    create_new_report(user_id, '/app/database/TEMPLATE01.txt', '23andme', "EXAMPLE", db_path, initail=True)

# 使用示例
if __name__ == "__main__":
    # 数据库文件路径
    db_file = '/rootara/rootara.db'
    if os.path.exists(db_file):
        print(f"数据库文件已存在: {db_file}")
    elif init_sqlite_db(db_file):
        print(f"数据库初始化成功: {db_file}")
    else:
        print("数据库初始化失败")
