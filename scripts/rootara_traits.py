# coding=utf-8
# 设计特征表的格式，用于读取默认的特征表形成一个特征表数据库
# 在需要渲染时，将特征表数据库输出为一个json文件

"""
id: 特征的id，默认的特征可以使用英文单词作为ID，自定义的特征使用随机生成的ID
name: {
    "en": "英文名称",
    "zh-CN": "中文名称",
    "default": ""
}
description: {}
icon: ""
confidence: ""
isDefault: true就是默认特征，false就是自定义特征
createdAt: ""
category: ""
rsids: []  用到的rsid列表
formula： "计算公式字符串"
scoreThresholds: {
    "default": {},
    "en": {},
    "zh-CN": {}
}
"""

from datetime import datetime
import random
import json
import sqlite3

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

# 转换默认json为默认特征表，用于初始化数据
def json_to_trait_table(json_file, db_path):
    data = json.load(open(json_file, 'r', encoding='utf-8'))
    
    # 连接到SQLite数据库
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 创建特征表 || 这个表暂时不考虑拆分用户的特征，不过可以将用户ID作为保留字段
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS traits (
        id TEXT PRIMARY KEY,
        name TEXT,
        description TEXT,
        icon TEXT,
        confidence TEXT,
        isDefault BOOLEAN,
        createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        category TEXT,
        rsids TEXT,
        formula TEXT,
        scoreThresholds TEXT
    )
    ''')
    conn.commit()

    # 遍历JSON数据，插入特征数据
    for item in data:
        # 生成随机ID
        id = item['id']
        # 将name和description转换为字典格式并存储
        name_dict = item['name'] if isinstance(item['name'], dict) else {'default': str(item['name'])}
        name_dict.setdefault('default', '')
        name = str(name_dict)
        desc_dict = item['description'] if isinstance(item['description'], dict) else {'default': str(item['description'])}
        desc_dict.setdefault('default', '')
        description = str(desc_dict)
        icon = item['icon']
        confidence = item['confidence']
        is_default = item['isDefault']
        created_at = datetime.now().isoformat()
        category = item['category']
        rsids = ";".join(item['rsids'])
        formula = item['formula']
        score_thresholds_dict = item['scoreThresholds'] if isinstance(item['scoreThresholds'], dict) else {'default': str(item['scoreThresholds'])}
        score_thresholds_dict.setdefault('default', '')
        score_thresholds = str(score_thresholds_dict)

        # 插入数据
        cursor.execute('''
        INSERT INTO traits (id, name, description, icon, confidence, isDefault, createdAt, category, rsids, formula, scoreThresholds)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (id, name, description, icon, confidence, is_default, created_at, category, rsids, formula, score_thresholds))
    
    # 提交更改并关闭连接
    conn.commit()
    conn.close()

# 将当前的特征表转换为json格式，同时需要从数据库中读取rsid表，需要REF和GT




# 新增特征 || 特征不支持修改




# 删除特征



# 导入自定义特征




# 导出自定义特征



