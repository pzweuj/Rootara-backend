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
description: {
    "en": "",
    "zh-CN": "中文描述",
}
icon: ""
confidence: ""
isDefault: true就是默认特征，false就是自定义特征
createdAt: ""
category: ""
rsids: []  用到的rsid列表
formula： "计算公式字符串"
scoreThresholds: {
    "default": {
        "cutoff": 0,
        "description": "" 
    },
    "en": {},
    "zh-CN": {}
}
"""

import os
import sys
from datetime import datetime
import random
import json
import sqlite3
import ast

# 根据脚本运行方式选择合适的导入路径
if __name__ == "__main__":
    # 将项目根目录添加到模块搜索路径
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from scripts.rootara_table_info import get_snp_info_by_rsid
else:
    # 作为模块导入时使用相对导入
    from scripts.rootara_table_info import get_snp_info_by_rsid

# 随机ID
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

# 新增特征 || 特征不支持修改
# data的格式与json的相同
def add_trait(item, db_path, add_mode=True):
    # 连接到SQLite数据库
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print('Process: ', item)

    # 判断item是否为字符串类型
    if isinstance(item, str):
        try:
            # 尝试将字符串解析为字典
            item = json.loads(item)
        except json.JSONDecodeError as e:
            raise ValueError(f"无法将字符串解析为有效的JSON格式: {e}")

    id = "TRA_" + generate_random_id()
    if not add_mode:
        id = item['id']

    # 将name和description转换为字典格式并存储
    if add_mode:
        name = str({'en': '', 'zh-CN': '', 'default': item['name']})
        description = str({'en': '', 'zh-CN': '', 'default': item['description']})
        score_thresholds = str({'default': str(item['scoreThresholds']), 'zh-CN': '{{}}', 'en': '{{}}'})
    else:
        name_dict = item['name'] if isinstance(item['name'], dict) else {'default': str(item['name'])}
        name_dict.setdefault('default', '')
        name = str(name_dict)
        desc_dict = item['description'] if isinstance(item['description'], dict) else {'default': str(item['description'])}
        desc_dict.setdefault('default', '')
        description = str(desc_dict)
        score_thresholds_dict = item['scoreThresholds'] if isinstance(item['scoreThresholds'], dict) else {'default': str(item['scoreThresholds'])}
        score_thresholds_dict.setdefault('default', '')
        score_thresholds = str(score_thresholds_dict)
    icon = item['icon']
    confidence = item['confidence']
    is_default = False if add_mode else True
    created_at = datetime.now().isoformat()
    category = item['category']
    rsids = ";".join(item['rsids'])
    formula = item['formula']
    result = str(item['result'])
    reference = ";".join(item['reference'])
    
    # 插入数据
    cursor.execute('''
    INSERT INTO traits (id, name, description, icon, confidence, isDefault, createdAt, category, rsids, formula, scoreThresholds, result, reference)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (id, name, description, icon, confidence, is_default, created_at, category, rsids, formula, score_thresholds, result, reference))

    conn.commit()
    conn.close()

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
        scoreThresholds TEXT,
        result TEXT,
        reference TEXT
    )
    ''')
    conn.commit()
    conn.close()

    # 遍历JSON数据，插入特征数据
    for item in data:
        add_trait(item, db_path, False)

# 删除自定义的特征
def delete_trait(id, db_path):
    if not id.startswith('TRA_'):
        return

    # 连接到SQLite数据库
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 删除数据
    cursor.execute('''
    DELETE FROM traits WHERE id = ?
    ''', (id,))

    # 提交更改并关闭连接
    conn.commit()
    conn.close()

# 导入自定义特征
def self_json_to_trait_table(data, db_path):
    # 遍历JSON数据，插入特征数据
    for item in data:
        add_trait(item, db_path, False)

# 导出自定义特征
def self_traits_to_json(db_path):
    # 将特征表处理为一个字典格式
    # 连接到SQLite数据库
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 查询数据 ||  WHERE isDefault = 0
    cursor.execute('''
    SELECT * FROM traits WHERE isDefault = 0
    ''')
    rows = cursor.fetchall()

    # 关闭连接
    conn.close()

    # 转换为字典格式
    traits = []
    for row in rows:
        try:
            # 使用ast.literal_eval更安全地解析字符串字典
            name_dict = ast.literal_eval(row[1])
            description_dict = ast.literal_eval(row[2])
            score_thresholds_dict = ast.literal_eval(row[10])
            result_dict = ast.literal_eval(row[11])
            
            trait = {
                'id': row[0],
                'name': name_dict,
                'description': description_dict,
                'icon': row[3],
                'confidence': row[4],
                'isDefault': bool(row[5]),
                'createdAt': row[6],
                'category': row[7],
                'rsids': row[8].split(';') if row[8] else [],
                'formula': row[9],
                'scoreThresholds': score_thresholds_dict,
                'result': result_dict,
                'reference': row[12].split(';') if row[12] else []
            }
            traits.append(trait)
        except (ValueError, SyntaxError) as e:
            print(f"解析数据时出错: {e}")
            print(f"出错的行数据: {row}")
            continue
    
    # 将字典格式转换为JSON字符串
    json_str = json.dumps(traits, indent=4, ensure_ascii=False)
    return json_str

# 公式解析器
def parse_formula(formula, genotype_dict):
    """
    解析公式并计算结果，支持SCORE、IF以及组合公式
    
    :param formula: 公式字符串，如 "SCORE(rs4988235:CT=5,CC=0,TT=10)" 或 
                   "IF(rs4988235:CT=true,CC=false,TT=true)" 或
                   "IF(rs4988235:CT=true){SCORE(rs182549:CT=5,CC=0,TT=10)}ELSE{SCORE(rs182549:CT=0,CC=0,TT=5)}"
    :param genotype_dict: 包含位点对应结果的字典，如 {'rs4988235': 'CT'}
    :return: 计算得到的结果（得分或布尔值）
    """
    # 检查是否为组合公式（包含IF...ELSE结构）
    if formula.startswith("IF(") and "{" in formula:
        return _parse_combined_formula(formula, genotype_dict)
    # 检查是否为简单IF公式
    elif formula.startswith("IF("):
        return _parse_if_formula(formula, genotype_dict)
    # 检查是否为SCORE公式
    elif formula.startswith("SCORE("):
        return _parse_score_formula(formula, genotype_dict)
    else:
        raise ValueError("公式格式不正确，应以SCORE(或IF(开头")

def _parse_score_formula(formula, genotype_dict):
    """
    解析SCORE公式并计算得分
    
    :param formula: 公式字符串，如 "SCORE(rs4988235:CT=5,CC=0,TT=10; rs182549:CT=5,CC=0,TT=10)"
    :param genotype_dict: 包含位点对应结果的字典，如 {'rs4988235': 'CT'}
    :return: 计算得到的得分
    """
    # 检查公式是否以SCORE开头
    if not formula.startswith("SCORE(") or not formula.endswith(")"):
        raise ValueError("SCORE公式格式不正确，应以SCORE(开头并以)结尾")
    
    # 提取SCORE()括号内的内容
    content = formula[6:-1].strip()
    
    # 按分号分割不同的位点规则
    rsid_rules = content.split(';')
    
    total_score = 0
    
    for rule in rsid_rules:
        rule = rule.strip()
        if not rule:
            continue
            
        # 分离位点ID和得分规则
        parts = rule.split(':')
        if len(parts) != 2:
            continue
            
        rsid = parts[0].strip()
        score_rules = parts[1].strip()
        
        # 如果该位点不在输入的基因型字典中，跳过
        if rsid not in genotype_dict:
            continue
            
        # 获取该位点的基因型
        genotype = genotype_dict[rsid]
        
        # 解析得分规则
        score_pairs = score_rules.split(',')
        for pair in score_pairs:
            pair = pair.strip()
            if not pair:
                continue
                
            # 分离基因型和对应得分
            gt_score = pair.split('=')
            if len(gt_score) != 2:
                continue
                
            gt = gt_score[0].strip()
            try:
                score = float(gt_score[1].strip())
            except ValueError:
                continue
                
            # 如果基因型匹配，累加得分
            if gt == genotype:
                total_score += score
                break  # 找到匹配的基因型后，不再检查该位点的其他规则
    
    return total_score

def _parse_if_formula(formula, genotype_dict):
    """
    解析IF公式并返回布尔结果
    
    :param formula: 公式字符串，如 "IF(rs4988235:CT=true,CC=false,TT=true)"
    :param genotype_dict: 包含位点对应结果的字典，如 {'rs4988235': 'CT'}
    :return: 布尔值结果
    """
    # 检查公式是否以IF开头
    if not formula.startswith("IF(") or not formula.endswith(")"):
        raise ValueError("IF公式格式不正确，应以IF(开头并以)结尾")
    
    # 提取IF()括号内的内容
    content = formula[3:-1].strip()
    
    # 按分号分割不同的位点规则
    rsid_rules = content.split(';')
    
    # 对每个规则进行逻辑与操作，所有规则都为真时结果为真
    for rule in rsid_rules:
        rule = rule.strip()
        if not rule:
            continue
            
        # 分离位点ID和条件规则
        parts = rule.split(':')
        if len(parts) != 2:
            continue
            
        rsid = parts[0].strip()
        condition_rules = parts[1].strip()
        
        # 如果该位点不在输入的基因型字典中，跳过
        if rsid not in genotype_dict:
            continue
            
        # 获取该位点的基因型
        genotype = genotype_dict[rsid]
        
        # 解析条件规则
        condition_pairs = condition_rules.split(',')
        rule_result = False  # 默认该规则为假
        
        for pair in condition_pairs:
            pair = pair.strip()
            if not pair:
                continue
                
            # 分离基因型和对应条件
            gt_condition = pair.split('=')
            if len(gt_condition) != 2:
                continue
                
            gt = gt_condition[0].strip()
            condition_str = gt_condition[1].strip().lower()
            
            # 将字符串转换为布尔值
            if condition_str == 'true':
                condition = True
            elif condition_str == 'false':
                condition = False
            else:
                continue
                
            # 如果基因型匹配，获取条件结果
            if gt == genotype:
                rule_result = condition
                break  # 找到匹配的基因型后，不再检查该位点的其他规则
        
        # 如果任一规则为假，整个结果为假（逻辑与）
        if not rule_result:
            return False
    
    # 所有规则都为真，结果为真
    return True

def _parse_combined_formula(formula, genotype_dict):
    """
    解析组合公式（IF...ELSE结构）
    
    :param formula: 公式字符串，如 "IF(rs4988235:CT=true){SCORE(rs182549:CT=5,CC=0,TT=10)}ELSE{SCORE(rs182549:CT=0,CC=0,TT=5)}"
    :param genotype_dict: 包含位点对应结果的字典，如 {'rs4988235': 'CT'}
    :return: 根据条件计算得到的结果
    """
    # 提取IF条件部分
    if_end_index = formula.find('{')
    if if_end_index == -1:
        raise ValueError("组合公式格式不正确，缺少{")
    
    if_condition = formula[:if_end_index]
    
    # 提取IF为真时执行的公式
    true_start_index = if_end_index + 1
    true_end_index = _find_matching_brace(formula, true_start_index)
    if true_end_index == -1:
        raise ValueError("组合公式格式不正确，缺少匹配的}")
    
    true_formula = formula[true_start_index:true_end_index]
    
    # 检查是否有ELSE部分
    else_formula = None
    if true_end_index + 1 < len(formula) and formula[true_end_index+1:].strip().startswith("ELSE{"):
        else_start_index = formula.find('{', true_end_index) + 1
        else_end_index = _find_matching_brace(formula, else_start_index)
        if else_end_index == -1:
            raise ValueError("组合公式格式不正确，ELSE部分缺少匹配的}")
        
        else_formula = formula[else_start_index:else_end_index]
    
    # 计算IF条件
    condition_result = _parse_if_formula(if_condition, genotype_dict)
    
    # 根据条件结果执行相应的公式
    if condition_result:
        return parse_formula(true_formula, genotype_dict)
    elif else_formula is not None:
        return parse_formula(else_formula, genotype_dict)
    else:
        return 0  # 如果条件为假且没有ELSE部分，返回0

def _find_matching_brace(text, start_index):
    """
    查找匹配的右花括号
    
    :param text: 文本字符串
    :param start_index: 左花括号后的起始索引
    :return: 匹配的右花括号索引，如果没有找到则返回-1
    """
    count = 1  # 已经找到一个左花括号
    for i in range(start_index, len(text)):
        if text[i] == '{':
            count += 1
        elif text[i] == '}':
            count -= 1
            if count == 0:
                return i
    return -1

# 获取当前特征表结果
def result_trait_data(report_id, db_path):
    # 将特征表处理为一个字典格式
    # 连接到SQLite数据库
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 查询数据
    cursor.execute('''
    SELECT * FROM traits
    ''')
    rows = cursor.fetchall()

    # 关闭连接
    conn.close()

    # 转换为字典格式
    traits = []
    for row in rows:
        try:
            # 使用ast.literal_eval更安全地解析字符串字典
            name_dict = ast.literal_eval(row[1])
            description_dict = ast.literal_eval(row[2])
            score_thresholds_dict = ast.literal_eval(row[10])
            result_dict = ast.literal_eval(row[11])
            
            trait = {
                'id': row[0],
                'name': name_dict,
                'description': description_dict,
                'icon': row[3],
                'confidence': row[4],
                'isDefault': bool(row[5]),
                'createdAt': row[6],
                'category': row[7],
                'rsids': row[8].split(';') if row[8] else [],
                'formula': row[9],
                'scoreThresholds': score_thresholds_dict,
                'result': result_dict,
                'reference': row[12].split(';') if row[12] else []
            }
            traits.append(trait)
        except (ValueError, SyntaxError) as e:
            print(f"解析数据时出错: {e}")
            print(f"出错的行数据: {row}")
            continue
    
    # 聚合所有的rsid，先查询
    rsids = []
    for item in traits:
        rsids.extend(item['rsids'])
    rsids = list(set(rsids))
    rsid_result = get_snp_info_by_rsid(rsids, report_id, db_path, True)
    rsid_gt_result = {}
    for i in rsid_result:
        rsid_gt_result[i] = rsid_result[i][1]
    
    for item in traits:
        # 计算得分或布尔值
        score_or_bool = parse_formula(item['formula'], rsid_gt_result)
        scoreThresholds = item['scoreThresholds']
        
        # 判断是得分还是布尔值
        result_key = None
        if isinstance(score_or_bool, bool):
            # 如果是布尔值，根据布尔值和阈值判断结果
            for threshold in scoreThresholds:
                if score_or_bool == scoreThresholds[threshold]:
                    result_key = threshold
                    break
        elif isinstance(score_or_bool, (int, float)):
            # 如果是得分，根据得分和阈值判断结果
            for threshold in scoreThresholds:
                if score_or_bool >= scoreThresholds[threshold]:
                    result_key = threshold
                    break
        
        # 根据结果键值获得结果
        if result_key is None:
            result = None
        else:
            result = item['result'].get(result_key, None)
        item['result_current'] = result

        # 调整RSID的顺序
        item['rsids'] = [rsid for rsid in item['rsids'] if rsid in rsid_result]
        item['referenceGenotypes'] = [rsid_result[rsid][0] if rsid in rsid_result else None for rsid in item['rsids']]
        item['yourGenotypes'] = [rsid_result[rsid][1] if rsid in rsid_result else None for rsid in item['rsids']]
    return traits
        