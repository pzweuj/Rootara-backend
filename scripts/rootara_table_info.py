# coding=utf-8
# pzw
# 单个表格的信息查询和处理
import sqlite3

# 根据RSID查询若干个SNP的信息
def get_snp_info_by_rsid(rsid_list, report_id, db_path):
    # 连接到数据库
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 检查表是否存在
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (report_id,))
    if not cursor.fetchone():
        # 表不存在时返回空结果
        empty_result = {}
        for rsid in rsid_list:
            empty_result[rsid] = {
                'chromosome': None,
                'position': None,
                'ref': None,
                'alt': None,
                'rsid': rsid,
                'gnomAD_AF': None,
                'gene': None,
                'clnsig': None,
                'clndn': None,
                'genotype': None,
                'check': None
            }
        conn.close()
        return empty_result
    
    # 创建结果字典
    result_dict = {}
    
    # 查询每个RSID的SNP信息
    for rsid in rsid_list:
        cursor.execute(f"SELECT * FROM {report_id} WHERE rsid=?", (rsid,))
        snp_info = cursor.fetchone()
        
        if snp_info:
            # 获取列名
            column_names = [description[0] for description in cursor.description]
            # 将结果转换为字典
            snp_dict = dict(zip(column_names, snp_info))
            result_dict[rsid] = snp_dict
        else:
            # 如果没有找到该RSID的信息，添加空记录
            result_dict[rsid] = {
                'chromosome': None,
                'position': None,
                'ref': None,
                'alt': None,
                'rsid': rsid,
                'gnomAD_AF': None,
                'gene': None,
                'clnsig': None,
                'clndn': None,
                'genotype': None,
                'check': None
            }
    
    # 关闭数据库连接
    conn.close()
    return result_dict

# 根据chromosome position ref alt查询若干个SNP的信息
# 这个输入是一个这样的列表[(chromosome, position, ref, alt), (chromosome, position, ref, alt)]
def get_snp_info_by_chromosome_position_ref_alt(query_list, report_id, db_path):
    # 连接到数据库
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 检查表是否存在
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (report_id,))
    if not cursor.fetchone():
        # 表不存在时返回空结果
        empty_result = {}
        for query in query_list:
            chromosome, position, ref, alt = query
            empty_result[f"{chromosome}:{position}:{ref}:{alt}"] = {
                'chromosome': chromosome,
                'position': position,
                'ref': ref,
                'alt': alt,
                'rsid': None,
                'gnomAD_AF': None,
                'gene': None,
                'clnsig': None,
                'clndn': None,
                'genotype': None,
                'check': None
            }
        conn.close()
        return empty_result

    # 创建结果字典
    result_dict = {}

    # 查询每个chromosome position ref alt的SNP信息
    for query in query_list:
        chromosome, position, ref, alt = query
        cursor.execute(f"SELECT * FROM {report_id} WHERE chromosome=? AND position=? AND ref=? AND alt=?",  
                      (chromosome, position, ref, alt))
        snp_info = cursor.fetchone()

        if snp_info:
            # 获取列名
            column_names = [description[0] for description in cursor.description]
            # 将结果转换为字典
            snp_dict = dict(zip(column_names, snp_info))
            result_dict[f"{chromosome}:{position}:{ref}:{alt}"] = snp_dict
        else:
            # 如果没有找到该chromosome position ref alt的信息，添加空记录
            result_dict[f"{chromosome}:{position}:{ref}:{alt}"] = {
                'chromosome': chromosome,
                'position': position,
                'ref': ref,
                'alt': alt,
                'rsid': None,
                'gnomAD_AF': None,
                'gene': None,
                'clnsig': None,
                'clndn': None,
                'genotype': None,
                'check': None
            }

    # 关闭数据库连接
    conn.close()
    return result_dict

# 整张表的信息输出，表格很大，使用懒惰加载方式处理 || 看看能否通过前端实现，不一定要用这个函数
def get_all_snp_info(report_id, db_path, page_size=1000, page=1):
    # 连接到数据库
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 检查表是否存在
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (report_id,))
    if not cursor.fetchone():
        # 表不存在时返回空结果
        empty_result = {}
        conn.close()
        return empty_result

    # 计算总记录数
    cursor.execute(f"SELECT COUNT(*) FROM {report_id}")
    total_count = cursor.fetchone()[0]
    
    # 计算偏移量
    offset = (page - 1) * page_size
    
    # 分页查询数据
    cursor.execute(f"SELECT * FROM {report_id} LIMIT ? OFFSET ?", (page_size, offset))
    
    # 获取列名
    column_names = [description[0] for description in cursor.description]
    
    # 使用字典推导式直接构建结果字典
    result = {
        "data": {},
        "pagination": {
            "total": total_count,
            "page": page,
            "page_size": page_size,
            "total_pages": (total_count + page_size - 1) // page_size
        }
    }
    
    # 使用迭代器处理查询结果，避免一次性加载所有数据到内存
    for row in cursor:
        snp_dict = dict(zip(column_names, row))
        result["data"][snp_dict['id']] = snp_dict
    
    # 关闭数据库连接
    conn.close()
    return result

# Clinvar表 || 看看能不能在前端实现，不一定要用这个函数
def get_clinvar_data(report_id, db_path, indel=False):
    # 连接到数据库
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 检查表是否存在
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (report_id,))
    if not cursor.fetchone():
        # 表不存在时返回空结果
        empty_result = {}
        conn.close()
        return empty_result

    # 构建查询语句
    # 首先查询clndn不是 . 和 null的行
    query = f"SELECT * FROM {report_id} WHERE clndn != '.' AND clndn IS NOT NULL"
    # 然后查询clnsig不是Conflicting_classifications_of_pathogenicity或null的行
    query += f" AND clnsig != 'Conflicting_classifications_of_pathogenicity' AND clnsig IS NOT NULL"
    # 处理clnsig中包含'/'的情况，取第一个值为准
    query += f" AND ("
    query += f"   CASE"
    query += f"     WHEN INSTR(clnsig, '/') > 0 THEN"
    query += f"       CASE"
    query += f"         WHEN SUBSTR(clnsig, 1, INSTR(clnsig, '/') - 1) = 'Pathogenic' THEN 1"
    query += f"         WHEN SUBSTR(clnsig, 1, INSTR(clnsig, '/') - 1) = 'Benign' THEN 1"
    query += f"         WHEN SUBSTR(clnsig, 1, INSTR(clnsig, '/') - 1) = 'Likely_pathogenic' THEN 1"
    query += f"         WHEN SUBSTR(clnsig, 1, INSTR(clnsig, '/') - 1) = 'Likely_benign' THEN 1"
    query += f"         WHEN SUBSTR(clnsig, 1, INSTR(clnsig, '/') - 1) = 'Uncertain_significance' THEN 1"
    query += f"         ELSE 0"
    query += f"       END"
    query += f"     ELSE"
    query += f"       CASE"
    query += f"         WHEN clnsig = 'Pathogenic' THEN 1"
    query += f"         WHEN clnsig = 'Benign' THEN 1"
    query += f"         WHEN clnsig = 'Likely_pathogenic' THEN 1"
    query += f"         WHEN clnsig = 'Likely_benign' THEN 1"
    query += f"         WHEN clnsig = 'Uncertain_significance' THEN 1"
    query += f"         ELSE 0"
    query += f"       END"
    query += f"   END = 1"
    query += f")"

    # 只取clnsig是Pathogenic、Benign、Likely_pathogenic、Likely_benign、Uncertain_significance的行，但是，如果行中包含'/'，需要进行拆分并以第一位为准
    query += f" AND (clnsig='Pathogenic' OR clnsig='Benign' OR clnsig='Likely_pathogenic' OR clnsig='Likely_benign' OR clnsig='Uncertain_significance')"
    
    if indel is False:
        # 不需要ref或者alt是I或者D的行
        query += f" AND (ref!='I' AND ref!='D' AND alt!='I' AND alt!='D')"
    
    # 执行查询
    cursor.execute(query)
    # 获取列名
    column_names = [description[0] for description in cursor.description]

    # 使用字典推导式直接构建结果字典
    result = {}
    for row in cursor:
        snp_dict = dict(zip(column_names, row))
        result[snp_dict['id']] = snp_dict

    # 关闭数据库连接
    conn.close()
    return result
