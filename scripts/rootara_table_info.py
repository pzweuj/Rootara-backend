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

# 整张表的信息输出，表格很大，使用懒惰加载方式处理，支持前端表格展示、搜索和筛选
def get_all_snp_info(report_id, db_path, page_size=1000, page=1, sort_by="", sort_order='asc', 
                     search_term="", filters={}):
    
    # 在函数内部添加检查
    if sort_by == "":
        sort_by = None
    if search_term == "":
        search_term = None
    if filters == {}:
        filters = None
    
    # 连接到数据库
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 检查表是否存在
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (report_id,))
    if not cursor.fetchone():
        # 表不存在时返回空结果
        empty_result = {
            "data": {},
            "columns": [],
            "pagination": {
                "total": 0,
                "page": page,
                "page_size": page_size,
                "total_pages": 0
            }
        }
        conn.close()
        return empty_result
    
    # 获取表的列信息
    cursor.execute(f"PRAGMA table_info({report_id})")
    columns_info = cursor.fetchall()
    columns = [col[1] for col in columns_info]
    
    # 构建基本查询
    base_query = f"FROM {report_id}"
    count_query = f"SELECT COUNT(*) {base_query}"
    data_query = f"SELECT * {base_query}"
    
    # 构建WHERE子句
    where_clauses = []
    query_params = []
    
    # 添加搜索条件 - 修改为完美匹配
    if search_term:
        search_conditions = []
        for col in columns:
            search_conditions.append(f"{col} = ?")  # 使用等号而不是LIKE进行完美匹配
            query_params.append(search_term)  # 不再添加%通配符
        
        if search_conditions:
            where_clauses.append(f"({' OR '.join(search_conditions)})")
    
    # 添加筛选条件
    if filters and isinstance(filters, dict):
        for col, value in filters.items():
            if col in columns:
                if isinstance(value, list):
                    # 处理多选筛选
                    placeholders = ', '.join(['?'] * len(value))
                    where_clauses.append(f"{col} IN ({placeholders})")
                    query_params.extend(value)
                else:
                    # 处理单值筛选
                    where_clauses.append(f"{col} = ?")
                    query_params.append(value)
    
    # 组合WHERE子句
    if where_clauses:
        where_clause = " WHERE " + " AND ".join(where_clauses)
        count_query += where_clause
        data_query += where_clause
    
    # 添加排序
    if sort_by and sort_by in columns:
        sort_direction = "DESC" if sort_order.lower() == 'desc' else "ASC"
        data_query += f" ORDER BY {sort_by} {sort_direction}"
    
    # 计算总记录数（考虑筛选条件）
    cursor.execute(count_query, query_params)
    total_count = cursor.fetchone()[0]
    
    # 计算偏移量
    offset = (page - 1) * page_size
    
    # 添加分页
    data_query += " LIMIT ? OFFSET ?"
    query_params.extend([page_size, offset])
    
    # 执行查询
    cursor.execute(data_query, query_params)
    
    # 获取列名
    column_names = [description[0] for description in cursor.description]
    
    # 构建结果字典
    result = {
        "data": {},
        "columns": column_names,
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
        result["data"][snp_dict['rsid']] = snp_dict
    
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
