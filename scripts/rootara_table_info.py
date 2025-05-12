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
# 改造后的Clinvar表函数，支持分页、排序和搜索，并增加致病性分类统计
def get_clinvar_data(report_id, db_path, sort_by="", sort_order='asc', 
                     search_term="", filters={}, indel=False):
    
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
            "statistics": {
                "pathogenic": 0,
                "likely_pathogenic": 0,
                "uncertain_significance": 0,
                "likely_benign": 0,
                "benign": 0
            }
        }
        conn.close()
        return empty_result
    
    # 获取表的列信息
    cursor.execute(f"PRAGMA table_info({report_id})")
    columns_info = cursor.fetchall()
    columns = [col[1] for col in columns_info]
    
    # 构建基本查询条件
    base_conditions = []
    base_params = []
    
    # 基础条件：gt不是. 和 null 还有WT
    base_conditions.append("gt!= '.' AND gt IS NOT NULL AND gt!='WT'")

    # 基础条件：clndn不是 . 和 null
    base_conditions.append("clndn != '.' AND clndn IS NOT NULL")
    
    # 基础条件：clnsig不是Conflicting_classifications_of_pathogenicity或null
    base_conditions.append("clnsig != 'Conflicting_classifications_of_pathogenicity' AND clnsig IS NOT NULL")
    
    # 基础条件：处理clnsig中包含'/'的情况，取第一个值为准
    clnsig_condition = """
    (CASE
        WHEN INSTR(clnsig, '/') > 0 THEN
            CASE
                WHEN SUBSTR(clnsig, 1, INSTR(clnsig, '/') - 1) = 'Pathogenic' THEN 1
                WHEN SUBSTR(clnsig, 1, INSTR(clnsig, '/') - 1) = 'Likely_pathogenic' THEN 1
                WHEN SUBSTR(clnsig, 1, INSTR(clnsig, '/') - 1) = 'Benign' THEN 1
                WHEN SUBSTR(clnsig, 1, INSTR(clnsig, '/') - 1) = 'Likely_benign' THEN 1
                WHEN SUBSTR(clnsig, 1, INSTR(clnsig, '/') - 1) = 'Uncertain_significance' THEN 1
                ELSE 0
            END
        ELSE
            CASE
                WHEN clnsig = 'Pathogenic' THEN 1
                WHEN clnsig = 'Likely_pathogenic' THEN 1
                WHEN clnsig = 'Benign' THEN 1
                WHEN clnsig = 'Likely_benign' THEN 1
                WHEN clnsig = 'Uncertain_significance' THEN 1
                ELSE 0
            END
    END) = 1
    """
    base_conditions.append(clnsig_condition)
    
    # 基础条件：只取特定clnsig值的行
    base_conditions.append("(clnsig='Pathogenic' OR clnsig='Likely_pathogenic' OR clnsig='Benign' OR clnsig='Likely_benign' OR clnsig='Uncertain_significance' OR clnsig LIKE '%/%')")
    
    # 基础条件：处理indel参数
    if indel is False:
        base_conditions.append("(ref!='I' AND ref!='D' AND alt!='I' AND alt!='D')")
    
    # 构建基本查询
    base_where = " WHERE " + " AND ".join(base_conditions)
    base_query = f"FROM {report_id}{base_where}"
    count_query = f"SELECT COUNT(*) {base_query}"
    data_query = f"SELECT * {base_query}"
    
    # 构建额外的WHERE子句（用户搜索和筛选）
    where_clauses = []
    query_params = list(base_params)  # 复制基础参数列表
    
    # 添加搜索条件 - 完美匹配
    if search_term:
        search_conditions = []
        for col in columns:
            search_conditions.append(f"{col} = ?")  # 使用等号进行完美匹配
            query_params.append(search_term)
        
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
    
    # 组合额外的WHERE子句
    if where_clauses:
        extra_where = " AND " + " AND ".join(where_clauses)
        count_query += extra_where
        data_query += extra_where
    
    # 添加排序
    if sort_by and sort_by in columns:
        sort_direction = "DESC" if sort_order.lower() == 'desc' else "ASC"
        data_query += f" ORDER BY {sort_by} {sort_direction}"
    
    # 计算总记录数（考虑筛选条件）
    cursor.execute(count_query, query_params)
    total_count = cursor.fetchone()[0]
    
    # 执行查询 - 不再使用分页限制，返回所有数据
    cursor.execute(data_query, query_params)
    
    # 获取列名
    column_names = [description[0] for description in cursor.description]
    
    # 构建结果字典 - 移除分页信息
    result = {
        "data": {},
        "columns": column_names,
        "total": total_count,  # 保留总记录数信息
        "statistics": {
            "pathogenic": 0,
            "likely_pathogenic": 0,
            "uncertain_significance": 0,
            "likely_benign": 0,
            "benign": 0
        }
    }
    
    # 使用迭代器处理查询结果，避免一次性加载所有数据到内存
    for row in cursor:
        snp_dict = dict(zip(column_names, row))
        
        # 处理clnsig字段，确定致病性分类
        clnsig = snp_dict.get('clnsig', '')
        if '/' in clnsig:
            # 如果有多个分类，取第一个
            primary_clnsig = clnsig.split('/')[0]
        else:
            primary_clnsig = clnsig
            
        # 根据优先级顺序更新统计数据
        if primary_clnsig == 'Pathogenic':
            result["statistics"]["pathogenic"] += 1
        elif primary_clnsig == 'Likely_pathogenic':
            result["statistics"]["likely_pathogenic"] += 1
        elif primary_clnsig == 'Benign':
            result["statistics"]["benign"] += 1
        elif primary_clnsig == 'Likely_benign':
            result["statistics"]["likely_benign"] += 1
        elif primary_clnsig == 'Uncertain_significance':
            result["statistics"]["uncertain_significance"] += 1
            
        # 将数据添加到结果集
        result["data"][snp_dict.get('id', '') or snp_dict.get('rsid', '')] = snp_dict
    
    # 获取完整的统计数据
    stats_query = f"""
    SELECT 
        SUM(CASE WHEN (INSTR(clnsig, '/') > 0 AND SUBSTR(clnsig, 1, INSTR(clnsig, '/') - 1) = 'Pathogenic') OR clnsig = 'Pathogenic' THEN 1 ELSE 0 END) as pathogenic,
        SUM(CASE WHEN (INSTR(clnsig, '/') > 0 AND SUBSTR(clnsig, 1, INSTR(clnsig, '/') - 1) = 'Likely_pathogenic') OR clnsig = 'Likely_pathogenic' THEN 1 ELSE 0 END) as likely_pathogenic,
        SUM(CASE WHEN (INSTR(clnsig, '/') > 0 AND SUBSTR(clnsig, 1, INSTR(clnsig, '/') - 1) = 'Benign') OR clnsig = 'Benign' THEN 1 ELSE 0 END) as benign,
        SUM(CASE WHEN (INSTR(clnsig, '/') > 0 AND SUBSTR(clnsig, 1, INSTR(clnsig, '/') - 1) = 'Likely_benign') OR clnsig = 'Likely_benign' THEN 1 ELSE 0 END) as likely_benign,
        SUM(CASE WHEN (INSTR(clnsig, '/') > 0 AND SUBSTR(clnsig, 1, INSTR(clnsig, '/') - 1) = 'Uncertain_significance') OR clnsig = 'Uncertain_significance' THEN 1 ELSE 0 END) as uncertain_significance
    {base_query}
    """
    
    # 不添加用户筛选条件，直接执行查询
    cursor.execute(stats_query, base_params)  # 只使用基础参数，不包含用户筛选条件
    stats = cursor.fetchone()
    
    if stats:
        result["statistics"] = {
            "pathogenic": stats[0] or 0,
            "likely_pathogenic": stats[1] or 0,
            "benign": stats[2] or 0,
            "likely_benign": stats[3] or 0,
            "uncertain_significance": stats[4] or 0
        }
    
    # 关闭数据库连接
    conn.close()
    return result
