# coding=utf-8
# pzw
# 20250422
# 这个脚本用于转换不同的厂商提供的结果文件，并储存到数据库中

"""
已支持：
- 23andMeV5
- Wegene
- AncestryDNA
"""

"""
rootara = read_rootara_core('Rootara.core.202404.txt.gz')
df = read_wegene_result('pzw_wegene.txt', rootara)
print(df.head())

# Clinvar
df_clinvar = df[df['CLNSIG'].isin(['Pathogenic', 'Likely_pathogenic', 'Benign', 'Likely_benign', 'Uncertain_significance'])]
df_clinvar = df_clinvar[df_clinvar['Check'].isin(['HET', 'HOM'])]
df_clinvar.to_excel('pzw_wegene_clinvar.xlsx', index = False)

匹配逻辑需要改一下，不然会有很多致病位点
考虑Clinvar只显示SNP致病位点，因为插入缺失测不准 || 留给前端处理

测试数据来源：
https://my.pgp-hms.org/public_genetic_data?data_type=23andMe
"""

import sys
import gzip
import pandas as pd
import argparse

# 读取rootara核心库
def read_rootara_core(file_path):
    """
    读取gzip压缩的文本文件，第一行为标题行
    :param file_path: 文件路径
    :param columns: 可选，列名列表。如果为None则使用文件第一行作为列名
    :return: pandas DataFrame
    """
    with gzip.open(file_path, 'rt') as f:
        df = pd.read_csv(
            f,
            sep = '\t',
            header = 0,
            low_memory=False
        )
    df.loc[:, 'Chrom'] = df['Chrom'].str.replace('chrM', 'MT')
    df.loc[:, 'Chrom'] = df['Chrom'].str.replace('chr', '')

    # 调整插入缺失
    def modify_indel(row):
        ref = row['Ref']
        alt = row['Alt']
        if len(ref) > len(alt):
            row['Ref'] = 'I'
            row['Alt'] = 'D'
        elif len(ref) < len(alt):
            row['Ref'] = 'D'
            row['Alt'] = 'I'
        elif len(ref) == len(alt):
            if len(ref) == 1:
                if alt == '-':
                    row['Ref'] = 'I'
                    row['Alt'] = 'D'
                elif ref == '-':
                    row['Ref'] = 'D'
                    row['Alt'] = 'I'
            # MNV || 难以处理，这种位点可以过滤掉
            else:
                row['Ref'] = 'NA'
                row['Alt'] = 'NA'
        return row
    
    df = df.apply(modify_indel, axis = 1)
    
    # MNV || 难以处理，这种位点可以过滤掉 || 芯片测序结果中无此信息
    df = df[df['Ref'] != 'NA']
    df = df[df['Alt'] != 'NA']
    return df

# 不用pandas的merge了，自行处理
def merge_dataframes(input_df, rootara_df):
    # 进行第一次merge，确认Ref
    df_merge = pd.merge(rootara_df, input_df, on=['Chrom', 'Start'], how='right')

    # 然后对合并后的结果进行过滤 对Ref Alt进一步匹配
    def ref_alt_match(row):
        all_type_list = [
            row['Ref'] + row['Ref'],
            row['Ref'] + row['Alt'],
            row['Alt'] + row['Alt'],
            row['Alt'] + row['Ref'],
        ]
        all_type_list = list(set(all_type_list))
        if row['Genotype'] in all_type_list:
            return row['Genotype']
        else:
            return 'NA'

    df_merge.loc[:, 'Genotype'] = df_merge.apply(ref_alt_match, axis=1)
    df_merge = df_merge[df_merge['Genotype'] != 'NA']
    df_merge = df_merge[df_merge['Genotype'] != '--']

    # 基因型转换
    def convert_genotype(row):
        ref = row['Ref']
        genotype = row['Genotype']
        genotype_check = genotype.count(ref)
        if genotype_check == 2:
            return 'WT'
        elif genotype_check == 1:
            return 'HET'
        elif genotype_check == 0:
            return 'HOM'
        return 'NA'

    df_merge['Check'] = 'NA'
    df_merge.loc[:, 'Check'] = df_merge.apply(convert_genotype, axis=1)
    df_merge = df_merge[df_merge['Check'] != 'NA']
    col_need = ['Chrom', 'Start', 'Ref', 'Alt', 'Gene', 'RSID_x', 'gnomAD_AF', 'CLNSIG', 'CLNDN', 'Genotype', 'Check']
    df_merge = df_merge[col_need]
    df_merge.rename(columns={'RSID_x': 'RSID'}, inplace=True)
    return df_merge

# wegene，通用格式
def read_uni_result(file_path, rootara_df):
    # 定义列名
    columns = ['RSID', 'Chrom', 'Start', 'Genotype']
    
    # 读取文件，跳过#开头的行和空行
    df = pd.read_csv(
        file_path, 
        sep = '\t',
        comment = '#',
        skip_blank_lines = True,
        names = columns,
        low_memory = False
    )
    before_count = df.shape[0]
    df_merge = merge_dataframes(df, rootara_df)
    after_count = df_merge.shape[0]

    trans_rate = after_count / before_count
    print('转换率：', "%.2f" % (trans_rate * 100) + '%')
    print('转换前数量：', before_count)
    print('转换后数量：', after_count)
    return df_merge

# 23andme的X、Y、MT只回报了单个碱基，特殊处理
def read_23andme_result(file_path, rootara_df):
    # 定义列名
    columns = ['RSID', 'Chrom', 'Start', 'Genotype']
    
    # 读取文件，跳过#开头的行和空行
    df = pd.read_csv(
        file_path, 
        sep = '\t',
        comment = '#',
        skip_blank_lines = True,
        names = columns,
        low_memory = False
    )

    # 处理X、Y、MT
    df.loc[:, 'Genotype'] = df.apply(
        lambda x:
            x['Genotype'] + x['Genotype'] if len(x['Genotype']) == 1 else x['Genotype'],
        axis = 1
    )

    before_count = df.shape[0]
    df_merge = merge_dataframes(df, rootara_df)
    after_count = df_merge.shape[0]

    trans_rate = after_count / before_count
    print('转换率：', "%.2f" % (trans_rate * 100) + '%')
    print('转换前数量：', before_count)
    print('转换后数量：', after_count)
    return df_merge

# AncestryDNA，23指chrX | 24指chrY | 25指chrY的PAR区 | 26指MT
def read_ancestry_result(file_path, rootara_df):
    # 读取文件，跳过#开头的行和空行
    df = pd.read_csv(
        file_path, 
        sep = '\t',
        comment = '#',
        skip_blank_lines = True,
        header = 0,
        low_memory = False
    )

    # 调整列名
    df.rename(columns={'rsid': 'RSID', 'chromosome': 'Chrom', 'position': 'Start'}, inplace=True)

    # 形成Genotype便于处理
    df.loc[:, 'Genotype'] = df.apply(lambda x: x['allele1'] + x['allele2'], axis = 1)

    # 处理X、Y、MT || 不需要PAR区
    df = df[df['Chrom'] != 25]
    df.loc[:, 'Chrom'] = df['Chrom'].astype(str)
    df.loc[:, 'Chrom'] = df['Chrom'].str.replace('23', 'X')
    df.loc[:, 'Chrom'] = df['Chrom'].str.replace('24', 'Y')
    df.loc[:, 'Chrom'] = df['Chrom'].str.replace('26', 'MT')

    before_count = df.shape[0]
    df_merge = merge_dataframes(df, rootara_df)
    after_count = df_merge.shape[0]

    trans_rate = after_count / before_count
    print('转换率：', "%.2f" % (trans_rate * 100) + '%')
    print('转换前数量：', before_count)
    print('转换后数量：', after_count)
    return df_merge

def csv_create(file_path, output_csv, method='23andme', rootara_core='Rootara.core.202404.txt.gz'):
    rootara_df = read_rootara_core(rootara_core)
    df_merge = pd.DataFrame()
    if method == '23andme':
        df_merge = read_23andme_result(file_path, rootara_df)
    elif method == 'ancestry':
        df_merge = read_ancestry_result(file_path, rootara_df)
    elif method == 'wegene':
        df_merge = read_uni_result(file_path, rootara_df)
    df_merge.to_csv(output_csv, index = False)

def main():
    parser = argparse.ArgumentParser(description='转换不同厂商的基因检测结果文件')
    parser.add_argument('--input', type=str, help='输入文件路径')
    parser.add_argument('--output', type=str, help='输出文件路径')
    parser.add_argument('--method', type=str, choices=['23andme', 'ancestry', 'wegene'], help='文件来源 (23andme/ancestry/wegene)', default='23andme')
    parser.add_argument('--rootara', type=str, help='Rootara核心库文件路径')
    args = parser.parse_args()

    # 检查是否提供了所有必需参数
    if not all([args.input, args.output, args.method, args.rootara]):
        parser.print_help()
        sys.exit(1)

    csv_create(args.input, args.output, args.method, args.rootara)

if __name__ == '__main__':
    main()
