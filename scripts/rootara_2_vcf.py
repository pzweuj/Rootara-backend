# coding=utf-8
# pzw
# 20250423
# 用于把rootara生成的csv结果文件转换为vcf格式
# 仅保留SNP
# vcf文件用于单倍群计算

import sys
import pandas as pd
import pysam
import argparse

def trans_rootara_to_vcf(rootara_csv, vcf_file):
    # 首先生成临时VCF文件
    if vcf_file.endswith('.vcf.gz'):
        vcf_file = vcf_file.replace('.vcf.gz', '.vcf')

    temp_vcf = vcf_file.replace('.vcf', '_temp.vcf')
    with open(temp_vcf, 'w', encoding='utf-8') as f:
        f.write('##fileformat=VCFv4.2\n')
        f.write('##source=rootara\n')
        f.write('##reference=GRCh37\n')
        f.write('#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tRootara\n')

        df = pd.read_csv(rootara_csv, sep=',', header=0, low_memory=False)
        df_filter = df[df['Check'] != 'WT']
        df_filter = df_filter[~df_filter['Genotype'].isin(['DD', 'II', 'DI', 'ID', '--'])]

        for i, row in df_filter.iterrows():
            chrom = row['Chrom']
            pos = row['Start']
            ref = row['Ref']
            alt = row['Alt']
            genotype = row['Check']

            gt = '0/0'
            if genotype == 'HET':
                gt = '0/1'
            elif genotype == 'HOM':
                gt = '1/1'

            f.write('{}\t{}\t.\t{}\t{}\t.\tPASS\t.\tGT\t{}\n'.format(str(chrom), str(pos), ref, alt, gt))
    
    # 使用pysam将VCF压缩为vcf.gz并创建索引
    pysam.tabix_compress(temp_vcf, vcf_file + '.gz', force=True)
    pysam.tabix_index(vcf_file + '.gz', preset='vcf', force=True)
    
    # 删除临时文件
    import os
    os.remove(temp_vcf)

def main():
    parser = argparse.ArgumentParser(description='转换CSV结果到VCF')
    parser.add_argument('--input', type=str, help='输入CSV文件')
    parser.add_argument('--output', type=str, help='输出VCF.gz文件')
    args = parser.parse_args()

    # 检查是否提供了所有必需参数
    if not all([args.input, args.output]):
        parser.print_help()
        sys.exit(1)

    trans_rootara_to_vcf(args.input, args.output)

if __name__ == '__main__':
    main()
