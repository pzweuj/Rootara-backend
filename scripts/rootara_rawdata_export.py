# coding=utf-8
# 原始数据导出

import os

def export_rawdata(report_id):
    if report_id == 'RPT_TEMPLATE01':
        print('模板报告，无法导出!')
        return
    rawdata_id = report_id.replace('RPT_', 'RDT_')
    rawdata_path = '/data/rawdata'
    if os.path.exists(rawdata_path):
        for file in os.listdir(rawdata_path):
            if file.startswith(rawdata_id) and len(rawdata_id) == 14:
                rawdata_file = os.path.join(rawdata_path, file)
                return rawdata_file
    return
