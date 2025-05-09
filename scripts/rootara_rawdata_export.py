# coding=utf-8
# 原始数据导出

import os

def export_rawdata(report_id):
    if report_id == 'RPT_TEMPLATE01':
        print('模板报告，无法导出!')
        return None, None
    
    rawdata_id = report_id.replace('RPT_', 'RDT_')
    rawdata_path = '/data/rawdata'
    
    if os.path.exists(rawdata_path):
        for file in os.listdir(rawdata_path):
            if file.startswith(rawdata_id) and len(rawdata_id) == 14:
                rawdata_file = os.path.join(rawdata_path, file)
                
                # 获取文件名和扩展名
                filename = os.path.basename(rawdata_file)
                
                # 读取文件内容
                try:
                    with open(rawdata_file, 'r', encoding='utf-8') as f:
                        file_content = f.read()
                    return filename, file_content
                except Exception as e:
                    print(f"读取文件失败: {str(e)}")
                    return None, None
    
    return None, None
