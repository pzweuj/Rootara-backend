# coding=utf-8
from fastapi import FastAPI, HTTPException, Query, Path as FastApiPath
from pydantic import BaseModel
from typing import List, Optional, Dict

# 自定义脚本API
from scripts.rootara_initial import init_sqlite_db                                                   # 初始化数据库
from scripts.rootara_report_create import create_new_report                                          # 创建新报告
from scripts.rootara_report_del import delete_report                                                 # 删除报告
from scripts.rootara_reports_info import update_report_name, get_report_info, list_all_report_ids    # 报告信息相关
from scripts.rootara_table_info import get_snp_info_by_rsid                                          # 位点表信息相关
from scripts.rootara_get_admixture import get_admixture_info                                         # 查询祖源分析信息
from scripts.rootara_get_haplogroup import get_haplogroup_info                                       # 查询单倍群分析信息

# API
app = FastAPI(
    title = 'Rootara API',
    description = 'Rootara API',
    version = '0.0.1'
)

# 定义请求与响应类型
class report_id_input(BaseModel):
    report_id: str

class rsid_input(BaseModel):
    rsid: str

# 标准输出
class StatusOutput(BaseModel):
    status_code: int

# 创建API路由
DB_PATH = 'database/rootara.db'
@app.get("/report/create", response_model=StatusOutput, tags=["report_create"])
async def api_create_new_report(user_id, input_data, source_from, report_name, default_report):
    """
    Create a new report.
    """
    try:
        code = create_new_report(user_id, input_data, source_from, report_name, DB_PATH, default_report, False)
    except:
        code = 204
    return StatusOutput(code)




