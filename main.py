# coding=utf-8
from fastapi import FastAPI
from pydantic import BaseModel

# 自定义脚本API
from scripts.rootara_initial import init_db                                                          # 初始化数据库
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
DB_PATH = '/app/database/rootara.db'

## 初始化数据库
@app.get("/database/init", response_model=StatusOutput, tags=["database_init"])
async def api_init_db(email, name):
    """
    Database initial.
    """
    init_db(email, name, DB_PATH)
    return StatusOutput(201)

## 创建报告
@app.get("/report/create", response_model=StatusOutput, tags=["report_create"])
async def api_create_new_report(user_id, input_data, source_from, report_name, default_report):
    """
    Create a new report.
    """
    create_new_report(user_id, input_data, source_from, report_name, DB_PATH, default_report, False)
    return StatusOutput(201)

## 删除报告
@app.get("/report/delete", response_model=StatusOutput, tags=["report_delete"])
async def api_delete_report(report_id):
    """
    Delete a report.
    """
    delete_report(report_id, DB_PATH)
    return StatusOutput(200)

## 更新报告自定义名称
@app.get("/report/rename", response_model=StatusOutput, tags=["report_rename"])
async def api_update_report_name(report_id, new_name):
    """
    Rename a report.
    """
    update_report_name(report_id, new_name, DB_PATH)
    return StatusOutput(200)

## 查询报告信息
@app.get("/report/info", response_model=StatusOutput, tags=["report_info"])
async def api_get_report_info(report_id):
    """
    Get report info.
    """
    get_report_info(report_id, DB_PATH)
    return StatusOutput(200)

## 列出所有的报告ID
@app.get("/report/list", response_model=StatusOutput, tags=["report_list"])
async def api_list_all_report_ids():
    """
    List all reports.
    """
    list_all_report_ids(DB_PATH)
    return StatusOutput(200)

## 查询位点信息
@app.get("/variant/rsid", response_model=StatusOutput, tags=["variant_rsid"])
async def api_get_snp_info_by_rsid(rsid_list, report_id):
    """
    RSID query.
    """
    result = get_snp_info_by_rsid(rsid_list, report_id, DB_PATH)
    return result

## 查询祖源分析结果
@app.get("/admixture/info", response_model=StatusOutput, tags=["admixture_info"])
async def api_get_admixture_info(report_id):
    """
    Admixture query.
    """
    result = get_admixture_info(report_id, DB_PATH)
    return result

## 查询单倍群结果
@app.get("/haplogroup/info", response_model=StatusOutput, tags=["haplogroup_info"])
async def api_get_haplogroup_info(report_id):
    """
    Haplogroup query.
    """
    result = get_haplogroup_info(report_id, DB_PATH)
    return result

# --- 运行应用 (通常在命令行中做，这里用于测试) ---
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
