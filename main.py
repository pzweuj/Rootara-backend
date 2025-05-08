# coding=utf-8
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any

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
class ReportIdInput(BaseModel):
    report_id: str

class RsidInput(BaseModel):
    rsid: str
    report_id: str

# 添加初始化数据库的请求模型
class InitDbInput(BaseModel):
    email: str
    name: str

# 标准输出
class StatusOutput(BaseModel):
    status_code: int

# 创建API路由
DB_PATH = '/data/rootara.db'
if not os.path.exists('/data'):
    os.makedirs('/data')

## 初始化数据库
@app.post("/database/init", response_model=StatusOutput, tags=["database_init"])
async def api_init_db(input_data: InitDbInput):
    """
    Database initial.
    """
    init_db(input_data.email, input_data.name, DB_PATH)
    return StatusOutput(status_code=201)

# 添加创建报告的请求模型
class CreateReportInput(BaseModel):
    user_id: str
    input_data: str
    source_from: str
    report_name: str
    default_report: bool = False

## 创建报告
@app.post("/report/create", response_model=StatusOutput, tags=["report_create"])
async def api_create_new_report(input_data: CreateReportInput):
    """
    Create a new report.
    """
    create_new_report(
        input_data.user_id, 
        input_data.input_data, 
        input_data.source_from, 
        input_data.report_name, 
        DB_PATH, 
        input_data.default_report, 
        False
    )
    return StatusOutput(status_code=201)

# 添加删除报告的请求模型
class DeleteReportInput(BaseModel):
    report_id: str

## 删除报告
@app.post("/report/delete", response_model=StatusOutput, tags=["report_delete"])
async def api_delete_report(input_data: DeleteReportInput):
    """
    Delete a report.
    """
    delete_report(input_data.report_id, DB_PATH)
    return StatusOutput(status_code=200)

# 添加更新报告名称的请求模型
class RenameReportInput(BaseModel):
    report_id: str
    new_name: str

## 更新报告自定义名称
@app.post("/report/rename", response_model=StatusOutput, tags=["report_rename"])
async def api_update_report_name(input_data: RenameReportInput):
    """
    Rename a report.
    """
    update_report_name(input_data.report_id, input_data.new_name, DB_PATH)
    return StatusOutput(status_code=200)

## 查询报告信息
@app.get("/report/{report_id}/info", tags=["report_info"])
async def api_get_report_info(report_id: str):
    """
    Get report info.
    """
    try:
        result = get_report_info(report_id, DB_PATH)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询报告信息失败: {str(e)}")

## 列出所有的报告ID
@app.get("/report/list", tags=["report_list"])
async def api_list_all_report_ids():
    """
    List all reports.
    """
    try:
        result = list_all_report_ids(DB_PATH)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取报告列表失败: {str(e)}")

## 查询位点信息
@app.post("/variant/rsid", tags=["variant_rsid"])
async def api_get_snp_info_by_rsid(input_data: RsidInput):
    """
    RSID query.
    """
    try:
        result = get_snp_info_by_rsid(input_data.rsid, input_data.report_id, DB_PATH)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询位点信息失败: {str(e)}")

## 查询祖源分析结果
@app.get("/report/{report_id}/admixture", tags=["admixture_info"])
async def api_get_admixture_info(report_id: str):
    """
    Admixture query.
    """
    try:
        result = get_admixture_info(report_id, DB_PATH)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询祖源分析结果失败: {str(e)}")

## 查询单倍群结果
@app.get("/report/{report_id}/haplogroup", tags=["haplogroup_info"])
async def api_get_haplogroup_info(report_id: str):
    """
    Haplogroup query.
    """
    try:
        result = get_haplogroup_info(report_id, DB_PATH)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询单倍群结果失败: {str(e)}")

# --- 运行应用 (通常在命令行中做，这里用于测试) ---
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
