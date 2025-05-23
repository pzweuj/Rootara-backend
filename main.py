# coding=utf-8
import os
from fastapi import FastAPI, HTTPException, Depends, Header, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Union

# 自定义脚本API
from scripts.rootara_get_user_id import get_user_id                                                  # 获取用户ID
from scripts.rootara_report_create import create_new_report                                          # 创建新报告
from scripts.rootara_report_del import delete_report                                                 # 删除报告
from scripts.rootara_report_set_default import set_default_report                                    # 设置默认报告
from scripts.rootara_rawdata_export import export_rawdata                                            # 导出原始数据
from scripts.rootara_reports_info import *                                                           # 报告信息相关
from scripts.rootara_table_info import get_snp_info_by_rsid, get_clinvar_data                        # 位点表信息相关
from scripts.rootara_get_admixture import get_admixture_info                                         # 查询祖源分析信息
from scripts.rootara_get_haplogroup import get_haplogroup_info                                       # 查询单倍群分析信息
from scripts.rootara_traits import *                                                                 # 查询特征分析信息

# API
app = FastAPI(
    # openapi_url = None,                 # 不生成文档
    title = 'Rootara API',
    description = 'Rootara API',
    version = '0.5.4'
)

# 允许请求 || 开发状态
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有来源，但需要token验证
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 设置API密钥 - 从环境变量读取
API_KEY = os.environ.get("ROOTARA_API_KEY", "rootara_api_key_default_001")  # 生产环境必须设置环境变量

# 验证API密钥的依赖函数
async def verify_api_key(x_api_key: str = Header(...)):
    if x_api_key != API_KEY:
        raise HTTPException(
            status_code=401,
            detail="无效的API密钥"
        )
    return x_api_key

# 定义请求与响应类型
class ReportIdInput(BaseModel):
    report_id: str

class RsidInput(BaseModel):
    rsid: list[str]  # rsid列表，每个元素为字符串类型
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

## 获取用户ID
@app.post("/user/id", tags=["user_id"])
async def api_get_user_id(api_key: str = Depends(verify_api_key)):
    """
    Get user ID.
    """
    user_id = get_user_id(DB_PATH)
    return {'status_code': 200, 'user_id': user_id}

# 添加创建报告的请求模型
class CreateReportInput(BaseModel):
    user_id: str
    input_data: str
    source_from: str
    report_name: str
    default_report: bool = False

## 创建报告
@app.post("/report/create", response_model=StatusOutput, tags=["report_create"])
async def api_create_new_report(input_data: CreateReportInput, api_key: str = Depends(verify_api_key)):
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

## 导出原始数据
@app.post("/report/{report_id}/rawdata", tags=["report_rawdata"])
async def api_export_rawdata(report_id: str, api_key: str = Depends(verify_api_key)):
    """
    Export raw data.
    """
    filename, file_content = export_rawdata(report_id)
    
    if filename is None or file_content is None:
        raise HTTPException(status_code=404, detail="原始数据文件不存在或无法读取")
    
    # 返回文件内容作为响应
    return Response(
        content=file_content,
        media_type="text/plain",
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )

## 设置默认报告
@app.post("/report/default", response_model=StatusOutput, tags=["report_default"])
async def api_set_default_report(report_id: str, api_key: str = Depends(verify_api_key)):
    """
    Set default report.
    """
    set_default_report(report_id, DB_PATH)
    return StatusOutput(status_code=200)

## 删除报告
@app.post("/report/delete", response_model=StatusOutput, tags=["report_delete"])
async def api_delete_report(input_data: ReportIdInput, api_key: str = Depends(verify_api_key)):
    """
    Delete a report.
    """
    delete_report(input_data.report_id, DB_PATH)
    return StatusOutput(status_code=200)

## 更新报告自定义名称
@app.post("/report/rename", response_model=StatusOutput, tags=["report_rename"])
async def api_update_report_name(report_id: str, new_name: str, api_key: str = Depends(verify_api_key)):
    """
    Rename a report.
    """
    update_report_name(report_id, new_name, DB_PATH)
    return StatusOutput(status_code=200)

## 查询报告信息 - 从GET改为POST
@app.post("/report/{report_id}/info", tags=["report_info"])
async def api_get_report_info(report_id: str, api_key: str = Depends(verify_api_key)):
    """
    Get report info.
    """
    try:
        result = get_report_info(report_id, DB_PATH)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询报告信息失败: {str(e)}")

## 列出所有的报告ID - 从GET改为POST
@app.post("/report/id", tags=["report_id"])
async def api_list_all_report_ids(api_key: str = Depends(verify_api_key)):
    """
    List all reports ID.
    """
    try:
        result = list_all_report_ids(DB_PATH)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取报告列表失败: {str(e)}")

## 列出所有的报告
@app.post("/report/all", tags=["report_all"])
async def api_get_all_report_info(api_key: str = Depends(verify_api_key)):
    """
    List all reports.
    """
    try:
        result = get_all_report_info(DB_PATH)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取报告列表失败: {str(e)}")

## 查询祖源分析结果 - 从GET改为POST
@app.post("/report/{report_id}/admixture", tags=["admixture_info"])
async def api_get_admixture_info(report_id: str, api_key: str = Depends(verify_api_key)):
    """
    Admixture query.
    """
    try:
        result = get_admixture_info(report_id, DB_PATH)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询祖源分析结果失败: {str(e)}")

## 查询单倍群结果 - 从GET改为POST
@app.post("/report/{report_id}/haplogroup", tags=["haplogroup_info"])
async def api_get_haplogroup_info(report_id: str, api_key: str = Depends(verify_api_key)):
    """
    Haplogroup query.
    """
    try:
        result = get_haplogroup_info(report_id, DB_PATH)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询单倍群结果失败: {str(e)}")

## 查询位点信息
@app.post("/variant/rsid", tags=["variant_rsid"])
async def api_get_snp_info_by_rsid(input_data: RsidInput, api_key: str = Depends(verify_api_key)):
    """
    RSID query.
    """
    try:
        result = get_snp_info_by_rsid(input_data.rsid, input_data.report_id, DB_PATH)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询位点信息失败: {str(e)}")

# 添加表格数据查询的请求模型
class TableQueryInput(BaseModel):
    report_id: str
    page_size: int = 1000
    page: int = 1
    sort_by: str = ""  # 修改为空字符串，而不是 None
    sort_order: str = "asc"
    search_term: str = ""  # 同样修改为空字符串
    filters: dict = {}

## 查询表格数据
@app.post("/report/table", tags=["report_table"])
async def api_get_table_data(input_data: TableQueryInput, api_key: str = Depends(verify_api_key)):
    """
    查询报告表格数据，支持分页、排序、搜索和筛选。
    """
    try:
        from scripts.rootara_table_info import get_all_snp_info
        result = get_all_snp_info(
            input_data.report_id,
            DB_PATH,
            input_data.page_size,
            input_data.page,
            input_data.sort_by,
            input_data.sort_order,
            input_data.search_term,
            input_data.filters
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询表格数据失败: {str(e)}")

# 添加ClinVar数据查询的请求模型
class ClinvarQueryInput(BaseModel):
    report_id: str
    sort_by: str = ""  # 默认为空字符串
    sort_order: str = "asc"
    search_term: str = ""  # 默认为空字符串
    filters: dict = {}
    indel: bool = False  # 是否包含插入删除变异

## 查询ClinVar数据
@app.post("/report/clinvar", tags=["report_clinvar"])
async def api_get_clinvar_data(input_data: ClinvarQueryInput, api_key: str = Depends(verify_api_key)):
    """
    查询报告中的ClinVar数据，支持分页、排序、搜索和筛选。
    返回结果包含致病性分类统计信息。
    """
    try:
        result = get_clinvar_data(
            input_data.report_id,
            DB_PATH,
            input_data.sort_by,
            input_data.sort_order,
            input_data.search_term,
            input_data.filters,
            input_data.indel
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询ClinVar数据失败: {str(e)}")

# 请求模型
class TraitInput(BaseModel):
    name: str                  # 特征名称
    description: str           # 特征描述
    scoreThresholds: str       # 分数阈值
    icon: str                  # 图标
    confidence: int            # 置信度
    category: str              # 分类
    rsids: List[str]           # rsid列表
    formula: str               # 公式
    result: str                # 结果（不同语言下的结果）
    reference: List[str]       # 参考文献列表

# 新增自定义特征
@app.post("/traits/add", tags=["traits_add"])
async def api_add_trait(input_data: TraitInput, api_key: str = Depends(verify_api_key)):
    """
    新增特征
    """
    add_trait(input_data.model_dump(), DB_PATH)
    return StatusOutput(status_code=201)

# 删除自定义特征
@app.post("/traits/delete", tags=["traits_delete"])
async def api_delete_trait(traits_id, api_key: str = Depends(verify_api_key)):
    """
    删除特征
    """
    delete_trait(traits_id, DB_PATH)
    return StatusOutput(status_code=200)

# 导入自定义特征
class TraitImportItem(BaseModel):
    id: str = Field(..., description="特征ID，导入时使用原始ID")
    name: Union[Dict[str, str], str] = Field(..., description="特征名称，可以是字符串或多语言字典")
    description: Union[Dict[str, str], str] = Field(..., description="特征描述，可以是字符串或多语言字典")
    icon: str = Field(..., description="图标")
    confidence: int = Field(..., description="置信度")
    category: str = Field(..., description="分类")
    rsids: List[str] = Field(default=[], description="rsid列表")
    formula: str = Field(..., description="公式")
    scoreThresholds: Union[Dict[str, Any], str] = Field(..., description="分数阈值")
    result: Union[Dict[str, Any], str] = Field(..., description="结果（不同语言下的结果）")
    reference: List[str] = Field(default=[], description="参考文献列表")

class TraitImportRequest(BaseModel):
    __root__: List[TraitImportItem] = Field(..., description="要导入的特征列表")

@app.post("/traits/import", tags=["traits_import"])
async def api_import_trait(input_data: TraitImportRequest, api_key: str = Depends(verify_api_key)):
    """
    导入特征
    """
    self_json_to_trait_table(input_data.__root__, DB_PATH)
    return StatusOutput(status_code=201)

# 导出自定义特征
class TraitExportItem(BaseModel):
    id: str = Field(..., description="特征ID")
    name: Dict[str, str] = Field(..., description="特征名称，多语言字典")
    description: Dict[str, str] = Field(..., description="特征描述，多语言字典")
    icon: str = Field(..., description="图标")
    confidence: int = Field(..., description="置信度")
    isDefault: bool = Field(..., description="是否为默认特征")
    createdAt: str = Field(..., description="创建时间")
    category: str = Field(..., description="分类")
    rsids: List[str] = Field(..., description="rsid列表")
    formula: str = Field(..., description="公式")
    scoreThresholds: Dict[str, Any] = Field(..., description="分数阈值")
    result: Dict[str, Any] = Field(..., description="结果（不同语言下的结果）")
    reference: List[str] = Field(..., description="参考文献列表")

class TraitExportResponse(BaseModel):
    __root__: List[TraitExportItem] = Field(..., description="导出的特征列表")

# 导出自定义特征
@app.post("/traits/export", tags=["traits_export"], response_model=TraitExportResponse)
async def api_export_trait(api_key: str = Depends(verify_api_key)):
    """
    导出特征
    """
    traits_json = self_traits_to_json(DB_PATH)
    # 将JSON字符串转换为Python对象
    traits_data = json.loads(traits_json)
    return TraitExportResponse(__root__=traits_data)

# 特征结果数据表
@app.post("/traits/info", tags=["traits_info"])
async def api_get_traits_info(report_id, api_key: str = Depends(verify_api_key)):
    """
    特征结果数据表
    """
    result = result_trait_data(report_id, DB_PATH)
    return result

# --- 运行应用 (通常在命令行中做，这里用于测试) ---
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
