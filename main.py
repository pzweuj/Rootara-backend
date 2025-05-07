# coding=utf-8
from fastapi import FastAPI, HTTPException, Query, Path as FastApiPath
from pydantic import BaseModel
from typing import List, Optional, Dict

# 自定义脚本API
from scripts.rootara_sqlite import init_sqlite_db              # 初始化数据库



# API
app = FastAPI(
    title = 'Rootara API',
    description = 'Rootara API',
    version = '0.0.1'
)



