import os
from functools import cache

@cache
def get_tushare_client():
    """获取 Tushare 客户端实例"""
    import tushare as ts
    token = os.environ.get("TUSHARE_TOKEN")
    if not token:
        raise RuntimeError("TUSHARE_TOKEN is required")
    return ts.pro_api(token=token)
