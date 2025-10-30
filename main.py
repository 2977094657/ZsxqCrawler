"""
知识星球数据采集器 - FastAPI 后端服务
提供RESTful API接口来操作现有的爬虫功能
"""

import os
import sys
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime
import json
import requests

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, Response
from pydantic import BaseModel, Field
import uvicorn
import mimetypes
import random
import time

# 添加项目根目录到Python路径（现在main.py就在根目录）
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.append(project_root)

# 导入现有的业务逻辑模块
from zsxq_interactive_crawler import ZSXQInteractiveCrawler, load_config
from db_path_manager import get_db_path_manager
from image_cache_manager import get_image_cache_manager
from accounts_manager import (
    get_accounts as am_get_accounts,
    add_account as am_add_account,
    delete_account as am_delete_account,
    set_default_account as am_set_default_account,
    assign_group_account as am_assign_group_account,
    get_account_for_group as am_get_account_for_group,
    get_account_summary_for_group as am_get_account_summary_for_group,
    get_default_account as am_get_default_account,
    get_account_by_id as am_get_account_by_id,
)
from account_info_db import get_account_info_db

app = FastAPI(
    title="知识星球数据采集器 API",
    description="为知识星球数据采集器提供RESTful API接口",
    version="1.0.0"
)

# 配置CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 前端地址
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 全局变量存储爬虫实例和任务状态
crawler_instance: Optional[ZSXQInteractiveCrawler] = None
current_tasks: Dict[str, Dict[str, Any]] = {}
task_counter = 0
task_logs: Dict[str, List[str]] = {}  # 存储任务日志
sse_connections: Dict[str, List] = {}  # 存储SSE连接
task_stop_flags: Dict[str, bool] = {}  # 任务停止标志
file_downloader_instances: Dict[str, Any] = {}  # 存储文件下载器实例

# =========================
# 本地群扫描（output 目录）
# =========================

# 可配置：默认 ./output；可通过环境变量 OUTPUT_DIR 覆盖
LOCAL_OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "output")
# 处理上限保护，默认 10000；可通过 LOCAL_GROUPS_SCAN_LIMIT 覆盖
try:
    LOCAL_SCAN_LIMIT = int(os.environ.get("LOCAL_GROUPS_SCAN_LIMIT", "10000"))
except Exception:
    LOCAL_SCAN_LIMIT = 10000

# 本地群缓存
_local_groups_cache = {
    "ids": set(),     # set[int]
    "scanned_at": 0.0 # epoch 秒
}


def _safe_listdir(path: str):
    """安全列目录，异常不抛出，返回空列表并告警"""
    try:
        return os.listdir(path)
    except Exception as e:
        print(f"⚠️ 无法读取目录 {path}: {e}")
        return []


def _collect_numeric_dirs(base: str, limit: int) -> set:
    """
    扫描 base 的一级子目录，收集纯数字目录名（^\d+$）作为群ID。
    忽略：非目录、软链接、隐藏目录（以 . 开头）。
    """
    ids = set()
    if not base:
        return ids

    base_abs = os.path.abspath(base)
    if not (os.path.exists(base_abs) and os.path.isdir(base_abs)):
        # 视为空集合，不报错
        print(f"⚠️ 目录不存在或不可读: {base_abs}，视为空集合")
        return ids

    processed = 0
    for name in _safe_listdir(base_abs):
        # 隐藏目录
        if not name or name.startswith('.'):
            continue

        path = os.path.join(base_abs, name)
        try:
            # 软链接/非目录忽略
            if os.path.islink(path) or not os.path.isdir(path):
                continue

            # 仅纯数字目录名
            if name.isdigit():
                ids.add(int(name))
                processed += 1
                if processed >= limit:
                    print(f"⚠️ 子目录数量超过上限 {limit}，已截断")
                    break
        except Exception:
            # 单项失败安全降级
            continue

    return ids


def scan_local_groups(output_dir: str = None, limit: int = None) -> set:
    """
    扫描本地 output 的一级子目录，获取群ID集合。
    同时兼容 output/databases 结构（如存在）。
    同步执行（用于手动刷新或强制刷新），异常安全降级。
    """
    try:
        odir = output_dir or LOCAL_OUTPUT_DIR
        lim = int(limit or LOCAL_SCAN_LIMIT)

        # 主路径：仅扫描 output 的一级子目录
        ids_primary = _collect_numeric_dirs(odir, lim)

        # 兼容路径：output/databases 的一级子目录（若存在）
        ids_secondary = _collect_numeric_dirs(os.path.join(odir, "databases"), lim)

        ids = set(ids_primary) | set(ids_secondary)

        # 更新缓存
        _local_groups_cache["ids"] = ids
        _local_groups_cache["scanned_at"] = time.time()

        return ids
    except Exception as e:
        print(f"⚠️ 本地群扫描异常: {e}")
        # 安全降级为旧缓存
        return _local_groups_cache.get("ids", set())


def get_cached_local_group_ids(force_refresh: bool = False) -> set:
    """
    获取缓存中的本地群ID集合；可选强制刷新。
    未扫描过或要求强更时触发同步扫描。
    """
    if force_refresh or not _local_groups_cache.get("ids"):
        return scan_local_groups()
    return _local_groups_cache.get("ids", set())


@app.on_event("startup")
async def _init_local_groups_scan():
    """
    应用启动时后台异步执行一次扫描，不阻塞主线程。
    """
    try:
        await asyncio.to_thread(scan_local_groups)
    except Exception as e:
        print(f"⚠️ 启动扫描本地群失败: {e}")
# Pydantic模型定义
class ConfigModel(BaseModel):
    cookie: str = Field(..., description="知识星球Cookie")
    group_id: str = Field(..., description="群组ID")
    db_path: str = Field(default="zsxq_interactive.db", description="数据库路径")

class CrawlHistoricalRequest(BaseModel):
    pages: int = Field(default=10, ge=1, le=1000, description="爬取页数")
    per_page: int = Field(default=20, ge=1, le=100, description="每页数量")
    crawlIntervalMin: Optional[float] = Field(default=None, ge=1.0, le=60.0, description="爬取间隔最小值(秒)")
    crawlIntervalMax: Optional[float] = Field(default=None, ge=1.0, le=60.0, description="爬取间隔最大值(秒)")
    longSleepIntervalMin: Optional[float] = Field(default=None, ge=60.0, le=3600.0, description="长休眠间隔最小值(秒)")
    longSleepIntervalMax: Optional[float] = Field(default=None, ge=60.0, le=3600.0, description="长休眠间隔最大值(秒)")
    pagesPerBatch: Optional[int] = Field(default=None, ge=5, le=50, description="每批次页面数")

class CrawlSettingsRequest(BaseModel):
    crawlIntervalMin: Optional[float] = Field(default=None, ge=1.0, le=60.0, description="爬取间隔最小值(秒)")
    crawlIntervalMax: Optional[float] = Field(default=None, ge=1.0, le=60.0, description="爬取间隔最大值(秒)")
    longSleepIntervalMin: Optional[float] = Field(default=None, ge=60.0, le=3600.0, description="长休眠间隔最小值(秒)")
    longSleepIntervalMax: Optional[float] = Field(default=None, ge=60.0, le=3600.0, description="长休眠间隔最大值(秒)")
    pagesPerBatch: Optional[int] = Field(default=None, ge=5, le=50, description="每批次页面数")

class FileDownloadRequest(BaseModel):
    max_files: Optional[int] = Field(default=None, description="最大下载文件数")
    sort_by: str = Field(default="download_count", description="排序方式: download_count 或 time")
    download_interval: float = Field(default=1.0, ge=0.1, le=300.0, description="单次下载间隔（秒）")
    long_sleep_interval: float = Field(default=60.0, ge=10.0, le=3600.0, description="长休眠间隔（秒）")
    files_per_batch: int = Field(default=10, ge=1, le=100, description="下载多少文件后触发长休眠")
    # 随机间隔范围参数（可选）
    download_interval_min: Optional[float] = Field(default=None, ge=1.0, le=300.0, description="随机下载间隔最小值（秒）")
    download_interval_max: Optional[float] = Field(default=None, ge=1.0, le=300.0, description="随机下载间隔最大值（秒）")
    long_sleep_interval_min: Optional[float] = Field(default=None, ge=10.0, le=3600.0, description="随机长休眠间隔最小值（秒）")
    long_sleep_interval_max: Optional[float] = Field(default=None, ge=10.0, le=3600.0, description="随机长休眠间隔最大值（秒）")

class AccountCreateRequest(BaseModel):
    cookie: str = Field(..., description="账号Cookie")
    name: Optional[str] = Field(default=None, description="账号名称")
    make_default: Optional[bool] = Field(default=False, description="是否设为默认账号")

class AssignGroupAccountRequest(BaseModel):
    account_id: str = Field(..., description="账号ID")

class GroupInfo(BaseModel):
    group_id: int
    name: str
    type: str
    background_url: Optional[str] = None
    owner: Optional[dict] = None
    statistics: Optional[dict] = None

class TaskResponse(BaseModel):
    task_id: str
    status: str  # pending, running, completed, failed
    message: str
    result: Optional[Dict[str, Any]] = None
    created_at: datetime
    updated_at: datetime

# 辅助函数
def get_crawler(log_callback=None) -> ZSXQInteractiveCrawler:
    """获取爬虫实例"""
    global crawler_instance
    if crawler_instance is None:
        config = load_config()
        if not config:
            raise HTTPException(status_code=500, detail="配置文件加载失败")

        auth_config = config.get('auth', {})

        cookie = auth_config.get('cookie', '')
        group_id = auth_config.get('group_id', '')

        if cookie == "your_cookie_here" or group_id == "your_group_id_here" or not cookie or not group_id:
            raise HTTPException(status_code=400, detail="请先在config.toml中配置Cookie和群组ID")

        # 使用路径管理器获取数据库路径
        path_manager = get_db_path_manager()
        db_path = path_manager.get_topics_db_path(group_id)

        crawler_instance = ZSXQInteractiveCrawler(cookie, group_id, db_path, log_callback)

    return crawler_instance

def get_crawler_for_group(group_id: str, log_callback=None) -> ZSXQInteractiveCrawler:
    """为指定群组获取爬虫实例"""
    config = load_config()
    if not config:
        raise HTTPException(status_code=500, detail="配置文件加载失败")

    # 自动匹配该群组所属账号，获取对应Cookie
    cookie = get_cookie_for_group(group_id)

    if not cookie or cookie == "your_cookie_here":
        raise HTTPException(status_code=400, detail="未找到可用Cookie，请先在账号管理或config.toml中配置")

    # 使用路径管理器获取指定群组的数据库路径
    path_manager = get_db_path_manager()
    db_path = path_manager.get_topics_db_path(group_id)

    return ZSXQInteractiveCrawler(cookie, group_id, db_path, log_callback)

def get_crawler_safe() -> Optional[ZSXQInteractiveCrawler]:
    """安全获取爬虫实例，配置未设置时返回None"""
    try:
        return get_crawler()
    except HTTPException:
        return None

def is_configured() -> bool:
    """检查是否已配置认证信息"""
    try:
        config = load_config()
        if not config:
            return False

        auth_config = config.get('auth', {})
        cookie = auth_config.get('cookie', '')
        group_id = auth_config.get('group_id', '')

        return (cookie != "your_cookie_here" and
                group_id != "your_group_id_here" and
                cookie and group_id)
    except:
        return False

def create_task(task_type: str, description: str) -> str:
    """创建新任务"""
    global task_counter
    task_counter += 1
    task_id = f"task_{task_counter}_{int(datetime.now().timestamp())}"
    
    current_tasks[task_id] = {
        "task_id": task_id,
        "type": task_type,
        "status": "pending",
        "message": description,
        "result": None,
        "created_at": datetime.now(),
        "updated_at": datetime.now()
    }

    # 初始化任务日志和停止标志
    task_logs[task_id] = []
    task_stop_flags[task_id] = False
    add_task_log(task_id, f"任务创建: {description}")

    return task_id

def add_task_log(task_id: str, log_message: str):
    """添加任务日志"""
    if task_id not in task_logs:
        task_logs[task_id] = []

    timestamp = datetime.now().strftime("%H:%M:%S")
    formatted_log = f"[{timestamp}] {log_message}"
    task_logs[task_id].append(formatted_log)

    # 广播日志到所有SSE连接
    broadcast_log(task_id, formatted_log)

def broadcast_log(task_id: str, log_message: str):
    """广播日志到SSE连接"""
    # 这个函数现在主要用于存储日志，实际的SSE广播在stream端点中实现
    pass

def build_stealth_headers(cookie: str) -> Dict[str, str]:
    """构造更接近官网的请求头，提升成功率"""
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    ]
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7",
        "Cache-Control": "no-cache",
        "Cookie": cookie,
        "Origin": "https://wx.zsxq.com",
        "Pragma": "no-cache",
        "Priority": "u=1, i",
        "Referer": "https://wx.zsxq.com/",
        "Sec-Ch-Ua": "\"Google Chrome\";v=\"137\", \"Chromium\";v=\"137\", \"Not/A)Brand\";v=\"24\"",
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "\"Windows\"",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "User-Agent": random.choice(user_agents),
        "X-Aduid": "a3be07cd6-dd67-3912-0093-862d844e7fe",
        "X-Request-Id": f"dcc5cb6ab-1bc3-8273-cc26-{random.randint(100000000000, 999999999999)}",
        "X-Signature": "733fd672ddf6d4e367730d9622cdd1e28a4b6203",
        "X-Timestamp": str(int(time.time())),
        "X-Version": "2.77.0",
    }
    return headers

def update_task(task_id: str, status: str, message: str, result: Optional[Dict[str, Any]] = None):
    """更新任务状态"""
    if task_id in current_tasks:
        current_tasks[task_id].update({
            "status": status,
            "message": message,
            "result": result,
            "updated_at": datetime.now()
        })

        # 添加状态变更日志
        add_task_log(task_id, f"状态更新: {message}")

def stop_task(task_id: str) -> bool:
    """停止任务"""
    if task_id not in current_tasks:
        return False

    task = current_tasks[task_id]

    if task["status"] not in ["pending", "running"]:
        return False

    # 设置停止标志
    task_stop_flags[task_id] = True
    add_task_log(task_id, "🛑 收到停止请求，正在停止任务...")

    # 如果有爬虫实例，也设置爬虫的停止标志
    global crawler_instance, file_downloader_instances
    if crawler_instance:
        crawler_instance.set_stop_flag()

    # 如果有文件下载器实例，也设置停止标志
    if task_id in file_downloader_instances:
        downloader = file_downloader_instances[task_id]
        downloader.set_stop_flag()

    update_task(task_id, "cancelled", "任务已被用户停止")

    return True

def is_task_stopped(task_id: str) -> bool:
    """检查任务是否被停止"""
    stopped = task_stop_flags.get(task_id, False)
    return stopped

# API路由定义
@app.get("/")
async def root():
    """根路径"""
    return {"message": "知识星球数据采集器 API 服务", "version": "1.0.0"}

@app.get("/api/health")
async def health_check():
    """健康检查"""
    return {"status": "healthy", "timestamp": datetime.now()}

@app.get("/api/config")
async def get_config():
    """获取当前配置"""
    try:
        config = load_config()
        if not config:
            raise HTTPException(status_code=500, detail="配置文件不存在")

        auth_config = config.get('auth', {})
        cookie = auth_config.get('cookie', '')
        group_id = auth_config.get('group_id', '')

        # 检查配置状态
        configured = is_configured()

        # 隐藏敏感信息
        safe_config = {
            "configured": configured,
            "auth": {
                "cookie": "***" if configured else "未配置",
                "group_id": group_id if group_id != "your_group_id_here" else "未配置"
            },
            "database": config.get('database', {}),
            "download": config.get('download', {})
        }

        return safe_config
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取配置失败: {str(e)}")

@app.post("/api/config")
async def update_config(config: ConfigModel):
    """更新配置"""
    try:
        # 使用路径管理器获取数据库路径
        path_manager = get_db_path_manager()
        topics_db_path = path_manager.get_topics_db_path(config.group_id)
        from pathlib import Path
        safe_topics_db_path = Path(topics_db_path).as_posix()

        # 创建配置内容
        config_content = f"""# 知识星球数据采集器配置文件
# 通过Web界面自动生成

[auth]
# 知识星球登录Cookie
cookie = "{config.cookie}"

# 知识星球群组ID
group_id = "{config.group_id}"

[database]
# 数据库文件路径（由路径管理器自动管理）
path = "{safe_topics_db_path}"

[download]
# 下载目录
dir = "downloads"
"""

        # 保存配置文件
        config_path = "config.toml"
        with open(config_path, 'w', encoding='utf-8') as f:
            f.write(config_content)

        # 重置爬虫实例，强制重新加载配置
        global crawler_instance
        crawler_instance = None

        return {"message": "配置更新成功", "success": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"更新配置失败: {str(e)}")

# 账号管理 API
@app.get("/api/accounts")
async def list_accounts():
    try:
        accounts = am_get_accounts(mask_cookie=True)
        # 若未添加本地账号，但在 config.toml 中存在默认 Cookie，则返回一个“默认账号”占位
        if not accounts:
            cfg = load_config()
            auth = cfg.get('auth', {}) if cfg else {}
            default_cookie = auth.get('cookie', '')
            if default_cookie and default_cookie != "your_cookie_here":
                accounts = [{
                    "id": "default",
                    "name": "默认账号",
                    "cookie": "***",
                    "is_default": True,
                    "created_at": None,
                }]
        return {"accounts": accounts}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取账号列表失败: {str(e)}")

@app.post("/api/accounts")
async def create_account(request: AccountCreateRequest):
    try:
        acc = am_add_account(request.cookie, request.name, request.make_default or False)
        safe_acc = am_get_account_by_id(acc.get("id"), mask_cookie=True)
        return {"account": safe_acc}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"新增账号失败: {str(e)}")

@app.delete("/api/accounts/{account_id}")
async def remove_account(account_id: str):
    try:
        ok = delete_account_success = am_delete_account(account_id)
        if not ok:
            raise HTTPException(status_code=404, detail="账号不存在")
        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"删除账号失败: {str(e)}")

@app.post("/api/accounts/{account_id}/default")
async def make_default_account(account_id: str):
    try:
        ok = am_set_default_account(account_id)
        if not ok:
            raise HTTPException(status_code=404, detail="账号不存在")
        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"设置默认账号失败: {str(e)}")

@app.post("/api/groups/{group_id}/assign-account")
async def assign_account_to_group(group_id: str, request: AssignGroupAccountRequest):
    try:
        ok, msg = am_assign_group_account(group_id, request.account_id)
        if not ok:
            raise HTTPException(status_code=400, detail=msg)
        return {"success": True, "message": msg}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"分配账号失败: {str(e)}")

@app.get("/api/groups/{group_id}/account")
async def get_group_account(group_id: str):
    try:
        summary = get_account_summary_for_group_auto(group_id)
        return {"account": summary}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取群组账号失败: {str(e)}")

# 账号“自我信息”持久化 (/v3/users/self)
@app.get("/api/accounts/{account_id}/self")
async def get_account_self(account_id: str):
    """获取并返回指定账号的已持久化自我信息；若无则尝试抓取并保存"""
    try:
        db = get_account_info_db()
        info = db.get_self_info(account_id)
        if info:
            return {"self": info}

        # 若数据库无记录则抓取，支持 'default' 伪账号（来自 config.toml）
        cookie = None
        acc = am_get_account_by_id(account_id, mask_cookie=False)
        if acc:
            cookie = acc.get("cookie", "")
        elif account_id == "default":
            cfg = load_config()
            auth = cfg.get('auth', {}) if cfg else {}
            cookie = auth.get('cookie', '')
        else:
            raise HTTPException(status_code=404, detail="账号不存在")

        if not cookie:
            raise HTTPException(status_code=400, detail="账号未配置Cookie")

        headers = build_stealth_headers(cookie)
        resp = requests.get('https://api.zsxq.com/v3/users/self', headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if not data.get('succeeded'):
            raise HTTPException(status_code=400, detail="API返回失败")

        rd = data.get('resp_data', {}) or {}
        user = rd.get('user', {}) or {}
        wechat = (rd.get('accounts', {}) or {}).get('wechat', {}) or {}

        self_info = {
            "uid": user.get("uid"),
            "name": user.get("name") or wechat.get("name"),
            "avatar_url": user.get("avatar_url") or wechat.get("avatar_url"),
            "location": user.get("location"),
            "user_sid": user.get("user_sid"),
            "grade": user.get("grade"),
        }
        db.upsert_self_info(account_id, self_info, raw_json=data)
        return {"self": db.get_self_info(account_id)}
    except HTTPException:
        raise
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"网络请求失败: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取账号信息失败: {str(e)}")

@app.post("/api/accounts/{account_id}/self/refresh")
async def refresh_account_self(account_id: str):
    """强制抓取 /v3/users/self 并更新持久化"""
    try:
        # 支持 'default' 伪账号（来自 config.toml）
        cookie = None
        acc = am_get_account_by_id(account_id, mask_cookie=False)
        if acc:
            cookie = acc.get("cookie", "")
        elif account_id == "default":
            cfg = load_config()
            auth = cfg.get('auth', {}) if cfg else {}
            cookie = auth.get('cookie', '')
        else:
            raise HTTPException(status_code=404, detail="账号不存在")

        if not cookie:
            raise HTTPException(status_code=400, detail="账号未配置Cookie")

        headers = build_stealth_headers(cookie)
        resp = requests.get('https://api.zsxq.com/v3/users/self', headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if not data.get('succeeded'):
            raise HTTPException(status_code=400, detail="API返回失败")

        rd = data.get('resp_data', {}) or {}
        user = rd.get('user', {}) or {}
        wechat = (rd.get('accounts', {}) or {}).get('wechat', {}) or {}

        self_info = {
            "uid": user.get("uid"),
            "name": user.get("name") or wechat.get("name"),
            "avatar_url": user.get("avatar_url") or wechat.get("avatar_url"),
            "location": user.get("location"),
            "user_sid": user.get("user_sid"),
            "grade": user.get("grade"),
        }
        db = get_account_info_db()
        db.upsert_self_info(account_id, self_info, raw_json=data)
        return {"self": db.get_self_info(account_id)}
    except HTTPException:
        raise
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"网络请求失败: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"刷新账号信息失败: {str(e)}")

@app.get("/api/groups/{group_id}/self")
async def get_group_account_self(group_id: str):
    """获取群组当前使用账号的自我信息（若无则尝试抓取并保存）"""
    try:
        summary = get_account_summary_for_group_auto(group_id)
        cookie = get_cookie_for_group(group_id)
        account_id = (summary or {}).get('id', 'default')

        if not cookie:
            raise HTTPException(status_code=400, detail="未找到可用Cookie，请先配置账号或默认Cookie")

        db = get_account_info_db()
        info = db.get_self_info(account_id)
        if info:
            return {"self": info}

        # 抓取并写入
        headers = build_stealth_headers(cookie)
        resp = requests.get('https://api.zsxq.com/v3/users/self', headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if not data.get('succeeded'):
            raise HTTPException(status_code=400, detail="API返回失败")

        rd = data.get('resp_data', {}) or {}
        user = rd.get('user', {}) or {}
        wechat = (rd.get('accounts', {}) or {}).get('wechat', {}) or {}

        self_info = {
            "uid": user.get("uid"),
            "name": user.get("name") or wechat.get("name"),
            "avatar_url": user.get("avatar_url") or wechat.get("avatar_url"),
            "location": user.get("location"),
            "user_sid": user.get("user_sid"),
            "grade": user.get("grade"),
        }
        db.upsert_self_info(account_id, self_info, raw_json=data)
        return {"self": db.get_self_info(account_id)}
    except HTTPException:
        raise
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"网络请求失败: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取群组账号信息失败: {str(e)}")

@app.post("/api/groups/{group_id}/self/refresh")
async def refresh_group_account_self(group_id: str):
    """强制抓取群组当前使用账号的自我信息并持久化"""
    try:
        summary = get_account_summary_for_group_auto(group_id)
        cookie = get_cookie_for_group(group_id)
        account_id = (summary or {}).get('id', 'default')

        if not cookie:
            raise HTTPException(status_code=400, detail="未找到可用Cookie，请先配置账号或默认Cookie")

        headers = build_stealth_headers(cookie)
        resp = requests.get('https://api.zsxq.com/v3/users/self', headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if not data.get('succeeded'):
            raise HTTPException(status_code=400, detail="API返回失败")

        rd = data.get('resp_data', {}) or {}
        user = rd.get('user', {}) or {}
        wechat = (rd.get('accounts', {}) or {}).get('wechat', {}) or {}

        self_info = {
            "uid": user.get("uid"),
            "name": user.get("name") or wechat.get("name"),
            "avatar_url": user.get("avatar_url") or wechat.get("avatar_url"),
            "location": user.get("location"),
            "user_sid": user.get("user_sid"),
            "grade": user.get("grade"),
        }
        db = get_account_info_db()
        db.upsert_self_info(account_id, self_info, raw_json=data)
        return {"self": db.get_self_info(account_id)}
    except HTTPException:
        raise
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"网络请求失败: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"刷新群组账号信息失败: {str(e)}")

@app.get("/api/database/stats")
async def get_database_stats():
    """获取数据库统计信息"""
    try:
        # 检查是否已配置
        if not is_configured():
            return {
                "configured": False,
                "topic_database": {
                    "stats": {},
                    "timestamp_info": {
                        "total_topics": 0,
                        "oldest_timestamp": "",
                        "newest_timestamp": "",
                        "has_data": False
                    }
                },
                "file_database": {
                    "stats": {}
                }
            }

        crawler = get_crawler_safe()
        if not crawler:
            return {
                "configured": False,
                "topic_database": {
                    "stats": {},
                    "timestamp_info": {
                        "total_topics": 0,
                        "oldest_timestamp": "",
                        "newest_timestamp": "",
                        "has_data": False
                    }
                },
                "file_database": {
                    "stats": {}
                }
            }

        # 获取话题数据库统计
        topic_stats = crawler.db.get_database_stats()
        timestamp_info = crawler.db.get_timestamp_range_info()

        # 获取文件数据库统计
        file_downloader = crawler.get_file_downloader()
        file_stats = file_downloader.file_db.get_database_stats()

        return {
            "configured": True,
            "topic_database": {
                "stats": topic_stats,
                "timestamp_info": timestamp_info
            },
            "file_database": {
                "stats": file_stats
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取数据库统计失败: {str(e)}")

@app.get("/api/tasks")
async def get_tasks():
    """获取所有任务状态"""
    return list(current_tasks.values())

@app.get("/api/tasks/{task_id}")
async def get_task(task_id: str):
    """获取特定任务状态"""
    if task_id not in current_tasks:
        raise HTTPException(status_code=404, detail="任务不存在")

    return current_tasks[task_id]

@app.post("/api/tasks/{task_id}/stop")
async def stop_task_api(task_id: str):
    """停止任务"""
    if stop_task(task_id):
        return {"message": "任务停止请求已发送", "task_id": task_id}
    else:
        raise HTTPException(status_code=404, detail="任务不存在或无法停止")

# 后台任务执行函数
def run_crawl_historical_task(task_id: str, group_id: str, pages: int, per_page: int, crawl_settings: CrawlHistoricalRequest = None):
    """后台执行历史数据爬取任务"""
    try:
        # 检查任务是否被停止
        if is_task_stopped(task_id):
            return

        update_task(task_id, "running", f"开始爬取历史数据 {pages} 页...")
        add_task_log(task_id, f"🚀 开始获取历史数据，{pages} 页，每页 {per_page} 条")

        # 检查任务是否被停止
        if is_task_stopped(task_id):
            return

        # 设置日志回调函数
        def log_callback(message: str):
            add_task_log(task_id, message)

        # 设置停止检查函数
        def stop_check():
            return is_task_stopped(task_id)

        # 为每个任务创建独立的爬虫实例，使用传入的group_id
        # 自动匹配该群组所属账号，获取对应Cookie
        cookie = get_cookie_for_group(group_id)
        # 使用传入的group_id而不是配置文件中的固定值
        path_manager = get_db_path_manager()
        db_path = path_manager.get_topics_db_path(group_id)

        crawler = ZSXQInteractiveCrawler(cookie, group_id, db_path, log_callback)
        # 设置停止检查函数
        crawler.stop_check_func = stop_check

        # 设置自定义间隔参数
        if crawl_settings:
            crawler.set_custom_intervals(
                crawl_interval_min=crawl_settings.crawlIntervalMin,
                crawl_interval_max=crawl_settings.crawlIntervalMax,
                long_sleep_interval_min=crawl_settings.longSleepIntervalMin,
                long_sleep_interval_max=crawl_settings.longSleepIntervalMax,
                pages_per_batch=crawl_settings.pagesPerBatch
            )

        # 检查任务是否在设置过程中被停止
        if is_task_stopped(task_id):
            add_task_log(task_id, "🛑 任务在初始化过程中被停止")
            return

        add_task_log(task_id, "📡 连接到知识星球API...")
        add_task_log(task_id, "🔍 检查数据库状态...")

        # 检查任务是否被停止
        if is_task_stopped(task_id):
            return

        result = crawler.crawl_incremental(pages, per_page)

        # 检查任务是否被停止
        if is_task_stopped(task_id):
            return

        # 检查是否是会员过期错误
        if result and result.get('expired'):
            add_task_log(task_id, f"❌ 会员已过期: {result.get('message', '成员体验已到期')}")
            update_task(task_id, "failed", "会员已过期", {"expired": True, "code": result.get('code'), "message": result.get('message')})
            return

        add_task_log(task_id, f"✅ 获取完成！新增话题: {result.get('new_topics', 0)}, 更新话题: {result.get('updated_topics', 0)}")
        update_task(task_id, "completed", "历史数据爬取完成", result)
    except Exception as e:
        if not is_task_stopped(task_id):
            add_task_log(task_id, f"❌ 获取失败: {str(e)}")
            update_task(task_id, "failed", f"爬取失败: {str(e)}")

def run_file_download_task(task_id: str, group_id: str, max_files: Optional[int], sort_by: str,
                          download_interval: float = 1.0, long_sleep_interval: float = 60.0,
                          files_per_batch: int = 10, download_interval_min: Optional[float] = None,
                          download_interval_max: Optional[float] = None,
                          long_sleep_interval_min: Optional[float] = None,
                          long_sleep_interval_max: Optional[float] = None):
    """后台执行文件下载任务"""
    try:
        update_task(task_id, "running", "开始文件下载...")

        def log_callback(message: str):
            add_task_log(task_id, message)

        # 设置停止检查函数
        def stop_check():
            return is_task_stopped(task_id)

        # 为每个任务创建独立的文件下载器实例，使用传入的group_id
        # 自动匹配该群组所属账号，获取对应Cookie
        cookie = get_cookie_for_group(group_id)

        # 使用传入的group_id而不是配置文件中的固定值
        from zsxq_file_downloader import ZSXQFileDownloader
        from db_path_manager import get_db_path_manager

        path_manager = get_db_path_manager()
        db_path = path_manager.get_files_db_path(group_id)

        downloader = ZSXQFileDownloader(
            cookie=cookie,
            group_id=group_id,
            db_path=db_path,
            download_interval=download_interval,
            long_sleep_interval=long_sleep_interval,
            files_per_batch=files_per_batch,
            download_interval_min=download_interval_min,
            download_interval_max=download_interval_max,
            long_sleep_interval_min=long_sleep_interval_min,
            long_sleep_interval_max=long_sleep_interval_max
        )
        # 设置日志回调和停止检查函数
        downloader.log_callback = log_callback
        downloader.stop_check_func = stop_check

        add_task_log(task_id, f"⚙️ 下载配置:")
        add_task_log(task_id, f"   ⏱️ 单次下载间隔: {download_interval}秒")
        add_task_log(task_id, f"   😴 长休眠间隔: {long_sleep_interval}秒")
        add_task_log(task_id, f"   📦 批次大小: {files_per_batch}个文件")

        # 将下载器实例存储到全局字典中
        global file_downloader_instances
        file_downloader_instances[task_id] = downloader

        # 检查任务是否在设置过程中被停止
        if is_task_stopped(task_id):
            add_task_log(task_id, "🛑 任务在初始化过程中被停止")
            return

        add_task_log(task_id, "📡 连接到知识星球API...")
        add_task_log(task_id, "🔍 开始收集文件列表...")

        # 先收集文件列表
        collect_result = downloader.collect_incremental_files()

        # 检查任务是否被停止
        if is_task_stopped(task_id):
            return

        add_task_log(task_id, f"📊 文件收集完成: {collect_result}")
        add_task_log(task_id, "🚀 开始下载文件...")

        # 根据排序方式下载文件
        if sort_by == "download_count":
            result = downloader.download_files_from_database(max_files=max_files, status_filter='pending')
        else:
            result = downloader.download_files_from_database(max_files=max_files, status_filter='pending')

        # 检查任务是否被停止
        if is_task_stopped(task_id):
            return

        add_task_log(task_id, f"✅ 文件下载完成！")
        update_task(task_id, "completed", "文件下载完成", {"downloaded_files": result})
    except Exception as e:
        if not is_task_stopped(task_id):
            add_task_log(task_id, f"❌ 文件下载失败: {str(e)}")
            update_task(task_id, "failed", f"文件下载失败: {str(e)}")
    finally:
        # 清理下载器实例
        if task_id in file_downloader_instances:
            del file_downloader_instances[task_id]

def run_single_file_download_task(task_id: str, group_id: str, file_id: int):
    """运行单个文件下载任务"""
    try:
        update_task(task_id, "running", f"开始下载文件 (ID: {file_id})...")

        def log_callback(message: str):
            add_task_log(task_id, message)

        # 设置停止检查函数
        def stop_check():
            return is_task_stopped(task_id)

        # 创建文件下载器实例
        # 自动匹配该群组所属账号，获取对应Cookie
        cookie = get_cookie_for_group(group_id)

        from zsxq_file_downloader import ZSXQFileDownloader
        from db_path_manager import get_db_path_manager

        path_manager = get_db_path_manager()
        db_path = path_manager.get_files_db_path(group_id)

        downloader = ZSXQFileDownloader(
            cookie=cookie,
            group_id=group_id,
            db_path=db_path
        )
        # 设置日志回调和停止检查函数
        downloader.log_callback = log_callback
        downloader.stop_check_func = stop_check

        # 将下载器实例存储到全局字典中
        global file_downloader_instances
        file_downloader_instances[task_id] = downloader

        # 检查任务是否在设置过程中被停止
        if is_task_stopped(task_id):
            add_task_log(task_id, "🛑 任务在初始化过程中被停止")
            return

        # 尝试从数据库获取文件信息
        downloader.file_db.cursor.execute('''
            SELECT file_id, name, size, download_count
            FROM files
            WHERE file_id = ?
        ''', (file_id,))

        result = downloader.file_db.cursor.fetchone()

        if result:
            # 如果数据库中有文件信息，使用数据库信息
            file_id_db, file_name, file_size, download_count = result
            add_task_log(task_id, f"📄 从数据库获取文件信息: {file_name} ({file_size} bytes)")

            # 构造文件信息结构
            file_info = {
                'file': {
                    'id': file_id,
                    'name': file_name,
                    'size': file_size,
                    'download_count': download_count
                }
            }
        else:
            # 如果数据库中没有文件信息，直接尝试下载
            add_task_log(task_id, f"📄 数据库中无文件信息，尝试直接下载文件 ID: {file_id}")

            # 构造最小文件信息结构
            file_info = {
                'file': {
                    'id': file_id,
                    'name': f'file_{file_id}',  # 使用默认文件名
                    'size': 0,  # 未知大小
                    'download_count': 0
                }
            }

        # 下载文件
        result = downloader.download_file(file_info)

        if result == "skipped":
            add_task_log(task_id, "✅ 文件已存在，跳过下载")
            update_task(task_id, "completed", "文件已存在")
        elif result:
            add_task_log(task_id, "✅ 文件下载成功")

            # 获取实际下载的文件信息
            actual_file_info = file_info['file']
            actual_file_name = actual_file_info.get('name', f'file_{file_id}')
            actual_file_size = actual_file_info.get('size', 0)

            # 检查本地文件获取实际大小
            import os
            safe_filename = "".join(c for c in actual_file_name if c.isalnum() or c in '._-（）()[]{}')
            if not safe_filename:
                safe_filename = f"file_{file_id}"
            local_path = os.path.join(downloader.download_dir, safe_filename)

            if os.path.exists(local_path):
                actual_file_size = os.path.getsize(local_path)

            # 更新或插入文件状态
            downloader.file_db.cursor.execute('''
                INSERT OR REPLACE INTO files
                (file_id, name, size, download_status, local_path, download_time, download_count)
                VALUES (?, ?, ?, 'downloaded', ?, CURRENT_TIMESTAMP, ?)
            ''', (file_id, actual_file_name, actual_file_size, local_path,
                  actual_file_info.get('download_count', 0)))
            downloader.file_db.conn.commit()

            update_task(task_id, "completed", "下载成功")
        else:
            add_task_log(task_id, "❌ 文件下载失败")
            update_task(task_id, "failed", "下载失败")

    except Exception as e:
        if not is_task_stopped(task_id):
            add_task_log(task_id, f"❌ 任务执行失败: {str(e)}")
            update_task(task_id, "failed", f"任务失败: {str(e)}")
    finally:
        # 清理下载器实例
        if task_id in file_downloader_instances:
            del file_downloader_instances[task_id]

def run_single_file_download_task_with_info(task_id: str, group_id: str, file_id: int,
                                           file_name: Optional[str] = None, file_size: Optional[int] = None):
    """运行单个文件下载任务（带文件信息）"""
    try:
        update_task(task_id, "running", f"开始下载文件 (ID: {file_id})...")

        def log_callback(message: str):
            add_task_log(task_id, message)

        # 设置停止检查函数
        def stop_check():
            return is_task_stopped(task_id)

        # 创建文件下载器实例
        # 自动匹配该群组所属账号，获取对应Cookie
        cookie = get_cookie_for_group(group_id)

        from zsxq_file_downloader import ZSXQFileDownloader
        from db_path_manager import get_db_path_manager

        path_manager = get_db_path_manager()
        db_path = path_manager.get_files_db_path(group_id)

        downloader = ZSXQFileDownloader(
            cookie=cookie,
            group_id=group_id,
            db_path=db_path
        )
        # 设置日志回调和停止检查函数
        downloader.log_callback = log_callback
        downloader.stop_check_func = stop_check

        # 将下载器实例存储到全局字典中
        global file_downloader_instances
        file_downloader_instances[task_id] = downloader

        # 检查任务是否在设置过程中被停止
        if is_task_stopped(task_id):
            add_task_log(task_id, "🛑 任务在初始化过程中被停止")
            return

        # 构造文件信息结构
        if file_name and file_size:
            add_task_log(task_id, f"📄 使用提供的文件信息: {file_name} ({file_size} bytes)")
            file_info = {
                'file': {
                    'id': file_id,
                    'name': file_name,
                    'size': file_size,
                    'download_count': 0
                }
            }
        else:
            # 尝试从数据库获取文件信息
            downloader.file_db.cursor.execute('''
                SELECT file_id, name, size, download_count
                FROM files
                WHERE file_id = ?
            ''', (file_id,))

            result = downloader.file_db.cursor.fetchone()

            if result:
                file_id_db, db_file_name, db_file_size, download_count = result
                add_task_log(task_id, f"📄 从数据库获取文件信息: {db_file_name} ({db_file_size} bytes)")
                file_info = {
                    'file': {
                        'id': file_id,
                        'name': db_file_name,
                        'size': db_file_size,
                        'download_count': download_count
                    }
                }
            else:
                add_task_log(task_id, f"📄 直接下载文件 ID: {file_id}")
                file_info = {
                    'file': {
                        'id': file_id,
                        'name': f'file_{file_id}',
                        'size': 0,
                        'download_count': 0
                    }
                }

        # 下载文件
        result = downloader.download_file(file_info)

        if result == "skipped":
            add_task_log(task_id, "✅ 文件已存在，跳过下载")
            update_task(task_id, "completed", "文件已存在")
        elif result:
            add_task_log(task_id, "✅ 文件下载成功")

            # 获取实际下载的文件信息
            actual_file_info = file_info['file']
            actual_file_name = actual_file_info.get('name', f'file_{file_id}')
            actual_file_size = actual_file_info.get('size', 0)

            # 检查本地文件获取实际大小
            import os
            safe_filename = "".join(c for c in actual_file_name if c.isalnum() or c in '._-（）()[]{}')
            if not safe_filename:
                safe_filename = f"file_{file_id}"
            local_path = os.path.join(downloader.download_dir, safe_filename)

            if os.path.exists(local_path):
                actual_file_size = os.path.getsize(local_path)

            # 更新或插入文件状态
            downloader.file_db.cursor.execute('''
                INSERT OR REPLACE INTO files
                (file_id, name, size, download_status, local_path, download_time, download_count)
                VALUES (?, ?, ?, 'downloaded', ?, CURRENT_TIMESTAMP, ?)
            ''', (file_id, actual_file_name, actual_file_size, local_path,
                  actual_file_info.get('download_count', 0)))
            downloader.file_db.conn.commit()

            update_task(task_id, "completed", "下载成功")
        else:
            add_task_log(task_id, "❌ 文件下载失败")
            update_task(task_id, "failed", "下载失败")

    except Exception as e:
        if not is_task_stopped(task_id):
            add_task_log(task_id, f"❌ 任务执行失败: {str(e)}")
            update_task(task_id, "failed", f"任务失败: {str(e)}")
    finally:
        # 清理下载器实例
        if task_id in file_downloader_instances:
            del file_downloader_instances[task_id]

# 群组相关辅助函数
def fetch_groups_from_api(cookie: str) -> List[dict]:
    """从知识星球API获取群组列表"""
    import requests

    # 如果是测试Cookie，返回模拟数据
    if cookie == "test_cookie":
        return [
            {
                "group_id": 123456,
                "name": "测试知识星球群组",
                "type": "public",
                "background_url": "https://via.placeholder.com/400x200/4f46e5/ffffff?text=Test+Group",
                "description": "这是一个用于测试的知识星球群组，包含各种技术讨论和学习资源分享。",
                "create_time": "2023-01-15T10:30:00+08:00",
                "subscription_time": "2024-01-01T00:00:00+08:00",
                "expiry_time": "2024-12-31T23:59:59+08:00",
                "status": "active",
                "owner": {
                    "user_id": 1001,
                    "name": "测试群主",
                    "avatar_url": "https://via.placeholder.com/64x64/10b981/ffffff?text=Owner"
                },
                "statistics": {
                    "members_count": 1250,
                    "topics_count": 89,
                    "files_count": 156
                }
            },
            {
                "group_id": 789012,
                "name": "技术交流群",
                "type": "private",
                "background_url": "https://via.placeholder.com/400x200/059669/ffffff?text=Tech+Group",
                "description": "专注于前端、后端、移动开发等技术领域的深度交流与实践分享。",
                "create_time": "2023-03-20T14:15:00+08:00",
                "subscription_time": "2024-02-15T00:00:00+08:00",
                "expiry_time": "2025-02-14T23:59:59+08:00",
                "status": "active",
                "owner": {
                    "user_id": 1002,
                    "name": "技术专家",
                    "avatar_url": "https://via.placeholder.com/64x64/dc2626/ffffff?text=Tech"
                },
                "statistics": {
                    "members_count": 856,
                    "topics_count": 234,
                    "files_count": 67
                }
            },
            {
                "group_id": 345678,
                "name": "产品设计讨论",
                "type": "public",
                "background_url": "https://via.placeholder.com/400x200/7c3aed/ffffff?text=Design+Group",
                "description": "UI/UX设计、产品思维、用户体验等设计相关话题的专业讨论社区。",
                "create_time": "2023-06-10T09:45:00+08:00",
                "subscription_time": "2024-03-01T00:00:00+08:00",
                "expiry_time": "2024-08-31T23:59:59+08:00",
                "status": "active",
                "owner": {
                    "user_id": 1003,
                    "name": "设计师",
                    "avatar_url": "https://via.placeholder.com/64x64/ea580c/ffffff?text=Design"
                },
                "statistics": {
                    "members_count": 432,
                    "topics_count": 156,
                    "files_count": 89
                }
            },
            {
                "group_id": 456789,
                "name": "创业投资圈",
                "type": "private",
                "background_url": "https://via.placeholder.com/400x200/dc2626/ffffff?text=Startup",
                "description": "创业者、投资人、行业专家的交流平台，分享创业经验和投资见解。",
                "create_time": "2023-08-05T16:20:00+08:00",
                "subscription_time": "2024-01-10T00:00:00+08:00",
                "expiry_time": "2024-07-09T23:59:59+08:00",
                "status": "expiring_soon",
                "owner": {
                    "user_id": 1004,
                    "name": "投资人",
                    "avatar_url": "https://via.placeholder.com/64x64/f59e0b/ffffff?text=VC"
                },
                "statistics": {
                    "members_count": 298,
                    "topics_count": 78,
                    "files_count": 45
                }
            },
            {
                "group_id": 567890,
                "name": "AI人工智能研究",
                "type": "public",
                "background_url": "https://via.placeholder.com/400x200/06b6d4/ffffff?text=AI+Research",
                "description": "人工智能、机器学习、深度学习等前沿技术的研究与应用讨论。",
                "create_time": "2023-09-12T11:30:00+08:00",
                "subscription_time": "2024-04-01T00:00:00+08:00",
                "expiry_time": "2025-03-31T23:59:59+08:00",
                "status": "active",
                "owner": {
                    "user_id": 1005,
                    "name": "AI研究员",
                    "avatar_url": "https://via.placeholder.com/64x64/8b5cf6/ffffff?text=AI"
                },
                "statistics": {
                    "members_count": 1876,
                    "topics_count": 345,
                    "files_count": 234
                }
            }
        ]

    headers = build_stealth_headers(cookie)

    try:
        response = requests.get('https://api.zsxq.com/v2/groups', headers=headers, timeout=30)
        response.raise_for_status()

        data = response.json()
        if data.get('succeeded'):
            return data.get('resp_data', {}).get('groups', [])
        else:
            raise Exception(f"API返回失败: {data.get('error_message', '未知错误')}")
    except requests.RequestException as e:
        raise Exception(f"网络请求失败: {str(e)}")
    except Exception as e:
        raise Exception(f"获取群组列表失败: {str(e)}")

# 爬取相关API路由
@app.post("/api/crawl/historical/{group_id}")
async def crawl_historical(group_id: str, request: CrawlHistoricalRequest, background_tasks: BackgroundTasks):
    """爬取历史数据"""
    try:
        task_id = create_task("crawl_historical", f"爬取历史数据 {request.pages} 页 (群组: {group_id})")

        # 添加后台任务
        background_tasks.add_task(run_crawl_historical_task, task_id, group_id, request.pages, request.per_page, request)

        return {"task_id": task_id, "message": "任务已创建，正在后台执行"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建爬取任务失败: {str(e)}")

@app.post("/api/crawl/all/{group_id}")
async def crawl_all(group_id: str, request: CrawlSettingsRequest, background_tasks: BackgroundTasks):
    """全量爬取所有历史数据"""
    try:
        task_id = create_task("crawl_all", f"全量爬取所有历史数据 (群组: {group_id})")

        def run_crawl_all_task(task_id: str, group_id: str, crawl_settings: CrawlSettingsRequest = None):
            try:
                update_task(task_id, "running", "开始全量爬取...")
                add_task_log(task_id, "🚀 开始全量爬取...")
                add_task_log(task_id, "⚠️ 警告：此模式将持续爬取直到没有数据，可能需要很长时间")

                # 创建日志回调函数
                def log_callback(message):
                    add_task_log(task_id, message)

                # 设置停止检查函数
                def stop_check():
                    return is_task_stopped(task_id)

                # 为这个任务创建新的爬虫实例（带日志回调），使用传入的group_id
                config = load_config()
                auth_config = config.get('auth', {})
                default_cookie = auth_config.get('cookie', '')
                account = am_get_account_for_group(group_id)
                cookie = account.get('cookie', '') if account else default_cookie
                # 使用传入的group_id而不是配置文件中的固定值
                path_manager = get_db_path_manager()
                db_path = path_manager.get_topics_db_path(group_id)

                crawler = ZSXQInteractiveCrawler(cookie, group_id, db_path, log_callback)
                # 设置停止检查函数
                crawler.stop_check_func = stop_check

                # 设置自定义间隔参数
                if crawl_settings:
                    crawler.set_custom_intervals(
                        crawl_interval_min=crawl_settings.crawlIntervalMin,
                        crawl_interval_max=crawl_settings.crawlIntervalMax,
                        long_sleep_interval_min=crawl_settings.longSleepIntervalMin,
                        long_sleep_interval_max=crawl_settings.longSleepIntervalMax,
                        pages_per_batch=crawl_settings.pagesPerBatch
                    )

                # 检查任务是否在设置过程中被停止
                if is_task_stopped(task_id):
                    add_task_log(task_id, "🛑 任务在初始化过程中被停止")
                    return

                add_task_log(task_id, "📡 连接到知识星球API...")
                add_task_log(task_id, "🔍 检查数据库状态...")

                # 检查任务是否被停止
                if is_task_stopped(task_id):
                    return

                # 获取数据库状态
                db_stats = crawler.db.get_database_stats()
                add_task_log(task_id, f"📊 当前数据库状态: 话题: {db_stats.get('topics', 0)}, 用户: {db_stats.get('users', 0)}")

                # 检查任务是否被停止
                if is_task_stopped(task_id):
                    return

                add_task_log(task_id, "🌊 开始无限历史爬取...")
                result = crawler.crawl_all_historical(per_page=20, auto_confirm=True)

                # 检查任务是否被停止
                if is_task_stopped(task_id):
                    return

                # 检查是否是会员过期错误
                if result and result.get('expired'):
                    add_task_log(task_id, f"❌ 会员已过期: {result.get('message', '成员体验已到期')}")
                    update_task(task_id, "failed", "会员已过期", {"expired": True, "code": result.get('code'), "message": result.get('message')})
                    return

                add_task_log(task_id, f"🎉 全量爬取完成！")
                add_task_log(task_id, f"📊 最终统计: 新增话题: {result.get('new_topics', 0)}, 更新话题: {result.get('updated_topics', 0)}, 总页数: {result.get('pages', 0)}")
                update_task(task_id, "completed", "全量爬取完成", result)
            except Exception as e:
                add_task_log(task_id, f"❌ 全量爬取失败: {str(e)}")
                update_task(task_id, "failed", f"全量爬取失败: {str(e)}")

        # 添加后台任务
        background_tasks.add_task(run_crawl_all_task, task_id, group_id, request)

        return {"task_id": task_id, "message": "任务已创建，正在后台执行"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建全量爬取任务失败: {str(e)}")

@app.post("/api/crawl/incremental/{group_id}")
async def crawl_incremental(group_id: str, request: CrawlHistoricalRequest, background_tasks: BackgroundTasks):
    """增量爬取历史数据"""
    try:
        task_id = create_task("crawl_incremental", f"增量爬取历史数据 {request.pages} 页 (群组: {group_id})")

        def run_crawl_incremental_task(task_id: str, group_id: str, pages: int, per_page: int, crawl_settings: CrawlHistoricalRequest = None):
            try:
                update_task(task_id, "running", "开始增量爬取...")

                def log_callback(message: str):
                    add_task_log(task_id, message)

                # 设置停止检查函数
                def stop_check():
                    return is_task_stopped(task_id)

                # 为每个任务创建独立的爬虫实例
                config = load_config()
                auth_config = config.get('auth', {})
                default_cookie = auth_config.get('cookie', '')
                account = am_get_account_for_group(group_id)
                cookie = account.get('cookie', '') if account else default_cookie
                # 使用传入的group_id而不是配置文件中的固定值
                path_manager = get_db_path_manager()
                db_path = path_manager.get_topics_db_path(group_id)

                crawler = ZSXQInteractiveCrawler(cookie, group_id, db_path, log_callback)
                # 设置停止检查函数
                crawler.stop_check_func = stop_check

                # 设置自定义间隔参数
                if crawl_settings:
                    crawler.set_custom_intervals(
                        crawl_interval_min=crawl_settings.crawlIntervalMin,
                        crawl_interval_max=crawl_settings.crawlIntervalMax,
                        long_sleep_interval_min=crawl_settings.longSleepIntervalMin,
                        long_sleep_interval_max=crawl_settings.longSleepIntervalMax,
                        pages_per_batch=crawl_settings.pagesPerBatch
                    )

                # 检查任务是否在设置过程中被停止
                if is_task_stopped(task_id):
                    add_task_log(task_id, "🛑 任务在初始化过程中被停止")
                    return

                add_task_log(task_id, "📡 连接到知识星球API...")
                add_task_log(task_id, "🔍 检查数据库状态...")

                result = crawler.crawl_incremental(pages, per_page)

                # 检查任务是否被停止
                if is_task_stopped(task_id):
                    return

                add_task_log(task_id, f"✅ 增量爬取完成！新增话题: {result.get('new_topics', 0)}, 更新话题: {result.get('updated_topics', 0)}")
                update_task(task_id, "completed", "增量爬取完成", result)
            except Exception as e:
                if not is_task_stopped(task_id):
                    add_task_log(task_id, f"❌ 增量爬取失败: {str(e)}")
                    update_task(task_id, "failed", f"增量爬取失败: {str(e)}")

        # 添加后台任务
        background_tasks.add_task(run_crawl_incremental_task, task_id, group_id, request.pages, request.per_page, request)

        return {"task_id": task_id, "message": "任务已创建，正在后台执行"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建增量爬取任务失败: {str(e)}")

@app.post("/api/crawl/latest-until-complete/{group_id}")
async def crawl_latest_until_complete(group_id: str, request: CrawlSettingsRequest, background_tasks: BackgroundTasks):
    """获取最新记录：智能增量更新"""
    try:
        task_id = create_task("crawl_latest_until_complete", f"获取最新记录 (群组: {group_id})")

        def run_crawl_latest_task(task_id: str, group_id: str, crawl_settings: CrawlSettingsRequest = None):
            try:
                update_task(task_id, "running", "开始获取最新记录...")

                def log_callback(message: str):
                    add_task_log(task_id, message)

                # 设置停止检查函数
                def stop_check():
                    return is_task_stopped(task_id)

                # 为每个任务创建独立的爬虫实例，使用传入的group_id
                config = load_config()
                auth_config = config.get('auth', {})
                default_cookie = auth_config.get('cookie', '')
                account = am_get_account_for_group(group_id)
                cookie = account.get('cookie', '') if account else default_cookie
                # 使用传入的group_id而不是配置文件中的固定值
                path_manager = get_db_path_manager()
                db_path = path_manager.get_topics_db_path(group_id)

                crawler = ZSXQInteractiveCrawler(cookie, group_id, db_path, log_callback)
                # 设置停止检查函数
                crawler.stop_check_func = stop_check

                # 设置自定义间隔参数
                if crawl_settings:
                    crawler.set_custom_intervals(
                        crawl_interval_min=crawl_settings.crawlIntervalMin,
                        crawl_interval_max=crawl_settings.crawlIntervalMax,
                        long_sleep_interval_min=crawl_settings.longSleepIntervalMin,
                        long_sleep_interval_max=crawl_settings.longSleepIntervalMax,
                        pages_per_batch=crawl_settings.pagesPerBatch
                    )

                # 检查任务是否在设置过程中被停止
                if is_task_stopped(task_id):
                    add_task_log(task_id, "🛑 任务在初始化过程中被停止")
                    return

                add_task_log(task_id, "📡 连接到知识星球API...")
                add_task_log(task_id, "🔍 检查数据库状态...")

                result = crawler.crawl_latest_until_complete()

                # 检查任务是否被停止
                if is_task_stopped(task_id):
                    return

                # 检查是否是会员过期错误
                if result and result.get('expired'):
                    add_task_log(task_id, f"❌ 会员已过期: {result.get('message', '成员体验已到期')}")
                    update_task(task_id, "failed", "会员已过期", {"expired": True, "code": result.get('code'), "message": result.get('message')})
                    return

                add_task_log(task_id, f"✅ 获取最新记录完成！新增话题: {result.get('new_topics', 0)}, 更新话题: {result.get('updated_topics', 0)}")
                update_task(task_id, "completed", "获取最新记录完成", result)
            except Exception as e:
                if not is_task_stopped(task_id):
                    add_task_log(task_id, f"❌ 获取最新记录失败: {str(e)}")
                    update_task(task_id, "failed", f"获取最新记录失败: {str(e)}")

        # 添加后台任务
        background_tasks.add_task(run_crawl_latest_task, task_id, group_id, request)

        return {"task_id": task_id, "message": "任务已创建，正在后台执行"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建获取最新记录任务失败: {str(e)}")

# 文件相关API路由
@app.post("/api/files/collect/{group_id}")
async def collect_files(group_id: str, background_tasks: BackgroundTasks):
    """收集文件列表"""
    try:
        task_id = create_task("collect_files", "收集文件列表")

        def run_collect_files_task(task_id: str, group_id: str):
            try:
                update_task(task_id, "running", "开始收集文件列表...")

                def log_callback(message: str):
                    add_task_log(task_id, message)

                # 设置停止检查函数
                def stop_check():
                    return is_task_stopped(task_id)

                # 为每个任务创建独立的文件下载器实例
                config = load_config()
                auth_config = config.get('auth', {})
                default_cookie = auth_config.get('cookie', '')
                account = am_get_account_for_group(group_id)
                cookie = account.get('cookie', '') if account else default_cookie

                from zsxq_file_downloader import ZSXQFileDownloader
                from db_path_manager import get_db_path_manager

                path_manager = get_db_path_manager()
                db_path = path_manager.get_files_db_path(group_id)

                downloader = ZSXQFileDownloader(cookie, group_id, db_path)
                downloader.log_callback = log_callback
                downloader.stop_check_func = stop_check

                # 将下载器实例存储到全局字典中
                global file_downloader_instances
                file_downloader_instances[task_id] = downloader

                # 检查任务是否在设置过程中被停止
                if is_task_stopped(task_id):
                    add_task_log(task_id, "🛑 任务在初始化过程中被停止")
                    return

                add_task_log(task_id, "📡 连接到知识星球API...")
                result = downloader.collect_incremental_files()

                # 检查任务是否被停止
                if is_task_stopped(task_id):
                    return

                add_task_log(task_id, f"✅ 文件列表收集完成！")
                update_task(task_id, "completed", "文件列表收集完成", result)
            except Exception as e:
                if not is_task_stopped(task_id):
                    add_task_log(task_id, f"❌ 文件列表收集失败: {str(e)}")
                    update_task(task_id, "failed", f"文件列表收集失败: {str(e)}")
            finally:
                # 清理下载器实例
                if task_id in file_downloader_instances:
                    del file_downloader_instances[task_id]

        # 添加后台任务
        background_tasks.add_task(run_collect_files_task, task_id, group_id)

        return {"task_id": task_id, "message": "任务已创建，正在后台执行"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建文件收集任务失败: {str(e)}")

@app.post("/api/files/download/{group_id}")
async def download_files(group_id: str, request: FileDownloadRequest, background_tasks: BackgroundTasks):
    """下载文件"""
    try:
        task_id = create_task("download_files", f"下载文件 (排序: {request.sort_by})")

        # 添加后台任务
        background_tasks.add_task(
            run_file_download_task,
            task_id,
            group_id,
            request.max_files,
            request.sort_by,
            request.download_interval,
            request.long_sleep_interval,
            request.files_per_batch,
            request.download_interval_min,
            request.download_interval_max,
            request.long_sleep_interval_min,
            request.long_sleep_interval_max
        )

        return {"task_id": task_id, "message": "任务已创建，正在后台执行"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建文件下载任务失败: {str(e)}")

@app.post("/api/files/download-single/{group_id}/{file_id}")
async def download_single_file(group_id: str, file_id: int, background_tasks: BackgroundTasks,
                              file_name: Optional[str] = None, file_size: Optional[int] = None):
    """下载单个文件"""
    try:
        task_id = create_task("download_single_file", f"下载单个文件 (ID: {file_id})")

        # 添加后台任务
        background_tasks.add_task(
            run_single_file_download_task_with_info,
            task_id,
            group_id,
            file_id,
            file_name,
            file_size
        )

        return {"task_id": task_id, "message": "单个文件下载任务已创建"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建单个文件下载任务失败: {str(e)}")

@app.get("/api/files/status/{group_id}/{file_id}")
async def get_file_status(group_id: str, file_id: int):
    """获取文件下载状态"""
    try:
        crawler = get_crawler_for_group(group_id)
        downloader = crawler.get_file_downloader()

        # 查询文件信息
        downloader.file_db.cursor.execute('''
            SELECT name, size, download_status
            FROM files
            WHERE file_id = ?
        ''', (file_id,))

        result = downloader.file_db.cursor.fetchone()

        if not result:
            # 文件不在数据库中，检查是否有同名文件在下载目录
            import os
            download_dir = downloader.download_dir

            # 尝试从话题详情中获取文件名（这里需要额外的逻辑）
            # 暂时返回文件不存在的状态
            return {
                "file_id": file_id,
                "name": f"file_{file_id}",
                "size": 0,
                "download_status": "not_collected",
                "local_exists": False,
                "local_size": 0,
                "local_path": None,
                "is_complete": False,
                "message": "文件信息未收集，请先运行文件收集任务"
            }

        file_name, file_size, download_status = result

        # 检查本地文件是否存在
        import os
        safe_filename = "".join(c for c in file_name if c.isalnum() or c in '._-（）()[]{}')
        if not safe_filename:
            safe_filename = f"file_{file_id}"

        download_dir = downloader.download_dir
        file_path = os.path.join(download_dir, safe_filename)

        local_exists = os.path.exists(file_path)
        local_size = os.path.getsize(file_path) if local_exists else 0

        return {
            "file_id": file_id,
            "name": file_name,
            "size": file_size,
            "download_status": download_status or "pending",
            "local_exists": local_exists,
            "local_size": local_size,
            "local_path": file_path if local_exists else None,
            "is_complete": local_exists and local_size == file_size
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取文件状态失败: {str(e)}")

@app.get("/api/files/check-local/{group_id}")
async def check_local_file_status(group_id: str, file_name: str, file_size: int):
    """检查本地文件状态（不依赖数据库）"""
    try:
        crawler = get_crawler_for_group(group_id)
        downloader = crawler.get_file_downloader()

        # 清理文件名
        import os
        safe_filename = "".join(c for c in file_name if c.isalnum() or c in '._-（）()[]{}')
        if not safe_filename:
            safe_filename = file_name

        download_dir = downloader.download_dir
        file_path = os.path.join(download_dir, safe_filename)

        local_exists = os.path.exists(file_path)
        local_size = os.path.getsize(file_path) if local_exists else 0

        return {
            "file_name": file_name,
            "safe_filename": safe_filename,
            "expected_size": file_size,
            "local_exists": local_exists,
            "local_size": local_size,
            "local_path": file_path if local_exists else None,
            "is_complete": local_exists and (file_size == 0 or local_size == file_size),
            "download_dir": download_dir
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"检查本地文件失败: {str(e)}")

@app.get("/api/files/stats/{group_id}")
async def get_file_stats(group_id: str):
    """获取指定群组的文件统计信息"""
    crawler = None
    try:
        crawler = get_crawler_for_group(group_id)
        downloader = crawler.get_file_downloader()

        # 获取文件数据库统计
        stats = downloader.file_db.get_database_stats()

        # 获取下载状态统计
        # 首先检查是否有download_status列
        downloader.file_db.cursor.execute("PRAGMA table_info(files)")
        columns = [col[1] for col in downloader.file_db.cursor.fetchall()]

        if 'download_status' in columns:
            # 新版本数据库，有download_status列
            downloader.file_db.cursor.execute("""
                SELECT
                    COUNT(*) as total_files,
                    COUNT(CASE WHEN download_status = 'completed' THEN 1 END) as downloaded,
                    COUNT(CASE WHEN download_status = 'pending' THEN 1 END) as pending,
                    COUNT(CASE WHEN download_status = 'failed' THEN 1 END) as failed
                FROM files
            """)
            download_stats = downloader.file_db.cursor.fetchone()
        else:
            # 旧版本数据库，没有download_status列，只统计总数
            downloader.file_db.cursor.execute("SELECT COUNT(*) FROM files")
            total_files = downloader.file_db.cursor.fetchone()[0]
            download_stats = (total_files, 0, 0, 0)  # 总数, 已下载, 待下载, 失败

        result = {
            "database_stats": stats,
            "download_stats": {
                "total_files": download_stats[0] if download_stats else 0,
                "downloaded": download_stats[1] if download_stats else 0,
                "pending": download_stats[2] if download_stats else 0,
                "failed": download_stats[3] if download_stats else 0
            }
        }

        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取文件统计失败: {str(e)}")
    finally:
        # 确保关闭数据库连接
        if crawler:
            try:
                if hasattr(crawler, 'file_downloader') and crawler.file_downloader:
                    if hasattr(crawler.file_downloader, 'file_db') and crawler.file_downloader.file_db:
                        crawler.file_downloader.file_db.close()
                if hasattr(crawler, 'db') and crawler.db:
                    crawler.db.close()
                print(f"🔒 已关闭群组 {group_id} 的数据库连接")
            except Exception as e:
                print(f"⚠️ 关闭数据库连接时出错: {e}")

@app.post("/api/files/clear/{group_id}")
async def clear_file_database(group_id: str):
    """删除指定群组的文件数据库文件"""
    try:
        path_manager = get_db_path_manager()
        db_path = path_manager.get_files_db_path(group_id)

        print(f"🗑️ 尝试删除文件数据库: {db_path}")

        if os.path.exists(db_path):
            # 强制关闭所有可能的数据库连接
            import gc
            import sqlite3

            # 尝试多种方式关闭连接
            try:
                # 方式1：通过爬虫实例关闭
                crawler = get_crawler_for_group(group_id)
                downloader = crawler.get_file_downloader()
                if hasattr(downloader, 'file_db') and downloader.file_db:
                    downloader.file_db.close()
                if hasattr(crawler, 'db') and crawler.db:
                    crawler.db.close()
                print(f"✅ 已关闭爬虫实例的数据库连接")
            except Exception as e:
                print(f"⚠️ 关闭爬虫数据库连接时出错: {e}")

            # 方式2：强制垃圾回收
            gc.collect()

            # 方式3：等待一小段时间让连接释放
            import time
            time.sleep(0.5)

            # 删除数据库文件
            try:
                os.remove(db_path)
                print(f"✅ 文件数据库已删除: {db_path}")

                # 同时删除该群组的图片缓存
                try:
                    from image_cache_manager import get_image_cache_manager, clear_group_cache_manager
                    cache_manager = get_image_cache_manager(group_id)
                    success, message = cache_manager.clear_cache()
                    if success:
                        print(f"✅ 图片缓存已清空: {message}")
                    else:
                        print(f"⚠️ 清空图片缓存失败: {message}")
                    # 清除缓存管理器实例
                    clear_group_cache_manager(group_id)
                except Exception as cache_error:
                    print(f"⚠️ 清空图片缓存时出错: {cache_error}")

                return {"message": f"群组 {group_id} 的文件数据库和图片缓存已删除"}
            except PermissionError as pe:
                print(f"❌ 文件被占用，无法删除: {pe}")
                raise HTTPException(status_code=500, detail=f"文件被占用，无法删除数据库文件。请稍后重试。")
        else:
            print(f"ℹ️ 文件数据库不存在: {db_path}")
            return {"message": f"群组 {group_id} 的文件数据库不存在"}
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ 删除文件数据库失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"删除文件数据库失败: {str(e)}")

@app.post("/api/topics/clear/{group_id}")
async def clear_topic_database(group_id: str):
    """删除指定群组的话题数据库文件"""
    try:
        path_manager = get_db_path_manager()
        db_path = path_manager.get_topics_db_path(group_id)

        print(f"🗑️ 尝试删除话题数据库: {db_path}")

        if os.path.exists(db_path):
            # 强制关闭所有可能的数据库连接
            import gc
            import time

            # 尝试多种方式关闭连接
            try:
                # 方式1：通过爬虫实例关闭
                crawler = get_crawler_for_group(group_id)
                if hasattr(crawler, 'db') and crawler.db:
                    crawler.db.close()
                if hasattr(crawler, 'file_downloader') and crawler.file_downloader:
                    if hasattr(crawler.file_downloader, 'file_db') and crawler.file_downloader.file_db:
                        crawler.file_downloader.file_db.close()
                print(f"✅ 已关闭爬虫实例的数据库连接")
            except Exception as e:
                print(f"⚠️ 关闭爬虫数据库连接时出错: {e}")

            # 方式2：强制垃圾回收
            gc.collect()

            # 方式3：等待一小段时间让连接释放
            time.sleep(0.5)

            # 删除数据库文件
            try:
                os.remove(db_path)
                print(f"✅ 话题数据库已删除: {db_path}")

                # 同时删除该群组的图片缓存
                try:
                    from image_cache_manager import get_image_cache_manager, clear_group_cache_manager
                    cache_manager = get_image_cache_manager(group_id)
                    success, message = cache_manager.clear_cache()
                    if success:
                        print(f"✅ 图片缓存已清空: {message}")
                    else:
                        print(f"⚠️ 清空图片缓存失败: {message}")
                    # 清除缓存管理器实例
                    clear_group_cache_manager(group_id)
                except Exception as cache_error:
                    print(f"⚠️ 清空图片缓存时出错: {cache_error}")

                return {"message": f"群组 {group_id} 的话题数据库和图片缓存已删除"}
            except PermissionError as pe:
                print(f"❌ 文件被占用，无法删除: {pe}")
                raise HTTPException(status_code=500, detail=f"文件被占用，无法删除数据库文件。请稍后重试。")
        else:
            print(f"ℹ️ 话题数据库不存在: {db_path}")
            return {"message": f"群组 {group_id} 的话题数据库不存在"}
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ 删除话题数据库失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"删除话题数据库失败: {str(e)}")

# 数据查询API路由
@app.get("/api/topics")
async def get_topics(page: int = 1, per_page: int = 20, search: Optional[str] = None):
    """获取话题列表"""
    try:
        crawler = get_crawler()

        offset = (page - 1) * per_page

        # 构建查询SQL
        if search:
            query = """
                SELECT topic_id, title, create_time, likes_count, comments_count, reading_count
                FROM topics
                WHERE title LIKE ?
                ORDER BY create_time DESC
                LIMIT ? OFFSET ?
            """
            params = (f"%{search}%", per_page, offset)
        else:
            query = """
                SELECT topic_id, title, create_time, likes_count, comments_count, reading_count
                FROM topics
                ORDER BY create_time DESC
                LIMIT ? OFFSET ?
            """
            params = (per_page, offset)

        crawler.db.cursor.execute(query, params)
        topics = crawler.db.cursor.fetchall()

        # 获取总数
        if search:
            crawler.db.cursor.execute("SELECT COUNT(*) FROM topics WHERE title LIKE ?", (f"%{search}%",))
        else:
            crawler.db.cursor.execute("SELECT COUNT(*) FROM topics")
        total = crawler.db.cursor.fetchone()[0]

        return {
            "topics": [
                {
                    "topic_id": topic[0],
                    "title": topic[1],
                    "create_time": topic[2],
                    "likes_count": topic[3],
                    "comments_count": topic[4],
                    "reading_count": topic[5]
                }
                for topic in topics
            ],
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total": total,
                "pages": (total + per_page - 1) // per_page
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取话题列表失败: {str(e)}")

@app.get("/api/files/{group_id}")
async def get_files(group_id: str, page: int = 1, per_page: int = 20, status: Optional[str] = None):
    """获取指定群组的文件列表"""
    try:
        crawler = get_crawler_for_group(group_id)
        downloader = crawler.get_file_downloader()

        offset = (page - 1) * per_page

        # 构建查询SQL
        if status:
            query = """
                SELECT file_id, name, size, download_count, create_time, download_status
                FROM files
                WHERE download_status = ?
                ORDER BY create_time DESC
                LIMIT ? OFFSET ?
            """
            params = (status, per_page, offset)
        else:
            query = """
                SELECT file_id, name, size, download_count, create_time, download_status
                FROM files
                ORDER BY create_time DESC
                LIMIT ? OFFSET ?
            """
            params = (per_page, offset)

        downloader.file_db.cursor.execute(query, params)
        files = downloader.file_db.cursor.fetchall()

        # 获取总数
        if status:
            downloader.file_db.cursor.execute("SELECT COUNT(*) FROM files WHERE download_status = ?", (status,))
        else:
            downloader.file_db.cursor.execute("SELECT COUNT(*) FROM files")
        total = downloader.file_db.cursor.fetchone()[0]

        return {
            "files": [
                {
                    "file_id": file[0],
                    "name": file[1],
                    "size": file[2],
                    "download_count": file[3],
                    "create_time": file[4],
                    "download_status": file[5] if len(file) > 5 else "unknown"
                }
                for file in files
            ],
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total": total,
                "pages": (total + per_page - 1) // per_page
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取文件列表失败: {str(e)}")

# 群组相关API端点
@app.post("/api/local-groups/refresh")
async def refresh_local_groups():
    """
    手动刷新本地群（output）扫描缓存；不抛错，异常时返回旧缓存。
    """
    try:
        ids = await asyncio.to_thread(scan_local_groups)
        return {"success": True, "count": len(ids), "groups": sorted(list(ids))}
    except Exception as e:
        cached = get_cached_local_group_ids(force_refresh=False) or set()
        # 不报错，返回降级结果
        return {"success": False, "count": len(cached), "groups": sorted(list(cached)), "error": str(e)}

@app.get("/api/groups")
async def get_groups():
    """获取群组列表：账号群 ∪ 本地目录群（去重合并）"""
    try:
        # 自动构建群组→账号映射（多账号支持）
        group_account_map = build_account_group_detection()
        local_ids = get_cached_local_group_ids(force_refresh=False)

        # 获取“当前账号”的群列表（若未配置则视为空集合）
        groups_data: List[dict] = []
        try:
            if is_configured():
                config = load_config()
                auth_config = config.get('auth', {}) if config else {}
                cookie = auth_config.get('cookie', '') or ''
                if cookie and cookie != "your_cookie_here":
                    groups_data = fetch_groups_from_api(cookie)
        except Exception as e:
            # 不阻断，记录告警
            print(f"⚠️ 获取账号群失败，降级为本地集合: {e}")
            groups_data = []

        # 组装账号侧群为字典（id -> info）
        by_id: Dict[int, dict] = {}

        for group in groups_data or []:
            # 提取用户特定信息
            user_specific = group.get('user_specific', {}) or {}
            validity = user_specific.get('validity', {}) or {}
            trial = user_specific.get('trial', {}) or {}

            # 过期信息与状态
            actual_expiry_time = trial.get('end_time') or validity.get('end_time')
            is_trial = bool(trial.get('end_time'))

            status = None
            if actual_expiry_time:
                from datetime import datetime, timezone
                try:
                    end_time = datetime.fromisoformat(actual_expiry_time.replace('Z', '+00:00'))
                    now = datetime.now(timezone.utc)
                    days_until_expiry = (end_time - now).days
                    if days_until_expiry < 0:
                        status = 'expired'
                    elif days_until_expiry <= 7:
                        status = 'expiring_soon'
                    else:
                        status = 'active'
                except Exception:
                    pass

            gid = group.get('group_id')
            try:
                gid = int(gid)
            except Exception:
                continue

            info = {
                "group_id": gid,
                "name": group.get('name', ''),
                "type": group.get('type', ''),
                "background_url": group.get('background_url', ''),
                "owner": group.get('owner', {}) or {},
                "statistics": group.get('statistics', {}) or {},
                "status": status,
                "create_time": group.get('create_time'),
                "subscription_time": validity.get('begin_time'),
                "expiry_time": actual_expiry_time,
                "join_time": user_specific.get('join_time'),
                "last_active_time": user_specific.get('last_active_time'),
                "description": group.get('description', ''),
                "is_trial": is_trial,
                "trial_end_time": trial.get('end_time'),
                "membership_end_time": validity.get('end_time'),
                "account": group_account_map.get(str(gid)),
                "source": "account"
            }
            by_id[gid] = info

        # 合并本地目录群
        for gid in local_ids or []:
            try:
                gid_int = int(gid)
            except Exception:
                continue
            if gid_int in by_id:
                # 标注来源为 account|local
                src = by_id[gid_int].get("source", "account")
                if "local" not in src:
                    by_id[gid_int]["source"] = "account|local"
            else:
                # 仅存在于本地
                by_id[gid_int] = {
                    "group_id": gid_int,
                    "name": "本地群（未绑定账号）",
                    "type": "local",
                    "background_url": "",
                    "owner": {},
                    "statistics": {},
                    "status": None,
                    "create_time": None,
                    "subscription_time": None,
                    "expiry_time": None,
                    "join_time": None,
                    "last_active_time": None,
                    "description": "",
                    "is_trial": False,
                    "trial_end_time": None,
                    "membership_end_time": None,
                    "account": None,
                    "source": "local"
                }

        # 排序：按群ID升序；如需二级排序再按来源（账号优先）
        merged = [by_id[k] for k in sorted(by_id.keys())]

        return {
            "groups": merged,
            "total": len(merged)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取群组列表失败: {str(e)}")

@app.get("/api/topics/{topic_id}/{group_id}")
async def get_topic_detail(topic_id: int, group_id: str):
    """获取话题详情"""
    try:
        crawler = get_crawler_for_group(group_id)
        topic_detail = crawler.db.get_topic_detail(topic_id)

        if not topic_detail:
            raise HTTPException(status_code=404, detail="话题不存在")

        return topic_detail
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取话题详情失败: {str(e)}")

@app.post("/api/topics/{topic_id}/{group_id}/refresh")
async def refresh_topic(topic_id: int, group_id: str):
    """实时更新单个话题信息"""
    try:
        crawler = get_crawler_for_group(group_id)

        # 使用知识星球API获取最新话题信息
        url = f"https://api.zsxq.com/v2/topics/{topic_id}/info"
        headers = crawler.get_stealth_headers()

        response = requests.get(url, headers=headers, timeout=30)

        if response.status_code == 200:
            data = response.json()
            if data.get('succeeded') and data.get('resp_data'):
                topic_data = data['resp_data']['topic']

                # 只更新话题的统计信息，避免创建重复记录
                success = crawler.db.update_topic_stats(topic_data)

                if not success:
                    return {"success": False, "message": "话题不存在或更新失败"}

                crawler.db.conn.commit()

                return {
                    "success": True,
                    "message": "话题信息已更新",
                    "updated_data": {
                        "likes_count": topic_data.get('likes_count', 0),
                        "comments_count": topic_data.get('comments_count', 0),
                        "reading_count": topic_data.get('reading_count', 0),
                        "readers_count": topic_data.get('readers_count', 0)
                    }
                }
            else:
                return {"success": False, "message": "API返回数据格式错误"}
        else:
            return {"success": False, "message": f"API请求失败: {response.status_code}"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"更新话题失败: {str(e)}")

@app.post("/api/topics/{topic_id}/{group_id}/fetch-comments")
async def fetch_more_comments(topic_id: int, group_id: str):
    """手动获取话题的更多评论"""
    try:
        crawler = get_crawler_for_group(group_id)

        # 先获取话题基本信息
        topic_detail = crawler.db.get_topic_detail(topic_id)
        if not topic_detail:
            raise HTTPException(status_code=404, detail="话题不存在")

        comments_count = topic_detail.get('comments_count', 0)
        if comments_count <= 8:
            return {
                "success": True,
                "message": f"话题只有 {comments_count} 条评论，无需获取更多",
                "comments_fetched": 0
            }

        # 获取更多评论
        try:
            additional_comments = crawler.fetch_all_comments(topic_id, comments_count)
            if additional_comments:
                crawler.db.import_additional_comments(topic_id, additional_comments)
                crawler.db.conn.commit()

                return {
                    "success": True,
                    "message": f"成功获取并导入 {len(additional_comments)} 条评论",
                    "comments_fetched": len(additional_comments)
                }
            else:
                return {
                    "success": False,
                    "message": "获取评论失败，可能是权限限制或网络问题",
                    "comments_fetched": 0
                }
        except Exception as e:
            return {
                "success": False,
                "message": f"获取评论时出错: {str(e)}",
                "comments_fetched": 0
            }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取更多评论失败: {str(e)}")

@app.delete("/api/topics/{topic_id}/{group_id}")
async def delete_single_topic(topic_id: int, group_id: int):
    """删除单个话题及其所有关联数据"""
    crawler = None
    try:
        # 使用指定群组的爬虫实例，以便复用其数据库连接
        crawler = get_crawler_for_group(str(group_id))

        # 检查话题是否存在且属于该群组
        crawler.db.cursor.execute('SELECT COUNT(*) FROM topics WHERE topic_id = ? AND group_id = ?', (topic_id, group_id))
        exists = crawler.db.cursor.fetchone()[0] > 0
        if not exists:
            return {"success": False, "message": "话题不存在"}

        # 依赖顺序删除关联数据
        tables_to_clean = [
            'user_liked_emojis',
            'like_emojis',
            'likes',
            'images',
            'comments',
            'answers',
            'questions',
            'articles',
            'talks',
            'topic_files',
            'topic_tags'
        ]

        for table in tables_to_clean:
            crawler.db.cursor.execute(f'DELETE FROM {table} WHERE topic_id = ?', (topic_id,))

        # 最后删除话题本身（限定群组）
        crawler.db.cursor.execute('DELETE FROM topics WHERE topic_id = ? AND group_id = ?', (topic_id, group_id))

        deleted = crawler.db.cursor.rowcount
        crawler.db.conn.commit()

        return {"success": True, "deleted_topic_id": topic_id, "deleted": deleted > 0}
    except Exception as e:
        try:
            if crawler and hasattr(crawler, 'db') and crawler.db:
                crawler.db.conn.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"删除话题失败: {str(e)}")

# 单个话题采集 API
@app.post("/api/topics/fetch-single/{group_id}/{topic_id}")
async def fetch_single_topic(group_id: str, topic_id: int, fetch_comments: bool = True):
    """爬取并导入单个话题（用于特殊话题测试），可选拉取完整评论"""
    try:
        # 使用该群的自动匹配账号
        crawler = get_crawler_for_group(str(group_id))

        # 拉取话题详细信息
        url = f"https://api.zsxq.com/v2/topics/{topic_id}/info"
        headers = crawler.get_stealth_headers()
        response = requests.get(url, headers=headers, timeout=30)

        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail="API请求失败")

        data = response.json()
        if not data.get("succeeded") or not data.get("resp_data"):
            raise HTTPException(status_code=400, detail="API返回失败")

        topic = (data.get("resp_data", {}) or {}).get("topic", {}) or {}

        if not topic:
            raise HTTPException(status_code=404, detail="未获取到有效话题数据")

        # 校验话题所属群组一致性
        topic_group_id = str((topic.get("group") or {}).get("group_id", ""))
        if topic_group_id and topic_group_id != str(group_id):
            raise HTTPException(status_code=400, detail="该话题不属于当前群组")

        # 判断话题是否已存在
        crawler.db.cursor.execute('SELECT topic_id FROM topics WHERE topic_id = ?', (topic_id,))
        existed = crawler.db.cursor.fetchone() is not None

        # 导入话题完整数据
        crawler.db.import_topic_data(topic)
        crawler.db.conn.commit()

        # 可选：获取完整评论
        comments_fetched = 0
        if fetch_comments:
            comments_count = topic.get("comments_count", 0) or 0
            if comments_count > 0:
                try:
                    additional_comments = crawler.fetch_all_comments(topic_id, comments_count)
                    if additional_comments:
                        crawler.db.import_additional_comments(topic_id, additional_comments)
                        crawler.db.conn.commit()
                        comments_fetched = len(additional_comments)
                except Exception as e:
                    # 不阻塞主流程
                    print(f"⚠️ 单话题评论获取失败: {e}")

        return {
            "success": True,
            "topic_id": topic_id,
            "group_id": int(group_id),
            "imported": "updated" if existed else "created",
            "comments_fetched": comments_fetched
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"单个话题采集失败: {str(e)}")

# 标签相关API端点
@app.get("/api/groups/{group_id}/tags")
async def get_group_tags(group_id: str):
    """获取指定群组的所有标签"""
    try:
        crawler = get_crawler_for_group(group_id)
        tags = crawler.db.get_tags_by_group(int(group_id))
        
        return {
            "tags": tags,
            "total": len(tags)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取标签列表失败: {str(e)}")

@app.get("/api/groups/{group_id}/tags/{tag_id}/topics")
async def get_topics_by_tag(group_id: int, tag_id: int, page: int = 1, per_page: int = 20):
    """根据标签获取指定群组的话题列表"""
    try:
        # 使用指定群组的爬虫实例
        crawler = get_crawler_for_group(str(group_id))
        
        # 验证标签是否存在于该群组中
        crawler.db.cursor.execute('SELECT COUNT(*) FROM tags WHERE tag_id = ? AND group_id = ?', (tag_id, group_id))
        tag_count = crawler.db.cursor.fetchone()[0]
        
        if tag_count == 0:
            raise HTTPException(status_code=404, detail="标签在该群组中不存在")
            
        result = crawler.db.get_topics_by_tag(tag_id, page, per_page)
        
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"根据标签获取话题失败: {str(e)}")

@app.get("/api/proxy-image")
async def proxy_image(url: str, group_id: str = None):
    """代理图片请求，支持本地缓存"""
    try:
        cache_manager = get_image_cache_manager(group_id)

        # 检查是否已缓存
        if cache_manager.is_cached(url):
            cached_path = cache_manager.get_cached_path(url)
            if cached_path and cached_path.exists():
                # 返回缓存的图片
                content_type = mimetypes.guess_type(str(cached_path))[0] or 'image/jpeg'

                with open(cached_path, 'rb') as f:
                    content = f.read()

                return Response(
                    content=content,
                    media_type=content_type,
                    headers={
                        'Cache-Control': 'public, max-age=86400',  # 缓存24小时
                        'Access-Control-Allow-Origin': '*',
                        'X-Cache-Status': 'HIT'
                    }
                )

        # 下载并缓存图片
        success, cached_path, error = cache_manager.download_and_cache(url)

        if success and cached_path and cached_path.exists():
            content_type = mimetypes.guess_type(str(cached_path))[0] or 'image/jpeg'

            with open(cached_path, 'rb') as f:
                content = f.read()

            return Response(
                content=content,
                media_type=content_type,
                headers={
                    'Cache-Control': 'public, max-age=86400',
                    'Access-Control-Allow-Origin': '*',
                    'X-Cache-Status': 'MISS'
                }
            )
        else:
            raise HTTPException(status_code=404, detail=f"图片加载失败: {error}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"代理图片失败: {str(e)}")


@app.get("/api/cache/images/info/{group_id}")
async def get_image_cache_info(group_id: str):
    """获取指定群组的图片缓存统计信息"""
    try:
        cache_manager = get_image_cache_manager(group_id)
        return cache_manager.get_cache_info()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取缓存信息失败: {str(e)}")


@app.delete("/api/cache/images/{group_id}")
async def clear_image_cache(group_id: str):
    """清空指定群组的图片缓存"""
    try:
        cache_manager = get_image_cache_manager(group_id)
        success, message = cache_manager.clear_cache()

        if success:
            return {"success": True, "message": message}
        else:
            raise HTTPException(status_code=500, detail=message)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"清空缓存失败: {str(e)}")


@app.get("/api/settings/crawl")
async def get_crawl_settings():
    """获取话题爬取设置"""
    try:
        # 返回默认设置
        return {
            "crawl_interval_min": 2.0,
            "crawl_interval_max": 5.0,
            "long_sleep_interval_min": 180.0,
            "long_sleep_interval_max": 300.0,
            "pages_per_batch": 15
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取爬取设置失败: {str(e)}")


@app.post("/api/settings/crawl")
async def update_crawl_settings(settings: dict):
    """更新话题爬取设置"""
    try:
        # 这里可以将设置保存到配置文件或数据库
        # 目前只是返回成功，实际设置通过API参数传递
        return {"success": True, "message": "爬取设置已更新"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"更新爬取设置失败: {str(e)}")


@app.get("/api/groups/{group_id}/info")
async def get_group_info(group_id: str):
    """获取群组信息（带本地回退，避免401/500导致前端报错）"""
    try:
        # 自动匹配该群组所属账号，获取对应Cookie
        cookie = get_cookie_for_group(group_id)

        # 本地回退数据构造（不访问官方API）
        def build_fallback(source: str = "fallback", note: str = None) -> dict:
            files_count = 0
            try:
                crawler = get_crawler_for_group(group_id)
                downloader = crawler.get_file_downloader()
                try:
                    downloader.file_db.cursor.execute("SELECT COUNT(*) FROM files")
                    row = downloader.file_db.cursor.fetchone()
                    files_count = (row[0] or 0) if row else 0
                except Exception:
                    files_count = 0
            except Exception:
                files_count = 0

            try:
                gid = int(group_id)
            except Exception:
                gid = group_id

            result = {
                "group_id": gid,
                "name": f"群组 {group_id}",
                "description": "",
                "statistics": {"files": {"count": files_count}},
                "background_url": None,
                "account": am_get_account_summary_for_group(group_id),
                "source": source,
            }
            if note:
                result["note"] = note
            return result

        # 若没有可用 Cookie，直接返回本地回退，避免抛 400/500
        if not cookie:
            return build_fallback(note="no_cookie")

        # 调用官方接口
        url = f"https://api.zsxq.com/v2/groups/{group_id}"
        headers = {
            'Cookie': cookie,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }

        response = requests.get(url, headers=headers, timeout=30)

        if response.status_code == 200:
            data = response.json()
            if data.get('succeeded'):
                group_data = data.get('resp_data', {}).get('group', {})
                return {
                    "group_id": group_data.get('group_id'),
                    "name": group_data.get('name'),
                    "description": group_data.get('description'),
                    "statistics": group_data.get('statistics', {}),
                    "background_url": group_data.get('background_url'),
                    "account": am_get_account_summary_for_group(group_id),
                    "source": "remote"
                }
            # 官方返回非 succeeded，也走回退
            return build_fallback(note="remote_response_failed")
        else:
            # 授权失败/权限不足 → 使用本地回退（200返回，减少前端告警）
            if response.status_code in (401, 403):
                return build_fallback(note=f"remote_api_{response.status_code}")
            # 其他状态码也回退
            return build_fallback(note=f"remote_api_{response.status_code}")

    except Exception:
        # 任何异常都回退为本地信息，避免 500
        return build_fallback(note="exception_fallback")

@app.get("/api/groups/{group_id}/topics")
async def get_group_topics(group_id: int, page: int = 1, per_page: int = 20, search: Optional[str] = None):
    """获取指定群组的话题列表"""
    try:
        # 使用指定群组的爬虫实例
        crawler = get_crawler_for_group(str(group_id))

        offset = (page - 1) * per_page

        # 构建查询SQL - 包含所有内容类型
        if search:
            query = """
                SELECT
                    t.topic_id, t.title, t.create_time, t.likes_count, t.comments_count,
                    t.reading_count, t.type, t.digested, t.sticky,
                    q.text as question_text,
                    a.text as answer_text,
                    tk.text as talk_text,
                    u.user_id, u.name, u.avatar_url, t.imported_at
                FROM topics t
                LEFT JOIN questions q ON t.topic_id = q.topic_id
                LEFT JOIN answers a ON t.topic_id = a.topic_id
                LEFT JOIN talks tk ON t.topic_id = tk.topic_id
                LEFT JOIN users u ON tk.owner_user_id = u.user_id
                WHERE t.group_id = ? AND (t.title LIKE ? OR q.text LIKE ? OR tk.text LIKE ?)
                ORDER BY t.create_time DESC
                LIMIT ? OFFSET ?
            """
            params = (group_id, f"%{search}%", f"%{search}%", f"%{search}%", per_page, offset)
        else:
            query = """
                SELECT
                    t.topic_id, t.title, t.create_time, t.likes_count, t.comments_count,
                    t.reading_count, t.type, t.digested, t.sticky,
                    q.text as question_text,
                    a.text as answer_text,
                    tk.text as talk_text,
                    u.user_id, u.name, u.avatar_url, t.imported_at
                FROM topics t
                LEFT JOIN questions q ON t.topic_id = q.topic_id
                LEFT JOIN answers a ON t.topic_id = a.topic_id
                LEFT JOIN talks tk ON t.topic_id = tk.topic_id
                LEFT JOIN users u ON tk.owner_user_id = u.user_id
                WHERE t.group_id = ?
                ORDER BY t.create_time DESC
                LIMIT ? OFFSET ?
            """
            params = (group_id, per_page, offset)

        crawler.db.cursor.execute(query, params)
        topics = crawler.db.cursor.fetchall()

        # 获取总数
        if search:
            crawler.db.cursor.execute("SELECT COUNT(*) FROM topics WHERE group_id = ? AND title LIKE ?", (group_id, f"%{search}%"))
        else:
            crawler.db.cursor.execute("SELECT COUNT(*) FROM topics WHERE group_id = ?", (group_id,))
        total = crawler.db.cursor.fetchone()[0]

        # 处理话题数据
        topics_list = []
        for topic in topics:
            topic_data = {
                "topic_id": topic[0],
                "title": topic[1],
                "create_time": topic[2],
                "likes_count": topic[3],
                "comments_count": topic[4],
                "reading_count": topic[5],
                "type": topic[6],
                "digested": bool(topic[7]) if topic[7] is not None else False,
                "sticky": bool(topic[8]) if topic[8] is not None else False,
                "imported_at": topic[15] if len(topic) > 15 else None  # 获取时间
            }

            # 添加内容文本
            if topic[6] == 'q&a':
                # 问答类型话题
                topic_data['question_text'] = topic[9] if topic[9] else ''
                topic_data['answer_text'] = topic[10] if topic[10] else ''
            else:
                # 其他类型话题（talk、article等）
                topic_data['talk_text'] = topic[11] if topic[11] else ''
                if topic[12]:  # 有作者信息
                    topic_data['author'] = {
                        'user_id': topic[12],
                        'name': topic[13],
                        'avatar_url': topic[14]
                    }

            topics_list.append(topic_data)

        return {
            "topics": topics_list,
            "pagination": {
                "page": page,
                "per_page": per_page,
                "total": total,
                "pages": (total + per_page - 1) // per_page
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取群组话题失败: {str(e)}")

@app.get("/api/groups/{group_id}/stats")
async def get_group_stats(group_id: int):
    """获取指定群组的统计信息"""
    try:
        # 使用指定群组的爬虫实例
        crawler = get_crawler_for_group(str(group_id))
        cursor = crawler.db.cursor

        # 获取话题统计
        cursor.execute("SELECT COUNT(*) FROM topics WHERE group_id = ?", (group_id,))
        topics_count = cursor.fetchone()[0]

        # 获取用户统计 - 从talks表获取，因为topics表没有user_id字段
        cursor.execute("""
            SELECT COUNT(DISTINCT t.owner_user_id)
            FROM talks t
            JOIN topics tp ON t.topic_id = tp.topic_id
            WHERE tp.group_id = ?
        """, (group_id,))
        users_count = cursor.fetchone()[0]

        # 获取最新话题时间
        cursor.execute("SELECT MAX(create_time) FROM topics WHERE group_id = ?", (group_id,))
        latest_topic_time = cursor.fetchone()[0]

        # 获取最早话题时间
        cursor.execute("SELECT MIN(create_time) FROM topics WHERE group_id = ?", (group_id,))
        earliest_topic_time = cursor.fetchone()[0]

        # 获取总点赞数
        cursor.execute("SELECT SUM(likes_count) FROM topics WHERE group_id = ?", (group_id,))
        total_likes = cursor.fetchone()[0] or 0

        # 获取总评论数
        cursor.execute("SELECT SUM(comments_count) FROM topics WHERE group_id = ?", (group_id,))
        total_comments = cursor.fetchone()[0] or 0

        # 获取总阅读数
        cursor.execute("SELECT SUM(reading_count) FROM topics WHERE group_id = ?", (group_id,))
        total_readings = cursor.fetchone()[0] or 0

        return {
            "group_id": group_id,
            "topics_count": topics_count,
            "users_count": users_count,
            "latest_topic_time": latest_topic_time,
            "earliest_topic_time": earliest_topic_time,
            "total_likes": total_likes,
            "total_comments": total_comments,
            "total_readings": total_readings
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取群组统计失败: {str(e)}")

@app.get("/api/groups/{group_id}/database-info")
async def get_group_database_info(group_id: int):
    """获取指定群组的数据库信息"""
    try:
        path_manager = get_db_path_manager()
        db_info = path_manager.get_database_info(str(group_id))

        return {
            "group_id": group_id,
            "database_info": db_info
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取数据库信息失败: {str(e)}")

@app.delete("/api/groups/{group_id}/topics")
async def delete_group_topics(group_id: int):
    """删除指定群组的所有话题数据"""
    try:
        # 使用指定群组的爬虫实例
        crawler = get_crawler_for_group(str(group_id))

        # 获取删除前的统计信息
        crawler.db.cursor.execute('SELECT COUNT(*) FROM topics WHERE group_id = ?', (group_id,))
        topics_count = crawler.db.cursor.fetchone()[0]

        if topics_count == 0:
            return {
                "message": "该群组没有话题数据",
                "deleted_count": 0
            }

        # 删除相关数据（按照外键依赖顺序）
        tables_to_clean = [
            ('user_liked_emojis', 'topic_id'),
            ('like_emojis', 'topic_id'),
            ('likes', 'topic_id'),
            ('images', 'topic_id'),
            ('comments', 'topic_id'),
            ('answers', 'topic_id'),
            ('questions', 'topic_id'),
            ('articles', 'topic_id'),
            ('talks', 'topic_id'),
            ('topic_files', 'topic_id'),  # 添加话题文件表
            ('topic_tags', 'topic_id'),   # 添加话题标签关联表
            ('topics', 'group_id')
        ]

        deleted_counts = {}

        for table, id_column in tables_to_clean:
            if id_column == 'group_id':
                # 直接按group_id删除
                crawler.db.cursor.execute(f'DELETE FROM {table} WHERE {id_column} = ?', (group_id,))
            else:
                # 按topic_id删除，需要先找到该群组的所有topic_id
                crawler.db.cursor.execute(f'''
                    DELETE FROM {table}
                    WHERE {id_column} IN (
                        SELECT topic_id FROM topics WHERE group_id = ?
                    )
                ''', (group_id,))

            deleted_counts[table] = crawler.db.cursor.rowcount

        # 提交事务
        crawler.db.conn.commit()

        return {
            "message": f"成功删除群组 {group_id} 的所有话题数据",
            "deleted_topics_count": topics_count,
            "deleted_details": deleted_counts
        }

    except Exception as e:
        # 回滚事务
        crawler.db.conn.rollback()
        raise HTTPException(status_code=500, detail=f"删除话题数据失败: {str(e)}")

@app.get("/api/tasks/{task_id}/logs")
async def get_task_logs(task_id: str):
    """获取任务日志"""
    if task_id not in task_logs:
        raise HTTPException(status_code=404, detail="任务不存在")

    return {
        "task_id": task_id,
        "logs": task_logs[task_id]
    }

@app.get("/api/tasks/{task_id}/stream")
async def stream_task_logs(task_id: str):
    """SSE流式传输任务日志"""
    async def event_stream():
        # 初始化连接
        if task_id not in sse_connections:
            sse_connections[task_id] = []

        # 发送历史日志
        if task_id in task_logs:
            for log in task_logs[task_id]:
                yield f"data: {json.dumps({'type': 'log', 'message': log})}\n\n"

        # 发送任务状态
        if task_id in current_tasks:
            task = current_tasks[task_id]
            yield f"data: {json.dumps({'type': 'status', 'status': task['status'], 'message': task['message']})}\n\n"

        # 记录当前日志数量，用于检测新日志
        last_log_count = len(task_logs.get(task_id, []))

        # 保持连接活跃
        try:
            while True:
                # 检查是否有新日志
                current_log_count = len(task_logs.get(task_id, []))
                if current_log_count > last_log_count:
                    # 发送新日志
                    new_logs = task_logs[task_id][last_log_count:]
                    for log in new_logs:
                        yield f"data: {json.dumps({'type': 'log', 'message': log})}\n\n"
                    last_log_count = current_log_count

                # 检查任务状态变化
                if task_id in current_tasks:
                    task = current_tasks[task_id]
                    yield f"data: {json.dumps({'type': 'status', 'status': task['status'], 'message': task['message']})}\n\n"

                    if task['status'] in ['completed', 'failed', 'cancelled']:
                        break

                # 发送心跳
                yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"
                await asyncio.sleep(0.5)  # 更频繁的检查

        except asyncio.CancelledError:
            # 客户端断开连接
            pass

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*",
        }
    )

# 图片代理API
@app.get("/api/proxy/image")
async def proxy_image(url: str):
    """图片代理，解决盗链问题"""
    import requests
    from fastapi.responses import Response

    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://wx.zsxq.com/',
            'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
        }

        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        return Response(
            content=response.content,
            media_type=response.headers.get('content-type', 'image/jpeg'),
            headers={
                'Cache-Control': 'public, max-age=3600',
                'Access-Control-Allow-Origin': '*'
            }
        )
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"图片加载失败: {str(e)}")

# 设置相关API路由
@app.get("/api/settings/crawler")
async def get_crawler_settings():
    """获取爬虫设置"""
    try:
        crawler = get_crawler_safe()
        if not crawler:
            return {
                "min_delay": 2.0,
                "max_delay": 5.0,
                "long_delay_interval": 15,
                "timestamp_offset_ms": 1,
                "debug_mode": False
            }

        return {
            "min_delay": crawler.min_delay,
            "max_delay": crawler.max_delay,
            "long_delay_interval": crawler.long_delay_interval,
            "timestamp_offset_ms": crawler.timestamp_offset_ms,
            "debug_mode": crawler.debug_mode
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取爬虫设置失败: {str(e)}")

class CrawlerSettingsRequest(BaseModel):
    min_delay: float = Field(default=2.0, ge=0.5, le=10.0)
    max_delay: float = Field(default=5.0, ge=1.0, le=20.0)
    long_delay_interval: int = Field(default=15, ge=5, le=100)
    timestamp_offset_ms: int = Field(default=1, ge=0, le=1000)
    debug_mode: bool = Field(default=False)

@app.post("/api/settings/crawler")
async def update_crawler_settings(request: CrawlerSettingsRequest):
    """更新爬虫设置"""
    try:
        crawler = get_crawler_safe()
        if not crawler:
            raise HTTPException(status_code=404, detail="爬虫未初始化")

        # 验证设置
        if request.min_delay >= request.max_delay:
            raise HTTPException(status_code=400, detail="最小延迟必须小于最大延迟")

        # 更新设置
        crawler.min_delay = request.min_delay
        crawler.max_delay = request.max_delay
        crawler.long_delay_interval = request.long_delay_interval
        crawler.timestamp_offset_ms = request.timestamp_offset_ms
        crawler.debug_mode = request.debug_mode

        return {
            "message": "爬虫设置已更新",
            "settings": {
                "min_delay": crawler.min_delay,
                "max_delay": crawler.max_delay,
                "long_delay_interval": crawler.long_delay_interval,
                "timestamp_offset_ms": crawler.timestamp_offset_ms,
                "debug_mode": crawler.debug_mode
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"更新爬虫设置失败: {str(e)}")

@app.get("/api/settings/downloader")
async def get_downloader_settings():
    """获取文件下载器设置"""
    try:
        crawler = get_crawler_safe()
        if not crawler:
            return {
                "download_interval_min": 30,
                "download_interval_max": 60,
                "long_delay_interval": 10,
                "long_delay_min": 300,
                "long_delay_max": 600
            }

        downloader = crawler.get_file_downloader()
        return {
            "download_interval_min": downloader.download_interval_min,
            "download_interval_max": downloader.download_interval_max,
            "long_delay_interval": downloader.long_delay_interval,
            "long_delay_min": downloader.long_delay_min,
            "long_delay_max": downloader.long_delay_max
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取下载器设置失败: {str(e)}")

class DownloaderSettingsRequest(BaseModel):
    download_interval_min: int = Field(default=30, ge=1, le=300)
    download_interval_max: int = Field(default=60, ge=5, le=600)
    long_delay_interval: int = Field(default=10, ge=1, le=100)
    long_delay_min: int = Field(default=300, ge=60, le=1800)
    long_delay_max: int = Field(default=600, ge=120, le=3600)

@app.post("/api/settings/downloader")
async def update_downloader_settings(request: DownloaderSettingsRequest):
    """更新文件下载器设置"""
    try:
        crawler = get_crawler_safe()
        if not crawler:
            raise HTTPException(status_code=404, detail="爬虫未初始化")

        # 验证设置
        if request.download_interval_min >= request.download_interval_max:
            raise HTTPException(status_code=400, detail="最小下载间隔必须小于最大下载间隔")

        if request.long_delay_min >= request.long_delay_max:
            raise HTTPException(status_code=400, detail="最小长休眠时间必须小于最大长休眠时间")

        downloader = crawler.get_file_downloader()

        # 更新设置
        downloader.download_interval_min = request.download_interval_min
        downloader.download_interval_max = request.download_interval_max
        downloader.long_delay_interval = request.long_delay_interval
        downloader.long_delay_min = request.long_delay_min
        downloader.long_delay_max = request.long_delay_max

        return {
            "message": "下载器设置已更新",
            "settings": {
                "download_interval_min": downloader.download_interval_min,
                "download_interval_max": downloader.download_interval_max,
                "long_delay_interval": downloader.long_delay_interval,
                "long_delay_min": downloader.long_delay_min,
                "long_delay_max": downloader.long_delay_max
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"更新下载器设置失败: {str(e)}")

# =========================
# 自动账号匹配缓存与辅助函数
# =========================
ACCOUNT_DETECT_TTL_SECONDS = 300
_account_detect_cache: Dict[str, Any] = {
    "built_at": 0,
    "group_to_account": {},
    "cookie_by_account": {}
}

def _get_all_account_sources() -> List[Dict[str, Any]]:
    """组合账号来源：accounts.json + config.toml默认账号"""
    sources: List[Dict[str, Any]] = []
    try:
        # 账号管理中的账号（含cookie）
        accounts = am_get_accounts(mask_cookie=False)
        if accounts:
            sources.extend(accounts)
    except Exception:
        pass
    # 追加 config.toml 的默认cookie作为伪账号
    try:
        cfg = load_config()
        auth = cfg.get('auth', {}) if cfg else {}
        default_cookie = auth.get('cookie', '')
        if default_cookie and default_cookie != "your_cookie_here":
            sources.append({
                "id": "default",
                "name": "默认账号",
                "cookie": default_cookie,
                "is_default": True,
                "created_at": None
            })
    except Exception:
        pass
    return sources

def build_account_group_detection(force_refresh: bool = False) -> Dict[str, Dict[str, Any]]:
    """
    构建自动匹配映射：group_id -> 账号摘要
    遍历所有账号来源，调用官方 /v2/groups 获取其可访问群组进行比对。
    使用内存缓存减少频繁请求。
    """
    now = time.time()
    cache = _account_detect_cache
    if (not force_refresh
        and cache.get("group_to_account")
        and now - cache.get("built_at", 0) < ACCOUNT_DETECT_TTL_SECONDS):
        return cache["group_to_account"]

    group_to_account: Dict[str, Dict[str, Any]] = {}
    cookie_by_account: Dict[str, str] = {}

    sources = _get_all_account_sources()
    for src in sources:
        cookie = src.get("cookie", "")
        acc_id = src.get("id", "default")
        if not cookie or cookie == "your_cookie_here":
            continue

        # 记录账号对应cookie
        cookie_by_account[acc_id] = cookie

        try:
            groups = fetch_groups_from_api(cookie)
            for g in groups or []:
                gid = str(g.get("group_id"))
                if gid and gid not in group_to_account:
                    group_to_account[gid] = {
                        "id": acc_id,
                        "name": src.get("name") or ("默认账号" if acc_id == "default" else acc_id),
                        "is_default": bool(src.get("is_default") or acc_id == "default"),
                        "created_at": src.get("created_at"),
                        "cookie": "***"
                    }
        except Exception:
            # 忽略单个账号失败
            continue

    cache["group_to_account"] = group_to_account
    cache["cookie_by_account"] = cookie_by_account
    cache["built_at"] = now
    return group_to_account

def get_cookie_for_group(group_id: str) -> str:
    """根据自动匹配结果选择用于该群组的Cookie，失败则回退到config.toml"""
    mapping = build_account_group_detection(force_refresh=False)
    summary = mapping.get(str(group_id))
    cookie = None
    if summary:
        cookie = _account_detect_cache.get("cookie_by_account", {}).get(summary["id"])
    if not cookie:
        cfg = load_config()
        auth = cfg.get('auth', {}) if cfg else {}
        cookie = auth.get('cookie', '')
    return cookie

def get_account_summary_for_group_auto(group_id: str) -> Optional[Dict[str, Any]]:
    """返回自动匹配到的账号摘要；若无命中且存在默认cookie，则返回默认占位摘要"""
    mapping = build_account_group_detection(force_refresh=False)
    summary = mapping.get(str(group_id))
    if summary:
        return summary
    cfg = load_config()
    auth = cfg.get('auth', {}) if cfg else {}
    default_cookie = auth.get('cookie', '')
    if default_cookie:
        return {
            "id": "default",
            "name": "默认账号",
            "is_default": True,
            "created_at": None,
            "cookie": "***"
        }
    return None

# =========================
# 新增：按时间区间爬取
# =========================

class CrawlTimeRangeRequest(BaseModel):
    startTime: Optional[str] = Field(default=None, description="开始时间，支持 YYYY-MM-DD 或 ISO8601，缺省则按 lastDays 推导")
    endTime: Optional[str] = Field(default=None, description="结束时间，默认当前时间（本地东八区）")
    lastDays: Optional[int] = Field(default=None, ge=1, le=3650, description="最近N天（与 startTime/endTime 互斥优先；当 startTime 缺省时可用）")
    perPage: Optional[int] = Field(default=20, ge=1, le=100, description="每页数量")
    # 可选的随机间隔设置（与其他爬取接口保持一致）
    crawlIntervalMin: Optional[float] = Field(default=None, ge=1.0, le=60.0, description="爬取间隔最小值(秒)")
    crawlIntervalMax: Optional[float] = Field(default=None, ge=1.0, le=60.0, description="爬取间隔最大值(秒)")
    longSleepIntervalMin: Optional[float] = Field(default=None, ge=60.0, le=3600.0, description="长休眠间隔最小值(秒)")
    longSleepIntervalMax: Optional[float] = Field(default=None, ge=60.0, le=3600.0, description="长休眠间隔最大值(秒)")
    pagesPerBatch: Optional[int] = Field(default=None, ge=5, le=50, description="每批次页面数")


def run_crawl_time_range_task(task_id: str, group_id: str, request: "CrawlTimeRangeRequest"):
    """后台执行“按时间区间爬取”任务：仅导入位于区间 [startTime, endTime] 内的话题"""
    try:
        from datetime import datetime, timedelta, timezone

        # 解析用户输入时间
        def parse_user_time(s: Optional[str]) -> Optional[datetime]:
            if not s:
                return None
            t = s.strip()
            try:
                # 仅日期：YYYY-MM-DD -> 当天00:00:00（东八区）
                if len(t) == 10 and t[4] == '-' and t[7] == '-':
                    dt = datetime.strptime(t, '%Y-%m-%d')
                    return dt.replace(tzinfo=timezone(timedelta(hours=8)))
                # datetime-local (无秒)：YYYY-MM-DDTHH:MM
                if 'T' in t and len(t) == 16:
                    t = t + ':00'
                # 尾部Z -> +00:00
                if t.endswith('Z'):
                    t = t.replace('Z', '+00:00')
                # 兼容 +0800 -> +08:00
                if len(t) >= 24 and (t[-5] in ['+', '-']) and t[-3] != ':':
                    t = t[:-2] + ':' + t[-2:]
                dt = datetime.fromisoformat(t)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone(timedelta(hours=8)))
                return dt
            except Exception:
                return None

        bj_tz = timezone(timedelta(hours=8))
        now_bj = datetime.now(bj_tz)

        start_dt = parse_user_time(request.startTime)
        end_dt = parse_user_time(request.endTime) if request.endTime else None

        # 若指定了最近N天，以 end_dt（默认现在）为终点推导 start_dt
        if request.lastDays and request.lastDays > 0:
            if end_dt is None:
                end_dt = now_bj
            start_dt = end_dt - timedelta(days=request.lastDays)

        # 默认 end_dt = 现在
        if end_dt is None:
            end_dt = now_bj
        # 默认 start_dt = end_dt - 30天
        if start_dt is None:
            start_dt = end_dt - timedelta(days=30)

        # 保证时间顺序
        if start_dt > end_dt:
            start_dt, end_dt = end_dt, start_dt

        update_task(task_id, "running", "开始按时间区间爬取...")
        add_task_log(task_id, f"🗓️ 时间范围: {start_dt.isoformat()} ~ {end_dt.isoformat()}")

        # 停止检查
        def stop_check():
            return is_task_stopped(task_id)

        # 爬虫实例（绑定该群组）
        def log_callback(message: str):
            add_task_log(task_id, message)

        cookie = get_cookie_for_group(group_id)
        path_manager = get_db_path_manager()
        db_path = path_manager.get_topics_db_path(group_id)

        crawler = ZSXQInteractiveCrawler(cookie, group_id, db_path, log_callback)
        crawler.stop_check_func = stop_check

        # 可选：应用自定义间隔设置
        if any([
            request.crawlIntervalMin, request.crawlIntervalMax,
            request.longSleepIntervalMin, request.longSleepIntervalMax,
            request.pagesPerBatch
        ]):
            crawler.set_custom_intervals(
                crawl_interval_min=request.crawlIntervalMin,
                crawl_interval_max=request.crawlIntervalMax,
                long_sleep_interval_min=request.longSleepIntervalMin,
                long_sleep_interval_max=request.longSleepIntervalMax,
                pages_per_batch=request.pagesPerBatch
            )

        per_page = request.perPage or 20
        total_stats = {'new_topics': 0, 'updated_topics': 0, 'errors': 0, 'pages': 0}
        end_time_param = None  # 从最新开始
        max_retries_per_page = 10

        while True:
            if is_task_stopped(task_id):
                add_task_log(task_id, "🛑 任务已停止")
                break

            retry = 0
            page_processed = False
            last_time_dt_in_page = None

            while retry < max_retries_per_page:
                if is_task_stopped(task_id):
                    break

                data = crawler.fetch_topics_safe(
                    scope="all",
                    count=per_page,
                    end_time=end_time_param,
                    is_historical=True if end_time_param else False
                )

                # 会员过期
                if data and isinstance(data, dict) and data.get('expired'):
                    add_task_log(task_id, f"❌ 会员已过期: {data.get('message')}")
                    update_task(task_id, "failed", "会员已过期", data)
                    return

                if not data:
                    retry += 1
                    total_stats['errors'] += 1
                    add_task_log(task_id, f"❌ 页面获取失败 (重试{retry}/{max_retries_per_page})")
                    continue

                topics = (data.get('resp_data', {}) or {}).get('topics', []) or []
                if not topics:
                    add_task_log(task_id, "📭 无更多数据，任务结束")
                    page_processed = True
                    break

                # 过滤时间范围
                from datetime import datetime
                filtered = []
                for t in topics:
                    ts = t.get('create_time')
                    dt = None
                    try:
                        if ts:
                            ts_fixed = ts.replace('+0800', '+08:00') if ts.endswith('+0800') else ts
                            dt = datetime.fromisoformat(ts_fixed)
                    except Exception:
                        dt = None

                    if dt:
                        last_time_dt_in_page = dt  # 该页数据按时间降序；循环结束后持有最后（最老）时间
                        if start_dt <= dt <= end_dt:
                            filtered.append(t)

                # 仅导入时间范围内的数据
                if filtered:
                    filtered_data = {'succeeded': True, 'resp_data': {'topics': filtered}}
                    page_stats = crawler.store_batch_data(filtered_data)
                    total_stats['new_topics'] += page_stats.get('new_topics', 0)
                    total_stats['updated_topics'] += page_stats.get('updated_topics', 0)
                    total_stats['errors'] += page_stats.get('errors', 0)

                total_stats['pages'] += 1
                page_processed = True

                # 计算下一页的 end_time（使用该页最老话题时间 - 偏移毫秒）
                oldest_in_page = topics[-1].get('create_time')
                try:
                    dt_oldest = datetime.fromisoformat(oldest_in_page.replace('+0800', '+08:00'))
                    dt_oldest = dt_oldest - timedelta(milliseconds=crawler.timestamp_offset_ms)
                    end_time_param = dt_oldest.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
                except Exception:
                    end_time_param = oldest_in_page

                # 若该页最老时间已早于 start_dt，则后续更老数据均不在范围内，结束
                if last_time_dt_in_page and last_time_dt_in_page < start_dt:
                    add_task_log(task_id, "✅ 已到达起始时间之前，任务结束")
                    break

                # 成功处理后进行长休眠检查
                crawler.check_page_long_delay()
                break  # 成功后跳出重试循环

            if not page_processed:
                add_task_log(task_id, "🚫 当前页面达到最大重试次数，终止任务")
                break

            # 结束条件：没有下一页时间或已越过起始边界
            if not end_time_param or (last_time_dt_in_page and last_time_dt_in_page < start_dt):
                break

        update_task(task_id, "completed", "时间区间爬取完成", total_stats)
    except Exception as e:
        if not is_task_stopped(task_id):
            add_task_log(task_id, f"❌ 时间区间爬取失败: {str(e)}")
            update_task(task_id, "failed", f"时间区间爬取失败: {str(e)}")


@app.post("/api/crawl/range/{group_id}")
async def crawl_by_time_range(group_id: str, request: CrawlTimeRangeRequest, background_tasks: BackgroundTasks):
    """按时间区间爬取话题（支持最近N天或自定义开始/结束时间）"""
    try:
        task_id = create_task("crawl_time_range", f"按时间区间爬取 (群组: {group_id})")
        background_tasks.add_task(run_crawl_time_range_task, task_id, group_id, request)
        return {"task_id": task_id, "message": "任务已创建，正在后台执行"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建时间区间爬取任务失败: {str(e)}")
@app.delete("/api/groups/{group_id}")
async def delete_group_local(group_id: str):
    """
    删除指定社群的本地数据（数据库、下载文件、图片缓存），不影响账号对该社群的访问权限
    """
    try:
        details = {
            "topics_db_removed": False,
            "files_db_removed": False,
            "downloads_dir_removed": False,
            "images_cache_removed": False,
            "group_dir_removed": False,
        }

        # 尝试关闭数据库连接，避免文件占用
        try:
            crawler = get_crawler_for_group(group_id)
            try:
                if hasattr(crawler, "file_downloader") and crawler.file_downloader:
                    if hasattr(crawler.file_downloader, "file_db") and crawler.file_downloader.file_db:
                        crawler.file_downloader.file_db.close()
                        print(f"✅ 已关闭文件数据库连接（群 {group_id}）")
            except Exception as e:
                print(f"⚠️ 关闭文件数据库连接时出错: {e}")
            try:
                if hasattr(crawler, "db") and crawler.db:
                    crawler.db.close()
                    print(f"✅ 已关闭话题数据库连接（群 {group_id}）")
            except Exception as e:
                print(f"⚠️ 关闭话题数据库连接时出错: {e}")
        except Exception as e:
            print(f"⚠️ 获取爬虫实例以关闭连接失败: {e}")

        # 垃圾回收 + 等待片刻，确保句柄释放
        import gc, time, shutil
        gc.collect()
        time.sleep(0.3)

        path_manager = get_db_path_manager()
        group_dir = path_manager.get_group_dir(group_id)
        topics_db = path_manager.get_topics_db_path(group_id)
        files_db = path_manager.get_files_db_path(group_id)

        # 删除话题数据库
        try:
            if os.path.exists(topics_db):
                os.remove(topics_db)
                details["topics_db_removed"] = True
                print(f"🗑️ 已删除话题数据库: {topics_db}")
        except PermissionError as pe:
            raise HTTPException(status_code=500, detail=f"话题数据库被占用，无法删除: {pe}")
        except Exception as e:
            print(f"⚠️ 删除话题数据库失败: {e}")

        # 删除文件数据库
        try:
            if os.path.exists(files_db):
                os.remove(files_db)
                details["files_db_removed"] = True
                print(f"🗑️ 已删除文件数据库: {files_db}")
        except PermissionError as pe:
            raise HTTPException(status_code=500, detail=f"文件数据库被占用，无法删除: {pe}")
        except Exception as e:
            print(f"⚠️ 删除文件数据库失败: {e}")

        # 删除下载目录
        downloads_dir = os.path.join(group_dir, "downloads")
        if os.path.exists(downloads_dir):
            try:
                shutil.rmtree(downloads_dir, ignore_errors=False)
                details["downloads_dir_removed"] = True
                print(f"🗑️ 已删除下载目录: {downloads_dir}")
            except Exception as e:
                print(f"⚠️ 删除下载目录失败: {e}")

        # 清空并删除图片缓存目录，同时释放缓存管理器
        try:
            from image_cache_manager import get_image_cache_manager, clear_group_cache_manager
            cache_manager = get_image_cache_manager(group_id)
            ok, msg = cache_manager.clear_cache()
            if ok:
                details["images_cache_removed"] = True
                print(f"🗑️ 图片缓存清空: {msg}")
            images_dir = os.path.join(group_dir, "images")
            if os.path.exists(images_dir):
                try:
                    shutil.rmtree(images_dir, ignore_errors=True)
                    print(f"🗑️ 已删除图片缓存目录: {images_dir}")
                except Exception as e:
                    print(f"⚠️ 删除图片缓存目录失败: {e}")
            clear_group_cache_manager(group_id)
        except Exception as e:
            print(f"⚠️ 清理图片缓存失败: {e}")

        # 若群组目录已空，则删除该目录
        try:
            if os.path.exists(group_dir) and len(os.listdir(group_dir)) == 0:
                os.rmdir(group_dir)
                details["group_dir_removed"] = True
                print(f"🗑️ 已删除空群组目录: {group_dir}")
        except Exception as e:
            print(f"⚠️ 删除群组目录失败: {e}")

        # 更新本地群缓存（从缓存集合移除）
        try:
            gid_int = int(group_id)
            if gid_int in _local_groups_cache.get("ids", set()):
                _local_groups_cache["ids"].discard(gid_int)
                _local_groups_cache["scanned_at"] = time.time()
        except Exception as e:
            print(f"⚠️ 更新本地群缓存失败: {e}")

        any_removed = any(details.values())
        return {
            "success": True,
            "message": f"群组 {group_id} 本地数据" + ("已删除" if any_removed else "不存在"),
            "details": details,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"删除群组本地数据失败: {str(e)}")
if __name__ == "__main__":
    import sys
    port = 8001 if len(sys.argv) > 1 and sys.argv[1] == "--port" and len(sys.argv) > 2 else 8000
    if len(sys.argv) > 2 and sys.argv[1] == "--port":
        try:
            port = int(sys.argv[2])
        except ValueError:
            port = 8000
    uvicorn.run(app, host="0.0.0.0", port=port)
