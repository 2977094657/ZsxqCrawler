"""
知识星球数据采集器 - FastAPI 后端服务
提供RESTful API接口来操作现有的爬虫功能
"""

import os
import sys
import asyncio
import base64
import gc
import shutil
import io
import posixpath
import sqlite3
import tempfile
import zipfile
from typing import Dict, Any, Optional, List
from datetime import datetime
from contextlib import asynccontextmanager
import json
import requests

from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, Response, FileResponse
from pydantic import BaseModel, Field
from starlette.background import BackgroundTask
import uvicorn
import mimetypes
import random
import time
from urllib.parse import urlparse

# 添加项目根目录到 Python 路径（后端代码位于 backend 包内）
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if project_root not in sys.path:
    sys.path.append(project_root)

# 导入现有的业务逻辑模块
from .zsxq_interactive_crawler import ZSXQInteractiveCrawler, load_config
from .zsxq_database import ZSXQDatabase
from .zsxq_file_database import ZSXQFileDatabase
from .db_path_manager import get_db_path_manager
from . import accounts_sql_manager as accounts_sql_module
from . import account_info_db as account_info_module
from . import image_cache_manager as image_cache_module
from .image_cache_manager import get_image_cache_manager
# 使用SQL账号管理器
from .accounts_sql_manager import get_accounts_sql_manager
from .account_info_db import get_account_info_db
from .zsxq_columns_database import ZSXQColumnsDatabase
from .logger_config import (
    bind_context,
    ensure_configured,
    log_debug,
    log_error,
    log_exception,
    log_info,
    log_task_event,
    log_warning,
)
from .zsxq_retry import (
    GLOBAL_API_MAX_RETRIES,
    is_global_retry_code,
    retry_wait_seconds,
    should_retry_api_code,
)
from .zsxq_markdown_exporter import (
    article_html_to_markdown,
    column_topic_detail_to_markdown,
    safe_filename,
    topic_detail_to_markdown,
    write_temp_markdown_file,
    write_temp_topic_archive,
)

# 初始化日志系统
ensure_configured()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理：启动时扫描本地群"""
    # 启动时执行
    try:
        log_info("应用启动: 开始扫描本地群")
        await asyncio.to_thread(scan_local_groups)
        log_info("应用启动: 本地群扫描完成")
    except Exception as e:
        log_error(f"应用启动: 本地群扫描失败: {e}", exception=e)
    yield
    log_info("应用关闭: lifespan 退出")


app = FastAPI(
    title="知识星球数据采集器 API",
    description="为知识星球数据采集器提供RESTful API接口",
    version="1.0.0",
    lifespan=lifespan
)

# 配置CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 前端地址
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_http_requests(request: Request, call_next):
    """记录每个 HTTP 请求的关键诊断信息。"""
    request_id = request.headers.get("X-Request-ID") or f"req_{int(time.time() * 1000)}_{random.randint(1000, 9999)}"
    request_logger = bind_context(request_id=request_id)
    started_at = time.perf_counter()
    client_host = request.client.host if request.client else "-"
    method = request.method
    path = request.url.path

    request_logger.debug(
        "HTTP request started: method={} path={} client={}",
        method,
        path,
        client_host,
    )

    try:
        response = await call_next(request)
    except Exception as exc:
        duration_ms = (time.perf_counter() - started_at) * 1000
        request_logger.opt(exception=exc).error(
            "HTTP request failed: method={} path={} duration_ms={:.2f} client={}",
            method,
            path,
            duration_ms,
            client_host,
        )
        raise

    duration_ms = (time.perf_counter() - started_at) * 1000
    response.headers["X-Request-ID"] = request_id
    level = "ERROR" if response.status_code >= 500 else "WARNING" if response.status_code >= 400 else "INFO"
    request_logger.log(
        level,
        "HTTP request finished: method={} path={} status={} duration_ms={:.2f} client={}",
        method,
        path,
        response.status_code,
        duration_ms,
        client_host,
    )
    return response

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


# Pydantic模型定义
class ConfigModel(BaseModel):
    cookie: str = Field(..., description="知识星球Cookie")

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

class ColumnsSettingsRequest(BaseModel):
    """专栏采集设置请求"""
    crawlIntervalMin: Optional[float] = Field(default=2.0, ge=1.0, le=60.0, description="采集间隔最小值(秒)")
    crawlIntervalMax: Optional[float] = Field(default=5.0, ge=1.0, le=60.0, description="采集间隔最大值(秒)")
    longSleepIntervalMin: Optional[float] = Field(default=30.0, ge=10.0, le=600.0, description="长休眠间隔最小值(秒)")
    longSleepIntervalMax: Optional[float] = Field(default=60.0, ge=10.0, le=600.0, description="长休眠间隔最大值(秒)")
    itemsPerBatch: Optional[int] = Field(default=10, ge=3, le=50, description="每批次处理数量")
    downloadFiles: Optional[bool] = Field(default=True, description="是否下载文件")
    downloadVideos: Optional[bool] = Field(default=True, description="是否下载视频(需要ffmpeg)")
    cacheImages: Optional[bool] = Field(default=True, description="是否缓存图片")
    incrementalMode: Optional[bool] = Field(default=False, description="增量模式：跳过已存在的文章详情")

class AccountCreateRequest(BaseModel):
    cookie: str = Field(..., description="账号Cookie")
    name: Optional[str] = Field(default=None, description="账号名称")

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

def get_primary_cookie() -> Optional[str]:
    """
    获取当前优先使用的Cookie：
    1. 若账号管理中存在账号，则优先使用第一个账号的Cookie
    2. 否则回退到 config.toml 中的 Cookie（若已配置）
    """
    # 1. 第一个账号
    try:
        sql_mgr = get_accounts_sql_manager()
        first_acc = sql_mgr.get_first_account(mask_cookie=False)
        if first_acc:
            cookie = (first_acc.get("cookie") or "").strip()
            if cookie:
                return cookie
    except Exception:
        pass

    # 2. config.toml 中的 Cookie
    try:
        config = load_config()
        if not config:
            return None
        auth_config = config.get("auth", {}) or {}
        cookie = (auth_config.get("cookie") or "").strip()
        if cookie and cookie != "your_cookie_here":
            return cookie
    except Exception:
        return None

    return None


def is_configured() -> bool:
    """检查是否已配置至少一个可用的认证Cookie（账号管理或config.toml 均可）"""
    return get_primary_cookie() is not None

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

    task_info = current_tasks.get(task_id, {})
    log_level = _infer_task_log_level(log_message)
    try:
        log_task_event(
            task_id,
            log_message,
            level=log_level,
            group_id=task_info.get("group_id"),
            task_type=task_info.get("type"),
        )
    except Exception as exc:
        # 任务日志落盘失败不能影响任务本身，错误仍尽量写入全局日志。
        log_error(f"写入任务日志失败: task_id={task_id}, error={exc}", exception=exc)

    # 广播日志到所有SSE连接
    broadcast_log(task_id, formatted_log)


def _infer_task_log_level(log_message: str) -> str:
    """根据任务日志内容推断日志级别，便于错误文件聚合。"""
    message = str(log_message)
    if any(marker in message for marker in ("❌", "失败", "异常", "错误", "failed", "error")):
        return "ERROR"
    if any(marker in message for marker in ("⚠️", "警告", "重试", "warning", "retry")):
        return "WARNING"
    return "INFO"

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

def get_active_task_ids() -> List[str]:
    return [
        task_id
        for task_id, task in current_tasks.items()
        if task.get("status") in {"pending", "running"}
    ]

def remove_sqlite_file(db_path: str) -> bool:
    removed = False
    for path in (db_path, f"{db_path}-wal", f"{db_path}-shm"):
        if os.path.exists(path):
            os.remove(path)
            removed = True
    return removed

def close_runtime_handles() -> None:
    global crawler_instance

    if crawler_instance:
        try:
            if hasattr(crawler_instance, "file_downloader") and crawler_instance.file_downloader:
                if hasattr(crawler_instance.file_downloader, "file_db") and crawler_instance.file_downloader.file_db:
                    crawler_instance.file_downloader.file_db.close()
        except Exception as e:
            print(f"⚠️ 关闭全局文件数据库连接失败: {e}")
        try:
            if hasattr(crawler_instance, "db") and crawler_instance.db:
                crawler_instance.db.close()
        except Exception as e:
            print(f"⚠️ 关闭全局话题数据库连接失败: {e}")
        crawler_instance = None

    for task_id, downloader in list(file_downloader_instances.items()):
        try:
            if hasattr(downloader, "file_db") and downloader.file_db:
                downloader.file_db.close()
        except Exception as e:
            print(f"⚠️ 关闭任务 {task_id} 文件数据库连接失败: {e}")
    file_downloader_instances.clear()

    try:
        sql_singleton = getattr(accounts_sql_module, "_sql_manager_singleton", None)
        if sql_singleton:
            sql_singleton.close()
        accounts_sql_module._sql_manager_singleton = None
    except Exception as e:
        print(f"⚠️ 关闭账号数据库连接失败: {e}")

    try:
        info_singleton = getattr(account_info_module, "_db_singleton", None)
        if info_singleton:
            info_singleton.close()
        account_info_module._db_singleton = None
    except Exception as e:
        print(f"⚠️ 关闭账号信息数据库连接失败: {e}")

    try:
        image_cache_module._cache_managers.clear()
    except Exception as e:
        print(f"⚠️ 清理图片缓存管理器失败: {e}")


def _get_output_dir() -> str:
    return LOCAL_OUTPUT_DIR if os.path.isabs(LOCAL_OUTPUT_DIR) else os.path.join(project_root, LOCAL_OUTPUT_DIR)


def _open_local_topics_db(group_id: str) -> ZSXQDatabase:
    path_manager = get_db_path_manager()
    db_path = path_manager.get_topics_db_path(str(group_id))
    if not os.path.exists(db_path):
        raise HTTPException(status_code=404, detail=f"群组 {group_id} 本地话题数据库不存在")
    return ZSXQDatabase(db_path)


def _open_local_files_db(group_id: str) -> ZSXQFileDatabase:
    path_manager = get_db_path_manager()
    db_path = path_manager.get_files_db_path(str(group_id))
    if not os.path.exists(db_path):
        raise HTTPException(status_code=404, detail=f"群组 {group_id} 本地文件数据库不存在")
    return ZSXQFileDatabase(db_path)


def _is_safe_zip_path(path: str) -> bool:
    normalized = path.replace("\\", "/")
    if not normalized or normalized.startswith("/"):
        return False
    drive, _ = os.path.splitdrive(normalized)
    if drive:
        return False
    parts = [part for part in normalized.split("/") if part]
    return bool(parts) and all(part not in {".", ".."} for part in parts)


def _safe_zip_join(base_dir: str, archive_path: str) -> str:
    if not _is_safe_zip_path(archive_path):
        raise HTTPException(status_code=400, detail=f"压缩包包含非法路径: {archive_path}")
    target = os.path.abspath(os.path.join(base_dir, *archive_path.replace("\\", "/").split("/")))
    base_abs = os.path.abspath(base_dir)
    if target != base_abs and not target.startswith(base_abs + os.sep):
        raise HTTPException(status_code=400, detail=f"压缩包路径越界: {archive_path}")
    return target


def _is_ignored_export_import_path(path: str) -> bool:
    normalized = path.replace("\\", "/").strip("/")
    filename = posixpath.basename(normalized)
    if not filename:
        return False
    if filename.endswith(("-wal", "-shm")):
        return True
    if filename in {"zsxq_config.db", "zsxq_config.db-wal", "zsxq_config.db-shm"}:
        return True
    return False


def _get_directory_size(path: str) -> int:
    total = 0
    if not os.path.exists(path):
        return total
    for root, _, files in os.walk(path):
        for filename in files:
            file_path = os.path.join(root, filename)
            rel_path = os.path.relpath(file_path, path).replace("\\", "/")
            if _is_ignored_export_import_path(rel_path):
                continue
            try:
                if os.path.isfile(file_path):
                    total += os.path.getsize(file_path)
            except OSError:
                continue
    return total


def _load_group_meta_from_file(group_dir: str) -> Dict[str, Any]:
    meta_path = os.path.join(group_dir, "group_meta.json")
    if not os.path.exists(meta_path):
        return {}
    try:
        with open(meta_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _safe_int_value(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _normalize_group_statistics(statistics: Any, *, topics_count: Optional[int] = None,
                                users_count: Optional[int] = None,
                                files_count: Optional[int] = None) -> Dict[str, Any]:
    normalized: Dict[str, Any] = dict(statistics) if isinstance(statistics, dict) else {}
    members = normalized.get("members") if isinstance(normalized.get("members"), dict) else {}
    topics = normalized.get("topics") if isinstance(normalized.get("topics"), dict) else {}
    files = normalized.get("files") if isinstance(normalized.get("files"), dict) else {}

    member_count = members.get("count", normalized.get("members_count"))
    topic_count = topics.get("topics_count", normalized.get("topics_count"))
    answer_count = topics.get("answers_count", normalized.get("answers_count"))
    digest_count = topics.get("digests_count", normalized.get("digests_count"))
    file_count = files.get("count", normalized.get("files_count"))

    if topics_count is not None and not topic_count:
        topic_count = topics_count
    if users_count is not None and not member_count:
        member_count = users_count
    if files_count is not None and not file_count:
        file_count = files_count

    normalized["members"] = {
        **members,
        "count": _safe_int_value(member_count),
    }
    normalized["topics"] = {
        **topics,
        "topics_count": _safe_int_value(topic_count),
        "answers_count": _safe_int_value(answer_count),
        "digests_count": _safe_int_value(digest_count),
    }
    normalized["files"] = {
        **files,
        "count": _safe_int_value(file_count),
    }
    normalized["members_count"] = normalized["members"]["count"]
    normalized["topics_count"] = normalized["topics"]["topics_count"]
    normalized["files_count"] = normalized["files"]["count"]
    return normalized


def _merge_group_statistics(*statistics_sources: Any) -> Dict[str, Any]:
    """合并多来源统计，保留每个字段里可信的最大非零值。"""
    merged = _normalize_group_statistics({})
    for source in statistics_sources:
        if not isinstance(source, dict):
            continue
        normalized = _normalize_group_statistics(source)
        merged["members"]["count"] = max(
            _safe_int_value(merged.get("members", {}).get("count")),
            _safe_int_value(normalized.get("members", {}).get("count")),
        )
        merged["topics"]["topics_count"] = max(
            _safe_int_value(merged.get("topics", {}).get("topics_count")),
            _safe_int_value(normalized.get("topics", {}).get("topics_count")),
        )
        merged["topics"]["answers_count"] = max(
            _safe_int_value(merged.get("topics", {}).get("answers_count")),
            _safe_int_value(normalized.get("topics", {}).get("answers_count")),
        )
        merged["topics"]["digests_count"] = max(
            _safe_int_value(merged.get("topics", {}).get("digests_count")),
            _safe_int_value(normalized.get("topics", {}).get("digests_count")),
        )
        merged["files"]["count"] = max(
            _safe_int_value(merged.get("files", {}).get("count")),
            _safe_int_value(normalized.get("files", {}).get("count")),
        )

    merged["members_count"] = merged["members"]["count"]
    merged["topics_count"] = merged["topics"]["topics_count"]
    merged["files_count"] = merged["files"]["count"]
    return merged


def _apply_local_package_counts(statistics: Any, local_statistics: Any) -> Dict[str, Any]:
    """
    导入/导出包中的话题数、文件数应代表包内本地数据库的真实数据量；
    成员数仍优先使用官方/元数据中的社群成员数，本地库仅作为兜底。
    """
    combined = _merge_group_statistics(statistics)
    local = _normalize_group_statistics(local_statistics or {})
    local_topics_count = _safe_int_value(local.get("topics", {}).get("topics_count"))
    local_files_count = _safe_int_value(local.get("files", {}).get("count"))
    local_members_count = _safe_int_value(local.get("members", {}).get("count"))

    if local_topics_count > 0:
        combined["topics"]["topics_count"] = local_topics_count
    if local_files_count > 0:
        combined["files"]["count"] = local_files_count
    if not _safe_int_value(combined.get("members", {}).get("count")) and local_members_count > 0:
        combined["members"]["count"] = local_members_count

    combined["members_count"] = combined["members"]["count"]
    combined["topics_count"] = combined["topics"]["topics_count"]
    combined["files_count"] = combined["files"]["count"]
    return combined


def _sqlite_count(cursor: sqlite3.Cursor, query: str, params: tuple = ()) -> int:
    try:
        cursor.execute(query, params)
        row = cursor.fetchone()
        return _safe_int_value(row[0] if row else 0)
    except Exception:
        return 0


def _merge_group_meta(base: Dict[str, Any], extra: Dict[str, Any], *, override: bool = False) -> Dict[str, Any]:
    if not isinstance(extra, dict):
        return base
    for key, value in extra.items():
        if value in (None, "", {}, []):
            continue
        if override or base.get(key) in (None, "", {}, []):
            base[key] = value
    return base


def _load_group_meta_from_db(group_id: str, group_dir: str) -> Dict[str, Any]:
    topics_db = os.path.join(group_dir, f"zsxq_topics_{group_id}.db")
    files_db = os.path.join(group_dir, f"zsxq_files_{group_id}.db")
    meta: Dict[str, Any] = {}
    topics_count = 0
    users_count = 0
    files_count = 0
    topic_files_count = 0
    answers_count = 0
    digests_count = 0
    conn = None
    if os.path.exists(topics_db):
        try:
            conn = sqlite3.connect(topics_db)
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT name, type, background_url FROM groups WHERE CAST(group_id AS TEXT) = ? LIMIT 1", (str(group_id),))
                row = cursor.fetchone()
                if row:
                    meta.update({
                        "name": row[0],
                        "type": row[1],
                        "background_url": row[2],
                    })
            except Exception:
                pass

            topics_count = _sqlite_count(
                cursor,
                "SELECT COUNT(*) FROM topics WHERE CAST(group_id AS TEXT) = ?",
                (str(group_id),),
            )
            if topics_count == 0:
                topics_count = _sqlite_count(cursor, "SELECT COUNT(*) FROM topics")

            users_count = _sqlite_count(
                cursor,
                """
                SELECT COUNT(DISTINCT t.owner_user_id)
                FROM talks t
                JOIN topics tp ON t.topic_id = tp.topic_id
                WHERE CAST(tp.group_id AS TEXT) = ?
                """,
                (str(group_id),),
            )
            if users_count == 0:
                users_count = _sqlite_count(cursor, "SELECT COUNT(DISTINCT owner_user_id) FROM talks")

            answers_count = _sqlite_count(
                cursor,
                "SELECT COUNT(*) FROM topics WHERE CAST(group_id AS TEXT) = ? AND answered = 1",
                (str(group_id),),
            )
            digests_count = _sqlite_count(
                cursor,
                "SELECT COUNT(*) FROM topics WHERE CAST(group_id AS TEXT) = ? AND digested = 1",
                (str(group_id),),
            )
            topic_files_count = _sqlite_count(cursor, "SELECT COUNT(*) FROM topic_files")
        except Exception:
            pass
        finally:
            if conn:
                conn.close()
    if os.path.exists(files_db):
        fconn = None
        try:
            fconn = sqlite3.connect(files_db)
            fcursor = fconn.cursor()
            fcursor.execute("SELECT COUNT(*) FROM files")
            files_count = fcursor.fetchone()[0] or 0
        except Exception:
            files_count = 0
        finally:
            if fconn:
                fconn.close()
    if files_count == 0 and topic_files_count > 0:
        files_count = topic_files_count
    meta["statistics"] = _normalize_group_statistics(
        {
            "topics": {
                "topics_count": topics_count,
                "answers_count": answers_count,
                "digests_count": digests_count,
            }
        },
        topics_count=topics_count,
        users_count=users_count,
        files_count=files_count,
    )
    return meta


def _account_group_to_meta(group: Dict[str, Any]) -> Dict[str, Any]:
    user_specific = group.get("user_specific", {}) or {}
    validity = user_specific.get("validity", {}) or {}
    trial = user_specific.get("trial", {}) or {}
    actual_expiry_time = trial.get("end_time") or validity.get("end_time")
    return {
        "group_id": group.get("group_id"),
        "name": group.get("name", ""),
        "type": group.get("type", ""),
        "background_url": group.get("background_url", ""),
        "owner": group.get("owner", {}) or {},
        "statistics": _normalize_group_statistics(group.get("statistics", {}) or {}),
        "description": group.get("description", ""),
        "create_time": group.get("create_time"),
        "subscription_time": validity.get("begin_time"),
        "expiry_time": actual_expiry_time,
        "join_time": user_specific.get("join_time"),
        "last_active_time": user_specific.get("last_active_time"),
        "status": group.get("status"),
        "is_trial": bool(trial.get("end_time")),
        "trial_end_time": trial.get("end_time"),
        "membership_end_time": validity.get("end_time"),
    }


def _load_account_groups_meta_map() -> Dict[str, Dict[str, Any]]:
    cookies: List[str] = []
    primary_cookie = get_primary_cookie()
    if primary_cookie:
        cookies.append(primary_cookie)
    try:
        sql_mgr = get_accounts_sql_manager()
        for account in sql_mgr.get_accounts(mask_cookie=False) or []:
            cookie = (account.get("cookie") or "").strip()
            if cookie and cookie != "your_cookie_here":
                cookies.append(cookie)
    except Exception:
        pass

    result: Dict[str, Dict[str, Any]] = {}
    seen_cookies = set()
    for cookie in cookies:
        if cookie in seen_cookies:
            continue
        seen_cookies.add(cookie)
        try:
            for group in fetch_groups_from_api(cookie) or []:
                gid = str(group.get("group_id") or "")
                if gid and gid not in result:
                    result[gid] = _account_group_to_meta(group)
        except Exception:
            continue
    return result


def _build_image_data_url(image_path: str) -> str:
    try:
        if not image_path or not os.path.exists(image_path):
            return ""
        if os.path.getsize(image_path) > 3 * 1024 * 1024:
            return ""
        content_type = mimetypes.guess_type(image_path)[0] or "image/jpeg"
        with open(image_path, "rb") as f:
            encoded = base64.b64encode(f.read()).decode("ascii")
        return f"data:{content_type};base64,{encoded}"
    except Exception:
        return ""


def _get_group_cover_image_data_url(group_dir: str, cover_url: str) -> str:
    if not cover_url:
        return ""
    try:
        cache_manager = image_cache_module.ImageCacheManager(os.path.join(group_dir, "images"))
        success, path, _ = cache_manager.download_and_cache(cover_url, timeout=8)
        if success and path:
            return _build_image_data_url(str(path))
    except Exception:
        return ""
    return ""


def _build_group_manifest_entry(group_id: str, group_dir: str,
                                account_meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    db_meta = _load_group_meta_from_db(group_id, group_dir)
    file_meta = _load_group_meta_from_file(group_dir)
    meta: Dict[str, Any] = {}
    _merge_group_meta(meta, db_meta, override=True)
    _merge_group_meta(meta, file_meta, override=True)
    _merge_group_meta(meta, account_meta or {}, override=False)

    meta_statistics = _merge_group_statistics(
        file_meta.get("statistics") if isinstance(file_meta, dict) else {},
        account_meta.get("statistics") if isinstance(account_meta, dict) else {},
    )
    statistics = _apply_local_package_counts(meta_statistics, db_meta.get("statistics") or {})
    background_url = meta.get("background_url") or meta.get("cover_url") or ""
    owner = meta.get("owner") or {}
    topics_count = statistics.get("topics", {}).get("topics_count", 0)
    members_count = statistics.get("members", {}).get("count", 0)
    files_count = statistics.get("files", {}).get("count", 0)
    return {
        "group_id": str(group_id),
        "name": meta.get("name") or f"本地群（{group_id}）",
        "type": meta.get("type") or "local",
        "background_url": background_url,
        "cover_url": background_url,
        "cover_image_data_url": _get_group_cover_image_data_url(group_dir, background_url),
        "owner": owner,
        "statistics": statistics,
        "members_count": members_count,
        "topics_count": topics_count,
        "files_count": files_count,
        "description": meta.get("description") or "",
        "create_time": meta.get("create_time"),
        "subscription_time": meta.get("subscription_time"),
        "join_time": meta.get("join_time"),
        "expiry_time": meta.get("expiry_time"),
        "last_active_time": meta.get("last_active_time"),
        "status": meta.get("status"),
        "is_trial": meta.get("is_trial", False),
        "trial_end_time": meta.get("trial_end_time"),
        "membership_end_time": meta.get("membership_end_time"),
        "directory": os.path.basename(group_dir),
        "size_bytes": _get_directory_size(group_dir),
    }


def _find_group_dirs_under_output(output_dir: str) -> List[Dict[str, str]]:
    results: List[Dict[str, str]] = []
    candidates = [
        output_dir,
        os.path.join(output_dir, "databases"),
    ]
    seen = set()
    for base in candidates:
        if not os.path.isdir(base):
            continue
        for item in os.listdir(base):
            item_path = os.path.join(base, item)
            if item.isdigit() and os.path.isdir(item_path):
                abs_path = os.path.abspath(item_path)
                if abs_path in seen:
                    continue
                seen.add(abs_path)
                results.append({"group_id": item, "path": item_path})
    return results


def _build_export_manifest(export_type: str, source_path: str, archive_root: str,
                           groups: List[Dict[str, str]]) -> Dict[str, Any]:
    exported_at = datetime.now().astimezone().isoformat(timespec="seconds")
    account_meta_map = _load_account_groups_meta_map()
    group_entries = [
        _build_group_manifest_entry(item["group_id"], item["path"], account_meta_map.get(str(item["group_id"])))
        for item in sorted(groups, key=lambda x: x["group_id"])
    ]
    return {
        "manifest_version": 1,
        "app": "ZsxqCrawler",
        "export_type": export_type,
        "exported_at": exported_at,
        "source_root": archive_root,
        "data_size_bytes": _get_directory_size(source_path),
        "groups_count": len(group_entries),
        "groups": group_entries,
    }


def _zip_directory_with_manifest(source_path: str, archive_root: str, manifest: Dict[str, Any]) -> str:
    temp = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
    temp_path = temp.name
    temp.close()
    source_abs = os.path.abspath(source_path)
    with zipfile.ZipFile(temp_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))
        for root, dirs, files in os.walk(source_abs):
            dirs.sort()
            files.sort()
            rel_dir = os.path.relpath(root, source_abs)
            zip_dir = archive_root if rel_dir == "." else posixpath.join(archive_root, rel_dir.replace("\\", "/"))
            if not files and not dirs:
                zf.writestr(zip_dir.rstrip("/") + "/", "")
            for filename in files:
                file_path = os.path.join(root, filename)
                rel_file = os.path.relpath(file_path, source_abs).replace("\\", "/")
                zip_name = posixpath.join(archive_root, rel_file)
                if _is_ignored_export_import_path(zip_name):
                    continue
                zf.write(file_path, zip_name)
    return temp_path


def _find_group_db_entry(zip_entries: List[str], group_id: str, db_kind: str) -> Optional[str]:
    """在导入包中查找指定社群的 topics/files 数据库。"""
    expected_name = f"zsxq_{db_kind}_{group_id}.db"
    matches = [
        name for name in zip_entries
        if name
        and not name.endswith("/")
        and posixpath.basename(name.rstrip("/")) == expected_name
        and not _is_ignored_export_import_path(name)
    ]
    if not matches:
        return None

    group_segment = f"/{group_id}/"
    preferred = [name for name in matches if group_segment in f"/{name}"]
    return sorted(preferred or matches, key=len)[0]


def _load_group_statistics_from_zip(zf: zipfile.ZipFile, zip_entries: List[str], group_id: str) -> Dict[str, Any]:
    """
    旧导出包的 manifest 可能没有正确写入本地话题/文件数量；
    预览时直接读取包内 SQLite 数据库兜底，避免有数据却显示 0。
    """
    topics_entry = _find_group_db_entry(zip_entries, group_id, "topics")
    files_entry = _find_group_db_entry(zip_entries, group_id, "files")
    if not topics_entry and not files_entry:
        return {}

    with tempfile.TemporaryDirectory(prefix="zsxq_import_preview_") as temp_dir:
        group_dir = os.path.join(temp_dir, str(group_id))
        os.makedirs(group_dir, exist_ok=True)
        for db_kind, entry in (("topics", topics_entry), ("files", files_entry)):
            if not entry:
                continue
            target = os.path.join(group_dir, f"zsxq_{db_kind}_{group_id}.db")
            try:
                with zf.open(entry) as src, open(target, "wb") as dst:
                    shutil.copyfileobj(src, dst)
            except Exception:
                continue
        return _load_group_meta_from_db(str(group_id), group_dir).get("statistics", {})


def _sync_group_count_fields(group: Dict[str, Any], statistics: Dict[str, Any]) -> Dict[str, Any]:
    normalized = _normalize_group_statistics(statistics)
    group["statistics"] = normalized
    group["members_count"] = normalized["members"]["count"]
    group["topics_count"] = normalized["topics"]["topics_count"]
    group["files_count"] = normalized["files"]["count"]
    return group


def _enrich_import_manifest_with_archive_counts(archive_bytes: bytes, manifest: Dict[str, Any]) -> Dict[str, Any]:
    groups = manifest.get("groups")
    if not isinstance(groups, list):
        return manifest

    enriched_manifest = dict(manifest)
    enriched_groups: List[Dict[str, Any]] = []
    try:
        with zipfile.ZipFile(io.BytesIO(archive_bytes)) as zf:
            zip_entries = zf.namelist()
            for group in groups:
                if not isinstance(group, dict):
                    enriched_groups.append(group)
                    continue
                enriched_group = dict(group)
                group_id = str(enriched_group.get("group_id") or "")
                local_statistics = _load_group_statistics_from_zip(zf, zip_entries, group_id)
                statistics = _merge_group_statistics(enriched_group.get("statistics") or {}, local_statistics)
                statistics = _apply_local_package_counts(statistics, local_statistics)
                enriched_groups.append(_sync_group_count_fields(enriched_group, statistics))
    except Exception:
        return manifest

    enriched_manifest["groups"] = enriched_groups
    return enriched_manifest


def _parse_import_archive(archive_bytes: bytes) -> Dict[str, Any]:
    try:
        zip_buffer = io.BytesIO(archive_bytes)
        with zipfile.ZipFile(zip_buffer) as zf:
            names = zf.namelist()
            if "manifest.json" not in names:
                raise HTTPException(status_code=400, detail="压缩包缺少根目录 manifest.json")
            for name in names:
                if not _is_safe_zip_path(name.rstrip("/")):
                    raise HTTPException(status_code=400, detail=f"压缩包包含非法路径: {name}")
            try:
                manifest = json.loads(zf.read("manifest.json").decode("utf-8"))
            except Exception:
                raise HTTPException(status_code=400, detail="manifest.json 不是有效的 JSON")
    except zipfile.BadZipFile:
        raise HTTPException(status_code=400, detail="上传文件不是有效的 zip 压缩包")

    if not isinstance(manifest, dict):
        raise HTTPException(status_code=400, detail="manifest.json 格式不正确")
    if manifest.get("app") != "ZsxqCrawler":
        raise HTTPException(status_code=400, detail="manifest.json 不是 ZsxqCrawler 导出包")
    groups = manifest.get("groups")
    if not isinstance(groups, list):
        raise HTTPException(status_code=400, detail="manifest.json 缺少社群信息")

    group_ids = []
    for group in groups:
        if not isinstance(group, dict):
            raise HTTPException(status_code=400, detail="manifest.json 社群信息格式不正确")
        gid = str(group.get("group_id") or "")
        if not gid.isdigit():
            raise HTTPException(status_code=400, detail=f"manifest.json 包含非法社群 ID: {gid}")
        group_ids.append(gid)

    data_entries = [name.rstrip("/") for name in names if name and name != "manifest.json"]
    root_entries = {name.split("/", 1)[0] for name in data_entries if name}
    if manifest.get("export_type") == "all_output":
        if "output" not in root_entries:
            raise HTTPException(status_code=400, detail="全部导出包缺少 output 根目录")
    elif manifest.get("export_type") == "single_group":
        if not group_ids:
            raise HTTPException(status_code=400, detail="单社群导出包缺少社群信息")
        expected_root = str(manifest.get("source_root") or "").strip().replace("\\", "/").strip("/")
        if not expected_root or not _is_safe_zip_path(expected_root):
            raise HTTPException(status_code=400, detail="单社群导出包 manifest.json 缺少有效目录信息")
        if not any(entry == expected_root or entry.startswith(expected_root + "/") for entry in data_entries):
            raise HTTPException(status_code=400, detail="单社群导出包内容与 manifest.json 目录信息不一致")
    else:
        raise HTTPException(status_code=400, detail="manifest.json 包含未知导出类型")

    return {
        "manifest": manifest,
        "group_ids": group_ids,
        "zip_entries": names,
    }


def _get_import_conflicts(group_ids: List[str]) -> List[Dict[str, Any]]:
    output_dir = _get_output_dir()
    path_manager = get_db_path_manager()
    conflicts: List[Dict[str, Any]] = []
    for group_id in group_ids:
        candidate_paths = [
            os.path.join(output_dir, group_id),
            os.path.join(output_dir, "databases", group_id),
            os.path.join(path_manager.base_dir, group_id),
        ]
        existing = []
        for path in candidate_paths:
            abs_path = os.path.abspath(path)
            if abs_path not in existing and os.path.exists(abs_path):
                existing.append(abs_path)
        if existing:
            conflicts.append({"group_id": group_id, "paths": existing})
    return conflicts


def _build_import_preview(archive_bytes: bytes) -> Dict[str, Any]:
    parsed = _parse_import_archive(archive_bytes)
    manifest = _enrich_import_manifest_with_archive_counts(archive_bytes, parsed["manifest"])
    conflicts = _get_import_conflicts(parsed["group_ids"])
    return {
        "success": True,
        "manifest": manifest,
        "groups": manifest.get("groups", []),
        "conflicts": conflicts,
        "can_import": len(conflicts) == 0,
    }


def _extract_import_archive(archive_bytes: bytes) -> Dict[str, Any]:
    preview = _build_import_preview(archive_bytes)
    if preview["conflicts"]:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "导入的社群本地数据已存在，请先删除已有本地数据后再导入",
                "conflicts": preview["conflicts"],
            },
        )

    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as zf:
        for member in zf.infolist():
            name = member.filename
            if name == "manifest.json":
                continue
            if _is_ignored_export_import_path(name):
                continue
            target = _safe_zip_join(project_root, name)
            if member.is_dir():
                os.makedirs(target, exist_ok=True)
                continue
            if os.path.isdir(target):
                raise HTTPException(status_code=400, detail=f"导入路径冲突，目标是目录: {name}")
            os.makedirs(os.path.dirname(target), exist_ok=True)
            with zf.open(member) as src, open(target, "wb") as dst:
                shutil.copyfileobj(src, dst)

    for group in preview["groups"]:
        try:
            group_id = int(str(group.get("group_id")))
            meta = dict(group)
            meta.pop("cover_image_data_url", None)
            if meta.get("cover_url") and not meta.get("background_url"):
                meta["background_url"] = meta.get("cover_url")
            _persist_group_meta_local(group_id, meta)
        except Exception:
            continue

    scan_local_groups()
    return {
        "success": True,
        "message": f"导入成功，共导入 {len(preview['groups'])} 个社群",
        "manifest": preview["manifest"],
        "groups": preview["groups"],
    }


# API路由定义
@app.get("/")
async def root():
    """根路径"""
    return {"message": "知识星球数据采集器 API 服务", "version": "1.0.0"}

@app.get("/api/health")
async def health_check():
    """健康检查"""
    return {"status": "healthy", "timestamp": datetime.now()}


@app.get("/api/groups/{group_id}/export")
async def export_group_folder(group_id: str):
    try:
        if not group_id.isdigit():
            raise HTTPException(status_code=400, detail="社群 ID 格式不正确")

        path_manager = get_db_path_manager()
        candidate_dirs = [
            os.path.join(path_manager.base_dir, group_id),
            os.path.join(_get_output_dir(), group_id),
        ]
        group_dir = next((path for path in candidate_dirs if os.path.isdir(path)), None)
        if not group_dir:
            raise HTTPException(status_code=404, detail=f"社群 {group_id} 本地文件夹不存在")

        archive_root = os.path.relpath(group_dir, project_root).replace("\\", "/")
        if not _is_safe_zip_path(archive_root):
            archive_root = posixpath.join("output", "databases", group_id)
        manifest = _build_export_manifest(
            "single_group",
            group_dir,
            archive_root,
            [{"group_id": group_id, "path": group_dir}],
        )
        zip_path = await asyncio.to_thread(_zip_directory_with_manifest, group_dir, archive_root, manifest)
        filename = f"zsxq_group_{group_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
        return FileResponse(
            zip_path,
            media_type="application/zip",
            filename=filename,
            background=BackgroundTask(lambda: os.path.exists(zip_path) and os.remove(zip_path)),
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"导出社群文件夹失败: {str(e)}")


@app.get("/api/export/all")
async def export_all_output_folder():
    try:
        output_dir = _get_output_dir()
        if not os.path.isdir(output_dir):
            raise HTTPException(status_code=404, detail="output 文件夹不存在")

        archive_root = os.path.relpath(output_dir, project_root).replace("\\", "/")
        if not _is_safe_zip_path(archive_root) or archive_root.startswith(".."):
            archive_root = "output"
        groups = _find_group_dirs_under_output(output_dir)
        manifest = _build_export_manifest("all_output", output_dir, archive_root, groups)
        zip_path = await asyncio.to_thread(_zip_directory_with_manifest, output_dir, archive_root, manifest)
        filename = f"zsxq_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
        return FileResponse(
            zip_path,
            media_type="application/zip",
            filename=filename,
            background=BackgroundTask(lambda: os.path.exists(zip_path) and os.remove(zip_path)),
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"导出全部 output 失败: {str(e)}")


@app.post("/api/import/preview")
async def preview_import_archive(request: Request):
    try:
        archive_bytes = await request.body()
        if not archive_bytes:
            raise HTTPException(status_code=400, detail="请上传 zip 文件")
        return await asyncio.to_thread(_build_import_preview, archive_bytes)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"读取导入清单失败: {str(e)}")


@app.post("/api/import/confirm")
async def confirm_import_archive(request: Request):
    try:
        active_task_ids = get_active_task_ids()
        if active_task_ids:
            raise HTTPException(
                status_code=409,
                detail=f"存在正在运行的任务，请先停止后再导入: {', '.join(active_task_ids)}"
            )
        archive_bytes = await request.body()
        if not archive_bytes:
            raise HTTPException(status_code=400, detail="请上传 zip 文件")
        return await asyncio.to_thread(_extract_import_archive, archive_bytes)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"导入数据失败: {str(e)}")


@app.get("/api/config")
async def get_config():
    """获取当前配置"""
    try:
        config = load_config()
        auth_config = (config or {}).get('auth', {}) if config else {}
        cookie = auth_config.get('cookie', '') if auth_config else ''

        configured = is_configured()

        # 隐藏敏感信息，仅返回配置状态和下载相关配置
        return {
            "configured": configured,
            "auth": {
                "cookie": "***" if cookie and cookie != "your_cookie_here" else "未配置",
            },
            "database": config.get('database', {}) if config else {},
            "download": config.get('download', {}) if config else {}
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取配置失败: {str(e)}")

@app.post("/api/config")
async def update_config(config: ConfigModel):
    """更新配置"""
    try:
        # 创建配置内容
        config_content = f"""# 知识星球数据采集器配置文件
# 通过Web界面自动生成

[auth]
# 知识星球登录Cookie
cookie = "{config.cookie}"

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

@app.delete("/api/local-data/reset")
async def reset_all_local_data():
    try:
        active_task_ids = get_active_task_ids()
        if active_task_ids:
            raise HTTPException(
                status_code=409,
                detail=f"存在正在运行的任务，请先停止后再重置: {', '.join(active_task_ids)}"
            )

        details = {
            "output_dir_removed": False,
            "database_dir_removed": False,
            "cache_dir_removed": False,
            "downloads_dir_removed": False,
            "config_removed": False,
            "env_files_removed": [],
            "accounts_json_reset": False,
            "accounts_json_backup_removed": False,
            "task_state_cleared": False,
        }
        errors = []

        close_runtime_handles()
        gc.collect()
        time.sleep(0.3)

        def remove_dir(key: str, path: str) -> None:
            if not os.path.exists(path):
                return
            try:
                shutil.rmtree(path, ignore_errors=False)
                details[key] = True
            except Exception as exc:
                errors.append({"path": path, "error": str(exc)})

        def remove_file(key: str, path: str) -> None:
            if not os.path.exists(path):
                return
            try:
                os.remove(path)
                details[key] = True
            except Exception as exc:
                errors.append({"path": path, "error": str(exc)})

        path_manager = get_db_path_manager()
        output_dir = LOCAL_OUTPUT_DIR if os.path.isabs(LOCAL_OUTPUT_DIR) else os.path.join(project_root, LOCAL_OUTPUT_DIR)
        database_root = (
            os.path.dirname(path_manager.base_dir)
            if os.path.basename(path_manager.base_dir) == "databases"
            else path_manager.base_dir
        )

        remove_dir("output_dir_removed", output_dir)
        if os.path.abspath(database_root) != os.path.abspath(output_dir):
            remove_dir("database_dir_removed", database_root)

        remove_dir("cache_dir_removed", os.path.join(project_root, "cache"))
        remove_dir("downloads_dir_removed", os.path.join(project_root, "downloads"))
        remove_file("config_removed", os.path.join(project_root, "config.toml"))

        for env_path in (
            os.path.join(project_root, ".env"),
            os.path.join(project_root, ".env.local"),
            os.path.join(project_root, "frontend", ".env"),
            os.path.join(project_root, "frontend", ".env.local"),
            os.path.join(project_root, "frontend", ".env.development.local"),
            os.path.join(project_root, "frontend", ".env.production.local"),
        ):
            if not os.path.exists(env_path):
                continue
            try:
                os.remove(env_path)
                details["env_files_removed"].append(env_path)
            except Exception as exc:
                errors.append({"path": env_path, "error": str(exc)})

        accounts_path = os.path.join(project_root, "accounts.json")
        try:
            with open(accounts_path, "w", encoding="utf-8") as f:
                json.dump({"accounts": [], "group_account_map": {}}, f, ensure_ascii=False, indent=2)
            details["accounts_json_reset"] = True
        except Exception as exc:
            errors.append({"path": accounts_path, "error": str(exc)})

        remove_file("accounts_json_backup_removed", accounts_path + ".backup")

        _local_groups_cache["ids"] = set()
        _local_groups_cache["scanned_at"] = time.time()
        _account_detect_cache["built_at"] = 0
        _account_detect_cache["group_to_account"] = {}
        _account_detect_cache["cookie_by_account"] = {}

        global task_counter
        current_tasks.clear()
        task_logs.clear()
        task_stop_flags.clear()
        sse_connections.clear()
        task_counter = 0
        details["task_state_cleared"] = True

        success = len(errors) == 0
        return {
            "success": success,
            "message": "已重置为初始状态" if success else "重置部分完成，部分文件删除失败",
            "details": details,
            "errors": errors,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"重置全部本地数据失败: {str(e)}")

# 账号管理 API
@app.get("/api/accounts")
async def list_accounts():
    """获取所有账号列表"""
    try:
        sql_mgr = get_accounts_sql_manager()
        accounts = sql_mgr.get_accounts(mask_cookie=True)
        return {"accounts": accounts}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve account list: {str(e)}")

@app.post("/api/accounts")
async def create_account(request: AccountCreateRequest):
    """创建新账号"""
    try:
        sql_mgr = get_accounts_sql_manager()
        acc = sql_mgr.add_account(request.cookie, request.name)
        new_id = acc.get("id")
        safe_acc = sql_mgr.get_account_by_id(new_id, mask_cookie=True)
        # 清除账号群组检测缓存，使新账号的群组立即可见
        clear_account_detect_cache()

        # 后台异步抓取该账号的 self 信息并落库（不阻塞响应）
        if new_id and request.cookie:
            try:
                import threading
                threading.Thread(
                    target=_fetch_and_store_account_self,
                    args=(new_id, request.cookie),
                    daemon=True,
                ).start()
            except Exception as e:
                print(f"⚠️ 启动后台抓取账号 self 信息失败: {e}")

        return {"account": safe_acc}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create account: {str(e)}")

@app.delete("/api/accounts/{account_id}")
async def remove_account(account_id: str):
    """删除账号"""
    try:
        sql_mgr = get_accounts_sql_manager()
        ok = sql_mgr.delete_account(account_id)
        if not ok:
            raise HTTPException(status_code=404, detail="Account does not exist")
        # 清除账号群组检测缓存
        clear_account_detect_cache()
        return {"success": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete account: {str(e)}")

@app.post("/api/groups/{group_id}/assign-account")
async def assign_account_to_group(group_id: str, request: AssignGroupAccountRequest):
    """分配群组到指定账号"""
    try:
        sql_mgr = get_accounts_sql_manager()
        ok, msg = sql_mgr.assign_group_account(group_id, request.account_id)
        if not ok:
            raise HTTPException(status_code=400, detail=msg)
        return {"success": True, "message": msg}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to assign account: {str(e)}")

@app.get("/api/groups/{group_id}/account")
async def get_group_account(group_id: str):
    try:
        summary = get_account_summary_for_group_auto(group_id)
        return {"account": summary}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取群组账号失败: {str(e)}")

# 账号“自我信息”持久化 (/v3/users/self)
def _fetch_and_store_account_self(account_id: str, cookie: str) -> Optional[Dict[str, Any]]:
    """同步抓取 /v3/users/self 并写入数据库；失败时返回 None。供后台线程或 to_thread 调用。"""
    try:
        if not cookie:
            return None
        headers = build_stealth_headers(cookie)
        resp = requests.get('https://api.zsxq.com/v3/users/self', headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if not data.get('succeeded'):
            return None

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
        return db.get_self_info(account_id)
    except Exception as e:
        print(f"⚠️ 抓取账号 {account_id} self 信息失败: {e}")
        return None


@app.get("/api/accounts/{account_id}/self")
async def get_account_self(account_id: str):
    """仅返回数据库中已持久化的账号自我信息；若无则返回 None（请前端调用 refresh 主动刷新）。

    注意：此端点必须保持只读且快速，否则会阻塞 FastAPI 事件循环并拖慢账号管理页切换。
    """
    try:
        db = get_account_info_db()
        info = db.get_self_info(account_id)
        return {"self": info}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve account info: {str(e)}")

@app.post("/api/accounts/{account_id}/self/refresh")
async def refresh_account_self(account_id: str):
    """强制抓取 /v3/users/self 并更新持久化（在线程池执行，不阻塞事件循环）"""
    try:
        sql_mgr = get_accounts_sql_manager()
        acc = sql_mgr.get_account_by_id(account_id, mask_cookie=False)
        if not acc:
            raise HTTPException(status_code=404, detail="Account does not exist")

        cookie = acc.get("cookie", "")
        if not cookie:
            raise HTTPException(status_code=400, detail="Account has no configured Cookie")

        info = await asyncio.to_thread(_fetch_and_store_account_self, account_id, cookie)
        if info is None:
            raise HTTPException(status_code=502, detail="抓取或解析 /v3/users/self 失败")
        return {"self": info}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to refresh account info: {str(e)}")

@app.get("/api/groups/{group_id}/self")
async def get_group_account_self(group_id: str):
    """仅返回群组当前使用账号的已持久化自我信息；若无则返回 None。

    注意：此端点只读，不会主动抓取 zsxq API，避免阻塞事件循环。
    """
    try:
        summary = get_account_summary_for_group_auto(group_id)
        account_id = (summary or {}).get('id', 'default')
        db = get_account_info_db()
        info = db.get_self_info(account_id)
        return {"self": info}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取群组账号信息失败: {str(e)}")

@app.post("/api/groups/{group_id}/self/refresh")
async def refresh_group_account_self(group_id: str):
    """强制抓取群组当前使用账号的自我信息并持久化（在线程池执行，不阻塞事件循环）"""
    try:
        summary = get_account_summary_for_group_auto(group_id)
        cookie = get_cookie_for_group(group_id)
        account_id = (summary or {}).get('id', 'default')

        if not cookie:
            raise HTTPException(status_code=400, detail="未找到可用Cookie，请先配置账号或默认Cookie")

        info = await asyncio.to_thread(_fetch_and_store_account_self, account_id, cookie)
        if info is None:
            raise HTTPException(status_code=502, detail="抓取或解析 /v3/users/self 失败")
        return {"self": info}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"刷新群组账号信息失败: {str(e)}")

@app.get("/api/database/stats")
async def get_database_stats():
    """获取数据库统计信息"""
    try:
        configured = is_configured()
        path_manager = get_db_path_manager()
        groups_info = path_manager.list_all_groups()
        has_local_data = bool(groups_info)
        if not configured and not has_local_data:
            return {
                "configured": False,
                "has_local_data": False,
                "topic_database": {
                    "stats": {},
                    "timestamp_info": {
                        "total_topics": 0,
                        "oldest_timestamp": "",
                        "newest_timestamp": "",
                        "has_data": False,
                    },
                },
                "file_database": {
                    "stats": {},
                },
            }

        # 聚合所有本地群组的数据库统计信息

        if not groups_info:
            # 已配置但尚未产生本地数据
            return {
                "configured": True,
                "has_local_data": False,
                "topic_database": {
                    "stats": {},
                    "timestamp_info": {
                        "total_topics": 0,
                        "oldest_timestamp": "",
                        "newest_timestamp": "",
                        "has_data": False,
                    },
                },
                "file_database": {
                    "stats": {},
                },
            }

        aggregated_topic_stats: Dict[str, int] = {}
        aggregated_file_stats: Dict[str, int] = {}

        oldest_ts: Optional[str] = None
        newest_ts: Optional[str] = None
        total_topics = 0
        has_data = False

        for gi in groups_info:
            group_id = gi.get("group_id")
            topics_db_path = gi.get("topics_db")
            if not topics_db_path:
                continue

            # 话题数据库统计
            db = ZSXQDatabase(topics_db_path)
            try:
                topic_stats = db.get_database_stats()
                ts_info = db.get_timestamp_range_info()
            finally:
                db.close()

            for table, count in (topic_stats or {}).items():
                aggregated_topic_stats[table] = aggregated_topic_stats.get(table, 0) + int(count or 0)

            if ts_info.get("has_data"):
                has_data = True
                ot = ts_info.get("oldest_timestamp")
                nt = ts_info.get("newest_timestamp")
                if ot:
                    if oldest_ts is None or ot < oldest_ts:
                        oldest_ts = ot
                if nt:
                    if newest_ts is None or nt > newest_ts:
                        newest_ts = nt
                total_topics += int(ts_info.get("total_topics") or 0)

            # 文件数据库统计（如存在）
            db_paths = path_manager.list_group_databases(str(group_id))
            files_db_path = db_paths.get("files")
            if files_db_path:
                fdb = ZSXQFileDatabase(files_db_path)
                try:
                    file_stats = fdb.get_database_stats()
                finally:
                    fdb.close()

                for table, count in (file_stats or {}).items():
                    aggregated_file_stats[table] = aggregated_file_stats.get(table, 0) + int(count or 0)

        timestamp_info = {
            "total_topics": total_topics,
            "oldest_timestamp": oldest_ts or "",
            "newest_timestamp": newest_ts or "",
            "has_data": has_data,
        }

        return {
            "configured": configured,
            "has_local_data": True,
            "topic_database": {
                "stats": aggregated_topic_stats,
                "timestamp_info": timestamp_info,
            },
            "file_database": {
                "stats": aggregated_file_stats,
            },
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
        from .zsxq_file_downloader import ZSXQFileDownloader
        from .db_path_manager import get_db_path_manager

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
            result = downloader.download_files_from_database(max_files=max_files, status_filter='pending',order_by='download_count DESC')
        else:
            result = downloader.download_files_from_database(max_files=max_files, status_filter='pending',order_by='create_time DESC')

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

        from .zsxq_file_downloader import ZSXQFileDownloader
        from .db_path_manager import get_db_path_manager

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

        from .zsxq_file_downloader import ZSXQFileDownloader
        from .db_path_manager import get_db_path_manager

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
                cookie = get_cookie_for_group(group_id)

                from .zsxq_file_downloader import ZSXQFileDownloader
                from .db_path_manager import get_db_path_manager

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
    file_db = None
    try:
        file_db = _open_local_files_db(group_id)

        # 查询文件信息
        file_db.cursor.execute('''
            SELECT name, size, download_status
            FROM files
            WHERE file_id = ?
        ''', (file_id,))

        result = file_db.cursor.fetchone()

        if not result:
            # 文件不在数据库中，检查是否有同名文件在下载目录
            import os

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

        path_manager = get_db_path_manager()
        download_dir = os.path.join(path_manager.get_group_dir(group_id), "downloads")
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
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取文件状态失败: {str(e)}")
    finally:
        if file_db:
            file_db.close()

@app.get("/api/files/check-local/{group_id}")
async def check_local_file_status(group_id: str, file_name: str, file_size: int):
    """检查本地文件状态（不依赖数据库）"""
    try:
        # 清理文件名
        import os
        safe_filename = "".join(c for c in file_name if c.isalnum() or c in '._-（）()[]{}')
        if not safe_filename:
            safe_filename = file_name

        path_manager = get_db_path_manager()
        download_dir = os.path.join(path_manager.get_group_dir(group_id), "downloads")
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
    file_db = None
    try:
        file_db = _open_local_files_db(group_id)

        # 获取文件数据库统计
        stats = file_db.get_database_stats()

        # 获取下载状态统计
        # 首先检查是否有download_status列
        file_db.cursor.execute("PRAGMA table_info(files)")
        columns = [col[1] for col in file_db.cursor.fetchall()]

        if 'download_status' in columns:
            # 新版本数据库，有download_status列
            file_db.cursor.execute("""
                SELECT
                    COUNT(*) as total_files,
                    COUNT(CASE WHEN download_status = 'completed' THEN 1 END) as downloaded,
                    COUNT(CASE WHEN download_status = 'pending' THEN 1 END) as pending,
                    COUNT(CASE WHEN download_status = 'failed' THEN 1 END) as failed
                FROM files
            """)
            download_stats = file_db.cursor.fetchone()
        else:
            # 旧版本数据库，没有download_status列，只统计总数
            file_db.cursor.execute("SELECT COUNT(*) FROM files")
            total_files = file_db.cursor.fetchone()[0]
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
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取文件统计失败: {str(e)}")
    finally:
        if file_db:
            file_db.close()

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
                    from .image_cache_manager import get_image_cache_manager, clear_group_cache_manager
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
                    from .image_cache_manager import get_image_cache_manager, clear_group_cache_manager
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
    file_db = None
    try:
        file_db = _open_local_files_db(group_id)

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

        file_db.cursor.execute(query, params)
        files = file_db.cursor.fetchall()

        # 获取总数
        if status:
            file_db.cursor.execute("SELECT COUNT(*) FROM files WHERE download_status = ?", (status,))
        else:
            file_db.cursor.execute("SELECT COUNT(*) FROM files")
        total = file_db.cursor.fetchone()[0]

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
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取文件列表失败: {str(e)}")
    finally:
        if file_db:
            file_db.close()

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

def _persist_group_meta_local(group_id: int, info: Dict[str, Any]):
    """
    将群组的封面、名称、群主与时间等元信息持久化到本地目录。
    这样即使后续账号 Cookie 失效，仅保留本地数据时，也能展示完整信息。
    """
    try:
        from pathlib import Path

        path_manager = get_db_path_manager()
        group_dir = path_manager.get_group_data_dir(str(group_id))
        meta_path = Path(group_dir) / "group_meta.json"

        meta = {
            "group_id": group_id,
            "name": info.get("name") or f"本地群（{group_id}）",
            "type": info.get("type", ""),
            "background_url": info.get("background_url", ""),
            "owner": info.get("owner", {}) or {},
            "statistics": info.get("statistics", {}) or {},
            "create_time": info.get("create_time"),
            "subscription_time": info.get("subscription_time"),
            "expiry_time": info.get("expiry_time"),
            "join_time": info.get("join_time"),
            "last_active_time": info.get("last_active_time"),
            "description": info.get("description", ""),
            "is_trial": info.get("is_trial", False),
            "trial_end_time": info.get("trial_end_time"),
            "membership_end_time": info.get("membership_end_time"),
        }

        with meta_path.open("w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"⚠️ 写入本地群组元数据失败: {e}")


@app.get("/api/groups")
async def get_groups():
    """获取群组列表：账号群 ∪ 本地目录群（去重合并）"""
    try:
        # 自动构建群组→账号映射（多账号支持）
        group_account_map = build_account_group_detection()
        local_ids = get_cached_local_group_ids(force_refresh=False)

        # 获取“当前账号”的群列表（优先账号默认账号，其次config.toml；若未配置则视为空集合）
        groups_data: List[dict] = []
        try:
            primary_cookie = get_primary_cookie()
            if primary_cookie:
                groups_data = fetch_groups_from_api(primary_cookie)
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
                # 标注来源为 account|local，并持久化一份元信息到本地
                src = by_id[gid_int].get("source", "account")
                if "local" not in src:
                    by_id[gid_int]["source"] = "account|local"
                _persist_group_meta_local(gid_int, by_id[gid_int])
            else:
                # 仅存在于本地：优先从 group_meta.json 读取元信息，其次从本地数据库补全
                local_name = f"本地群（{gid_int}）"
                local_type = "local"
                local_bg = ""
                owner: Dict[str, Any] = {}
                join_time = None
                expiry_time = None
                last_active_time = None
                description = ""
                statistics: Dict[str, Any] = {}

                # 1. 优先读取本地元数据文件（如果之前有账号+本地时已经落盘）
                try:
                    from pathlib import Path

                    path_manager = get_db_path_manager()
                    group_dir = path_manager.get_group_data_dir(str(gid_int))
                    meta_path = Path(group_dir) / "group_meta.json"
                    if meta_path.exists():
                        with meta_path.open("r", encoding="utf-8") as f:
                            meta = json.load(f)
                        local_name = meta.get("name", local_name)
                        local_type = meta.get("type", local_type)
                        local_bg = meta.get("background_url", local_bg)
                        owner = meta.get("owner", {}) or owner
                        statistics = meta.get("statistics", {}) or statistics
                        join_time = meta.get("join_time", join_time)
                        expiry_time = meta.get("expiry_time", expiry_time)
                        last_active_time = meta.get("last_active_time", last_active_time)
                        description = meta.get("description", description)
                except Exception as e:
                    print(f"⚠️ 读取本地群组 {gid_int} 元数据文件失败: {e}")

                # 2. 若元数据文件中仍缺少信息，再从本地数据库补充
                try:
                    path_manager = get_db_path_manager()
                    db_paths = path_manager.list_group_databases(str(gid_int))
                    topics_db = db_paths.get("topics")
                    if topics_db and os.path.exists(topics_db):
                        db = ZSXQDatabase(topics_db)
                        try:
                            cur = db.cursor
                            # 群组基础信息
                            if not local_bg or local_name.startswith("本地群（"):
                                cur.execute(
                                    "SELECT name, type, background_url FROM groups WHERE group_id = ? LIMIT 1",
                                    (gid_int,),
                                )
                                row = cur.fetchone()
                                if row:
                                    if row[0]:
                                        local_name = row[0]
                                    if row[1]:
                                        local_type = row[1]
                                    if row[2]:
                                        local_bg = row[2]

                            # 本地数据时间范围（以话题时间替代“加入/过期时间”的近似）
                            if not join_time or not expiry_time:
                                cur.execute(
                                    """
                                    SELECT MIN(create_time), MAX(create_time)
                                    FROM topics
                                    WHERE group_id = ? AND create_time IS NOT NULL AND create_time != ''
                                    """,
                                    (gid_int,),
                                )
                                trow = cur.fetchone()
                                if trow:
                                    if not join_time:
                                        join_time = trow[0]
                                    if not expiry_time:
                                        expiry_time = trow[1]
                                    if not last_active_time:
                                        last_active_time = trow[1]

                            # 简单统计：话题数量
                            if not statistics:
                                cur.execute(
                                    "SELECT COUNT(*) FROM topics WHERE group_id = ?",
                                    (gid_int,),
                                )
                                topics_count = cur.fetchone()[0] or 0
                                statistics = {
                                    "topics": {
                                        "topics_count": topics_count,
                                        "answers_count": 0,
                                        "digests_count": 0,
                                    }
                                }
                        finally:
                            db.close()
                except Exception as e:
                    # 出错时降级为占位信息，不中断整个接口
                    print(f"⚠️ 读取本地群组 {gid_int} 元数据失败: {e}")

                by_id[gid_int] = {
                    "group_id": gid_int,
                    "name": local_name,
                    "type": local_type,
                    "background_url": local_bg,
                    "owner": owner,
                    "statistics": statistics,
                    "status": None,
                    "create_time": join_time,
                    "subscription_time": None,
                    "expiry_time": expiry_time,
                    "join_time": join_time,
                    "last_active_time": last_active_time,
                    "description": description,
                    "is_trial": False,
                    "trial_end_time": None,
                    "membership_end_time": None,
                    "account": None,
                    "source": "local",
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
    """获取话题详情（仅从本地数据库读取，不主动爬取）

    注意：
    - 如果本地 topics 表中不存在该 topic_id，会返回 404；
    - 不会调用知识星球官方 API 拉取最新数据，如需补采请调用
      POST /api/topics/fetch-single/{group_id}/{topic_id}。
    """
    try:
        db = _open_local_topics_db(group_id)
        try:
            topic_detail = db.get_topic_detail(topic_id)
        finally:
            db.close()

        if not topic_detail:
            # 业务上这是一个“正常”的不存在场景，直接向外抛 404，
            # 避免被下面的通用异常包装成 500。
            raise HTTPException(status_code=404, detail="话题不存在")

        return topic_detail
    except HTTPException:
        # 保留原有的状态码（例如上面的 404）
        raise
    except Exception as e:
        # 只有真正的非 HTTPException 异常才包装为 500
        raise HTTPException(status_code=500, detail=f"获取话题详情失败: {str(e)}")

def _download_markdown_response(markdown: str, filename_stem: str):
    """Build a Markdown file download response."""
    file_path = write_temp_markdown_file(markdown, filename_stem)
    filename = f"{safe_filename(filename_stem)}.md"
    return FileResponse(
        file_path,
        media_type="text/markdown; charset=utf-8",
        filename=filename,
    )


def _topic_image_downloader(group_id: str):
    """构建用于 ZIP 归档的图片下载回调。

    优先使用 image_cache_manager 的本地缓存（避免重复下载）；缓存未命中时主动下载。
    返回的 callable 接收 url，返回本地图片路径或 None。
    """
    cache_manager = get_image_cache_manager(group_id)

    def _download(url: str):
        if not url:
            return None
        try:
            success, path, _err = cache_manager.download_and_cache(url, timeout=20)
            if success and path:
                return path
        except Exception as e:
            log_warning(f"导出 ZIP 时下载图片失败: url={url}, error={e}")
        return None

    return _download


def _download_topic_archive_response(detail: Dict[str, Any], filename_stem: str,
                                      *, group_id: str,
                                      render=topic_detail_to_markdown,
                                      render_kwargs: Optional[Dict[str, Any]] = None):
    """Build a ZIP download response containing Markdown + assets/."""
    file_path = write_temp_topic_archive(
        detail,
        filename_stem,
        render=render,
        render_kwargs=render_kwargs,
        image_downloader=_topic_image_downloader(group_id),
    )
    filename = f"{safe_filename(filename_stem)}.zip"
    return FileResponse(
        file_path,
        media_type="application/zip",
        filename=filename,
    )


def _fetch_article_markdown(article_url: str, headers: Dict[str, str], fallback_title: str) -> str:
    """Fetch article_url and convert the returned HTML to Markdown when possible."""
    if not article_url:
        return ""

    parsed = urlparse(article_url)
    if parsed.scheme not in {"http", "https"}:
        return ""

    try:
        response = requests.get(article_url, headers=headers, timeout=30, allow_redirects=True)
        response.raise_for_status()
        content_type = response.headers.get("content-type", "")
        if "json" in content_type:
            return ""
        response.encoding = response.encoding or "utf-8"
        return article_html_to_markdown(response.text, fallback_title=fallback_title)
    except Exception as e:
        log_warning(f"Failed to export article_url as Markdown: url={article_url}, error={e}")
        return ""


@app.get("/api/topics/{topic_id}/{group_id}/export-md")
async def export_topic_markdown(topic_id: int, group_id: str,
                                fetch_article: bool = True,
                                format: str = "zip"):
    """导出话题为 Markdown 单文件或包含资源的 ZIP 归档。

    - format=zip（默认）：返回 README.md + assets/ 目录的 zip 包，含头像与图片，离线可读
    - format=md：返回单个 .md 文件（图片用远程 URL）
    """
    try:
        crawler = get_crawler_for_group(group_id)
        topic_detail = crawler.db.get_topic_detail(topic_id)

        if not topic_detail:
            raise HTTPException(status_code=404, detail="Topic not found")

        article = ((topic_detail.get("talk") or {}).get("article") or {})
        article_url = article.get("article_url") or article.get("inline_article_url") or ""
        title = topic_detail.get("title") or article.get("title") or f"topic_{topic_id}"

        # 仅当导出格式为 md 且明确请求拉取外部文章时，才尝试外站抓取
        external_article_md = ""
        if format == "md" and fetch_article and article_url:
            external_article_md = await asyncio.to_thread(
                _fetch_article_markdown,
                article_url,
                crawler.get_stealth_headers(),
                title,
            )

        if format == "zip":
            return _download_topic_archive_response(
                topic_detail,
                f"{topic_id}_{title}",
                group_id=group_id,
                render=topic_detail_to_markdown,
                render_kwargs={"source_url": article_url or None},
            )

        # 单 .md 模式
        if external_article_md:
            markdown = external_article_md.rstrip() + (
                f"\n\n---\n\nSource: [{article.get('title') or article_url}]({article_url})\n"
            )
        else:
            markdown = topic_detail_to_markdown(topic_detail, source_url=article_url or None)

        return _download_markdown_response(markdown, f"{topic_id}_{title}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export Markdown failed: {str(e)}")


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
    """手动获取话题的更多评论（在已存在本地话题记录的前提下）"""
    try:
        crawler = get_crawler_for_group(group_id)

        # 先获取话题基本信息（仅查本地）
        topic_detail = crawler.db.get_topic_detail(topic_id)
        if not topic_detail:
            # 同样这属于业务层面的“话题未采集”，直接返回 404
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

    except HTTPException:
        # 保留显式抛出的业务错误（例如 404）
        raise
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
        db = _open_local_topics_db(group_id)
        try:
            tags = db.get_tags_by_group(int(group_id))
        finally:
            db.close()
        
        return {
            "tags": tags,
            "total": len(tags)
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取标签列表失败: {str(e)}")

@app.get("/api/groups/{group_id}/tags/{tag_id}/topics")
async def get_topics_by_tag(group_id: int, tag_id: int, page: int = 1, per_page: int = 20):
    """根据标签获取指定群组的话题列表"""
    try:
        db = _open_local_topics_db(str(group_id))
        try:
            db.cursor.execute('SELECT COUNT(*) FROM tags WHERE tag_id = ? AND group_id = ?', (tag_id, group_id))
            tag_count = db.cursor.fetchone()[0]
            
            if tag_count == 0:
                raise HTTPException(status_code=404, detail="标签在该群组中不存在")

            result = db.get_topics_by_tag(tag_id, page, per_page)
        finally:
            db.close()
        
        return result
    except HTTPException:
        raise
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


@app.get("/api/groups/{group_id}/images/{image_path:path}")
async def get_local_image(group_id: str, image_path: str):
    """获取群组本地缓存的图片"""
    from pathlib import Path
    
    try:
        path_manager = get_db_path_manager()
        group_dir = path_manager.get_group_data_dir(group_id)
        images_dir = Path(group_dir) / "images"
        
        # 安全检查：确保路径在图片目录内
        image_file = (images_dir / image_path).resolve()
        if not str(image_file).startswith(str(images_dir.resolve())):
            raise HTTPException(status_code=403, detail="禁止访问该路径")
        
        if not image_file.exists():
            raise HTTPException(status_code=404, detail="图片不存在")
        
        # 获取 MIME 类型
        content_type = mimetypes.guess_type(str(image_file))[0] or 'application/octet-stream'
        
        # 读取并返回图片
        with open(image_file, 'rb') as f:
            content = f.read()
        
        return Response(content=content, media_type=content_type)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取图片失败: {str(e)}")


@app.get("/api/groups/{group_id}/videos/{video_path:path}")
async def get_local_video(group_id: str, video_path: str):
    """获取群组本地缓存的视频（支持范围请求，用于视频流播放）"""
    from pathlib import Path
    from fastapi.responses import FileResponse
    from fastapi import Request
    
    try:
        path_manager = get_db_path_manager()
        group_dir = path_manager.get_group_dir(group_id)
        videos_dir = Path(group_dir) / "column_videos"
        
        # 安全检查：确保路径在视频目录内
        video_file = (videos_dir / video_path).resolve()
        if not str(video_file).startswith(str(videos_dir.resolve())):
            raise HTTPException(status_code=403, detail="禁止访问该路径")
        
        if not video_file.exists():
            raise HTTPException(status_code=404, detail="视频不存在")
        
        # 获取 MIME 类型
        content_type = mimetypes.guess_type(str(video_file))[0] or 'video/mp4'
        
        # 使用 FileResponse 支持范围请求（视频拖动进度条）
        return FileResponse(
            path=str(video_file),
            media_type=content_type,
            filename=video_file.name
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取视频失败: {str(e)}")


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


@app.get("/api/groups/{group_id}/columns/summary")
async def get_group_columns_summary(group_id: str):
    """获取群组专栏摘要信息，检查是否存在专栏内容"""
    try:
        path_manager = get_db_path_manager()
        columns_db_path = path_manager.get_columns_db_path(group_id)
        if os.path.exists(columns_db_path):
            db = ZSXQColumnsDatabase(columns_db_path)
            try:
                stats = db.get_stats(int(group_id))
                columns = db.get_columns(int(group_id))
                if stats.get("columns_count", 0) > 0 or stats.get("topics_count", 0) > 0:
                    return {
                        "has_columns": True,
                        "title": columns[0].get("name") if columns else "专栏",
                        "source": "local",
                    }
            finally:
                db.close()

        # 自动匹配该群组所属账号，获取对应Cookie
        cookie = get_cookie_for_group(group_id)
        
        if not cookie:
            return {
                "has_columns": False,
                "title": None,
                "error": "未找到可用Cookie"
            }
        
        headers = build_stealth_headers(cookie)
        url = f"https://api.zsxq.com/v2/groups/{group_id}/columns/summary"
        
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('succeeded'):
                resp_data = data.get('resp_data', {})
                return {
                    "has_columns": resp_data.get('has_columns', False),
                    "title": resp_data.get('title', None)
                }
            else:
                return {
                    "has_columns": False,
                    "title": None,
                    "error": data.get('error_message', 'API返回失败')
                }
        else:
            return {
                "has_columns": False,
                "title": None,
                "error": f"HTTP {response.status_code}"
            }
    except requests.RequestException as e:
        return {
            "has_columns": False,
            "title": None,
            "error": f"网络请求失败: {str(e)}"
        }
    except Exception as e:
        return {
            "has_columns": False,
            "title": None,
            "error": f"获取专栏信息失败: {str(e)}"
        }


@app.get("/api/groups/{group_id}/info")
async def get_group_info(group_id: str):
    """获取群组信息（带本地回退，避免401/500导致前端报错）"""
    try:
        # 本地回退数据构造（不访问官方API）
        def build_fallback(source: str = "fallback", note: str = None) -> dict:
            files_count = 0
            try:
                file_db = _open_local_files_db(group_id)
                try:
                    file_db.cursor.execute("SELECT COUNT(*) FROM files")
                    row = file_db.cursor.fetchone()
                    files_count = (row[0] or 0) if row else 0
                finally:
                    file_db.close()
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
                "account": get_account_summary_for_group_auto(group_id),
                "source": source,
            }
            if note:
                result["note"] = note
            return result

        # 自动匹配该群组所属账号，获取对应Cookie
        cookie = get_cookie_for_group(group_id)

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
                    "account": get_account_summary_for_group_auto(group_id),
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
    db = None
    try:
        db = _open_local_topics_db(str(group_id))
        cursor = db.cursor

        # 🧪 调试：打印当前使用的数据库路径
        try:
            db_path = getattr(db, "db_path", None)
            print(f"[DEBUG get_group_topics] group_id={group_id}, db_path={db_path}, page={page}, per_page={per_page}")
        except Exception as e:
            print(f"[DEBUG get_group_topics] failed to print db_path: {e}")

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

        cursor.execute(query, params)
        topics = cursor.fetchall()

        # 🧪 调试：打印前若干条话题的 topic_id 和标题
        try:
            debug_rows = topics[:10]
            debug_list = [(row[0], row[1]) for row in debug_rows]
            print(f"[DEBUG get_group_topics] first topics from DB (topic_id, title): {debug_list}")

            # 特别打印“Offer选择”这条（如果存在）
            for row in debug_rows:
                title = row[1] or ""
                if isinstance(title, str) and title.startswith("Offer选择"):
                    print(f"[DEBUG get_group_topics] Offer topic row from DB: topic_id={row[0]}, title={title}")
        except Exception as e:
            print(f"[DEBUG get_group_topics] failed to debug topics: {e}")

        # 获取总数
        if search:
            cursor.execute("SELECT COUNT(*) FROM topics WHERE group_id = ? AND title LIKE ?", (group_id, f"%{search}%"))
        else:
            cursor.execute("SELECT COUNT(*) FROM topics WHERE group_id = ?", (group_id,))
        total = cursor.fetchone()[0]

        # 处理话题数据
        topics_list = []
        for topic in topics:
            # 注意：topic_id 可能超过 JavaScript 的安全整数范围（2^53-1），
            # 如果以数字形式传递到前端会发生精度丢失（例如 82811852151825212 变成 82811852151825220）。
            # 因此这里统一将 topic_id 序列化为字符串，前端也应按字符串处理。
            topic_data = {
                "topic_id": str(topic[0]) if topic[0] is not None else None,
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
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取群组话题失败: {str(e)}")
    finally:
        if db:
            db.close()

@app.get("/api/groups/{group_id}/stats")
async def get_group_stats(group_id: int):
    """获取指定群组的统计信息"""
    try:
        path_manager = get_db_path_manager()
        db_path = path_manager.get_topics_db_path(str(group_id))
        db = ZSXQDatabase(db_path)
        cursor = db.cursor

        try:
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
        finally:
            db.close()
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

def clear_account_detect_cache():
    """清除账号群组检测缓存，使新账号/删除账号后群组立即刷新"""
    _account_detect_cache["built_at"] = 0

def _get_all_account_sources() -> List[Dict[str, Any]]:
    """获取所有账号来源"""
    sources: List[Dict[str, Any]] = []
    try:
        sql_mgr = get_accounts_sql_manager()
        accounts = sql_mgr.get_accounts(mask_cookie=False)
        if accounts:
            sources.extend(accounts)
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
        acc_id = src.get("id")
        if not cookie or cookie == "your_cookie_here" or not acc_id:
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
                        "name": src.get("name") or acc_id,
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
    """返回自动匹配到的账号摘要"""
    mapping = build_account_group_detection(force_refresh=False)
    summary = mapping.get(str(group_id))
    if summary:
        return summary

    # 如果没有匹配的账号，返回第一个账号
    try:
        sql_mgr = get_accounts_sql_manager()
        first_acc = sql_mgr.get_first_account(mask_cookie=True)
        if first_acc:
            return {
                "id": first_acc["id"],
                "name": first_acc["name"],
                "created_at": first_acc["created_at"],
                "cookie": first_acc["cookie"]
            }
    except Exception:
        pass

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
            "columns_db_removed": False,
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
        gc.collect()
        time.sleep(0.3)

        path_manager = get_db_path_manager()
        group_dir = path_manager.get_group_dir(group_id)
        topics_db = path_manager.get_topics_db_path(group_id)
        files_db = path_manager.get_files_db_path(group_id)
        columns_db = path_manager.get_columns_db_path(group_id)

        # 删除话题数据库
        try:
            if remove_sqlite_file(topics_db):
                details["topics_db_removed"] = True
                print(f"🗑️ 已删除话题数据库: {topics_db}")
        except PermissionError as pe:
            raise HTTPException(status_code=500, detail=f"话题数据库被占用，无法删除: {pe}")
        except Exception as e:
            print(f"⚠️ 删除话题数据库失败: {e}")

        # 删除文件数据库
        try:
            if remove_sqlite_file(files_db):
                details["files_db_removed"] = True
                print(f"🗑️ 已删除文件数据库: {files_db}")
        except PermissionError as pe:
            raise HTTPException(status_code=500, detail=f"文件数据库被占用，无法删除: {pe}")
        except Exception as e:
            print(f"⚠️ 删除文件数据库失败: {e}")

        try:
            if remove_sqlite_file(columns_db):
                details["columns_db_removed"] = True
                print(f"🗑️ 已删除专栏数据库: {columns_db}")
        except PermissionError as pe:
            raise HTTPException(status_code=500, detail=f"专栏数据库被占用，无法删除: {pe}")
        except Exception as e:
            print(f"⚠️ 删除专栏数据库失败: {e}")

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
            from .image_cache_manager import get_image_cache_manager, clear_group_cache_manager
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


# =========================
# 专栏相关 API
# =========================

def get_columns_db(group_id: str) -> ZSXQColumnsDatabase:
    """获取指定群组的专栏数据库实例"""
    path_manager = get_db_path_manager()
    db_path = path_manager.get_columns_db_path(group_id)
    return ZSXQColumnsDatabase(db_path)


@app.get("/api/groups/{group_id}/columns")
async def get_group_columns(group_id: str):
    """获取群组的专栏目录列表（从本地数据库）"""
    try:
        db = get_columns_db(group_id)
        columns = db.get_columns(int(group_id))
        stats = db.get_stats(int(group_id))
        db.close()
        return {
            "columns": columns,
            "stats": stats
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取专栏目录失败: {str(e)}")


@app.get("/api/groups/{group_id}/columns/{column_id}/topics")
async def get_column_topics(group_id: str, column_id: int):
    """获取专栏下的文章列表（从本地数据库）"""
    try:
        db = get_columns_db(group_id)
        topics = db.get_column_topics(column_id)
        column = db.get_column(column_id)
        db.close()
        return {
            "column": column,
            "topics": topics
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取专栏文章列表失败: {str(e)}")


@app.get("/api/groups/{group_id}/columns/topics/{topic_id}")
async def get_column_topic_detail(group_id: str, topic_id: int):
    """获取专栏文章详情（从本地数据库）"""
    try:
        db = get_columns_db(group_id)
        detail = db.get_topic_detail(topic_id)
        db.close()

        if not detail:
            raise HTTPException(status_code=404, detail="文章详情不存在")

        # 解析 raw_json 获取 Q&A 类型内容
        if detail.get('raw_json'):
            try:
                raw_data = json.loads(detail['raw_json'])
                topic_type = raw_data.get('type', '')

                # Q&A 类型：提取 question 和 answer
                if topic_type == 'q&a':
                    question = raw_data.get('question', {})
                    answer = raw_data.get('answer', {})

                    detail['question'] = {
                        'text': question.get('text', ''),
                        'owner': question.get('owner'),
                        'images': question.get('images', []),
                    }
                    detail['answer'] = {
                        'text': answer.get('text', ''),
                        'owner': answer.get('owner'),
                        'images': answer.get('images', []),
                    }
                    # 如果 full_text 为空，使用 answer.text
                    if not detail.get('full_text') and answer.get('text'):
                        detail['full_text'] = answer.get('text', '')

                # talk 类型：如果 full_text 为空，从 talk 提取
                elif topic_type == 'talk':
                    talk = raw_data.get('talk', {})
                    if not detail.get('full_text') and talk.get('text'):
                        detail['full_text'] = talk.get('text', '')

            except (json.JSONDecodeError, TypeError):
                pass

        return detail
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取文章详情失败: {str(e)}")


def _hydrate_column_topic_detail(detail: Dict[str, Any]) -> Dict[str, Any]:
    """Hydrate Q&A/talk fields for column topic export."""
    if detail.get('raw_json'):
        try:
            raw_data = json.loads(detail['raw_json'])
            topic_type = raw_data.get('type', '')

            if topic_type == 'q&a':
                question = raw_data.get('question', {})
                answer = raw_data.get('answer', {})
                detail['question'] = {
                    'text': question.get('text', ''),
                    'owner': question.get('owner'),
                    'images': question.get('images', []),
                }
                detail['answer'] = {
                    'text': answer.get('text', ''),
                    'owner': answer.get('owner'),
                    'images': answer.get('images', []),
                }
                if not detail.get('full_text') and answer.get('text'):
                    detail['full_text'] = answer.get('text', '')
            elif topic_type == 'talk':
                talk = raw_data.get('talk', {})
                if not detail.get('full_text') and talk.get('text'):
                    detail['full_text'] = talk.get('text', '')
        except (json.JSONDecodeError, TypeError):
            pass
    return detail


@app.get("/api/groups/{group_id}/columns/topics/{topic_id}/export-md")
async def export_column_topic_markdown(group_id: str, topic_id: int, format: str = "zip"):
    """导出专栏文章为 Markdown 单文件或包含资源的 ZIP 归档。

    - format=zip（默认）：返回 README.md + assets/ 目录的 zip 包，含头像与图片
    - format=md：返回单个 .md 文件（图片用远程 URL）
    """
    try:
        db = get_columns_db(group_id)
        detail = db.get_topic_detail(topic_id)
        db.close()

        if not detail:
            raise HTTPException(status_code=404, detail="Column topic detail not found")

        detail = _hydrate_column_topic_detail(detail)
        title = detail.get("title") or f"topic_{topic_id}"

        if format == "zip":
            return _download_topic_archive_response(
                detail,
                f"{topic_id}_{title}",
                group_id=group_id,
                render=column_topic_detail_to_markdown,
            )

        markdown = column_topic_detail_to_markdown(detail)
        return _download_markdown_response(markdown, f"{topic_id}_{title}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Export Markdown failed: {str(e)}")


@app.post("/api/groups/{group_id}/columns/fetch")
async def fetch_group_columns(group_id: str, request: ColumnsSettingsRequest, background_tasks: BackgroundTasks):
    """采集群组的所有专栏内容（后台任务）"""
    global task_counter
    
    try:
        task_counter += 1
        task_id = f"columns_{group_id}_{task_counter}"
        
        # 创建任务记录
        current_tasks[task_id] = {
            "task_id": task_id,
            "type": "columns_fetch",
            "group_id": group_id,
            "status": "running",
            "message": "正在采集专栏内容...",
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "result": None
        }
        task_logs[task_id] = []
        task_stop_flags[task_id] = False
        
        # 添加到后台任务
        background_tasks.add_task(
            _fetch_columns_task,
            task_id,
            group_id,
            request
        )
        
        return {
            "success": True,
            "task_id": task_id,
            "message": "专栏采集任务已启动"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"启动专栏采集失败: {str(e)}")


async def _fetch_columns_task(task_id: str, group_id: str, settings: ColumnsSettingsRequest):
    """专栏采集后台任务"""
    log_id = None
    db = None
    
    try:
        # 获取配置参数
        crawl_interval_min = settings.crawlIntervalMin or 2.0
        crawl_interval_max = settings.crawlIntervalMax or 5.0
        long_sleep_min = settings.longSleepIntervalMin or 30.0
        long_sleep_max = settings.longSleepIntervalMax or 60.0
        items_per_batch = settings.itemsPerBatch or 10
        download_files = settings.downloadFiles if settings.downloadFiles is not None else True
        download_videos = settings.downloadVideos if settings.downloadVideos is not None else True
        cache_images = settings.cacheImages if settings.cacheImages is not None else True
        incremental_mode = settings.incrementalMode if settings.incrementalMode is not None else False
        
        add_task_log(task_id, f"📚 开始采集群组 {group_id} 的专栏内容")
        add_task_log(task_id, "=" * 50)
        add_task_log(task_id, "⚙️ 采集配置:")
        add_task_log(task_id, f"   ⏱️ 请求间隔: {crawl_interval_min}~{crawl_interval_max} 秒")
        add_task_log(task_id, f"   😴 长休眠间隔: {long_sleep_min}~{long_sleep_max} 秒")
        add_task_log(task_id, f"   📦 批次大小: {items_per_batch} 个请求")
        add_task_log(task_id, f"   📥 下载文件: {'是' if download_files else '否'}")
        add_task_log(task_id, f"   🎬 下载视频: {'是' if download_videos else '否'}")
        add_task_log(task_id, f"   🖼️ 缓存图片: {'是' if cache_images else '否'}")
        add_task_log(task_id, f"   🔄 增量模式: {'是（跳过已存在）' if incremental_mode else '否（全量采集）'}")
        add_task_log(task_id, "=" * 50)
        
        cookie = get_cookie_for_group(group_id)
        if not cookie:
            raise Exception("未找到可用Cookie，请先配置账号")
        
        headers = build_stealth_headers(cookie)
        db = get_columns_db(group_id)
        log_id = db.start_crawl_log(int(group_id), 'full_fetch')
        
        columns_count = 0
        topics_count = 0
        details_count = 0
        files_count = 0
        images_count = 0
        videos_count = 0
        skipped_count = 0  # 增量模式跳过的文章数
        files_skipped = 0  # 跳过的文件数（已存在）
        videos_skipped = 0  # 跳过的视频数（已存在）
        request_count = 0  # 请求计数器，用于触发长休眠
        
        # 1. 获取专栏目录列表（带重试机制）
        add_task_log(task_id, "📂 获取专栏目录列表...")
        columns_url = f"https://api.zsxq.com/v2/groups/{group_id}/columns"
        max_retries = GLOBAL_API_MAX_RETRIES
        columns = None
        
        for retry in range(max_retries):
            if is_task_stopped(task_id):
                break
            
            try:
                resp = requests.get(columns_url, headers=headers, timeout=30)
                request_count += 1
            except Exception as req_err:
                log_exception(f"获取专栏目录请求异常: group_id={group_id}, url={columns_url}")
                if retry < max_retries - 1:
                    wait_time = 2 if retry < 3 else (5 if retry < 6 else 10)
                    add_task_log(task_id, f"   ⚠️ 请求异常，等待{wait_time}秒后重试 ({retry+1}/{max_retries})")
                    await asyncio.sleep(wait_time)
                    continue
                raise Exception(f"获取专栏目录请求异常: {req_err}")
            
            if resp.status_code != 200:
                log_error(f"获取专栏目录失败: group_id={group_id}, HTTP {resp.status_code}, response={resp.text[:500] if resp.text else 'empty'}")
                if retry < max_retries - 1:
                    wait_time = 2 if retry < 3 else (5 if retry < 6 else 10)
                    add_task_log(task_id, f"   ⚠️ HTTP {resp.status_code}，等待{wait_time}秒后重试 ({retry+1}/{max_retries})")
                    await asyncio.sleep(wait_time)
                    continue
                raise Exception(f"获取专栏目录失败: HTTP {resp.status_code}")
            
            try:
                data = resp.json()
            except Exception as json_err:
                log_exception(f"解析专栏目录JSON失败: group_id={group_id}, response={resp.text[:500] if resp.text else 'empty'}")
                raise Exception(f"解析专栏目录失败: {json_err}")
                
            if not data.get('succeeded'):
                error_code = data.get('code')
                error_msg = data.get('error_message', '未知错误')
                
                # 检查是否是会员过期
                if 'expired' in error_msg.lower() or data.get('resp_data', {}).get('expired'):
                    raise Exception(f"会员已过期: {error_msg}")
                
                if should_retry_api_code(error_code, retry, max_retries):
                    wait_time = retry_wait_seconds(retry)
                    add_task_log(task_id, f"   ⚠️ 命中全局重试错误码 {error_code}，等待{wait_time}秒后重试 ({retry+1}/{max_retries})")
                    await asyncio.sleep(wait_time)
                    continue

                if is_global_retry_code(error_code):
                    log_error(f"获取专栏目录重试{max_retries}次后仍失败: group_id={group_id}, code={error_code}")
                    raise Exception(f"获取专栏目录失败，重试{max_retries}次后仍遇到反爬限制")
                else:
                    log_error(f"获取专栏目录API失败: group_id={group_id}, code={error_code}, message={error_msg}, response={json.dumps(data, ensure_ascii=False)[:500]}")
                    raise Exception(f"API返回失败: {error_msg} (code={error_code})")
            else:
                # 成功获取
                columns = data.get('resp_data', {}).get('columns', [])
                if retry > 0:
                    add_task_log(task_id, f"   ✅ 重试成功 (第{retry+1}次尝试)")
                break
        
        if columns is None:
            raise Exception("获取专栏目录失败")
        add_task_log(task_id, f"✅ 获取到 {len(columns)} 个专栏目录")
        
        if len(columns) == 0:
            add_task_log(task_id, "ℹ️ 该群组没有专栏内容")
            update_task(task_id, "completed", "该群组没有专栏内容")
            db.close()
            return
        
        # 2. 遍历每个专栏
        for col_idx, column in enumerate(columns, 1):
            if is_task_stopped(task_id):
                add_task_log(task_id, "🛑 任务已被用户停止")
                break
            
            column_id = column.get('column_id')
            column_name = column.get('name', '未命名')
            column_topics_count = column.get('statistics', {}).get('topics_count', 0)
            db.insert_column(int(group_id), column)
            columns_count += 1
            
            add_task_log(task_id, "")
            add_task_log(task_id, f"📁 [{col_idx}/{len(columns)}] 专栏: {column_name}")
            add_task_log(task_id, f"   📊 预计文章数: {column_topics_count}")
            
            # 检查是否需要长休眠
            if request_count > 0 and request_count % items_per_batch == 0:
                sleep_time = random.uniform(long_sleep_min, long_sleep_max)
                add_task_log(task_id, f"   😴 已完成 {request_count} 次请求，休眠 {sleep_time:.1f} 秒...")
                await asyncio.sleep(sleep_time)
            
            # 随机延迟
            delay = random.uniform(crawl_interval_min, crawl_interval_max)
            add_task_log(task_id, f"   ⏳ 等待 {delay:.1f} 秒后获取文章列表...")
            await asyncio.sleep(delay)
            
            # 获取专栏文章列表
            topics_url = f"https://api.zsxq.com/v2/groups/{group_id}/columns/{column_id}/topics?count=100&sort=default&direction=desc"
            topics_list = None
            max_topic_list_retries = GLOBAL_API_MAX_RETRIES
            for retry in range(max_topic_list_retries):
                if is_task_stopped(task_id):
                    break

                if retry > 0:
                    add_task_log(task_id, f"   🔄 重试获取文章列表 ({retry+1}/{max_topic_list_retries})")

                try:
                    topics_resp = requests.get(topics_url, headers=headers, timeout=30)
                    request_count += 1
                except Exception as req_err:
                    log_exception(f"获取专栏文章列表请求异常: column_id={column_id}, url={topics_url}")
                    if retry < max_topic_list_retries - 1:
                        wait_time = 2 if retry < 3 else (5 if retry < 6 else 10)
                        add_task_log(task_id, f"   ⚠️ 请求异常，等待{wait_time}秒后重试 ({retry+1}/{max_topic_list_retries})")
                        await asyncio.sleep(wait_time)
                        continue
                    add_task_log(task_id, f"   ⚠️ 获取文章列表请求异常: {req_err}")
                    break

                if topics_resp.status_code != 200:
                    log_error(f"获取专栏文章列表失败: column_id={column_id}, HTTP {topics_resp.status_code}, response={topics_resp.text[:500] if topics_resp.text else 'empty'}")
                    if retry < max_topic_list_retries - 1:
                        wait_time = 2 if retry < 3 else (5 if retry < 6 else 10)
                        add_task_log(task_id, f"   ⚠️ HTTP {topics_resp.status_code}，等待{wait_time}秒后重试 ({retry+1}/{max_topic_list_retries})")
                        await asyncio.sleep(wait_time)
                        continue
                    add_task_log(task_id, f"   ⚠️ 获取文章列表失败: HTTP {topics_resp.status_code}")
                    break

                try:
                    topics_data = topics_resp.json()
                except Exception as json_err:
                    log_exception(f"解析专栏文章列表JSON失败: column_id={column_id}, response={topics_resp.text[:500] if topics_resp.text else 'empty'}")
                    if retry < max_topic_list_retries - 1:
                        wait_time = 2 if retry < 3 else (5 if retry < 6 else 10)
                        add_task_log(task_id, f"   ⚠️ 解析文章列表失败，等待{wait_time}秒后重试 ({retry+1}/{max_topic_list_retries})")
                        await asyncio.sleep(wait_time)
                        continue
                    add_task_log(task_id, f"   ⚠️ 解析文章列表失败: {json_err}")
                    break

                if not topics_data.get('succeeded'):
                    error_code = topics_data.get('code', 'unknown')
                    error_message = topics_data.get('error_message', '未知错误')

                    if should_retry_api_code(error_code, retry, max_topic_list_retries):
                        wait_time = retry_wait_seconds(retry)
                        add_task_log(task_id, f"   ⚠️ 命中全局重试错误码 {error_code}，等待{wait_time}秒后重试 ({retry+1}/{max_topic_list_retries})")
                        await asyncio.sleep(wait_time)
                        continue

                    if is_global_retry_code(error_code):
                        log_error(f"获取专栏文章列表重试{max_topic_list_retries}次后仍失败: column_id={column_id}, code={error_code}, message={error_message}")
                        add_task_log(task_id, f"   ❌ 文章列表重试{max_topic_list_retries}次后仍失败: {error_message} (code={error_code})")
                        break

                    log_error(f"获取专栏文章列表失败: column_id={column_id}, code={error_code}, message={error_message}")
                    add_task_log(task_id, f"   ⚠️ 获取文章列表失败: {error_message} (code={error_code})")
                    break

                topics_list = topics_data.get('resp_data', {}).get('topics', [])
                if retry > 0:
                    add_task_log(task_id, f"   ✅ 文章列表重试成功 (第{retry+1}次尝试)")
                break

            if topics_list is None:
                continue
            add_task_log(task_id, f"   📝 获取到 {len(topics_list)} 篇文章")
            
            # 3. 遍历每篇文章
            for topic_idx, topic in enumerate(topics_list, 1):
                if is_task_stopped(task_id):
                    break
                
                topic_id = topic.get('topic_id')
                topic_title = topic.get('title', '无标题')[:30]
                db.insert_column_topic(column_id, int(group_id), topic)
                topics_count += 1
                
                # 增量模式：检查文章详情是否已存在
                if incremental_mode and db.topic_detail_exists(topic_id):
                    add_task_log(task_id, f"   📄 [{topic_idx}/{len(topics_list)}] {topic_title}... ⏭️ 跳过（已存在）")
                    skipped_count += 1
                    continue
                
                add_task_log(task_id, f"   📄 [{topic_idx}/{len(topics_list)}] {topic_title}...")
                
                # 获取文章详情（带重试机制）
                max_retries = GLOBAL_API_MAX_RETRIES
                topic_detail = None
                
                for retry in range(max_retries):
                    if is_task_stopped(task_id):
                        break
                    
                    # 检查是否需要长休眠
                    if request_count > 0 and request_count % items_per_batch == 0:
                        sleep_time = random.uniform(long_sleep_min, long_sleep_max)
                        add_task_log(task_id, f"      😴 已完成 {request_count} 次请求，休眠 {sleep_time:.1f} 秒...")
                        await asyncio.sleep(sleep_time)
                    
                    # 随机延迟
                    delay = random.uniform(crawl_interval_min, crawl_interval_max)
                    await asyncio.sleep(delay)
                    
                    # 获取文章详情
                    detail_url = f"https://api.zsxq.com/v2/topics/{topic_id}/info"
                    try:
                        detail_resp = requests.get(detail_url, headers=headers, timeout=30)
                        request_count += 1
                    except Exception as req_err:
                        log_exception(f"获取文章详情请求异常: topic_id={topic_id}, url={detail_url}")
                        add_task_log(task_id, f"      ⚠️ 获取详情请求异常: {req_err}")
                        continue
                    
                    if detail_resp.status_code != 200:
                        log_error(f"获取文章详情失败: topic_id={topic_id}, HTTP {detail_resp.status_code}, response={detail_resp.text[:500] if detail_resp.text else 'empty'}")
                        add_task_log(task_id, f"      ⚠️ 获取详情失败: HTTP {detail_resp.status_code}")
                        continue
                    
                    try:
                        detail_data = detail_resp.json()
                    except Exception as json_err:
                        log_exception(f"解析文章详情JSON失败: topic_id={topic_id}, response={detail_resp.text[:500] if detail_resp.text else 'empty'}")
                        add_task_log(task_id, f"      ⚠️ 解析详情失败: {json_err}")
                        continue
                        
                    if not detail_data.get('succeeded'):
                        error_code = detail_data.get('code')
                        error_message = detail_data.get('error_message', '未知错误')
                        
                        if should_retry_api_code(error_code, retry, max_retries):
                            wait_time = retry_wait_seconds(retry)
                            add_task_log(task_id, f"      ⚠️ 命中全局重试错误码 {error_code}，等待{wait_time}秒后重试 ({retry+1}/{max_retries})")
                            await asyncio.sleep(wait_time)
                            continue

                        if is_global_retry_code(error_code):
                            log_error(f"获取文章详情重试{max_retries}次后仍失败: topic_id={topic_id}, code={error_code}, message={error_message}")
                            add_task_log(task_id, f"      ❌ 重试{max_retries}次后仍失败: {error_message} (code={error_code})")
                            break
                        else:
                            log_error(f"获取文章详情失败: topic_id={topic_id}, code={error_code}, message={error_message}, full_response={json.dumps(detail_data, ensure_ascii=False)[:500]}")
                            add_task_log(task_id, f"      ⚠️ 获取详情失败: {error_message} (code={error_code})")
                            break
                    else:
                        # 成功获取详情
                        topic_detail = detail_data.get('resp_data', {}).get('topic', {})
                        if retry > 0:
                            add_task_log(task_id, f"      ✅ 重试成功 (第{retry+1}次尝试)")
                        break
                
                # 如果没有获取到详情，跳过后续处理
                if not topic_detail:
                    continue
                db.insert_topic_detail(int(group_id), topic_detail, json.dumps(topic_detail, ensure_ascii=False))
                details_count += 1
                
                # 处理文件下载
                if download_files:
                    talk = topic_detail.get('talk', {})
                    topic_files = talk.get('files', [])
                    content_voice = topic_detail.get('content_voice')
                    
                    all_files = topic_files.copy()
                    if content_voice:
                        all_files.append(content_voice)
                    
                    for file_info in all_files:
                        if is_task_stopped(task_id):
                            break
                        
                        file_id = file_info.get('file_id')
                        file_name = file_info.get('name', '')
                        file_size = file_info.get('size', 0)
                        
                        if file_id:
                            add_task_log(task_id, f"      📥 下载文件: {file_name[:40]}...")
                            
                            # 检查是否需要长休眠
                            if request_count > 0 and request_count % items_per_batch == 0:
                                sleep_time = random.uniform(long_sleep_min, long_sleep_max)
                                add_task_log(task_id, f"      😴 已完成 {request_count} 次请求，休眠 {sleep_time:.1f} 秒...")
                                await asyncio.sleep(sleep_time)
                            
                            delay = random.uniform(crawl_interval_min, crawl_interval_max)
                            await asyncio.sleep(delay)
                            
                            try:
                                result = await _download_column_file(
                                    group_id, file_id, file_name, file_size,
                                    topic_id, db, headers, task_id
                                )
                                if result == "downloaded":
                                    files_count += 1
                                    request_count += 1
                                    add_task_log(task_id, f"         ✅ 文件下载成功")
                                elif result == "skipped":
                                    files_skipped += 1
                                # "skipped" 时日志已在函数内输出
                            except Exception as fe:
                                log_exception(f"文件下载失败: file_id={file_id}, file_name={file_name}, topic_id={topic_id}")
                                add_task_log(task_id, f"         ⚠️ 文件下载失败: {fe}")
                
                # 缓存图片
                if cache_images:
                    talk = topic_detail.get('talk', {}) if 'talk' in topic_detail else {}
                    topic_images = talk.get('images', [])
                    
                    for image in topic_images:
                        if is_task_stopped(task_id):
                            break
                        
                        original_url = image.get('original', {}).get('url')
                        image_id = image.get('image_id')
                        
                        if original_url and image_id:
                            try:
                                cache_manager = get_image_cache_manager(group_id)
                                success, local_path, error_msg = cache_manager.download_and_cache(original_url)
                                if success and local_path:
                                    db.update_image_local_path(image_id, str(local_path))
                                    images_count += 1
                                elif error_msg:
                                    add_task_log(task_id, f"      ⚠️ 图片缓存失败: {error_msg}")
                            except Exception as ie:
                                log_exception(f"图片缓存失败: image_id={image_id}, url={original_url}")
                                add_task_log(task_id, f"      ⚠️ 图片缓存失败: {ie}")
                
                # 处理视频
                talk_for_video = topic_detail.get('talk', {}) if 'talk' in topic_detail else {}
                video = talk_for_video.get('video')
                
                if video and video.get('video_id'):
                    video_id = video.get('video_id')
                    video_size = video.get('size', 0)
                    video_duration = video.get('duration', 0)
                    cover = video.get('cover', {})
                    cover_url = cover.get('url')
                    
                    add_task_log(task_id, f"      🎬 发现视频: ID={video_id}, 大小={video_size/(1024*1024):.1f}MB, 时长={video_duration}秒")
                    
                    # 缓存视频封面（跟随图片缓存选项）
                    if cache_images and cover_url:
                        try:
                            cache_manager = get_image_cache_manager(group_id)
                            success, cover_local, error_msg = cache_manager.download_and_cache(cover_url)
                            if success and cover_local:
                                db.update_video_cover_path(video_id, str(cover_local))
                                add_task_log(task_id, f"      ✅ 视频封面缓存成功")
                            elif error_msg:
                                log_warning(f"视频封面缓存失败: video_id={video_id}, url={cover_url}, error={error_msg}")
                                add_task_log(task_id, f"      ⚠️ 视频封面缓存失败: {error_msg}")
                        except Exception as ve:
                            log_exception(f"视频封面缓存失败: video_id={video_id}, url={cover_url}")
                            add_task_log(task_id, f"      ⚠️ 视频封面缓存失败: {ve}")
                    
                    # 下载视频（单独控制）
                    if download_videos:
                        if request_count > 0 and request_count % items_per_batch == 0:
                            sleep_time = random.uniform(long_sleep_min, long_sleep_max)
                            add_task_log(task_id, f"      😴 已完成 {request_count} 次请求，休眠 {sleep_time:.1f} 秒...")
                            await asyncio.sleep(sleep_time)
                        
                        delay = random.uniform(crawl_interval_min, crawl_interval_max)
                        await asyncio.sleep(delay)
                        
                        try:
                            result = await _download_column_video(
                                group_id, video_id, video_size, video_duration,
                                topic_id, db, headers, task_id
                            )
                            if result == "downloaded":
                                videos_count += 1
                                request_count += 1
                            elif result == "skipped":
                                videos_skipped += 1
                            # "skipped" 时日志已在函数内输出
                        except Exception as ve:
                            log_exception(f"视频下载失败: video_id={video_id}, topic_id={topic_id}, size={video_size}")
                            add_task_log(task_id, f"      ⚠️ 视频下载失败: {ve}")
                    else:
                        add_task_log(task_id, f"      ⏭️ 跳过视频下载（已禁用）")
                
                # 更新进度
                update_task(task_id, "running", f"进度: {details_count} 篇文章, {files_count} 个文件, {videos_count} 个视频, {images_count} 张图片")
        
        # 完成
        add_task_log(task_id, "")
        add_task_log(task_id, "=" * 50)
        add_task_log(task_id, "🎉 专栏采集完成！")
        add_task_log(task_id, f"📊 统计:")
        add_task_log(task_id, f"   📁 专栏目录: {columns_count} 个")
        add_task_log(task_id, f"   📝 文章列表: {topics_count} 篇")
        add_task_log(task_id, f"   📄 文章详情: {details_count} 篇（新增）")
        if skipped_count > 0:
            add_task_log(task_id, f"   ⏭️ 跳过已存在文章: {skipped_count} 篇")
        add_task_log(task_id, f"   📥 下载文件: {files_count} 个" + (f" (跳过 {files_skipped} 个已存在)" if files_skipped > 0 else ""))
        add_task_log(task_id, f"   🎬 下载视频: {videos_count} 个" + (f" (跳过 {videos_skipped} 个已存在)" if videos_skipped > 0 else ""))
        add_task_log(task_id, f"   🖼️ 缓存图片: {images_count} 张")
        add_task_log(task_id, f"   📡 总请求数: {request_count} 次")
        add_task_log(task_id, "=" * 50)
        
        db.update_crawl_log(log_id, columns_count=columns_count, topics_count=topics_count,
                          details_count=details_count, files_count=files_count, status='completed')
        db.close()
        
        skipped_info = f", 跳过 {skipped_count} 篇" if skipped_count > 0 else ""
        result_msg = f"采集完成: {columns_count} 个专栏, {details_count} 篇新文章{skipped_info}, {files_count} 个文件, {videos_count} 个视频"
        update_task(task_id, "completed", result_msg)
        
    except Exception as e:
        error_msg = str(e)
        add_task_log(task_id, "")
        add_task_log(task_id, f"❌ 采集失败: {error_msg}")
        update_task(task_id, "failed", f"采集失败: {error_msg}")
        
        try:
            if db and log_id:
                db.update_crawl_log(log_id, status='failed', error_message=error_msg)
                db.close()
        except:
            pass


async def _download_column_file(group_id: str, file_id: int, file_name: str, file_size: int,
                                topic_id: int, db: ZSXQColumnsDatabase, headers: dict, task_id: str = None) -> str:
    """下载专栏文件
    
    Returns:
        str: "downloaded" 表示新下载, "skipped" 表示已存在跳过, 或抛出异常
    """
    # 先检查本地文件是否已存在
    path_manager = get_db_path_manager()
    group_dir = path_manager.get_group_dir(group_id)
    downloads_dir = os.path.join(group_dir, "column_downloads")
    local_path = os.path.join(downloads_dir, file_name)
    
    # 如果文件已存在且大小匹配，跳过下载
    if os.path.exists(local_path):
        existing_size = os.path.getsize(local_path)
        if existing_size == file_size or (file_size == 0 and existing_size > 0):
            db.update_file_download_status(file_id, 'completed', local_path)
            if task_id:
                add_task_log(task_id, f"         ⏭️ 文件已存在，跳过下载 ({existing_size/(1024*1024):.2f}MB)")
            return "skipped"
    
    # 获取下载URL（带重试机制）
    download_url = f"https://api.zsxq.com/v2/files/{file_id}/download_url"
    max_retries = GLOBAL_API_MAX_RETRIES
    real_url = None
    
    for retry in range(max_retries):
        try:
            resp = requests.get(download_url, headers=headers, timeout=30)
        except Exception as req_err:
            if retry < max_retries - 1:
                wait_time = 2 if retry < 3 else (5 if retry < 6 else 10)
                await asyncio.sleep(wait_time)
                continue
            log_exception(f"获取下载链接请求异常: file_id={file_id}")
            raise Exception(f"获取下载链接请求异常: {req_err}")
        
        if resp.status_code != 200:
            if retry < max_retries - 1:
                wait_time = 2 if retry < 3 else (5 if retry < 6 else 10)
                await asyncio.sleep(wait_time)
                continue
            error_msg = f"获取下载链接失败: HTTP {resp.status_code}, URL={download_url}, Response={resp.text[:500] if resp.text else 'empty'}"
            log_error(error_msg)
            raise Exception(error_msg)
        
        data = resp.json()
        if not data.get('succeeded'):
            error_code = data.get('code')
            error_message = data.get('error_message', '未知错误')
            
            if should_retry_api_code(error_code, retry, max_retries):
                wait_time = retry_wait_seconds(retry)
                await asyncio.sleep(wait_time)
                continue

            if is_global_retry_code(error_code):
                log_error(f"获取下载链接重试{max_retries}次后仍失败: file_id={file_id}, code={error_code}")
                raise Exception(f"获取下载链接失败，重试{max_retries}次后仍遇到反爬限制")
            else:
                error_msg = f"获取下载链接失败: code={error_code}, message={error_message}, file_id={file_id}, file_name={file_name}"
                log_error(error_msg)
                raise Exception(f"获取下载链接失败: {error_message} (code={error_code})")
        else:
            real_url = data.get('resp_data', {}).get('download_url')
            break
    
    if not real_url:
        raise Exception("下载链接为空")
    
    # 创建下载目录（downloads_dir 和 local_path 在函数开头已定义）
    os.makedirs(downloads_dir, exist_ok=True)
    
    # 下载文件（带重试机制，处理 SSL 错误等网络问题）
    download_retries = 3
    last_error = None
    
    for download_attempt in range(download_retries):
        try:
            file_resp = requests.get(real_url, headers=headers, stream=True, timeout=300)
            if file_resp.status_code == 200:
                with open(local_path, 'wb') as f:
                    for chunk in file_resp.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                db.update_file_download_status(file_id, 'completed', local_path)
                return "downloaded"
            else:
                last_error = f"HTTP {file_resp.status_code}"
                if download_attempt < download_retries - 1:
                    log_warning(f"文件下载失败 (尝试 {download_attempt + 1}/{download_retries}): {last_error}, file_id={file_id}")
                    await asyncio.sleep(2 * (download_attempt + 1))  # 递增等待
                    continue
        except requests.exceptions.SSLError as ssl_err:
            last_error = f"SSL错误: {ssl_err}"
            if download_attempt < download_retries - 1:
                log_warning(f"文件下载SSL错误 (尝试 {download_attempt + 1}/{download_retries}): file_id={file_id}, error={ssl_err}")
                await asyncio.sleep(3 * (download_attempt + 1))  # SSL错误等待更久
                continue
        except requests.exceptions.RequestException as req_err:
            last_error = f"网络错误: {req_err}"
            if download_attempt < download_retries - 1:
                log_warning(f"文件下载网络错误 (尝试 {download_attempt + 1}/{download_retries}): file_id={file_id}, error={req_err}")
                await asyncio.sleep(2 * (download_attempt + 1))
                continue
    
    # 所有重试都失败
    db.update_file_download_status(file_id, 'failed')
    raise Exception(f"下载失败 (重试{download_retries}次): {last_error}")


async def _download_column_video(group_id: str, video_id: int, video_size: int, video_duration: int,
                                 topic_id: int, db: ZSXQColumnsDatabase, headers: dict, task_id: str = None) -> str:
    """下载专栏视频（m3u8格式）
    
    Returns:
        str: "downloaded" 表示新下载, "skipped" 表示已存在跳过, 或抛出异常
    """
    import subprocess
    import re
    
    # 先检查本地视频是否已存在
    path_manager = get_db_path_manager()
    group_dir = path_manager.get_group_dir(group_id)
    videos_dir = os.path.join(group_dir, "column_videos")
    video_filename = f"video_{video_id}.mp4"
    local_path = os.path.join(videos_dir, video_filename)
    
    # 如果视频已存在且大小>0，跳过下载
    if os.path.exists(local_path):
        existing_size = os.path.getsize(local_path)
        if existing_size > 0:
            db.update_video_download_status(video_id, 'completed', '', local_path)
            if task_id:
                add_task_log(task_id, f"         ⏭️ 视频已存在，跳过下载 ({existing_size/(1024*1024):.1f}MB)")
            return "skipped"
    
    # 获取视频URL（带重试机制）
    video_url_api = f"https://api.zsxq.com/v2/videos/{video_id}/url"
    max_retries = GLOBAL_API_MAX_RETRIES
    m3u8_url = None
    
    for retry in range(max_retries):
        try:
            resp = requests.get(video_url_api, headers=headers, timeout=30)
        except Exception as req_err:
            if retry < max_retries - 1:
                wait_time = 2 if retry < 3 else (5 if retry < 6 else 10)
                await asyncio.sleep(wait_time)
                continue
            log_exception(f"获取视频链接请求异常: video_id={video_id}")
            raise Exception(f"获取视频链接请求异常: {req_err}")
        
        if resp.status_code != 200:
            if retry < max_retries - 1:
                wait_time = 2 if retry < 3 else (5 if retry < 6 else 10)
                await asyncio.sleep(wait_time)
                continue
            error_msg = f"获取视频链接失败: HTTP {resp.status_code}, URL={video_url_api}, Response={resp.text[:500] if resp.text else 'empty'}"
            log_error(error_msg)
            raise Exception(error_msg)
        
        data = resp.json()
        if not data.get('succeeded'):
            error_code = data.get('code')
            error_message = data.get('error_message', '未知错误')
            
            if should_retry_api_code(error_code, retry, max_retries):
                wait_time = retry_wait_seconds(retry)
                await asyncio.sleep(wait_time)
                continue

            if is_global_retry_code(error_code):
                log_error(f"获取视频链接重试{max_retries}次后仍失败: video_id={video_id}, code={error_code}")
                raise Exception(f"获取视频链接失败，重试{max_retries}次后仍遇到反爬限制")
            else:
                error_msg = f"获取视频链接失败: code={error_code}, message={error_message}, video_id={video_id}, topic_id={topic_id}"
                log_error(error_msg)
                raise Exception(f"获取视频链接失败: {error_message} (code={error_code})")
        else:
            m3u8_url = data.get('resp_data', {}).get('url')
            break
    
    if not m3u8_url:
        raise Exception("视频链接为空")
    
    # 创建视频下载目录（videos_dir 和 local_path 在函数开头已定义）
    os.makedirs(videos_dir, exist_ok=True)
    
    # 更新状态为下载中
    db.update_video_download_status(video_id, 'downloading', m3u8_url)
    
    # 使用ffmpeg下载m3u8视频
    try:
        # 检查ffmpeg是否可用
        ffmpeg_check = subprocess.run(['ffmpeg', '-version'], capture_output=True, text=True)
        if ffmpeg_check.returncode != 0:
            raise Exception("ffmpeg not available")
        
        # 构建 HTTP headers 字符串给 ffmpeg
        # ffmpeg 需要的格式是 "Header1: Value1\r\nHeader2: Value2\r\n"
        ffmpeg_headers = ""
        if headers.get('Cookie'):
            ffmpeg_headers += f"Cookie: {headers['Cookie']}\r\n"
        if headers.get('cookie'):
            ffmpeg_headers += f"Cookie: {headers['cookie']}\r\n"
        ffmpeg_headers += "Referer: https://wx.zsxq.com/\r\n"
        ffmpeg_headers += "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36\r\n"
        ffmpeg_headers += "Origin: https://wx.zsxq.com\r\n"
        
        # 使用ffmpeg下载（带请求头和进度显示）
        cmd = [
            'ffmpeg', '-y',
            '-headers', ffmpeg_headers,
            '-i', m3u8_url,
            '-c', 'copy',
            '-bsf:a', 'aac_adtstoasc',
            '-progress', 'pipe:1',  # 输出进度信息到 stdout
            local_path
        ]
        
        log_info(f"开始下载视频: video_id={video_id}, url={m3u8_url[:100]}...")
        if task_id:
            add_task_log(task_id, f"         🎬 开始下载视频 (预计时长: {video_duration}秒, 大小: {video_size/(1024*1024):.1f}MB)")
        
        # 使用 Popen 实时读取进度
        # 在 Windows 上需要特殊处理管道缓冲
        import threading
        import queue
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1
        )
        
        stderr_output = []
        stdout_queue = queue.Queue()
        
        # 使用线程读取 stdout，避免阻塞
        def read_stdout():
            try:
                for line in iter(process.stdout.readline, ''):
                    if line:
                        stdout_queue.put(line)
                    if process.poll() is not None:
                        break
            except:
                pass
        
        # 使用线程读取 stderr
        def read_stderr():
            try:
                for line in iter(process.stderr.readline, ''):
                    if line:
                        stderr_output.append(line)
            except:
                pass
        
        stdout_thread = threading.Thread(target=read_stdout, daemon=True)
        stderr_thread = threading.Thread(target=read_stderr, daemon=True)
        stdout_thread.start()
        stderr_thread.start()
        
        last_log_time = time.time()
        start_time = time.time()
        
        # 读取进度信息
        try:
            while process.poll() is None:
                # 非阻塞方式获取进度
                try:
                    line = stdout_queue.get(timeout=1)
                    
                    # 解析 ffmpeg 进度信息
                    # 格式: out_time_ms=123456789
                    if line.startswith('out_time_ms='):
                        try:
                            time_ms = int(line.split('=')[1].strip())
                            current_seconds = time_ms / 1000000
                            
                            # 每 3 秒更新一次日志，避免刷屏
                            now = time.time()
                            if task_id and (now - last_log_time) >= 3:
                                if video_duration > 0:
                                    progress_pct = min(100, (current_seconds / video_duration) * 100)
                                    # 生成进度条
                                    bar_length = 20
                                    filled = int(bar_length * progress_pct / 100)
                                    bar = '█' * filled + '░' * (bar_length - filled)
                                    add_task_log(task_id, f"         📊 下载进度: [{bar}] {progress_pct:.1f}% ({current_seconds:.0f}s/{video_duration}s)")
                                else:
                                    add_task_log(task_id, f"         📊 下载进度: {current_seconds:.0f}秒")
                                last_log_time = now
                        except:
                            pass
                except queue.Empty:
                    # 队列为空，检查是否需要显示等待中的进度
                    now = time.time()
                    elapsed = now - start_time
                    if task_id and (now - last_log_time) >= 5:
                        add_task_log(task_id, f"         ⏳ 下载中... (已用时 {elapsed:.0f}秒)")
                        last_log_time = now
                    continue
            
            # 等待线程结束
            stdout_thread.join(timeout=5)
            stderr_thread.join(timeout=5)
                
        except Exception as e:
            process.kill()
            raise Exception(f"视频下载异常: {e}")
        
        returncode = process.returncode
        stderr_text = ''.join(stderr_output)
        
        # 检查文件是否成功下载（ffmpeg 可能返回非 0 但文件已成功下载）
        if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
            db.update_video_download_status(video_id, 'completed', m3u8_url, local_path)
            final_size = os.path.getsize(local_path)
            log_info(f"视频下载成功: video_id={video_id}, path={local_path}, size={final_size}")
            if task_id:
                add_task_log(task_id, f"         ✅ 视频下载完成 ({final_size/(1024*1024):.1f}MB)")
            return "downloaded"
        else:
            db.update_video_download_status(video_id, 'failed', m3u8_url)
            # 从 stderr 中提取真正的错误信息（跳过版本信息等）
            stderr_lines = stderr_text.strip().split('\n')
            # 查找包含 "error" 或 "failed" 的行
            error_lines = [line for line in stderr_lines if 'error' in line.lower() or 'failed' in line.lower() or 'invalid' in line.lower()]
            if error_lines:
                error_msg = '; '.join(error_lines[-3:])  # 取最后 3 条错误
            else:
                # 如果没找到明确错误，取最后几行
                error_msg = '; '.join(stderr_lines[-3:]) if stderr_lines else 'unknown error'
            log_error(f"ffmpeg下载失败: video_id={video_id}, returncode={returncode}, error={error_msg}")
            raise Exception(f"ffmpeg下载失败: {error_msg[:300]}")
            
    except FileNotFoundError:
        # ffmpeg不可用，保存m3u8链接供手动下载
        db.update_video_download_status(video_id, 'pending_manual', m3u8_url)
        # 保存m3u8链接到文件
        m3u8_link_file = os.path.join(videos_dir, f"video_{video_id}.m3u8.txt")
        with open(m3u8_link_file, 'w', encoding='utf-8') as f:
            f.write(f"Video ID: {video_id}\n")
            f.write(f"Duration: {video_duration} seconds\n")
            f.write(f"Size: {video_size} bytes\n")
            f.write(f"M3U8 URL: {m3u8_url}\n")
        raise Exception("ffmpeg未安装，已保存m3u8链接到文件，请手动下载")
    except subprocess.TimeoutExpired:
        db.update_video_download_status(video_id, 'failed', m3u8_url)
        raise Exception("视频下载超时")


@app.get("/api/groups/{group_id}/columns/stats")
async def get_columns_stats(group_id: str):
    """获取专栏统计信息"""
    try:
        db = get_columns_db(group_id)
        stats = db.get_stats(int(group_id))
        db.close()
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取专栏统计失败: {str(e)}")


@app.delete("/api/groups/{group_id}/columns/all")
async def delete_all_columns(group_id: str):
    """删除群组的所有专栏数据"""
    try:
        db = get_columns_db(group_id)
        stats = db.clear_all_data(int(group_id))
        db.close()
        return {
            "success": True,
            "message": f"已清空专栏数据",
            "deleted": stats
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"删除专栏数据失败: {str(e)}")


@app.get("/api/groups/{group_id}/columns/topics/{topic_id}/comments")
async def get_column_topic_full_comments(group_id: str, topic_id: int):
    """获取专栏文章的完整评论列表（从API实时获取并持久化到数据库）"""
    try:
        # 获取该群组使用的账号
        manager = get_accounts_sql_manager()
        account = manager.get_account_for_group(group_id, mask_cookie=False)
        if not account or not account.get('cookie'):
            raise HTTPException(status_code=400, detail="No valid account found for this group")

        cookie = account['cookie']
        # 使用与专栏采集相同的请求头构建方式
        headers = build_stealth_headers(cookie)

        # 获取完整评论（参数与官网一致）
        comments_url = f"https://api.zsxq.com/v2/topics/{topic_id}/comments?sort=asc&count=30&with_sticky=true"
        log_info(f"Fetching comments from: {comments_url}")
        resp = requests.get(comments_url, headers=headers, timeout=30)

        if resp.status_code != 200:
            log_error(f"Failed to fetch comments: HTTP {resp.status_code}, response={resp.text[:500] if resp.text else 'empty'}")
            raise HTTPException(status_code=resp.status_code, detail=f"Failed to fetch comments: HTTP {resp.status_code}")

        data = resp.json()
        log_debug(f"Comments API response: succeeded={data.get('succeeded')}, resp_data keys={list(data.get('resp_data', {}).keys()) if data.get('resp_data') else 'None'}")

        if not data.get('succeeded'):
            # 尝试多种错误消息格式
            resp_data = data.get('resp_data', {})
            error_msg = resp_data.get('message') or resp_data.get('error_msg') or data.get('error_msg') or data.get('message')
            error_code = resp_data.get('code') or resp_data.get('error_code') or data.get('code')
            log_error(f"Comments API failed: code={error_code}, message={error_msg}, full_response={json.dumps(data, ensure_ascii=False)[:500]}")
            raise HTTPException(status_code=400, detail=f"API error: {error_msg or 'Request failed'} (code: {error_code})")

        comments = data.get('resp_data', {}).get('comments', [])

        # 处理评论数据，包括 replied_comments
        processed_comments = []
        for comment in comments:
            processed = {
                'comment_id': comment.get('comment_id'),
                'parent_comment_id': comment.get('parent_comment_id'),
                'text': comment.get('text', ''),
                'create_time': comment.get('create_time'),
                'likes_count': comment.get('likes_count', 0),
                'rewards_count': comment.get('rewards_count', 0),
                'replies_count': comment.get('replies_count', 0),
                'sticky': comment.get('sticky', False),
                'owner': comment.get('owner'),
                'repliee': comment.get('repliee'),
                'images': comment.get('images', []),
            }

            # 处理嵌套的 replied_comments
            replied_comments = comment.get('replied_comments', [])
            if replied_comments:
                processed['replied_comments'] = [
                    {
                        'comment_id': rc.get('comment_id'),
                        'parent_comment_id': rc.get('parent_comment_id'),
                        'text': rc.get('text', ''),
                        'create_time': rc.get('create_time'),
                        'likes_count': rc.get('likes_count', 0),
                        'owner': rc.get('owner'),
                        'repliee': rc.get('repliee'),
                        'images': rc.get('images', []),
                    }
                    for rc in replied_comments
                ]

            processed_comments.append(processed)

        # 持久化评论到数据库
        try:
            db = get_columns_db(group_id)
            saved_count = db.import_comments(topic_id, processed_comments)
            db.close()
            log_info(f"Saved {saved_count} comments to database for topic {topic_id}")
        except Exception as e:
            log_error(f"Failed to save comments to database: {e}")
            # 不阻断流程，评论仍然返回给前端

        # 计算总评论数（包括嵌套回复）
        total_count = sum(1 + len(c.get('replied_comments', [])) for c in processed_comments)

        return {
            'success': True,
            'comments': processed_comments,
            'total': total_count
        }

    except HTTPException:
        raise
    except Exception as e:
        log_exception(f"获取专栏完整评论失败: topic_id={topic_id}")
        raise HTTPException(status_code=500, detail=f"获取完整评论失败: {str(e)}")


if __name__ == "__main__":
    import sys
    port = 8208  # 默认端口
    if len(sys.argv) > 2 and sys.argv[1] == "--port":
        try:
            port = int(sys.argv[2])
        except ValueError:
            port = 8208
    log_info(f"启动后端服务: host=0.0.0.0, port={port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_config=None, access_log=True)
