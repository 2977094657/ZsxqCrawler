#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
知识星球批量导出器
按搜索条件批量导出话题（Markdown + 图片 + 附件文件）为 ZIP 包

复用 zsxq_markdown_exporter.build_topic_staging() 处理单话题的 Markdown/图片，
本模块仅负责：话题搜索、附件下载、批量编排和最终打包。
"""

import datetime
import os
import shutil
import zipfile
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import requests

from .db_path_manager import get_db_path_manager
from .image_cache_manager import get_image_cache_manager
from .zsxq_database import ZSXQDatabase
from .zsxq_markdown_exporter import (
    article_html_to_markdown,
    build_topic_staging,
    safe_filename,
    topic_detail_to_markdown,
)
from .zsxq_request_profiles import (
    build_zsxq_file_stream_headers,
    build_zsxq_mobile_headers,
    is_mobile_only_error,
)
from .zsxq_retry import (
    ensure_global_max_retries,
    retry_wait_seconds,
    should_retry_api_code,
)
from .logger_config import log_error, log_info, log_warning


class ZSXQBatchExporter:
    """批量导出器：搜索话题 → 按个调用 build_topic_staging() → 下载附件 → 打包 ZIP"""

    def __init__(self, group_id: str, cookie: str, export_dir: str,
                 log_callback: Optional[Callable[[str], None]] = None):
        self.group_id = str(group_id)
        self.cookie = cookie
        self.export_dir = export_dir
        self.log_callback = log_callback
        self.session = requests.Session()
        self.base_url = "https://api.zsxq.com"

        path_manager = get_db_path_manager()
        self.db_path = path_manager.get_topics_db_path(self.group_id)
        self.db = ZSXQDatabase(self.db_path)

        os.makedirs(self.export_dir, exist_ok=True)
        self.stop_flag = False

    def log(self, message: str):
        if self.log_callback:
            self.log_callback(message)
        else:
            log_info(message)

    def _fetch_article_content(self, article_url: str, title: str) -> Optional[str]:
        """抓取关联文章页面 HTML 并转 Markdown"""
        from urllib.parse import urlparse
        parsed = urlparse(article_url)
        if parsed.scheme not in {"http", "https"}:
            return None
        if self.is_stopped():
            return None
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "zh-CN,zh;q=0.9",
            }
            # ZSXQ 文章页需要 Cookie 才能访问完整内容
            if "zsxq.com" in parsed.netloc:
                headers["Cookie"] = self.cookie
            resp = self.session.get(article_url, headers=headers, timeout=30, allow_redirects=True)
            resp.raise_for_status()
            content_type = resp.headers.get("content-type", "")
            if "json" in content_type:
                return None
            resp.encoding = resp.encoding or "utf-8"
            return article_html_to_markdown(resp.text, fallback_title=title)
        except Exception:
            return None

    def _export_single_topic(
        self,
        topic_id: str,
        topic_dir: str,
        download_files: bool = True,
        download_images: bool = True,
    ):
        """导出单个话题到指定目录"""
        detail = self.db.get_topic_detail(int(topic_id))
        if not detail:
            raise FileNotFoundError(f"话题 {topic_id} 不存在于数据库中")

        image_downloader = None
        if download_images:
            cache_manager = get_image_cache_manager(self.group_id)
            def _download(url: str) -> Optional[Path]:
                if self.is_stopped():
                    return None
                try:
                    success, path, _err = cache_manager.download_and_cache(url, timeout=20)
                    if success and path:
                        return Path(path)
                except Exception:
                    pass
                return None
            image_downloader = _download

        file_map: Dict[str, str] = {}
        if download_files:
            file_map = self._download_topic_files(detail, topic_dir)

        def file_resolver(file_id, _name) -> Optional[str]:
            return file_map.get(str(file_id))

        render_kwargs = {
            "file_resolver": file_resolver,
            "include_comments": False,
            "article_fetcher": self._fetch_article_content,
        }

        title = detail.get("title") or f"topic_{topic_id}"
        md_name = safe_filename(title, max_length=80) + ".md"
        if not md_name or md_name == ".md":
            md_name = f"topic_{topic_id}.md"

        build_topic_staging(
            topic_dir,
            detail,
            render=topic_detail_to_markdown,
            render_kwargs=render_kwargs,
            image_downloader=image_downloader,
            md_filename=md_name,
        )

    # ------------------------------------------------------------------
    # 附件下载
    # ------------------------------------------------------------------

    def _download_topic_files(self, detail: Dict[str, Any], topic_dir: str) -> Dict[str, str]:
        """下载话题附件到 topic_dir/files/，返回 file_id -> 相对路径 映射"""
        files_dir = os.path.join(topic_dir, "files")
        os.makedirs(files_dir, exist_ok=True)

        talk = detail.get("talk") or {}
        files = talk.get("files") or []
        file_map: Dict[str, str] = {}

        for f in files:
            if self.is_stopped():
                break
            file_id = f.get("file_id")
            file_name = f.get("name") or f"file_{file_id}"
            if file_id is None:
                continue

            safe_name = safe_filename(file_name, max_length=120)
            if not safe_name:
                safe_name = f"file_{file_id}"

            target_path = os.path.join(files_dir, safe_name)

            if not os.path.exists(target_path):
                if self._download_single_file(int(file_id), target_path):
                    file_map[str(file_id)] = f"./files/{safe_name}"
                    self.log(f"   📎 已下载: {file_name}")
                else:
                    self.log(f"   ⚠️ 下载失败: {file_name}")
            else:
                file_map[str(file_id)] = f"./files/{safe_name}"

        return file_map

    def _download_single_file(self, file_id: int, output_path: str) -> bool:
        """下载单个文件到指定路径"""
        if self.is_stopped():
            return False

        download_url = self._get_download_url(file_id)
        if not download_url:
            return False

        try:
            stream_headers = build_zsxq_file_stream_headers(self.cookie, self.group_id, include_cookie=False)
            response = self.session.get(download_url, headers=stream_headers, timeout=300, stream=True)

            if self.is_stopped():
                response.close()
                return False

            if response.status_code in [401, 403]:
                response.close()
                stream_headers = build_zsxq_file_stream_headers(self.cookie, self.group_id, include_cookie=True)
                response = self.session.get(download_url, headers=stream_headers, timeout=300, stream=True)

            if response.status_code == 200:
                with open(output_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if self.is_stopped():
                            return False
                        if chunk:
                            f.write(chunk)
                return True
            else:
                self.log(f"   ❌ 文件下载 HTTP {response.status_code}: file_id={file_id}")
                return False
        except Exception as e:
            self.log(f"   ❌ 文件下载异常: file_id={file_id}, error={e}")
            return False

    def _get_download_url(self, file_id: int) -> Optional[str]:
        """获取文件下载链接"""
        url = f"{self.base_url}/v2/files/{file_id}/download_url"
        max_retries = ensure_global_max_retries(5)

        for attempt in range(max_retries):
            if self.is_stopped():
                return None
            if attempt > 0:
                import time as _time
                import random as _random
                _time.sleep(_random.uniform(15, 30))

            headers = build_zsxq_mobile_headers(self.cookie, self.group_id)
            try:
                response = self.session.get(url, headers=headers, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    if data.get("succeeded"):
                        dl_url = data.get("resp_data", {}).get("download_url")
                        if dl_url:
                            return dl_url
                    else:
                        error_code = data.get("code")
                        error_msg = data.get("message", data.get("error", ""))
                        if is_mobile_only_error(error_code, error_msg):
                            return None
                        if should_retry_api_code(error_code, attempt, max_retries):
                            import time as _time
                            _time.sleep(retry_wait_seconds(attempt))
                            continue
                        return None
                elif response.status_code in [429, 500, 502, 503, 504]:
                    if attempt < max_retries - 1:
                        continue
                return None
            except Exception:
                if attempt < max_retries - 1:
                    continue
                return None

        return None

    # ------------------------------------------------------------------
    # 工具方法
    # ------------------------------------------------------------------

    def _unique_folder_name(self, title: str, used: set) -> str:
        base = safe_filename(title, max_length=80)
        if not base:
            base = "untitled"
        name = base
        counter = 2
        while name in used:
            name = f"{base}_{counter}"
            counter += 1
        return name

    def _zip_directory(self, source_dir: str, zip_path: str) -> str:
        """将目录打包为 ZIP"""
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for root, _dirs, files in os.walk(source_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, source_dir)
                    zf.write(file_path, arcname)
        return zip_path

    def close(self):
        """关闭资源"""
        try:
            self.db.close()
        except Exception:
            pass
