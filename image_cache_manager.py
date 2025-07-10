"""
图片缓存管理器
负责下载、缓存和提供本地图片服务
"""

import os
import hashlib
import requests
import mimetypes
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import urlparse
import time


class ImageCacheManager:
    """图片缓存管理器"""

    def __init__(self, cache_dir: str = "cache/images"):
        """
        初始化图片缓存管理器

        Args:
            cache_dir: 缓存目录路径
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # 支持的图片格式
        self.supported_formats = {
            'image/jpeg': '.jpg',
            'image/jpg': '.jpg',
            'image/png': '.png',
            'image/gif': '.gif',
            'image/webp': '.webp',
            'image/bmp': '.bmp'
        }
        
        # 默认请求头
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://wx.zsxq.com/',
            'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }
    
    def _get_cache_key(self, url: str) -> str:
        """
        根据URL生成缓存键
        
        Args:
            url: 图片URL
            
        Returns:
            缓存键（文件名前缀）
        """
        return hashlib.md5(url.encode('utf-8')).hexdigest()
    
    def _get_file_extension(self, content_type: str, url: str) -> str:
        """
        根据Content-Type和URL获取文件扩展名
        
        Args:
            content_type: HTTP响应的Content-Type
            url: 图片URL
            
        Returns:
            文件扩展名
        """
        # 优先使用Content-Type
        if content_type in self.supported_formats:
            return self.supported_formats[content_type]
        
        # 从URL路径推断
        parsed_url = urlparse(url)
        path = parsed_url.path.lower()
        for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']:
            if path.endswith(ext):
                return ext if ext != '.jpeg' else '.jpg'
        
        # 默认使用jpg
        return '.jpg'
    
    def _get_cache_path(self, url: str, content_type: str = None) -> Path:
        """
        获取缓存文件路径
        
        Args:
            url: 图片URL
            content_type: 内容类型
            
        Returns:
            缓存文件路径
        """
        cache_key = self._get_cache_key(url)
        
        # 如果已存在文件，直接返回
        for ext in ['.jpg', '.png', '.gif', '.webp', '.bmp']:
            existing_file = self.cache_dir / f"{cache_key}{ext}"
            if existing_file.exists():
                return existing_file
        
        # 生成新文件路径
        extension = self._get_file_extension(content_type or '', url)
        return self.cache_dir / f"{cache_key}{extension}"
    
    def is_cached(self, url: str) -> bool:
        """
        检查图片是否已缓存
        
        Args:
            url: 图片URL
            
        Returns:
            是否已缓存
        """
        if not url:
            return False
            
        cache_key = self._get_cache_key(url)
        
        # 检查是否存在任何格式的缓存文件
        for ext in ['.jpg', '.png', '.gif', '.webp', '.bmp']:
            cache_file = self.cache_dir / f"{cache_key}{ext}"
            if cache_file.exists():
                return True
        
        return False
    
    def get_cached_path(self, url: str) -> Optional[Path]:
        """
        获取已缓存图片的路径
        
        Args:
            url: 图片URL
            
        Returns:
            缓存文件路径，如果不存在则返回None
        """
        if not self.is_cached(url):
            return None
        
        cache_key = self._get_cache_key(url)
        
        # 查找存在的缓存文件
        for ext in ['.jpg', '.png', '.gif', '.webp', '.bmp']:
            cache_file = self.cache_dir / f"{cache_key}{ext}"
            if cache_file.exists():
                return cache_file
        
        return None
    
    def download_and_cache(self, url: str, timeout: int = 30) -> Tuple[bool, Optional[Path], Optional[str]]:
        """
        下载并缓存图片
        
        Args:
            url: 图片URL
            timeout: 请求超时时间
            
        Returns:
            (是否成功, 缓存文件路径, 错误信息)
        """
        if not url:
            return False, None, "URL为空"
        
        try:
            # 检查是否已缓存
            if self.is_cached(url):
                cached_path = self.get_cached_path(url)
                return True, cached_path, None
            
            # 下载图片
            response = requests.get(url, headers=self.headers, timeout=timeout, stream=True)
            response.raise_for_status()
            
            # 检查内容类型
            content_type = response.headers.get('content-type', '').lower()
            if not any(fmt in content_type for fmt in self.supported_formats.keys()):
                return False, None, f"不支持的图片格式: {content_type}"
            
            # 获取缓存路径
            cache_path = self._get_cache_path(url, content_type)
            
            # 保存文件
            with open(cache_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            return True, cache_path, None
            
        except requests.exceptions.RequestException as e:
            return False, None, f"下载失败: {str(e)}"
        except Exception as e:
            return False, None, f"缓存失败: {str(e)}"
    
    def get_cache_info(self) -> dict:
        """
        获取缓存统计信息
        
        Returns:
            缓存统计信息
        """
        if not self.cache_dir.exists():
            return {
                "total_files": 0,
                "total_size": 0,
                "cache_dir": str(self.cache_dir)
            }
        
        total_files = 0
        total_size = 0
        
        for file_path in self.cache_dir.iterdir():
            if file_path.is_file():
                total_files += 1
                total_size += file_path.stat().st_size
        
        return {
            "total_files": total_files,
            "total_size": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "cache_dir": str(self.cache_dir)
        }
    
    def clear_cache(self) -> Tuple[bool, str]:
        """
        清空缓存
        
        Returns:
            (是否成功, 消息)
        """
        try:
            if not self.cache_dir.exists():
                return True, "缓存目录不存在"
            
            deleted_count = 0
            for file_path in self.cache_dir.iterdir():
                if file_path.is_file():
                    file_path.unlink()
                    deleted_count += 1
            
            return True, f"已删除 {deleted_count} 个缓存文件"
            
        except Exception as e:
            return False, f"清空缓存失败: {str(e)}"


# 全局缓存管理器实例字典，按群组ID存储
_cache_managers = {}


def get_image_cache_manager(group_id: str = None) -> ImageCacheManager:
    """
    获取图片缓存管理器实例

    Args:
        group_id: 群组ID，如果提供则使用群组专用缓存目录

    Returns:
        图片缓存管理器实例
    """
    global _cache_managers

    if group_id:
        # 使用群组专用缓存目录
        if group_id not in _cache_managers:
            from db_path_manager import get_db_path_manager
            path_manager = get_db_path_manager()
            # 在群组数据库目录下创建images子目录
            db_dir = path_manager.get_group_data_dir(group_id)
            cache_dir = db_dir / "images"
            _cache_managers[group_id] = ImageCacheManager(str(cache_dir))
        return _cache_managers[group_id]
    else:
        # 使用默认全局缓存目录
        if 'default' not in _cache_managers:
            _cache_managers['default'] = ImageCacheManager()
        return _cache_managers['default']


def clear_group_cache_manager(group_id: str):
    """清除指定群组的缓存管理器实例"""
    global _cache_managers
    if group_id in _cache_managers:
        del _cache_managers[group_id]
