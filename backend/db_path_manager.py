#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from typing import Dict, Any

_DEFAULT_CONFIG_TOML = """# 知识星球数据采集器配置文件
# 首次启动自动生成；请按需修改

[auth]
# 知识星球登录 Cookie（Web 模式可留空，推荐使用“账号管理”配置）
cookie = "your_cookie_here"
# 交互式命令行模式需要单个群组ID；Web 模式可留空
group_id = "your_group_id_here"

[download]
# 下载目录
dir = "downloads"

[database]
# 可选：自定义数据库路径；留空则由路径管理器自动管理
# path = ""
"""


class DatabasePathManager:
    """数据库路径管理器 - 统一管理所有数据库文件的存储位置"""
    
    def __init__(self, base_dir: str = "output/databases"):
        # backend 包位于项目根目录下，统一把上一级目录作为项目根目录。
        self.project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
        self._ensure_config_toml()

        # 确保使用项目根目录的绝对路径
        self.base_dir = base_dir if os.path.isabs(base_dir) else os.path.join(self.project_root, base_dir)

        self._ensure_base_dir()

    def _ensure_config_toml(self) -> None:
        """确保 config.toml 存在（不存在则创建默认模板）。"""
        config_path = os.path.join(self.project_root, "config.toml")
        if os.path.exists(config_path):
            return

        try:
            with open(config_path, "w", encoding="utf-8") as f:
                f.write(_DEFAULT_CONFIG_TOML)
        except Exception as e:
            # 不能因为写配置失败导致程序无法启动；后续 load_config 会给出提示
            print(f"⚠️ 无法自动创建 config.toml: {e}")
    
    def _ensure_base_dir(self):
        """确保基础目录存在"""
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir, exist_ok=True)
            print(f"📁 创建数据库目录: {self.base_dir}")
    
    def get_group_dir(self, group_id: str) -> str:
        """获取指定群组的数据库目录"""
        group_dir = os.path.join(self.base_dir, str(group_id))
        if not os.path.exists(group_dir):
            os.makedirs(group_dir, exist_ok=True)
            print(f"📁 创建群组目录: {group_dir}")
        return group_dir

    def get_group_data_dir(self, group_id: str):
        """获取指定群组的数据目录（返回Path对象）"""
        from pathlib import Path
        return Path(self.get_group_dir(group_id))
    
    def get_topics_db_path(self, group_id: str) -> str:
        """获取话题数据库路径"""
        group_dir = self.get_group_dir(group_id)
        return os.path.join(group_dir, f"zsxq_topics_{group_id}.db")
    
    def get_files_db_path(self, group_id: str) -> str:
        """获取文件数据库路径"""
        group_dir = self.get_group_dir(group_id)
        return os.path.join(group_dir, f"zsxq_files_{group_id}.db")
    
    def get_columns_db_path(self, group_id: str) -> str:
        """获取专栏数据库路径"""
        group_dir = self.get_group_dir(group_id)
        return os.path.join(group_dir, f"zsxq_columns_{group_id}.db")
    
    def get_config_db_path(self) -> str:
        """获取配置数据库路径（全局配置，不按群组分）"""
        return os.path.join(self.base_dir, "zsxq_config.db")
    
    def get_main_db_path(self, group_id: str) -> str:
        """获取主数据库路径（兼容旧版本）"""
        return self.get_topics_db_path(group_id)
    
    def list_group_databases(self, group_id: str) -> Dict[str, str]:
        """列出指定群组的所有数据库文件"""
        group_dir = self.get_group_dir(group_id)
        databases = {}
        
        # 话题数据库
        topics_db = self.get_topics_db_path(group_id)
        if os.path.exists(topics_db):
            databases['topics'] = topics_db
        
        # 文件数据库
        files_db = self.get_files_db_path(group_id)
        if os.path.exists(files_db):
            databases['files'] = files_db
        
        return databases
    
    def get_database_info(self, group_id: str) -> Dict[str, Any]:
        """获取数据库信息"""
        databases = self.list_group_databases(group_id)
        info = {
            'group_id': group_id,
            'group_dir': self.get_group_dir(group_id),
            'databases': {}
        }
        
        for db_type, db_path in databases.items():
            if os.path.exists(db_path):
                stat = os.stat(db_path)
                info['databases'][db_type] = {
                    'path': db_path,
                    'size': stat.st_size,
                    'modified': stat.st_mtime
                }
        
        return info
    
    def migrate_old_databases(self, group_id: str, old_paths: Dict[str, str]) -> Dict[str, str]:
        """迁移旧的数据库文件到新的目录结构"""
        migration_results = {}
        
        for db_type, old_path in old_paths.items():
            if not os.path.exists(old_path):
                continue
            
            if db_type == 'topics':
                new_path = self.get_topics_db_path(group_id)
            elif db_type == 'files':
                new_path = self.get_files_db_path(group_id)
            else:
                continue
            
            try:
                # 如果新路径已存在，备份
                if os.path.exists(new_path):
                    backup_path = f"{new_path}.backup"
                    os.rename(new_path, backup_path)
                    print(f"📦 备份现有数据库: {backup_path}")
                
                # 移动文件
                os.rename(old_path, new_path)
                migration_results[db_type] = {
                    'old_path': old_path,
                    'new_path': new_path,
                    'status': 'success'
                }
                print(f"✅ 迁移数据库: {old_path} -> {new_path}")
                
            except Exception as e:
                migration_results[db_type] = {
                    'old_path': old_path,
                    'new_path': new_path,
                    'status': 'failed',
                    'error': str(e)
                }
                print(f"❌ 迁移失败: {old_path} -> {new_path}, 错误: {e}")
        
        return migration_results
    
    def list_all_groups(self) -> list:
        """列出所有存在的群组ID"""
        groups = []
        if not os.path.exists(self.base_dir):
            return groups
        
        for item in os.listdir(self.base_dir):
            item_path = os.path.join(self.base_dir, item)
            if os.path.isdir(item_path) and item.isdigit():  # 群组ID目录
                # 检查是否有数据库文件
                topics_db = self.get_topics_db_path(item)
                if os.path.exists(topics_db):
                    groups.append({
                        'group_id': item,
                        'group_dir': item_path,
                        'topics_db': topics_db
                    })
        
        return groups
    
    def cleanup_empty_dirs(self):
        """清理空的群组目录"""
        if not os.path.exists(self.base_dir):
            return
        
        for item in os.listdir(self.base_dir):
            item_path = os.path.join(self.base_dir, item)
            if os.path.isdir(item_path) and item.isdigit():  # 群组ID目录
                if not os.listdir(item_path):  # 空目录
                    os.rmdir(item_path)
                    print(f"🗑️ 删除空目录: {item_path}")

# 全局实例
db_path_manager = DatabasePathManager()

def get_db_path_manager() -> DatabasePathManager:
    """获取数据库路径管理器实例"""
    return db_path_manager
