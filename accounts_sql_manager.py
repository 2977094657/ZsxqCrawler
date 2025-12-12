#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
账号SQL管理器 - 使用SQLite存储账号信息
支持多账号管理、默认账号设置、群组账号分配等功能
"""

import os
import sqlite3
import threading
import time
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime

from db_path_manager import get_db_path_manager

_lock = threading.RLock()  # 使用可重入锁，避免同一线程重复获取锁导致死锁


def _ensure_dir(path: str):
    """确保目录存在"""
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def _now_iso() -> str:
    """当前时间 ISO 字符串"""
    return datetime.now().isoformat(timespec="seconds")


def _mask_cookie(cookie: str) -> str:
    """掩码显示 Cookie"""
    if not cookie:
        return ""
    tail = cookie[-8:] if len(cookie) >= 8 else cookie
    return f"***{tail}"


class AccountsSQLManager:
    """
    账号SQL管理器
    数据库存放路径：DatabasePathManager.get_config_db_path()
    表：accounts, group_account_map
    """

    def __init__(self, db_path: Optional[str] = None):
        pm = get_db_path_manager()
        self.db_path = db_path or pm.get_config_db_path()
        _ensure_dir(self.db_path)
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")
        self.cursor = self.conn.cursor()
        self._ensure_schema()

    def _ensure_schema(self):
        """创建表结构"""
        with _lock:
            # 账号表
            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS accounts (
                    id TEXT PRIMARY KEY,
                    name TEXT,
                    cookie TEXT NOT NULL,
                    is_default INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT
                )
                """
            )

            # 检查是否需要添加updated_at列（兼容旧表）
            self.cursor.execute("PRAGMA table_info(accounts)")
            columns = [row[1] for row in self.cursor.fetchall()]
            if "updated_at" not in columns:
                self.cursor.execute("ALTER TABLE accounts ADD COLUMN updated_at TEXT")
                self.conn.commit()

            # 群组账号映射表
            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS group_account_map (
                    group_id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    assigned_at TEXT NOT NULL,
                    FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
                )
                """
            )
            # 创建索引
            self.cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_accounts_is_default ON accounts(is_default)"
            )
            self.cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_group_account_map_account_id ON group_account_map(account_id)"
            )
            self.conn.commit()

    def get_accounts(self, mask_cookie: bool = True) -> List[Dict[str, Any]]:
        """
        获取所有账号列表
        mask_cookie: 是否对 cookie 进行掩码
        """
        with _lock:
            self.cursor.execute(
                """
                SELECT id, name, cookie, created_at, updated_at
                FROM accounts
                ORDER BY created_at ASC
                """
            )
            rows = self.cursor.fetchall()
            accounts = []
            for row in rows:
                acc = {
                    "id": row[0],
                    "name": row[1],
                    "cookie": _mask_cookie(row[2]) if mask_cookie else row[2],
                    "created_at": row[3],
                    "updated_at": row[4],
                }
                accounts.append(acc)
            return accounts

    def get_account_by_id(self, account_id: str, mask_cookie: bool = False) -> Optional[Dict[str, Any]]:
        """根据ID获取账号"""
        with _lock:
            self.cursor.execute(
                """
                SELECT id, name, cookie, created_at, updated_at
                FROM accounts
                WHERE id = ?
                """,
                (account_id,),
            )
            row = self.cursor.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "name": row[1],
                "cookie": _mask_cookie(row[2]) if mask_cookie else row[2],
                "created_at": row[3],
                "updated_at": row[4],
            }

    def add_account(self, cookie: str, name: Optional[str] = None) -> Dict[str, Any]:
        """新增账号"""
        if not cookie or not cookie.strip():
            raise ValueError("cookie cannot be empty")

        with _lock:
            # 生成账号ID
            account_id = f"acc_{int(time.time() * 1000)}"
            now = _now_iso()

            # 插入新账号
            self.cursor.execute(
                """
                INSERT INTO accounts (id, name, cookie, is_default, created_at)
                VALUES (?, ?, ?, 0, ?)
                """,
                (
                    account_id,
                    name or f"账号{int(time.time() * 1000) % 10000}",
                    cookie.strip(),
                    now,
                ),
            )
            self.conn.commit()

            return self.get_account_by_id(account_id, mask_cookie=False)

    def delete_account(self, account_id: str) -> bool:
        """删除账号，同时清理映射"""
        with _lock:
            # 检查账号是否存在
            account = self.get_account_by_id(account_id)
            if not account:
                return False

            # 删除账号（级联删除映射）
            self.cursor.execute("DELETE FROM accounts WHERE id = ?", (account_id,))
            self.conn.commit()
            return True

    def get_first_account(self, mask_cookie: bool = False) -> Optional[Dict[str, Any]]:
        """获取第一个账号（按创建时间排序）"""
        with _lock:
            self.cursor.execute(
                """
                SELECT id, name, cookie, created_at, updated_at
                FROM accounts
                ORDER BY created_at ASC
                LIMIT 1
                """
            )
            row = self.cursor.fetchone()
            if not row:
                return None

            return {
                "id": row[0],
                "name": row[1],
                "cookie": _mask_cookie(row[2]) if mask_cookie else row[2],
                "created_at": row[3],
                "updated_at": row[4],
            }

    def assign_group_account(self, group_id: str, account_id: str) -> Tuple[bool, str]:
        """将群组分配到指定账号"""
        if not group_id:
            return False, "group_id cannot be empty"

        with _lock:
            # 检查账号是否存在
            account = self.get_account_by_id(account_id)
            if not account:
                return False, "Account does not exist"

            # 插入或更新映射
            now = _now_iso()
            self.cursor.execute(
                """
                INSERT INTO group_account_map (group_id, account_id, assigned_at)
                VALUES (?, ?, ?)
                ON CONFLICT(group_id) DO UPDATE SET
                    account_id = excluded.account_id,
                    assigned_at = excluded.assigned_at
                """,
                (str(group_id), account_id, now),
            )
            self.conn.commit()
            return True, "Assignment successful"

    def get_group_account_mapping(self) -> Dict[str, str]:
        """获取群组与账号ID映射"""
        with _lock:
            self.cursor.execute("SELECT group_id, account_id FROM group_account_map")
            rows = self.cursor.fetchall()
            return {row[0]: row[1] for row in rows}

    def get_account_for_group(self, group_id: str, mask_cookie: bool = False) -> Optional[Dict[str, Any]]:
        """获取某群组使用的账号（优先映射，其次第一个账号）"""
        with _lock:
            # 先查询映射
            self.cursor.execute(
                """
                SELECT account_id FROM group_account_map
                WHERE group_id = ?
                """,
                (str(group_id),),
            )
            row = self.cursor.fetchone()

            if row:
                account = self.get_account_by_id(row[0], mask_cookie=mask_cookie)
                if account:
                    return account

            # 否则返回第一个账号
            return self.get_first_account(mask_cookie=mask_cookie)

    def get_account_summary_for_group(self, group_id: str) -> Optional[Dict[str, Any]]:
        """获取群组所属账号的摘要信息"""
        acc = self.get_account_for_group(group_id, mask_cookie=True)
        if not acc:
            return None
        return {
            "id": acc.get("id"),
            "name": acc.get("name"),
            "created_at": acc.get("created_at"),
            "cookie": acc.get("cookie"),  # 已掩码
        }

    def close(self):
        """关闭数据库连接"""
        with _lock:
            try:
                self.cursor.close()
            except Exception:
                pass
            try:
                self.conn.close()
            except Exception:
                pass


# 单例模式
_sql_manager_singleton: Optional[AccountsSQLManager] = None
_sql_manager_lock = threading.RLock()  # 使用可重入锁，避免同一线程重复获取锁导致死锁


def get_accounts_sql_manager() -> AccountsSQLManager:
    """获取账号SQL管理器单例"""
    global _sql_manager_singleton
    if _sql_manager_singleton is None:
        with _sql_manager_lock:
            if _sql_manager_singleton is None:
                _sql_manager_singleton = AccountsSQLManager()
    return _sql_manager_singleton
