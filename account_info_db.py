#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import sqlite3
import threading
from datetime import datetime
from typing import Optional, Dict, Any

from db_path_manager import get_db_path_manager

_lock = threading.Lock()


def _ensure_dir(path: str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


class AccountInfoDB:
    """
    账号信息数据库：持久化 /v3/users/self 的用户信息
    数据库存放路径：DatabasePathManager.get_config_db_path()
    表：accounts_self
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
        with _lock:
            self.cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS accounts_self (
                    account_id TEXT PRIMARY KEY,
                    uid TEXT,
                    name TEXT,
                    avatar_url TEXT,
                    location TEXT,
                    user_sid TEXT,
                    grade TEXT,
                    raw_json TEXT,
                    fetched_at TEXT
                )
                """
            )
            self.conn.commit()

    def upsert_self_info(
        self,
        account_id: str,
        self_info: Dict[str, Any],
        raw_json: Optional[Dict[str, Any]] = None,
    ):
        """
        保存/更新用户信息
        self_info 期望字段：uid, name, avatar_url, location, user_sid, grade
        """
        if not account_id:
            raise ValueError("account_id 不能为空")

        now = datetime.now().isoformat(timespec="seconds")
        raw_json_str = json.dumps(raw_json or {}, ensure_ascii=False)

        with _lock:
            self.cursor.execute(
                """
                INSERT INTO accounts_self (account_id, uid, name, avatar_url, location, user_sid, grade, raw_json, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    uid=excluded.uid,
                    name=excluded.name,
                    avatar_url=excluded.avatar_url,
                    location=excluded.location,
                    user_sid=excluded.user_sid,
                    grade=excluded.grade,
                    raw_json=excluded.raw_json,
                    fetched_at=excluded.fetched_at
                """,
                (
                    account_id,
                    self_info.get("uid"),
                    self_info.get("name"),
                    self_info.get("avatar_url"),
                    self_info.get("location"),
                    self_info.get("user_sid"),
                    self_info.get("grade"),
                    raw_json_str,
                    now,
                ),
            )
            self.conn.commit()

    def get_self_info(self, account_id: str) -> Optional[Dict[str, Any]]:
        if not account_id:
            return None
        with _lock:
            self.cursor.execute(
                """
                SELECT account_id, uid, name, avatar_url, location, user_sid, grade, raw_json, fetched_at
                FROM accounts_self
                WHERE account_id = ?
                """,
                (account_id,),
            )
            row = self.cursor.fetchone()
            if not row:
                return None
            return {
                "account_id": row[0],
                "uid": row[1],
                "name": row[2],
                "avatar_url": row[3],
                "location": row[4],
                "user_sid": row[5],
                "grade": row[6],
                "raw_json": self._safe_load_json(row[7]),
                "fetched_at": row[8],
            }

    def _safe_load_json(self, s: Optional[str]) -> Any:
        if not s:
            return None
        try:
            return json.loads(s)
        except Exception:
            return None

    def close(self):
        with _lock:
            try:
                self.cursor.close()
            except Exception:
                pass
            try:
                self.conn.close()
            except Exception:
                pass


_db_singleton: Optional[AccountInfoDB] = None
_db_lock = threading.Lock()


def get_account_info_db() -> AccountInfoDB:
    global _db_singleton
    if _db_singleton is None:
        with _db_lock:
            if _db_singleton is None:
                _db_singleton = AccountInfoDB()
    return _db_singleton