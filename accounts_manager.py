#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import threading
from typing import Dict, Any, List, Optional, Tuple

_lock = threading.Lock()


def _get_project_root() -> str:
    """
    在当前文件夹向上查找包含 config.toml 的目录，作为项目根目录。
    找不到则返回当前文件所在目录。
    """
    cur = os.path.abspath(os.path.dirname(__file__))
    while True:
        if os.path.exists(os.path.join(cur, "config.toml")):
            return cur
        parent = os.path.dirname(cur)
        if parent == cur:
            return os.path.abspath(os.path.dirname(__file__))
        cur = parent


_ACCOUNTS_FILE = os.path.join(_get_project_root(), "accounts.json")


def _ensure_store() -> None:
    """确保账户存储文件存在"""
    if not os.path.exists(_ACCOUNTS_FILE):
        with _lock:
            # 双重检查
            if not os.path.exists(_ACCOUNTS_FILE):
                data = {"accounts": [], "group_account_map": {}}
                _write_data(data)


def _read_data() -> Dict[str, Any]:
    """读取账户与映射数据"""
    _ensure_store()
    with _lock:
        try:
            with open(_ACCOUNTS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            # 文件损坏时重置
            data = {"accounts": [], "group_account_map": {}}
            _write_data(data)
            return data


def _write_data(data: Dict[str, Any]) -> None:
    """原子写入数据"""
    tmp_path = _ACCOUNTS_FILE + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, _ACCOUNTS_FILE)


def _mask_cookie(cookie: str) -> str:
    """掩码显示 Cookie"""
    if not cookie:
        return ""
    tail = cookie[-8:] if len(cookie) >= 8 else cookie
    return f"***{tail}"


def _now_iso() -> str:
    """当前时间 ISO 字符串"""
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())


def get_accounts(mask_cookie: bool = True) -> List[Dict[str, Any]]:
    """
    获取所有账号列表
    mask_cookie: 是否对 cookie 进行掩码
    """
    data = _read_data()
    accounts = data.get("accounts", [])
    if mask_cookie:
        safe = []
        for acc in accounts:
            acc_copy = dict(acc)
            acc_copy["cookie"] = _mask_cookie(acc_copy.get("cookie", ""))
            safe.append(acc_copy)
        return safe
    return accounts


def get_account_by_id(account_id: str, mask_cookie: bool = False) -> Optional[Dict[str, Any]]:
    """根据ID获取账号"""
    data = _read_data()
    for acc in data.get("accounts", []):
        if acc.get("id") == account_id:
            if mask_cookie:
                acc_cp = dict(acc)
                acc_cp["cookie"] = _mask_cookie(acc_cp.get("cookie", ""))
                return acc_cp
            return acc
    return None


def add_account(cookie: str, name: Optional[str] = None, make_default: bool = False) -> Dict[str, Any]:
    """新增账号"""
    if not cookie or not cookie.strip():
        raise ValueError("cookie 不能为空")

    data = _read_data()
    accounts = data.get("accounts", [])

    account_id = f"acc_{int(time.time() * 1000)}"
    acc = {
        "id": account_id,
        "name": name or f"账号{len(accounts) + 1}",
        "cookie": cookie.strip(),
        "created_at": _now_iso(),
        "is_default": False,
    }

    if make_default or len(accounts) == 0:
        # 设置为默认，并取消其他默认
        for a in accounts:
            a["is_default"] = False
        acc["is_default"] = True

    accounts.append(acc)
    data["accounts"] = accounts
    _write_data(data)
    return acc


def delete_account(account_id: str) -> bool:
    """删除账号，同时清理映射。如果删除默认账号，则将第一个账号设为默认（如存在）"""
    data = _read_data()
    accounts = data.get("accounts", [])
    group_map = data.get("group_account_map", {})

    idx = next((i for i, a in enumerate(accounts) if a.get("id") == account_id), None)
    if idx is None:
        return False

    was_default = accounts[idx].get("is_default", False)
    accounts.pop(idx)

    # 清理映射
    group_map = {gid: aid for gid, aid in group_map.items() if aid != account_id}

    # 若删除了默认账号，且仍有账号，则设置第一个为默认
    if was_default and accounts:
        for i, a in enumerate(accounts):
            a["is_default"] = (i == 0)

    data["accounts"] = accounts
    data["group_account_map"] = group_map
    _write_data(data)
    return True


def set_default_account(account_id: str) -> bool:
    """设置默认账号"""
    data = _read_data()
    accounts = data.get("accounts", [])
    if not any(a.get("id") == account_id for a in accounts):
        return False

    for a in accounts:
        a["is_default"] = (a.get("id") == account_id)

    data["accounts"] = accounts
    _write_data(data)
    return True


def get_default_account(mask_cookie: bool = False) -> Optional[Dict[str, Any]]:
    """获取默认账号"""
    data = _read_data()
    accounts = data.get("accounts", [])
    default = next((a for a in accounts if a.get("is_default")), None)
    if not default and accounts:
        default = accounts[0]
    if not default:
        return None
    if mask_cookie:
        cp = dict(default)
        cp["cookie"] = _mask_cookie(cp.get("cookie", ""))
        return cp
    return default


def assign_group_account(group_id: str, account_id: str) -> Tuple[bool, str]:
    """将群组分配到指定账号"""
    if not group_id:
        return False, "group_id 不能为空"

    data = _read_data()
    if not any(a.get("id") == account_id for a in data.get("accounts", [])):
        return False, "账号不存在"

    group_map = data.get("group_account_map", {})
    group_map[str(group_id)] = account_id
    data["group_account_map"] = group_map
    _write_data(data)
    return True, "分配成功"


def get_group_account_mapping() -> Dict[str, str]:
    """获取群组与账号ID映射"""
    data = _read_data()
    return data.get("group_account_map", {})


def get_account_for_group(group_id: str, mask_cookie: bool = False) -> Optional[Dict[str, Any]]:
    """获取某群组使用的账号（优先映射，其次默认）"""
    data = _read_data()
    group_map = data.get("group_account_map", {})
    acc_id = group_map.get(str(group_id))
    account = None
    if acc_id:
        account = next((a for a in data.get("accounts", []) if a.get("id") == acc_id), None)
    if not account:
        account = get_default_account(mask_cookie=False)
    if not account:
        return None
    if mask_cookie:
        cp = dict(account)
        cp["cookie"] = _mask_cookie(cp.get("cookie", ""))
        return cp
    return account


def get_account_summary_for_group(group_id: str) -> Optional[Dict[str, Any]]:
    """获取群组所属账号的摘要信息"""
    acc = get_account_for_group(group_id, mask_cookie=True)
    if not acc:
        return None
    return {
        "id": acc.get("id"),
        "name": acc.get("name"),
        "is_default": acc.get("is_default", False),
        "created_at": acc.get("created_at"),
        "cookie": acc.get("cookie"),  # 已掩码
    }