#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
知识星球 API 全局重试策略。

所有接口只要返回 code=1059，都按统一策略最多重试 10 次，避免各业务模块漏处理。
"""

from typing import Any, Optional


ANTI_CRAWL_ERROR_CODE = 1059
GLOBAL_API_RETRY_CODES = {ANTI_CRAWL_ERROR_CODE}
GLOBAL_API_MAX_RETRIES = 10


def normalize_error_code(error_code: Any) -> Optional[int]:
    """把 API 返回的错误码统一转换为 int，兼容字符串错误码。"""
    try:
        return int(error_code)
    except (TypeError, ValueError):
        return None


def is_global_retry_code(error_code: Any) -> bool:
    """判断错误码是否命中全局重试策略。"""
    return normalize_error_code(error_code) in GLOBAL_API_RETRY_CODES


def ensure_global_max_retries(max_retries: Optional[int] = None) -> int:
    """确保全局重试错误码至少拥有 10 次重试机会。"""
    try:
        retries = int(max_retries or 0)
    except (TypeError, ValueError):
        retries = 0
    return max(retries, GLOBAL_API_MAX_RETRIES)


def retry_wait_seconds(retry_index: int) -> int:
    """统一的递增等待策略，retry_index 为 0 基。"""
    if retry_index < 3:
        return 2
    if retry_index < 6:
        return 5
    return 10


def should_retry_api_code(error_code: Any, retry_index: int, max_retries: Optional[int] = None) -> bool:
    """判断本次 API 错误码是否应该继续重试。"""
    return (
        is_global_retry_code(error_code)
        and retry_index < ensure_global_max_retries(max_retries) - 1
    )


def retryable_error_codes(*extra_codes: int) -> set:
    """返回包含全局错误码的可重试错误码集合。"""
    return GLOBAL_API_RETRY_CODES.union(extra_codes)


def is_retryable_error_code(error_code: Any, *extra_codes: int) -> bool:
    """判断错误码是否属于全局或调用方补充的可重试错误码。"""
    normalized = normalize_error_code(error_code)
    return normalized in retryable_error_codes(*extra_codes)
