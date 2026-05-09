"""
日志配置模块。

目标：
- 统一接管 Loguru、标准 logging、uvicorn/FastAPI 日志。
- 将控制台、全量调试、业务信息、错误堆栈、任务日志分别落盘。
- 自动脱敏 Cookie、Token、Authorization 等敏感字段，避免排障日志泄露凭据。
"""

from __future__ import annotations

import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from loguru import logger


# 导入模块时先移除 Loguru 默认 handler，避免在 ensure_configured 前重复输出。
logger.remove()


LOG_ROOT = Path(os.getenv("ZSXQ_LOG_DIR", "output/logs"))
LOG_LEVEL = os.getenv("ZSXQ_LOG_LEVEL", "DEBUG").upper()
CONSOLE_LOG_LEVEL = os.getenv("ZSXQ_CONSOLE_LOG_LEVEL", "INFO").upper()
LOG_ROTATION = os.getenv("ZSXQ_LOG_ROTATION", "00:00")
LOG_RETENTION = os.getenv("ZSXQ_LOG_RETENTION", "30 days")
LOG_COMPRESSION = os.getenv("ZSXQ_LOG_COMPRESSION", "zip")
CAPTURE_PRINT = os.getenv("ZSXQ_CAPTURE_PRINT", "1").lower() not in {"0", "false", "no"}
DIAGNOSE_TRACEBACK = os.getenv("ZSXQ_LOG_DIAGNOSE", "0").lower() in {"1", "true", "yes"}

LOG_FORMAT = (
    "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | "
    "pid={process.id} thread={thread.name} | "
    "req={extra[request_id]} task={extra[task_id]} group={extra[group_id]} | "
    "{name}:{function}:{line} - {message}"
)

CONSOLE_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
    "task=<magenta>{extra[task_id]}</magenta> group=<magenta>{extra[group_id]}</magenta> - "
    "<level>{message}</level>"
)


_configured = False
_stdout_captured = False
_original_stdout = sys.stdout
_original_stderr = sys.stderr


SENSITIVE_PATTERNS = (
    # Cookie 通常是一整段分号分隔内容，日志中只保留字段名。
    (re.compile(r"(?i)((?:\"|')?cookie(?:\"|')?\s*[:=]\s*)([^\n\r]+)"), r"\1***"),
    # 常见认证头。
    (re.compile(r"(?i)((?:\"|')?authorization(?:\"|')?\s*[:=]\s*bearer\s+)[A-Za-z0-9._~+/=-]+"), r"\1***"),
    (re.compile(r"(?i)((?:\"|')?authorization(?:\"|')?\s*[:=]\s*)(?!bearer\b)([^\s,\n\r]+)"), r"\1***"),
    # URL 或 JSON 中的 token/signature。
    (re.compile(r"(?i)((?:access_)?token[\"'\s:=]+)([^,\"'\s}]+)"), r"\1***"),
    (re.compile(r"(?i)((?:x-)?signature[\"'\s:=]+)([^,\"'\s}]+)"), r"\1***"),
    (re.compile(r"(?i)(auth_key[\"'\s:=]+)([^,\"'\s}]+)"), r"\1***"),
)


def _normalize_level(level: str, default: str = "INFO") -> str:
    """校验日志级别，避免环境变量填错导致启动失败。"""
    try:
        logger.level(level)
        return level
    except Exception:
        return default


def redact_sensitive(value: Any) -> str:
    """对日志内容做基础脱敏。"""
    text = str(value)
    for pattern, replacement in SENSITIVE_PATTERNS:
        text = pattern.sub(replacement, text)
    return text


def _patch_record(record: dict[str, Any]) -> None:
    """补齐 extra 字段并对消息脱敏。"""
    record["message"] = redact_sensitive(record["message"])
    extra = record["extra"]
    extra.setdefault("request_id", "-")
    extra.setdefault("task_id", "-")
    extra.setdefault("group_id", "-")
    extra.setdefault("task_type", "-")


def _dated_log_path(file_name: str) -> str:
    """生成按日期分目录的 Loguru 文件路径模板。"""
    return str(LOG_ROOT / "{time:YYYY}" / "{time:MM}" / "{time:DD}" / file_name)


def get_log_path(level: str) -> str:
    """
    获取当天指定类型日志文件路径，兼容旧调用。

    Args:
        level: 日志类型，例如 app、debug、error、info。

    Returns:
        当天日志文件路径。
    """
    now = datetime.now()
    log_dir = LOG_ROOT / str(now.year) / f"{now.month:02d}" / f"{now.day:02d}"
    log_dir.mkdir(parents=True, exist_ok=True)
    return str(log_dir / f"{level}.log")


class InterceptHandler(logging.Handler):
    """将标准 logging 转发到 Loguru。"""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level: str | int = logger.level(record.levelname).name
        except Exception:
            level = record.levelno

        frame = logging.currentframe()
        depth = 2
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


class StreamToLogger:
    """把 print/stdout/stderr 输出拆行写入 Loguru。"""

    def __init__(self, level: str) -> None:
        self.level = level
        self._buffer = ""
        self.encoding = getattr(_original_stdout, "encoding", "utf-8")

    def write(self, message: str) -> int:
        if not message:
            return 0

        self._buffer += message
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            self._log_line(line)
        return len(message)

    def flush(self) -> None:
        if self._buffer:
            self._log_line(self._buffer)
            self._buffer = ""

    def isatty(self) -> bool:
        return False

    def _log_line(self, line: str) -> None:
        clean_line = line.rstrip()
        if clean_line:
            logger.opt(depth=3).log(self.level, clean_line)


def _configure_standard_logging() -> None:
    """接管标准 logging，覆盖 uvicorn/fastapi/requests 等库的默认 handler。"""
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)

    # 这些 logger 经常自带 handler，清空后统一向 root 传播，避免重复输出。
    for logger_name in (
        "uvicorn",
        "uvicorn.error",
        "uvicorn.access",
        "fastapi",
        "starlette",
        "requests",
        "urllib3",
    ):
        std_logger = logging.getLogger(logger_name)
        std_logger.handlers = []
        std_logger.propagate = True


def _install_print_capture() -> None:
    """接管 print 输出，确保历史 print 也能落盘。"""
    global _stdout_captured
    if _stdout_captured or not CAPTURE_PRINT:
        return

    sys.stdout = StreamToLogger("INFO")
    sys.stderr = StreamToLogger("ERROR")
    _stdout_captured = True


def setup_logger():
    """
    配置完整日志系统。

    文件结构：
    - output/logs/YYYY/MM/DD/app.log：INFO 及以上业务日志。
    - output/logs/YYYY/MM/DD/debug.log：DEBUG 及以上完整日志。
    - output/logs/YYYY/MM/DD/error.log：ERROR 及以上错误堆栈。
    - output/logs/YYYY/MM/DD/tasks/{task_id}.log：单任务日志。
    """
    logger.remove()
    LOG_ROOT.mkdir(parents=True, exist_ok=True)

    app_level = _normalize_level(LOG_LEVEL, "DEBUG")
    console_level = _normalize_level(CONSOLE_LOG_LEVEL, "INFO")

    logger.configure(patcher=_patch_record)

    # 控制台 sink 使用原始 stdout，避免 print 捕获后递归。
    console_sink = sys.__stdout__ or _original_stdout
    logger.add(
        console_sink,
        format=CONSOLE_FORMAT,
        level=console_level,
        colorize=True,
        enqueue=True,
        backtrace=True,
        diagnose=DIAGNOSE_TRACEBACK,
    )

    logger.add(
        _dated_log_path("debug.log"),
        format=LOG_FORMAT,
        level=app_level,
        rotation=LOG_ROTATION,
        retention=LOG_RETENTION,
        compression=LOG_COMPRESSION,
        encoding="utf-8",
        enqueue=True,
        backtrace=True,
        diagnose=DIAGNOSE_TRACEBACK,
    )

    logger.add(
        _dated_log_path("app.log"),
        format=LOG_FORMAT,
        level="INFO",
        rotation=LOG_ROTATION,
        retention=LOG_RETENTION,
        compression=LOG_COMPRESSION,
        encoding="utf-8",
        enqueue=True,
        backtrace=True,
        diagnose=DIAGNOSE_TRACEBACK,
    )

    logger.add(
        _dated_log_path("error.log"),
        format=LOG_FORMAT,
        level="ERROR",
        rotation=LOG_ROTATION,
        retention=LOG_RETENTION,
        compression=LOG_COMPRESSION,
        encoding="utf-8",
        enqueue=True,
        backtrace=True,
        diagnose=DIAGNOSE_TRACEBACK,
    )

    _configure_standard_logging()
    _install_print_capture()
    return logger


def ensure_configured():
    """确保日志系统只初始化一次。"""
    global _configured
    if not _configured:
        setup_logger()
        _configured = True
        logger.bind(task_id="-", group_id="-").info(
            "日志系统初始化完成: root={}, level={}, console_level={}, capture_print={}",
            LOG_ROOT,
            LOG_LEVEL,
            CONSOLE_LOG_LEVEL,
            CAPTURE_PRINT,
        )


def get_logger():
    """获取配置好的 Loguru logger 实例。"""
    ensure_configured()
    return logger


def bind_context(**extra: Any):
    """获取绑定了上下文字段的 logger。"""
    ensure_configured()
    return logger.bind(**extra)


def _task_log_path(task_id: str) -> Path:
    now = datetime.now()
    task_dir = LOG_ROOT / str(now.year) / f"{now.month:02d}" / f"{now.day:02d}" / "tasks"
    task_dir.mkdir(parents=True, exist_ok=True)
    safe_task_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", task_id)
    return task_dir / f"{safe_task_id}.log"


def log_task_event(
    task_id: str,
    message: str,
    *,
    level: str = "INFO",
    group_id: Optional[str] = None,
    task_type: Optional[str] = None,
) -> None:
    """记录任务日志，同时写入全局日志和单任务日志文件。"""
    ensure_configured()
    safe_level = _normalize_level(level.upper(), "INFO")
    safe_message = redact_sensitive(message)
    context_logger = logger.bind(
        task_id=task_id or "-",
        group_id=group_id or "-",
        task_type=task_type or "-",
    )
    context_logger.opt(depth=1).log(safe_level, safe_message)

    timestamp = f"{datetime.now():%Y-%m-%d %H:%M:%S.%f}"[:-3]
    line = (
        f"{timestamp} | {safe_level:<8} | task={task_id or '-'} "
        f"group={group_id or '-'} type={task_type or '-'} | {safe_message}\n"
    )
    with _task_log_path(task_id or "unknown").open("a", encoding="utf-8") as file_obj:
        file_obj.write(line)


def log_debug(message: str, **kwargs: Any) -> None:
    """记录 DEBUG 级别日志。"""
    ensure_configured()
    logger.opt(depth=1).debug(redact_sensitive(message), **kwargs)


def log_info(message: str, **kwargs: Any) -> None:
    """记录 INFO 级别日志。"""
    ensure_configured()
    logger.opt(depth=1).info(redact_sensitive(message), **kwargs)


def log_success(message: str, **kwargs: Any) -> None:
    """记录 SUCCESS 级别日志。"""
    ensure_configured()
    logger.opt(depth=1).success(redact_sensitive(message), **kwargs)


def log_warning(message: str, **kwargs: Any) -> None:
    """记录 WARNING 级别日志。"""
    ensure_configured()
    logger.opt(depth=1).warning(redact_sensitive(message), **kwargs)


def log_error(message: str, exception: Exception | None = None, **kwargs: Any) -> None:
    """
    记录 ERROR 级别日志。

    Args:
        message: 错误消息。
        exception: 异常对象，如果提供则记录完整堆栈。
    """
    ensure_configured()
    if exception:
        logger.opt(depth=1, exception=exception).error(redact_sensitive(message), **kwargs)
    else:
        logger.opt(depth=1).error(redact_sensitive(message), **kwargs)


def log_exception(message: str, **kwargs: Any) -> None:
    """记录异常，自动捕获当前堆栈。"""
    ensure_configured()
    logger.opt(depth=1).exception(redact_sensitive(message), **kwargs)
