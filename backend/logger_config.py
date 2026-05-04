"""
日志配置模块
使用 loguru 实现按日期分目录、info/error 分文件的日志系统
"""

import sys
from datetime import datetime
from pathlib import Path
from loguru import logger

# 移除默认的 handler
logger.remove()

# 日志根目录
LOG_ROOT = Path("output/logs")


def get_log_path(level: str) -> str:
    """
    获取日志文件路径
    格式: output/logs/年/月/日/日_level.log
    
    Args:
        level: 日志级别 (info, error)
    
    Returns:
        日志文件路径
    """
    now = datetime.now()
    log_dir = LOG_ROOT / str(now.year) / f"{now.month:02d}" / f"{now.day:02d}"
    log_dir.mkdir(parents=True, exist_ok=True)
    return str(log_dir / f"{now.day:02d}_{level}.log")


def setup_logger():
    """
    配置日志系统
    - 控制台输出所有级别
    - info.log 记录 INFO 及以上级别
    - error.log 只记录 ERROR 及以上级别
    """
    # 控制台输出 - 彩色格式
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="DEBUG",
        colorize=True,
    )
    
    # INFO 日志文件 - 记录所有 INFO 及以上级别
    logger.add(
        lambda msg: _write_to_dated_log(msg, "info"),
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="INFO",
        filter=lambda record: record["level"].no >= logger.level("INFO").no,
    )
    
    # ERROR 日志文件 - 只记录 ERROR 及以上级别，包含完整堆栈
    logger.add(
        lambda msg: _write_to_dated_log(msg, "error"),
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="ERROR",
        backtrace=True,
        diagnose=True,
    )
    
    return logger


def _write_to_dated_log(message: str, level: str):
    """
    写入按日期分目录的日志文件
    
    Args:
        message: 日志消息
        level: 日志级别 (info, error)
    """
    log_path = get_log_path(level)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(str(message))


def get_logger():
    """
    获取配置好的 logger 实例
    
    Returns:
        loguru logger 实例
    """
    return logger


# 模块加载时自动配置
_configured = False

def ensure_configured():
    """确保日志系统已配置"""
    global _configured
    if not _configured:
        setup_logger()
        _configured = True
        logger.info("日志系统初始化完成")


# 提供便捷的日志函数
def log_info(message: str, **kwargs):
    """记录 INFO 级别日志"""
    ensure_configured()
    logger.opt(depth=1).info(message, **kwargs)


def log_warning(message: str, **kwargs):
    """记录 WARNING 级别日志"""
    ensure_configured()
    logger.opt(depth=1).warning(message, **kwargs)


def log_error(message: str, exception: Exception = None, **kwargs):
    """
    记录 ERROR 级别日志
    
    Args:
        message: 错误消息
        exception: 异常对象，如果提供则记录完整堆栈
    """
    ensure_configured()
    if exception:
        logger.opt(depth=1, exception=exception).error(message, **kwargs)
    else:
        logger.opt(depth=1).error(message, **kwargs)


def log_exception(message: str, **kwargs):
    """记录异常，自动捕获当前堆栈"""
    ensure_configured()
    logger.opt(depth=1).exception(message, **kwargs)


def log_debug(message: str, **kwargs):
    """记录 DEBUG 级别日志"""
    ensure_configured()
    logger.opt(depth=1).debug(message, **kwargs)


def log_success(message: str, **kwargs):
    """记录 SUCCESS 级别日志"""
    ensure_configured()
    logger.opt(depth=1).success(message, **kwargs)

