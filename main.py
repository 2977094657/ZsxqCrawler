"""项目根目录后端启动入口。

实际 FastAPI 应用位于 backend.main；保留该文件用于兼容 `uv run main.py`。
"""

import sys

import uvicorn

from backend.main import app
from backend.logger_config import ensure_configured, log_info


def main() -> None:
    """启动 FastAPI 后端服务。"""
    ensure_configured()
    port = 8208
    if len(sys.argv) > 2 and sys.argv[1] == "--port":
        try:
            port = int(sys.argv[2])
        except ValueError:
            port = 8208
    log_info(f"启动后端服务: host=0.0.0.0, port={port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_config=None, access_log=True)


if __name__ == "__main__":
    main()
