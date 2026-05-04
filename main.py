"""项目根目录后端启动入口。

实际 FastAPI 应用位于 backend.main；保留该文件用于兼容 `uv run main.py`。
"""

import sys

import uvicorn

from backend.main import app


def main() -> None:
    """启动 FastAPI 后端服务。"""
    port = 8208
    if len(sys.argv) > 2 and sys.argv[1] == "--port":
        try:
            port = int(sys.argv[2])
        except ValueError:
            port = 8208
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
