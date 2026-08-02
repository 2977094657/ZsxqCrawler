"""Local interactive web login for Knowledge Planet.

The login itself always happens in an official zsxq.com browser window.  This
module only observes the resulting zsxq.com cookies in an isolated Playwright
browser context and hands them to the application after the caller validates
them against the official API.
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional


AuthCallback = Callable[[str, Optional[str]], Optional[Dict[str, Any]]]


@dataclass
class _AuthJob:
    job_id: str
    requested_name: Optional[str]
    status: str = "starting"
    message: str = "正在打开知识星球官方登录页"
    account: Optional[Dict[str, Any]] = None
    created_at: float = field(default_factory=time.time)
    cancel_event: threading.Event = field(default_factory=threading.Event)


class WebAuthManager:
    """Run at most one visible local browser login at a time."""

    def __init__(self, on_authenticated: AuthCallback, timeout_seconds: int = 300):
        self._on_authenticated = on_authenticated
        self._timeout_seconds = timeout_seconds
        self._jobs: Dict[str, _AuthJob] = {}
        self._lock = threading.RLock()

    def start(self, requested_name: Optional[str] = None) -> Dict[str, Any]:
        with self._lock:
            for job in self._jobs.values():
                if job.status in {"starting", "awaiting_login"}:
                    raise RuntimeError("已有网页登录正在进行，请先完成或取消")

            job = _AuthJob(
                job_id=f"auth_{uuid.uuid4().hex}",
                requested_name=(requested_name or "").strip() or None,
            )
            self._jobs[job.job_id] = job

        threading.Thread(target=self._run, args=(job,), daemon=True).start()
        return self._public(job)

    def get(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            job = self._jobs.get(job_id)
            return self._public(job) if job else None

    def cancel(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return None
            if job.status in {"starting", "awaiting_login"}:
                job.cancel_event.set()
                job.status = "cancelled"
                job.message = "网页登录已取消"
            return self._public(job)

    def _update(self, job: _AuthJob, **values: Any) -> None:
        with self._lock:
            for key, value in values.items():
                setattr(job, key, value)

    @staticmethod
    def _public(job: _AuthJob) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "job_id": job.job_id,
            "status": job.status,
            "message": job.message,
        }
        if job.account is not None:
            result["account"] = job.account
        return result

    def _run(self, job: _AuthJob) -> None:
        try:
            from playwright.sync_api import sync_playwright
        except Exception:
            self._update(
                job,
                status="failed",
                message="网页登录组件未安装，请重新安装后端依赖",
            )
            return

        try:
            with sync_playwright() as playwright:
                browser = None
                launch_errors = []
                for channel in ("msedge", "chrome", None):
                    try:
                        kwargs = {"headless": False}
                        if channel:
                            kwargs["channel"] = channel
                        browser = playwright.chromium.launch(**kwargs)
                        break
                    except Exception as exc:
                        launch_errors.append(str(exc))

                if browser is None:
                    raise RuntimeError("无法启动 Edge/Chrome 登录窗口；请确认浏览器已安装")

                try:
                    context = browser.new_context()
                    page = context.new_page()
                    page.goto(
                        "https://www.zsxq.com/",
                        wait_until="domcontentloaded",
                        timeout=60_000,
                    )
                    self._update(
                        job,
                        status="awaiting_login",
                        message="请在弹出的知识星球官方页面完成扫码或手机号登录",
                    )

                    deadline = time.time() + self._timeout_seconds
                    last_cookie_header = ""
                    while time.time() < deadline:
                        if job.cancel_event.wait(1.5):
                            return

                        cookies = context.cookies("https://api.zsxq.com/")
                        cookie_header = "; ".join(
                            f"{item['name']}={item['value']}"
                            for item in cookies
                            if item.get("name") and item.get("value")
                        )
                        if not cookie_header or cookie_header == last_cookie_header:
                            continue
                        last_cookie_header = cookie_header

                        account = self._on_authenticated(cookie_header, job.requested_name)
                        if account:
                            self._update(
                                job,
                                status="completed",
                                message="网页登录成功，账号已自动导入",
                                account=account,
                            )
                            time.sleep(1)
                            return

                    self._update(
                        job,
                        status="expired",
                        message="登录等待已超时，请重新发起网页登录",
                    )
                finally:
                    browser.close()
        except Exception as exc:
            if job.cancel_event.is_set():
                return
            self._update(job, status="failed", message=f"网页登录失败: {exc}")
