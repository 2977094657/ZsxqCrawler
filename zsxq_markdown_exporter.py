#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Markdown export helpers for ZSXQ content."""

import html
import os
import re
import shutil
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import unquote


def safe_filename(name: str, max_length: int = 80) -> str:
    """Return a filename that is safe across common operating systems."""
    cleaned = re.sub(r'[\\/:*?"<>|\r\n\t]+', "_", name or "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .")
    if not cleaned:
        cleaned = "untitled"
    return cleaned[:max_length].rstrip(" .") or "untitled"


# ----------------------- ZSXQ 自定义 <e ... /> 标签解析 -----------------------
# 知识星球的话题/评论文本中包含一类自定义标签，例如：
#   <e type="text_bold" title="..." />
#   <e type="hashtag" hid="..." title="..." />
#   <e type="web_url" href="..." title="..." />
#   <e type="mention" uid="..." title="..." />
# 这些 title/href 都是 URL 编码字符串。早期 html_to_markdown 直接把所有未知 HTML 标签
# 用正则吞掉，导致这些内容（包含正文链接和提及）在 Markdown 导出中完全丢失。
# 该函数把它们转换为标准 Markdown / HTML 片段，再交给 html_to_markdown 处理。

_E_TAG_RE = re.compile(r"<e\s+([^/>]+?)/?\s*>", flags=re.IGNORECASE | re.DOTALL)
_E_ATTR_RE = re.compile(r'(\w+)\s*=\s*"([^"]*)"')


def _decode_zsxq_attr(value: Optional[str]) -> str:
    """ZSXQ 标签里的 title/href 通常是 percent-encoded 的，需要解码。"""
    if not value:
        return ""
    try:
        return unquote(value)
    except Exception:
        return value


def _render_zsxq_inline_tags(text: str) -> str:
    """把 ZSXQ 自定义内联标签转换为标准 Markdown / HTML 片段。

    保持纯文本风格输出，便于 Markdown 渲染器（GitHub、Typora、VS Code）正确显示。
    """
    if not text or "<e" not in text:
        return text or ""

    def _replace(match: re.Match) -> str:
        attrs_str = match.group(1) or ""
        attrs = {k.lower(): v for k, v in _E_ATTR_RE.findall(attrs_str)}
        etype = (attrs.get("type") or "").lower()
        title = _decode_zsxq_attr(attrs.get("title"))
        href = _decode_zsxq_attr(attrs.get("href"))
        hid = attrs.get("hid") or ""

        if etype == "text_bold":
            return f"**{title}**" if title else ""
        if etype == "text_italic":
            return f"*{title}*" if title else ""
        if etype == "text_strikethrough":
            return f"~~{title}~~" if title else ""
        if etype == "text_underline":
            return f"<u>{title}</u>" if title else ""
        if etype == "hashtag":
            clean_title = title.strip("#") if title else ""
            if clean_title and hid:
                tag_url = f"https://wx.zsxq.com/tags/{clean_title}/{hid}"
                return f"[#{clean_title}]({tag_url})"
            return f"#{clean_title}" if clean_title else ""
        if etype == "mention":
            clean_title = (title or "").lstrip("@")
            return f"@{clean_title}" if clean_title else ""
        if etype in ("web_url", "web"):
            label = title or href
            if href:
                return f"[{label}]({href})"
            return label or ""

        # 未知类型：尽量保留 title 文本，避免内容丢失
        if title:
            return title
        if href:
            return href
        return ""

    return _E_TAG_RE.sub(_replace, text)


def html_to_markdown(text: Optional[str]) -> str:
    """Convert common HTML snippets returned by ZSXQ APIs to Markdown."""
    if not text:
        return ""

    result = str(text).replace("\r\n", "\n").replace("\r", "\n")
    # 先把 ZSXQ 自定义 <e ... /> 标签转成 Markdown 片段，避免后面被通用 <[^>]+> 正则吞掉
    result = _render_zsxq_inline_tags(result)
    result = re.sub(r"<br\s*/?>", "\n", result, flags=re.IGNORECASE)
    result = re.sub(r"</p\s*>", "\n\n", result, flags=re.IGNORECASE)
    result = re.sub(r"<p[^>]*>", "", result, flags=re.IGNORECASE)
    result = re.sub(r"<strong[^>]*>(.*?)</strong>", r"**\1**", result, flags=re.IGNORECASE | re.DOTALL)
    result = re.sub(r"<b[^>]*>(.*?)</b>", r"**\1**", result, flags=re.IGNORECASE | re.DOTALL)
    result = re.sub(r"<em[^>]*>(.*?)</em>", r"*\1*", result, flags=re.IGNORECASE | re.DOTALL)
    result = re.sub(r"<i[^>]*>(.*?)</i>", r"*\1*", result, flags=re.IGNORECASE | re.DOTALL)
    result = re.sub(
        r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
        lambda m: f"[{re.sub(r'<[^>]+>', '', m.group(2)).strip() or m.group(1)}]({m.group(1)})",
        result,
        flags=re.IGNORECASE | re.DOTALL,
    )
    # 仅保留 <u> 包裹（前面 _render_zsxq_inline_tags 用到了），其他剩余 HTML 标签清掉
    result = re.sub(r"<(?!/?u\b)[^>]+>", "", result)
    result = html.unescape(result)
    result = re.sub(r"\n{3,}", "\n\n", result)
    return result.strip()


# ----------------------- 元信息 / 头像 / 时间格式 -----------------------

def _format_create_time(value: Optional[str]) -> str:
    """把 ISO8601 时间转换为 2024/02/05 这种紧凑格式，解析失败则原样返回。"""
    if not value:
        return ""
    try:
        normalized = str(value).replace("Z", "+00:00")
        if re.search(r"[+-]\d{4}$", normalized):
            normalized = normalized[:-5] + normalized[-5:-2] + ":" + normalized[-2:]
        dt = datetime.fromisoformat(normalized)
        return dt.strftime("%Y/%m/%d")
    except Exception:
        return str(value).split("T", 1)[0].replace("-", "/")


def _format_create_time_long(value: Optional[str]) -> str:
    """评论用的稍长时间格式 2024/02/18 17:43。"""
    if not value:
        return ""
    try:
        normalized = str(value).replace("Z", "+00:00")
        if re.search(r"[+-]\d{4}$", normalized):
            normalized = normalized[:-5] + normalized[-5:-2] + ":" + normalized[-2:]
        dt = datetime.fromisoformat(normalized)
        return dt.strftime("%Y/%m/%d %H:%M")
    except Exception:
        return str(value).split(".", 1)[0].replace("T", " ")


def _avatar_md(url: str, size: int = 20) -> str:
    """渲染 Markdown 中圆形头像（GitHub / Typora / VS Code 都支持内联 HTML img）。"""
    if not url:
        return ""
    safe_url = html.escape(url, quote=True)
    return (
        f'<img src="{safe_url}" width="{size}" height="{size}" '
        f'style="border-radius:50%; vertical-align:middle;" alt="avatar" />'
    )


def _format_owner_inline(owner: Optional[Dict[str, Any]], avatar_size: int = 20) -> str:
    """渲染 `头像 + 姓名` 的内联片段（用于元信息行 / 评论标题）。"""
    if not owner:
        return ""
    name = owner.get("name") or owner.get("alias") or "匿名用户"
    avatar = _avatar_md(owner.get("avatar_url") or "", size=avatar_size)
    if avatar:
        return f"{avatar} **{name}**"
    return f"**{name}**"


def _meta_line(detail: Dict[str, Any]) -> str:
    """生成顶部元信息行，对齐图 1 视觉：作者 · 日期 · ❤ · 💬 · 👁。"""
    talk = detail.get("talk") or {}
    owner = talk.get("owner") or detail.get("owner") or {}
    parts: List[str] = []

    inline_owner = _format_owner_inline(owner, avatar_size=24)
    if inline_owner:
        parts.append(inline_owner)

    create_time = _format_create_time(detail.get("create_time"))
    if create_time:
        parts.append(f"🕒 {create_time}")

    likes_count = detail.get("likes_count") or 0
    if likes_count:
        parts.append(f"❤ {likes_count}")

    comments_count = detail.get("comments_count") or 0
    if comments_count:
        parts.append(f"💬 {comments_count}")

    readers_count = detail.get("readers_count") or detail.get("reading_count") or 0
    if readers_count:
        parts.append(f"👁 {readers_count}")

    return " · ".join(parts)


def article_html_to_markdown(page_html: str, fallback_title: str = "") -> str:
    """Convert a fetched article HTML page to Markdown as a best effort."""
    if not page_html:
        return ""

    html_text = re.sub(r"<script[^>]*>.*?</script>", "", page_html, flags=re.IGNORECASE | re.DOTALL)
    html_text = re.sub(r"<style[^>]*>.*?</style>", "", html_text, flags=re.IGNORECASE | re.DOTALL)

    title = fallback_title
    title_match = re.search(r"<title[^>]*>(.*?)</title>", html_text, flags=re.IGNORECASE | re.DOTALL)
    if title_match:
        title = html.unescape(re.sub(r"<[^>]+>", "", title_match.group(1))).strip() or title

    # Prefer article/main/body content; fall back to the whole HTML document.
    body = html_text
    for pattern in (r"<article[^>]*>(.*?)</article>", r"<main[^>]*>(.*?)</main>", r"<body[^>]*>(.*?)</body>"):
        match = re.search(pattern, html_text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            body = match.group(1)
            break

    body = re.sub(
        r'<img[^>]+src=["\']([^"\']+)["\'][^>]*>',
        lambda m: f"\n\n![]({m.group(1)})\n\n",
        body,
        flags=re.IGNORECASE | re.DOTALL,
    )
    body = re.sub(r"<h1[^>]*>(.*?)</h1>", r"\n# \1\n", body, flags=re.IGNORECASE | re.DOTALL)
    body = re.sub(r"<h2[^>]*>(.*?)</h2>", r"\n## \1\n", body, flags=re.IGNORECASE | re.DOTALL)
    body = re.sub(r"<h3[^>]*>(.*?)</h3>", r"\n### \1\n", body, flags=re.IGNORECASE | re.DOTALL)
    body = re.sub(r"<li[^>]*>(.*?)</li>", r"\n- \1", body, flags=re.IGNORECASE | re.DOTALL)

    markdown = html_to_markdown(body)
    if title and not markdown.lstrip().startswith("#"):
        markdown = f"# {html_to_markdown(title)}\n\n{markdown}".strip()
    return markdown + "\n" if markdown else ""


# ----------------------- 图片 / 文件 / 评论渲染 -----------------------
# 所有渲染函数支持可选的 asset_resolver(url, kind) → str 回调：
#   - 默认（None）：直接使用原始远程 URL，生成单 .md 文件
#   - ZIP 模式：把头像/图片下载到 zip 内 assets/ 目录，返回相对路径

AssetResolver = Callable[[str, str], str]


def _image_url(image: Dict[str, Any]) -> str:
    for key in ("original", "large", "thumbnail"):
        value = image.get(key) or {}
        if value.get("url"):
            return value["url"]
    return image.get("url") or ""


def _resolve(url: str, kind: str, resolver: Optional[AssetResolver]) -> str:
    """统一调用 asset_resolver，失败时退化为原 URL。"""
    if not url:
        return ""
    if resolver is None:
        return url
    try:
        return resolver(url, kind) or url
    except Exception:
        return url


def _avatar_md_resolved(owner: Optional[Dict[str, Any]], resolver: Optional[AssetResolver],
                        size: int = 20) -> str:
    if not owner:
        return ""
    raw = owner.get("avatar_url") or ""
    if not raw:
        return ""
    return _avatar_md(_resolve(raw, "avatar", resolver), size=size)


def _format_owner_inline_resolved(owner: Optional[Dict[str, Any]],
                                  resolver: Optional[AssetResolver],
                                  avatar_size: int = 20) -> str:
    if not owner:
        return ""
    name = owner.get("name") or owner.get("alias") or "匿名用户"
    avatar = _avatar_md_resolved(owner, resolver, size=avatar_size)
    if avatar:
        return f"{avatar} **{name}**"
    return f"**{name}**"


def _append_images(lines: List[str], images: Optional[List[Dict[str, Any]]],
                   title: str = "图片", resolver: Optional[AssetResolver] = None) -> None:
    if not images:
        return
    lines.append("")
    lines.append(f"### {title}")
    lines.append("")
    for index, image in enumerate(images, 1):
        url = _image_url(image)
        if not url:
            continue
        resolved = _resolve(url, "image", resolver)
        lines.append(f"![{title} {index}]({resolved})")
        lines.append("")


def _append_files(lines: List[str], files: Optional[List[Dict[str, Any]]]) -> None:
    if not files:
        return
    lines.append("")
    lines.append("### 附件")
    lines.append("")
    for file_info in files:
        name = file_info.get("name") or f"file_{file_info.get('file_id', '')}"
        parts = [f"📎 **{name}**"]
        size = file_info.get("size")
        if size:
            try:
                size_mb = int(size) / (1024 * 1024)
                if size_mb >= 1:
                    parts.append(f"{size_mb:.2f} MB")
                else:
                    parts.append(f"{int(size) / 1024:.1f} KB")
            except Exception:
                parts.append(f"{size} bytes")
        if file_info.get("download_count") is not None:
            parts.append(f"下载 {file_info.get('download_count')} 次")
        lines.append(f"- {' · '.join(parts)}")


def _format_meta_inline_for_comment(comment: Dict[str, Any]) -> str:
    """评论内的小元信息：时间 + 点赞数。"""
    parts: List[str] = []
    create_time = _format_create_time_long(comment.get("create_time"))
    if create_time:
        parts.append(create_time)
    likes = comment.get("likes_count") or 0
    if likes:
        parts.append(f"❤ {likes}")
    return " · ".join(parts)


def _append_comments(lines: List[str], comments: Optional[List[Dict[str, Any]]],
                     resolver: Optional[AssetResolver] = None) -> None:
    """以 blockquote 嵌套呈现评论与回复，靠近图 1 的视觉层次。"""
    if not comments:
        return
    total = len(comments)
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append(f"## 💬 评论（{total}）")
    lines.append("")

    def append_one(comment: Dict[str, Any], depth: int = 0) -> None:
        owner_inline = _format_owner_inline_resolved(comment.get("owner") or {}, resolver, avatar_size=20)
        meta = _format_meta_inline_for_comment(comment)
        repliee = comment.get("repliee") or {}
        repliee_part = ""
        if repliee.get("name"):
            repliee_part = f" 回复 **{repliee.get('name')}**"

        prefix = "> " * (depth + 1)
        header_parts = [p for p in [owner_inline + repliee_part, meta] if p]
        lines.append(f"{prefix}{' · '.join(header_parts)}")

        # 评论引用块内的"空行"应该是 prefix 去尾空格后的纯 `>`（多层嵌套时为 `> >`），
        # 而不是再额外追加一个 `>`，否则会出现 `>>` / `> > >` 这类多一层的视觉错位。
        empty_quote_line = prefix.rstrip()

        text = html_to_markdown(comment.get("text"))
        if text:
            lines.append(empty_quote_line)
            for line in text.splitlines():
                lines.append(f"{prefix}{line}" if line else empty_quote_line)

        # 评论中的图片
        images = comment.get("images") or []
        for index, image in enumerate(images, 1):
            url = _image_url(image)
            if not url:
                continue
            resolved = _resolve(url, "image", resolver)
            lines.append(empty_quote_line)
            lines.append(f"{prefix}![评论图 {index}]({resolved})")

        for reply in comment.get("replied_comments") or []:
            lines.append("")
            append_one(reply, depth + 1)

    for index, comment in enumerate(comments):
        if index > 0:
            lines.append("")
        append_one(comment)


def topic_detail_to_markdown(detail: Dict[str, Any], source_url: Optional[str] = None,
                             *, asset_resolver: Optional[AssetResolver] = None) -> str:
    """将话题详情转换为 Markdown，对齐前端展示页的视觉风格。

    asset_resolver(url, kind) 用于把远程图片/头像 URL 转换为打包文件的相对路径，
    None 表示直接保留原 URL（导出单 .md 文件时使用）。
    """
    title = detail.get("title") or (detail.get("talk") or {}).get("article", {}).get("title") or f"topic_{detail.get('topic_id')}"
    lines: List[str] = []

    # 标题（话题精华用 ⭐ 前缀，置顶用 📌）
    title_prefix_parts: List[str] = []
    if detail.get("digested"):
        title_prefix_parts.append("⭐")
    if detail.get("sticky"):
        title_prefix_parts.append("📌")
    title_prefix = " ".join(title_prefix_parts)
    title_md = html_to_markdown(title)
    if title_prefix:
        lines.append(f"# {title_prefix} {title_md}")
    else:
        lines.append(f"# {title_md}")
    lines.append("")

    # 元信息行（作者头像 · 时间 · ❤ · 💬 · 👁）
    talk = detail.get("talk") or {}
    owner = talk.get("owner") or {}
    meta_parts: List[str] = []
    inline_owner = _format_owner_inline_resolved(owner, asset_resolver, avatar_size=24)
    if inline_owner:
        meta_parts.append(inline_owner)
    create_time = _format_create_time(detail.get("create_time"))
    if create_time:
        meta_parts.append(f"🕒 {create_time}")
    if detail.get("likes_count"):
        meta_parts.append(f"❤ {detail['likes_count']}")
    if detail.get("comments_count"):
        meta_parts.append(f"💬 {detail['comments_count']}")
    if detail.get("readers_count") or detail.get("reading_count"):
        meta_parts.append(f"👁 {detail.get('readers_count') or detail.get('reading_count')}")
    if meta_parts:
        lines.append(" · ".join(meta_parts))
        lines.append("")

    if source_url:
        lines.append(f"> 原文链接：<{source_url}>")
        lines.append("")

    # 主体内容
    if detail.get("type") == "q&a":
        question = detail.get("question") or {}
        answer = detail.get("answer") or {}
        if question.get("text"):
            lines.append("## ❓ 问题")
            lines.append("")
            lines.append(html_to_markdown(question.get("text")))
            lines.append("")
            _append_images(lines, question.get("images"), "问题图片", asset_resolver)
        if answer.get("text"):
            lines.append("## 💡 回答")
            lines.append("")
            lines.append(html_to_markdown(answer.get("text")))
            lines.append("")
            _append_images(lines, answer.get("images"), "回答图片", asset_resolver)
    else:
        body = html_to_markdown(talk.get("text") or "")
        if body:
            lines.append(body)
            lines.append("")
        article = talk.get("article") or {}
        article_url = article.get("article_url") or article.get("inline_article_url")
        if article_url:
            label = article.get("title") or article_url
            lines.append(f"🔗 **关联文章**：[{label}]({article_url})")
            lines.append("")
        _append_images(lines, talk.get("images"), "图片", asset_resolver)
        _append_files(lines, talk.get("files"))

    _append_comments(lines, detail.get("show_comments"), asset_resolver)

    return "\n".join(lines).strip() + "\n"


def column_topic_detail_to_markdown(detail: Dict[str, Any],
                                    *, asset_resolver: Optional[AssetResolver] = None) -> str:
    """将专栏文章详情转换为 Markdown。"""
    title = detail.get("title") or f"topic_{detail.get('topic_id')}"
    lines: List[str] = [f"# {html_to_markdown(title)}", ""]

    # 元信息
    owner = detail.get("owner") or (detail.get("question") or {}).get("owner") or {}
    meta_parts: List[str] = []
    inline_owner = _format_owner_inline_resolved(owner, asset_resolver, avatar_size=24)
    if inline_owner:
        meta_parts.append(inline_owner)
    create_time = _format_create_time(detail.get("create_time"))
    if create_time:
        meta_parts.append(f"🕒 {create_time}")
    if detail.get("likes_count"):
        meta_parts.append(f"❤ {detail['likes_count']}")
    if detail.get("comments_count"):
        meta_parts.append(f"💬 {detail['comments_count']}")
    if meta_parts:
        lines.append(" · ".join(meta_parts))
        lines.append("")

    if detail.get("type") == "q&a":
        question = detail.get("question") or {}
        answer = detail.get("answer") or {}
        if question.get("text"):
            lines.append("## ❓ 问题")
            lines.append("")
            lines.append(html_to_markdown(question.get("text")))
            lines.append("")
            _append_images(lines, question.get("images"), "问题图片", asset_resolver)
        if answer.get("text"):
            lines.append("## 💡 回答")
            lines.append("")
            lines.append(html_to_markdown(answer.get("text")))
            lines.append("")
            _append_images(lines, answer.get("images"), "回答图片", asset_resolver)
    elif detail.get("full_text"):
        lines.append(html_to_markdown(detail.get("full_text")))
        lines.append("")

    _append_images(lines, detail.get("images"), "图片", asset_resolver)
    _append_files(lines, detail.get("files"))
    _append_comments(lines, detail.get("comments"), asset_resolver)
    return "\n".join(lines).strip() + "\n"


def write_markdown_file(markdown: str, output_dir: str, filename_stem: str) -> str:
    """Write Markdown to a file and return the absolute path."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    path = Path(output_dir) / f"{safe_filename(filename_stem)}.md"
    path.write_text(markdown, encoding="utf-8")
    return str(path.resolve())


def write_temp_markdown_file(markdown: str, filename_stem: str) -> str:
    """Write Markdown to a temporary file for HTTP download responses."""
    tmp_dir = tempfile.mkdtemp(prefix="zsxq_md_")
    return write_markdown_file(markdown, tmp_dir, filename_stem)


# ----------------------- ZIP 归档：MD + assets/ -----------------------
# 为了让导出包"开箱即看"，我们把头像/正文图片/评论图片下载到 zip 内的 assets/ 目录，
# Markdown 中以相对路径 `./assets/xxx.jpg` 引用。

ImageDownloader = Callable[[str], Optional[Path]]


def _safe_asset_name(url: str, content_path: Optional[Path] = None) -> str:
    """根据 URL 和内容路径生成 zip 内 assets/ 下的文件名。
    使用 URL 的 md5 作为基名，避免重名；扩展名优先取实际下载到的文件，其次从 URL 推断。"""
    import hashlib

    digest = hashlib.md5(url.encode("utf-8")).hexdigest()[:16]
    ext = ""
    if content_path is not None:
        ext = content_path.suffix
    if not ext:
        # 从 URL 推断
        m = re.search(r"\.(jpe?g|png|gif|webp|bmp|svg)(?:\?|$)", url, flags=re.IGNORECASE)
        if m:
            ext = "." + m.group(1).lower()
    if not ext:
        ext = ".jpg"
    return f"{digest}{ext}"


def build_topic_archive(
    detail: Dict[str, Any],
    output_zip_path: str,
    *,
    render: Callable[..., str] = topic_detail_to_markdown,
    render_kwargs: Optional[Dict[str, Any]] = None,
    image_downloader: Optional[ImageDownloader] = None,
    md_filename: str = "README.md",
) -> str:
    """生成包含 Markdown + 资源文件的 zip 归档。

    image_downloader(url) -> Optional[Path]：下载并返回图片本地路径；返回 None 表示失败，
        此时 MD 中保留远程 URL，避免破坏链接。
    返回最终 zip 文件的路径。
    """
    render_kwargs = dict(render_kwargs or {})
    staging_dir = Path(tempfile.mkdtemp(prefix="zsxq_zip_"))
    assets_dir = staging_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)

    # url -> 相对路径 缓存，避免重复下载和命名冲突
    url_to_relpath: Dict[str, str] = {}

    def resolver(url: str, kind: str) -> str:  # kind: avatar / image / file
        if not url:
            return url
        cached = url_to_relpath.get(url)
        if cached is not None:
            return cached

        local_path: Optional[Path] = None
        if image_downloader is not None:
            try:
                local_path = image_downloader(url)
            except Exception:
                local_path = None

        if local_path is None or not Path(local_path).exists():
            # 下载失败 → 退化为远程 URL，仍然记录避免下次重试
            url_to_relpath[url] = url
            return url

        # 复制到 assets/，使用 md5 命名避免冲突；同源同 URL 仅复制一次
        target_name = _safe_asset_name(url, Path(local_path))
        target_path = assets_dir / target_name
        if not target_path.exists():
            try:
                shutil.copyfile(str(local_path), str(target_path))
            except Exception:
                # 复制失败时退化为远程 URL
                url_to_relpath[url] = url
                return url

        rel = f"./assets/{target_name}"
        url_to_relpath[url] = rel
        return rel

    # 渲染 Markdown
    render_kwargs["asset_resolver"] = resolver
    markdown = render(detail, **render_kwargs)

    # 写 Markdown 文件
    md_path = staging_dir / md_filename
    md_path.write_text(markdown, encoding="utf-8")

    # 打包 zip
    zip_path = Path(output_zip_path)
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(str(zip_path), "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(str(md_path), arcname=md_filename)
        for asset_file in sorted(assets_dir.iterdir()):
            if asset_file.is_file():
                zf.write(str(asset_file), arcname=f"assets/{asset_file.name}")

    # 清理 staging
    try:
        shutil.rmtree(str(staging_dir), ignore_errors=True)
    except Exception:
        pass

    return str(zip_path.resolve())


def write_temp_topic_archive(
    detail: Dict[str, Any],
    filename_stem: str,
    *,
    render: Callable[..., str] = topic_detail_to_markdown,
    render_kwargs: Optional[Dict[str, Any]] = None,
    image_downloader: Optional[ImageDownloader] = None,
) -> str:
    """Build a temporary zip archive for HTTP download."""
    tmp_dir = tempfile.mkdtemp(prefix="zsxq_zip_out_")
    safe_stem = safe_filename(filename_stem)
    zip_path = Path(tmp_dir) / f"{safe_stem}.zip"
    return build_topic_archive(
        detail,
        str(zip_path),
        render=render,
        render_kwargs=render_kwargs,
        image_downloader=image_downloader,
    )
