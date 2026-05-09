#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
知识星球专栏数据库管理模块
用于存储专栏目录、文章和相关信息
"""

import sqlite3
import re
from typing import Dict, List, Any, Optional
from datetime import datetime
from urllib.parse import unquote


_ZSXQ_TAG_TITLE_RE = re.compile(r'<e\b[^>]*\btitle="([^"]*)"[^>]*\/?>')
_HTML_TAG_RE = re.compile(r'<[^>]+>')
_WHITESPACE_RE = re.compile(r'[ \t\r\f\v]+')


def _decode_zsxq_title(value: str) -> str:
    """解码知识星球标签中的 title 字段。"""
    try:
        return unquote(value or "")
    except Exception:
        return value or ""


def _plain_text_from_zsxq_text(text: Optional[str]) -> str:
    """把知识星球正文片段转换为可用于标题兜底的纯文本。"""
    if not text:
        return ""

    def replace_tag(match: re.Match) -> str:
        return _decode_zsxq_title(match.group(1))

    plain = _ZSXQ_TAG_TITLE_RE.sub(replace_tag, str(text))
    plain = _HTML_TAG_RE.sub("", plain)
    plain = (
        plain.replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
    )
    plain = "\n".join(_WHITESPACE_RE.sub(" ", line).strip() for line in plain.splitlines())
    return "\n".join(line for line in plain.splitlines() if line).strip()


def _short_title_from_text(text: Optional[str], max_length: int = 32) -> str:
    """从正文第一段生成短标题，避免前端显示“无标题”。"""
    plain = _plain_text_from_zsxq_text(text)
    if not plain:
        return ""
    first_line = plain.splitlines()[0].strip()
    if len(first_line) <= max_length:
        return first_line
    return f"{first_line[:max_length].rstrip()}..."


def derive_topic_title(topic_data: Dict[str, Any]) -> str:
    """根据不同接口返回结构推导话题标题。"""
    if not isinstance(topic_data, dict):
        return ""

    talk = topic_data.get("talk") or {}
    article = topic_data.get("article") or (talk.get("article") if isinstance(talk, dict) else {}) or {}
    ai_content = topic_data.get("ai_content") or {}

    # 优先使用服务端明确给出的标题，其次使用文章卡片或 AI 摘要标题。
    for candidate in (
        topic_data.get("title"),
        article.get("title") if isinstance(article, dict) else None,
        ai_content.get("title") if isinstance(ai_content, dict) else None,
    ):
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()

    # 部分 talk 类型在专栏列表中没有 title，但会带 text。
    for text_candidate in (
        topic_data.get("text"),
        talk.get("text") if isinstance(talk, dict) else None,
        (topic_data.get("question") or {}).get("text") if isinstance(topic_data.get("question"), dict) else None,
        (topic_data.get("answer") or {}).get("text") if isinstance(topic_data.get("answer"), dict) else None,
    ):
        fallback = _short_title_from_text(text_candidate)
        if fallback:
            return fallback

    return ""


class ZSXQColumnsDatabase:
    """知识星球专栏数据库管理器"""
    
    def __init__(self, db_path: str = "zsxq_columns.db"):
        """初始化数据库连接"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self._init_database()
    
    def _init_database(self):
        """初始化数据库表结构"""
        
        # 1. 专栏目录表 (columns)
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS columns (
                column_id INTEGER PRIMARY KEY,
                group_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                cover_url TEXT,
                topics_count INTEGER DEFAULT 0,
                create_time TEXT,
                last_topic_attach_time TEXT,
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 2. 专栏文章表 (column_topics) - 存储文章列表信息
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS column_topics (
                topic_id INTEGER PRIMARY KEY,
                column_id INTEGER NOT NULL,
                group_id INTEGER NOT NULL,
                title TEXT,
                text TEXT,
                create_time TEXT,
                attached_to_column_time TEXT,
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (column_id) REFERENCES columns (column_id)
            )
        ''')
        
        # 3. 文章详情表 (topic_details) - 存储完整的文章内容
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS topic_details (
                topic_id INTEGER PRIMARY KEY,
                group_id INTEGER NOT NULL,
                type TEXT,
                title TEXT,
                full_text TEXT,
                likes_count INTEGER DEFAULT 0,
                comments_count INTEGER DEFAULT 0,
                readers_count INTEGER DEFAULT 0,
                digested BOOLEAN DEFAULT FALSE,
                sticky BOOLEAN DEFAULT FALSE,
                create_time TEXT,
                modify_time TEXT,
                raw_json TEXT,
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 4. 用户表
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                alias TEXT,
                avatar_url TEXT,
                description TEXT,
                location TEXT,
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 5. 文章作者关联表
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS topic_owners (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                owner_type TEXT DEFAULT 'talk',
                FOREIGN KEY (topic_id) REFERENCES topic_details (topic_id),
                FOREIGN KEY (user_id) REFERENCES users (user_id),
                UNIQUE(topic_id, owner_type)
            )
        ''')
        
        # 6. 图片表
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS images (
                image_id INTEGER PRIMARY KEY,
                topic_id INTEGER NOT NULL,
                comment_id INTEGER,
                type TEXT,
                thumbnail_url TEXT,
                thumbnail_width INTEGER,
                thumbnail_height INTEGER,
                large_url TEXT,
                large_width INTEGER,
                large_height INTEGER,
                original_url TEXT,
                original_width INTEGER,
                original_height INTEGER,
                original_size INTEGER,
                local_path TEXT,
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (topic_id) REFERENCES topic_details (topic_id),
                FOREIGN KEY (comment_id) REFERENCES comments (comment_id)
            )
        ''')
        
        # 7. 文件表
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS files (
                file_id INTEGER PRIMARY KEY,
                topic_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                hash TEXT,
                size INTEGER,
                duration INTEGER,
                download_count INTEGER DEFAULT 0,
                create_time TEXT,
                download_status TEXT DEFAULT 'pending',
                local_path TEXT,
                download_time TIMESTAMP,
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (topic_id) REFERENCES topic_details (topic_id)
            )
        ''')
        
        # 8. 评论表
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS comments (
                comment_id INTEGER PRIMARY KEY,
                topic_id INTEGER NOT NULL,
                owner_user_id INTEGER,
                parent_comment_id INTEGER,
                repliee_user_id INTEGER,
                text TEXT,
                create_time TEXT,
                likes_count INTEGER DEFAULT 0,
                rewards_count INTEGER DEFAULT 0,
                replies_count INTEGER DEFAULT 0,
                sticky BOOLEAN DEFAULT FALSE,
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (topic_id) REFERENCES topic_details (topic_id),
                FOREIGN KEY (owner_user_id) REFERENCES users (user_id)
            )
        ''')
        
        # 8.5 视频表
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS videos (
                video_id INTEGER PRIMARY KEY,
                topic_id INTEGER NOT NULL,
                size INTEGER,
                duration INTEGER,
                cover_url TEXT,
                cover_width INTEGER,
                cover_height INTEGER,
                cover_local_path TEXT,
                video_url TEXT,
                download_status TEXT DEFAULT 'pending',
                local_path TEXT,
                download_time TIMESTAMP,
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (topic_id) REFERENCES topic_details (topic_id)
            )
        ''')
        
        # 9. 采集日志表
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS crawl_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER NOT NULL,
                crawl_type TEXT NOT NULL,
                start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                end_time TIMESTAMP,
                columns_count INTEGER DEFAULT 0,
                topics_count INTEGER DEFAULT 0,
                details_count INTEGER DEFAULT 0,
                files_count INTEGER DEFAULT 0,
                status TEXT DEFAULT 'running',
                error_message TEXT
            )
        ''')
        
        # 创建索引
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_column_topics_column_id ON column_topics (column_id)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_column_topics_group_id ON column_topics (group_id)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_topic_details_group_id ON topic_details (group_id)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_images_topic_id ON images (topic_id)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_files_topic_id ON files (topic_id)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_comments_topic_id ON comments (topic_id)')
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_videos_topic_id ON videos (topic_id)')

        # 迁移：为 images 表添加 comment_id 列（如果不存在）
        self.cursor.execute("PRAGMA table_info(images)")
        columns = [row[1] for row in self.cursor.fetchall()]
        if 'comment_id' not in columns:
            self.cursor.execute('ALTER TABLE images ADD COLUMN comment_id INTEGER')

        # 为 comment_id 创建索引（新表和迁移后的旧表都需要）
        self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_images_comment_id ON images (comment_id)')

        self.conn.commit()
        print(f"✅ 专栏数据库初始化完成: {self.db_path}")
    
    # ==================== 专栏目录操作 ====================
    
    def insert_column(self, group_id: int, column_data: Dict[str, Any]) -> Optional[int]:
        """插入或更新专栏目录"""
        if not column_data or not column_data.get('column_id'):
            return None
        
        statistics = column_data.get('statistics', {})
        
        self.cursor.execute('''
            INSERT OR REPLACE INTO columns 
            (column_id, group_id, name, cover_url, topics_count, create_time, last_topic_attach_time)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            column_data.get('column_id'),
            group_id,
            column_data.get('name', ''),
            column_data.get('cover_url'),
            statistics.get('topics_count', 0),
            column_data.get('create_time'),
            column_data.get('last_topic_attach_time')
        ))
        self.conn.commit()
        return column_data.get('column_id')
    
    def get_columns(self, group_id: int) -> List[Dict[str, Any]]:
        """获取群组的所有专栏目录"""
        self.cursor.execute('''
            SELECT column_id, group_id, name, cover_url, topics_count, 
                   create_time, last_topic_attach_time, imported_at
            FROM columns 
            WHERE group_id = ?
            ORDER BY create_time DESC
        ''', (group_id,))
        
        columns = []
        for row in self.cursor.fetchall():
            columns.append({
                'column_id': row[0],
                'group_id': row[1],
                'name': row[2],
                'cover_url': row[3],
                'topics_count': row[4],
                'create_time': row[5],
                'last_topic_attach_time': row[6],
                'imported_at': row[7]
            })
        return columns
    
    def get_column(self, column_id: int) -> Optional[Dict[str, Any]]:
        """获取单个专栏目录"""
        self.cursor.execute('''
            SELECT column_id, group_id, name, cover_url, topics_count,
                   create_time, last_topic_attach_time, imported_at
            FROM columns WHERE column_id = ?
        ''', (column_id,))
        
        row = self.cursor.fetchone()
        if row:
            return {
                'column_id': row[0],
                'group_id': row[1],
                'name': row[2],
                'cover_url': row[3],
                'topics_count': row[4],
                'create_time': row[5],
                'last_topic_attach_time': row[6],
                'imported_at': row[7]
            }
        return None
    
    # ==================== 专栏文章操作 ====================
    
    def insert_column_topic(self, column_id: int, group_id: int, topic_data: Dict[str, Any]) -> Optional[int]:
        """插入或更新专栏文章列表项"""
        if not topic_data or not topic_data.get('topic_id'):
            return None

        title = derive_topic_title(topic_data)
        
        self.cursor.execute('''
            INSERT OR REPLACE INTO column_topics 
            (topic_id, column_id, group_id, title, text, create_time, attached_to_column_time)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            topic_data.get('topic_id'),
            column_id,
            group_id,
            title or topic_data.get('title'),
            topic_data.get('text'),
            topic_data.get('create_time'),
            topic_data.get('attached_to_column_time')
        ))
        self.conn.commit()
        return topic_data.get('topic_id')
    
    def get_column_topics(self, column_id: int) -> List[Dict[str, Any]]:
        """获取专栏下的所有文章列表"""
        self.cursor.execute('''
            SELECT ct.topic_id, ct.column_id, ct.group_id, ct.title, ct.text, 
                   ct.create_time, ct.attached_to_column_time, ct.imported_at,
                   td.title as detail_title,
                   CASE WHEN td.topic_id IS NOT NULL THEN 1 ELSE 0 END as has_detail
            FROM column_topics ct
            LEFT JOIN topic_details td ON ct.topic_id = td.topic_id
            WHERE ct.column_id = ?
            ORDER BY ct.attached_to_column_time DESC
        ''', (column_id,))
        
        topics = []
        for row in self.cursor.fetchall():
            title = row[3] or row[8] or _short_title_from_text(row[4])
            topics.append({
                'topic_id': row[0],
                'column_id': row[1],
                'group_id': row[2],
                'title': title,
                'text': row[4],
                'create_time': row[5],
                'attached_to_column_time': row[6],
                'imported_at': row[7],
                'has_detail': bool(row[9])
            })
        return topics
    
    # ==================== 文章详情操作 ====================
    
    def insert_user(self, user_data: Dict[str, Any]) -> Optional[int]:
        """插入或更新用户信息"""
        if not user_data or not user_data.get('user_id'):
            return None
        
        self.cursor.execute('''
            INSERT OR REPLACE INTO users 
            (user_id, name, alias, avatar_url, description, location)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            user_data.get('user_id'),
            user_data.get('name', ''),
            user_data.get('alias'),
            user_data.get('avatar_url'),
            user_data.get('description'),
            user_data.get('location')
        ))
        return user_data.get('user_id')
    
    def insert_topic_detail(self, group_id: int, topic_data: Dict[str, Any], raw_json: str = None) -> Optional[int]:
        """插入或更新文章详情"""
        if not topic_data or not topic_data.get('topic_id'):
            return None
        
        topic_id = topic_data.get('topic_id')
        
        # 获取文本内容
        talk = topic_data.get('talk') or {}
        if not isinstance(talk, dict):
            talk = {}
        question = topic_data.get('question', {}) if isinstance(topic_data.get('question'), dict) else {}
        answer = topic_data.get('answer', {}) if isinstance(topic_data.get('answer'), dict) else {}
        full_text = talk.get('text', '')
        if not full_text:
            full_text = answer.get('text') or question.get('text') or ''
        title = derive_topic_title(topic_data)
        
        self.cursor.execute('''
            INSERT OR REPLACE INTO topic_details 
            (topic_id, group_id, type, title, full_text, likes_count, comments_count,
             readers_count, digested, sticky, create_time, modify_time, raw_json, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ''', (
            topic_id,
            group_id,
            topic_data.get('type'),
            title or topic_data.get('title'),
            full_text,
            topic_data.get('likes_count', 0),
            topic_data.get('comments_count', 0),
            topic_data.get('readers_count', 0),
            topic_data.get('digested', False),
            topic_data.get('sticky', False),
            topic_data.get('create_time'),
            topic_data.get('modify_time'),
            raw_json
        ))

        # 详情接口通常包含更完整的标题，回填专栏列表历史空标题数据。
        if title or full_text:
            self.cursor.execute('''
                UPDATE column_topics
                SET title = COALESCE(NULLIF(title, ''), ?),
                    text = COALESCE(NULLIF(text, ''), ?)
                WHERE topic_id = ?
            ''', (title or None, full_text or None, topic_id))
        
        # 处理作者信息
        if talk and talk.get('owner'):
            owner = talk['owner']
            user_id = self.insert_user(owner)
            if user_id:
                self.cursor.execute('''
                    INSERT OR REPLACE INTO topic_owners (topic_id, user_id, owner_type)
                    VALUES (?, ?, 'talk')
                ''', (topic_id, user_id))
        
        # 处理图片
        images = talk.get('images', [])
        for image in images:
            self._insert_image(topic_id, image)
        
        # 处理文件
        files = talk.get('files', [])
        for file in files:
            self._insert_file(topic_id, file)
        
        # 处理语音文件 (content_voice)
        content_voice = topic_data.get('content_voice')
        if content_voice:
            self._insert_file(topic_id, content_voice)
        
        # 处理视频 (talk.video)
        video = talk.get('video')
        if video:
            self._insert_video(topic_id, video)
        
        # 处理评论
        comments = topic_data.get('show_comments', [])
        for comment in comments:
            self._insert_comment(topic_id, comment)
        
        self.conn.commit()
        return topic_id
    
    def _insert_image(self, topic_id: int, image_data: Dict[str, Any]):
        """插入图片信息"""
        if not image_data or not image_data.get('image_id'):
            return
        
        thumbnail = image_data.get('thumbnail', {})
        large = image_data.get('large', {})
        original = image_data.get('original', {})
        
        self.cursor.execute('''
            INSERT OR REPLACE INTO images 
            (image_id, topic_id, type, thumbnail_url, thumbnail_width, thumbnail_height,
             large_url, large_width, large_height, original_url, original_width, 
             original_height, original_size)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            image_data.get('image_id'),
            topic_id,
            image_data.get('type'),
            thumbnail.get('url'),
            thumbnail.get('width'),
            thumbnail.get('height'),
            large.get('url'),
            large.get('width'),
            large.get('height'),
            original.get('url'),
            original.get('width'),
            original.get('height'),
            original.get('size')
        ))
    
    def _insert_file(self, topic_id: int, file_data: Dict[str, Any]):
        """插入文件信息"""
        if not file_data or not file_data.get('file_id'):
            return
        
        self.cursor.execute('''
            INSERT OR REPLACE INTO files 
            (file_id, topic_id, name, hash, size, duration, download_count, create_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            file_data.get('file_id'),
            topic_id,
            file_data.get('name', ''),
            file_data.get('hash'),
            file_data.get('size'),
            file_data.get('duration'),
            file_data.get('download_count', 0),
            file_data.get('create_time')
        ))
    
    def _insert_video(self, topic_id: int, video_data: Dict[str, Any]):
        """插入视频信息"""
        if not video_data or not video_data.get('video_id'):
            return
        
        cover = video_data.get('cover', {})
        
        self.cursor.execute('''
            INSERT OR REPLACE INTO videos 
            (video_id, topic_id, size, duration, cover_url, cover_width, cover_height)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            video_data.get('video_id'),
            topic_id,
            video_data.get('size'),
            video_data.get('duration'),
            cover.get('url'),
            cover.get('width'),
            cover.get('height')
        ))
    
    def _insert_comment(self, topic_id: int, comment_data: Dict[str, Any]):
        """插入评论信息"""
        if not comment_data or not comment_data.get('comment_id'):
            return
        
        # 处理评论作者
        owner = comment_data.get('owner', {})
        owner_id = self.insert_user(owner) if owner else None
        
        # 处理被回复者
        repliee = comment_data.get('repliee', {})
        repliee_id = self.insert_user(repliee) if repliee else None
        
        self.cursor.execute('''
            INSERT OR REPLACE INTO comments 
            (comment_id, topic_id, owner_user_id, parent_comment_id, repliee_user_id,
             text, create_time, likes_count, rewards_count, replies_count, sticky)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            comment_data.get('comment_id'),
            topic_id,
            owner_id,
            comment_data.get('parent_comment_id'),
            repliee_id,
            comment_data.get('text', ''),
            comment_data.get('create_time'),
            comment_data.get('likes_count', 0),
            comment_data.get('rewards_count', 0),
            comment_data.get('replies_count', 0),
            comment_data.get('sticky', False)
        ))

    def import_comments(self, topic_id: int, comments: List[Dict[str, Any]]):
        """导入评论列表（包括嵌套回复），用于持久化从API获取的完整评论"""
        if not comments:
            return 0

        count = 0
        for comment in comments:
            # 插入主评论
            self._insert_comment(topic_id, comment)
            count += 1

            # 插入嵌套的回复评论
            replied_comments = comment.get('replied_comments', [])
            for reply in replied_comments:
                # 确保子评论有正确的 parent_comment_id
                if not reply.get('parent_comment_id'):
                    reply['parent_comment_id'] = comment.get('comment_id')
                self._insert_comment(topic_id, reply)
                count += 1

        self.conn.commit()
        return count

    def get_topic_detail(self, topic_id: int) -> Optional[Dict[str, Any]]:
        """获取文章详情"""
        self.cursor.execute('''
            SELECT td.topic_id, td.group_id, td.type, td.title, td.full_text,
                   td.likes_count, td.comments_count, td.readers_count,
                   td.digested, td.sticky, td.create_time, td.modify_time,
                   td.raw_json, td.imported_at, td.updated_at,
                   u.user_id, u.name, u.alias, u.avatar_url, u.description, u.location
            FROM topic_details td
            LEFT JOIN topic_owners tow ON td.topic_id = tow.topic_id AND tow.owner_type = 'talk'
            LEFT JOIN users u ON tow.user_id = u.user_id
            WHERE td.topic_id = ?
        ''', (topic_id,))
        
        row = self.cursor.fetchone()
        if not row:
            return None
        
        result = {
            'topic_id': row[0],
            'group_id': row[1],
            'type': row[2],
            'title': row[3],
            'full_text': row[4],
            'likes_count': row[5],
            'comments_count': row[6],
            'readers_count': row[7],
            'digested': bool(row[8]),
            'sticky': bool(row[9]),
            'create_time': row[10],
            'modify_time': row[11],
            'raw_json': row[12],
            'imported_at': row[13],
            'updated_at': row[14],
            'owner': None,
            'images': [],
            'files': [],
            'comments': []
        }
        
        # 设置作者信息
        if row[15]:
            result['owner'] = {
                'user_id': row[15],
                'name': row[16],
                'alias': row[17],
                'avatar_url': row[18],
                'description': row[19],
                'location': row[20]
            }
        
        # 获取图片
        result['images'] = self.get_topic_images(topic_id)
        
        # 获取文件
        result['files'] = self.get_topic_files(topic_id)
        
        # 获取视频
        result['videos'] = self.get_topic_videos(topic_id)
        
        # 获取评论
        result['comments'] = self.get_topic_comments(topic_id)
        
        return result
    
    def get_topic_images(self, topic_id: int) -> List[Dict[str, Any]]:
        """获取文章的所有图片"""
        self.cursor.execute('''
            SELECT image_id, type, thumbnail_url, thumbnail_width, thumbnail_height,
                   large_url, large_width, large_height, original_url, original_width,
                   original_height, original_size, local_path
            FROM images WHERE topic_id = ?
        ''', (topic_id,))
        
        images = []
        for row in self.cursor.fetchall():
            images.append({
                'image_id': row[0],
                'type': row[1],
                'thumbnail': {
                    'url': row[2],
                    'width': row[3],
                    'height': row[4]
                },
                'large': {
                    'url': row[5],
                    'width': row[6],
                    'height': row[7]
                },
                'original': {
                    'url': row[8],
                    'width': row[9],
                    'height': row[10],
                    'size': row[11]
                },
                'local_path': row[12]
            })
        return images
    
    def get_topic_files(self, topic_id: int) -> List[Dict[str, Any]]:
        """获取文章的所有文件"""
        self.cursor.execute('''
            SELECT file_id, name, hash, size, duration, download_count, 
                   create_time, download_status, local_path, download_time
            FROM files WHERE topic_id = ?
        ''', (topic_id,))
        
        files = []
        for row in self.cursor.fetchall():
            files.append({
                'file_id': row[0],
                'name': row[1],
                'hash': row[2],
                'size': row[3],
                'duration': row[4],
                'download_count': row[5],
                'create_time': row[6],
                'download_status': row[7],
                'local_path': row[8],
                'download_time': row[9]
            })
        return files
    
    def get_topic_videos(self, topic_id: int) -> List[Dict[str, Any]]:
        """获取文章的所有视频"""
        self.cursor.execute('''
            SELECT video_id, size, duration, cover_url, cover_width, cover_height,
                   cover_local_path, video_url, download_status, local_path, download_time
            FROM videos WHERE topic_id = ?
        ''', (topic_id,))
        
        videos = []
        for row in self.cursor.fetchall():
            videos.append({
                'video_id': row[0],
                'size': row[1],
                'duration': row[2],
                'cover': {
                    'url': row[3],
                    'width': row[4],
                    'height': row[5],
                    'local_path': row[6]
                },
                'video_url': row[7],
                'download_status': row[8],
                'local_path': row[9],
                'download_time': row[10]
            })
        return videos
    
    def update_video_cover_path(self, video_id: int, local_path: str):
        """更新视频封面本地缓存路径"""
        self.cursor.execute('''
            UPDATE videos SET cover_local_path = ?
            WHERE video_id = ?
        ''', (local_path, video_id))
        self.conn.commit()
    
    def update_video_download_status(self, video_id: int, status: str, video_url: str = None, local_path: str = None):
        """更新视频下载状态"""
        if local_path:
            self.cursor.execute('''
                UPDATE videos SET download_status = ?, video_url = ?, local_path = ?, download_time = CURRENT_TIMESTAMP
                WHERE video_id = ?
            ''', (status, video_url, local_path, video_id))
        elif video_url:
            self.cursor.execute('''
                UPDATE videos SET download_status = ?, video_url = ?
                WHERE video_id = ?
            ''', (status, video_url, video_id))
        else:
            self.cursor.execute('''
                UPDATE videos SET download_status = ?
                WHERE video_id = ?
            ''', (status, video_id))
        self.conn.commit()
    
    def get_pending_videos(self, group_id: int = None) -> List[Dict[str, Any]]:
        """获取待下载的视频列表"""
        if group_id:
            self.cursor.execute('''
                SELECT v.video_id, v.topic_id, v.size, v.duration, v.cover_url, td.group_id
                FROM videos v
                JOIN topic_details td ON v.topic_id = td.topic_id
                WHERE v.download_status = 'pending' AND td.group_id = ?
            ''', (group_id,))
        else:
            self.cursor.execute('''
                SELECT v.video_id, v.topic_id, v.size, v.duration, v.cover_url, td.group_id
                FROM videos v
                JOIN topic_details td ON v.topic_id = td.topic_id
                WHERE v.download_status = 'pending'
            ''')
        
        videos = []
        for row in self.cursor.fetchall():
            videos.append({
                'video_id': row[0],
                'topic_id': row[1],
                'size': row[2],
                'duration': row[3],
                'cover_url': row[4],
                'group_id': row[5]
            })
        return videos
    
    def get_topic_comments(self, topic_id: int) -> List[Dict[str, Any]]:
        """获取文章的所有评论（支持嵌套结构）"""
        self.cursor.execute('''
            SELECT c.comment_id, c.parent_comment_id, c.text, c.create_time,
                   c.likes_count, c.rewards_count, c.replies_count, c.sticky,
                   u.user_id, u.name, u.alias, u.avatar_url, u.location,
                   r.user_id, r.name, r.alias, r.avatar_url
            FROM comments c
            LEFT JOIN users u ON c.owner_user_id = u.user_id
            LEFT JOIN users r ON c.repliee_user_id = r.user_id
            WHERE c.topic_id = ?
            ORDER BY c.create_time ASC
        ''', (topic_id,))

        # 先收集所有评论，然后构建嵌套结构
        all_comments = {}  # comment_id -> comment_data
        parent_comments = []  # 顶级评论
        child_comments = []   # 子评论（有parent_comment_id的）

        for row in self.cursor.fetchall():
            comment_id = row[0]
            parent_comment_id = row[1]

            comment = {
                'comment_id': comment_id,
                'parent_comment_id': parent_comment_id,
                'text': row[2],
                'create_time': row[3],
                'likes_count': row[4],
                'rewards_count': row[5],
                'replies_count': row[6],
                'sticky': bool(row[7]),
                'owner': None,
                'repliee': None
            }

            if row[8]:
                comment['owner'] = {
                    'user_id': row[8],
                    'name': row[9],
                    'alias': row[10],
                    'avatar_url': row[11],
                    'location': row[12]
                }

            if row[13]:
                comment['repliee'] = {
                    'user_id': row[13],
                    'name': row[14],
                    'alias': row[15],
                    'avatar_url': row[16]
                }

            # 获取评论图片
            self.cursor.execute('''
                SELECT image_id, type, thumbnail_url, thumbnail_width, thumbnail_height,
                       large_url, large_width, large_height, original_url, original_width,
                       original_height, original_size
                FROM images WHERE comment_id = ?
            ''', (comment_id,))

            images = []
            for img_row in self.cursor.fetchall():
                images.append({
                    'image_id': img_row[0],
                    'type': img_row[1],
                    'thumbnail': {
                        'url': img_row[2],
                        'width': img_row[3],
                        'height': img_row[4]
                    },
                    'large': {
                        'url': img_row[5],
                        'width': img_row[6],
                        'height': img_row[7]
                    },
                    'original': {
                        'url': img_row[8],
                        'width': img_row[9],
                        'height': img_row[10],
                        'size': img_row[11]
                    }
                })
            if images:
                comment['images'] = images

            # 存储评论并分类
            all_comments[comment_id] = comment
            if parent_comment_id:
                child_comments.append(comment)
            else:
                parent_comments.append(comment)

        # 构建嵌套结构：将子评论附加到父评论的 replied_comments 中
        for child in child_comments:
            parent_id = child.get("parent_comment_id")
            if parent_id and parent_id in all_comments:
                parent = all_comments[parent_id]
                if "replied_comments" not in parent:
                    parent["replied_comments"] = []
                parent["replied_comments"].append(child)

        return parent_comments
    
    # ==================== 文件下载状态 ====================
    
    def update_file_download_status(self, file_id: int, status: str, local_path: str = None):
        """更新文件下载状态"""
        if local_path:
            self.cursor.execute('''
                UPDATE files SET download_status = ?, local_path = ?, download_time = CURRENT_TIMESTAMP
                WHERE file_id = ?
            ''', (status, local_path, file_id))
        else:
            self.cursor.execute('''
                UPDATE files SET download_status = ?
                WHERE file_id = ?
            ''', (status, file_id))
        self.conn.commit()
    
    def get_pending_files(self, group_id: int = None) -> List[Dict[str, Any]]:
        """获取待下载的文件列表"""
        if group_id:
            self.cursor.execute('''
                SELECT f.file_id, f.topic_id, f.name, f.size, f.hash, td.group_id
                FROM files f
                JOIN topic_details td ON f.topic_id = td.topic_id
                WHERE f.download_status = 'pending' AND td.group_id = ?
            ''', (group_id,))
        else:
            self.cursor.execute('''
                SELECT f.file_id, f.topic_id, f.name, f.size, f.hash, td.group_id
                FROM files f
                JOIN topic_details td ON f.topic_id = td.topic_id
                WHERE f.download_status = 'pending'
            ''')
        
        files = []
        for row in self.cursor.fetchall():
            files.append({
                'file_id': row[0],
                'topic_id': row[1],
                'name': row[2],
                'size': row[3],
                'hash': row[4],
                'group_id': row[5]
            })
        return files
    
    # ==================== 图片缓存 ====================
    
    def update_image_local_path(self, image_id: int, local_path: str):
        """更新图片本地缓存路径"""
        self.cursor.execute('''
            UPDATE images SET local_path = ?
            WHERE image_id = ?
        ''', (local_path, image_id))
        self.conn.commit()
    
    def get_uncached_images(self, group_id: int = None) -> List[Dict[str, Any]]:
        """获取未缓存的图片列表"""
        if group_id:
            self.cursor.execute('''
                SELECT i.image_id, i.topic_id, i.original_url, td.group_id
                FROM images i
                JOIN topic_details td ON i.topic_id = td.topic_id
                WHERE i.local_path IS NULL AND i.original_url IS NOT NULL AND td.group_id = ?
            ''', (group_id,))
        else:
            self.cursor.execute('''
                SELECT i.image_id, i.topic_id, i.original_url, td.group_id
                FROM images i
                JOIN topic_details td ON i.topic_id = td.topic_id
                WHERE i.local_path IS NULL AND i.original_url IS NOT NULL
            ''')
        
        images = []
        for row in self.cursor.fetchall():
            images.append({
                'image_id': row[0],
                'topic_id': row[1],
                'original_url': row[2],
                'group_id': row[3]
            })
        return images
    
    # ==================== 统计信息 ====================
    
    def get_stats(self, group_id: int) -> Dict[str, Any]:
        """获取专栏数据库统计信息"""
        stats = {
            'columns_count': 0,
            'topics_count': 0,
            'details_count': 0,
            'images_count': 0,
            'files_count': 0,
            'files_downloaded': 0,
            'videos_count': 0,
            'videos_downloaded': 0,
            'comments_count': 0
        }
        
        self.cursor.execute('SELECT COUNT(*) FROM columns WHERE group_id = ?', (group_id,))
        stats['columns_count'] = self.cursor.fetchone()[0]
        
        self.cursor.execute('SELECT COUNT(*) FROM column_topics WHERE group_id = ?', (group_id,))
        stats['topics_count'] = self.cursor.fetchone()[0]
        
        self.cursor.execute('SELECT COUNT(*) FROM topic_details WHERE group_id = ?', (group_id,))
        stats['details_count'] = self.cursor.fetchone()[0]
        
        self.cursor.execute('''
            SELECT COUNT(*) FROM images i
            JOIN topic_details td ON i.topic_id = td.topic_id
            WHERE td.group_id = ?
        ''', (group_id,))
        stats['images_count'] = self.cursor.fetchone()[0]
        
        self.cursor.execute('''
            SELECT COUNT(*) FROM files f
            JOIN topic_details td ON f.topic_id = td.topic_id
            WHERE td.group_id = ?
        ''', (group_id,))
        stats['files_count'] = self.cursor.fetchone()[0]
        
        self.cursor.execute('''
            SELECT COUNT(*) FROM files f
            JOIN topic_details td ON f.topic_id = td.topic_id
            WHERE td.group_id = ? AND f.download_status = 'completed'
        ''', (group_id,))
        stats['files_downloaded'] = self.cursor.fetchone()[0]
        
        self.cursor.execute('''
            SELECT COUNT(*) FROM videos v
            JOIN topic_details td ON v.topic_id = td.topic_id
            WHERE td.group_id = ?
        ''', (group_id,))
        stats['videos_count'] = self.cursor.fetchone()[0]
        
        self.cursor.execute('''
            SELECT COUNT(*) FROM videos v
            JOIN topic_details td ON v.topic_id = td.topic_id
            WHERE td.group_id = ? AND v.download_status = 'completed'
        ''', (group_id,))
        stats['videos_downloaded'] = self.cursor.fetchone()[0]
        
        self.cursor.execute('''
            SELECT COUNT(*) FROM comments c
            JOIN topic_details td ON c.topic_id = td.topic_id
            WHERE td.group_id = ?
        ''', (group_id,))
        stats['comments_count'] = self.cursor.fetchone()[0]
        
        return stats
    
    # ==================== 采集日志 ====================
    
    def start_crawl_log(self, group_id: int, crawl_type: str) -> int:
        """开始采集日志"""
        self.cursor.execute('''
            INSERT INTO crawl_log (group_id, crawl_type)
            VALUES (?, ?)
        ''', (group_id, crawl_type))
        self.conn.commit()
        return self.cursor.lastrowid
    
    def update_crawl_log(self, log_id: int, columns_count: int = 0, topics_count: int = 0,
                         details_count: int = 0, files_count: int = 0,
                         status: str = None, error_message: str = None):
        """更新采集日志"""
        updates = []
        values = []
        
        if columns_count:
            updates.append('columns_count = ?')
            values.append(columns_count)
        if topics_count:
            updates.append('topics_count = ?')
            values.append(topics_count)
        if details_count:
            updates.append('details_count = ?')
            values.append(details_count)
        if files_count:
            updates.append('files_count = ?')
            values.append(files_count)
        if status:
            updates.append('status = ?')
            values.append(status)
            if status in ('completed', 'failed'):
                updates.append('end_time = CURRENT_TIMESTAMP')
        if error_message:
            updates.append('error_message = ?')
            values.append(error_message)
        
        if updates:
            values.append(log_id)
            self.cursor.execute(f'''
                UPDATE crawl_log SET {', '.join(updates)}
                WHERE id = ?
            ''', values)
            self.conn.commit()
    
    # ==================== 增量爬取支持 ====================
    
    def topic_detail_exists(self, topic_id: int) -> bool:
        """检查文章详情是否已存在"""
        self.cursor.execute('SELECT 1 FROM topic_details WHERE topic_id = ?', (topic_id,))
        return self.cursor.fetchone() is not None
    
    def get_existing_topic_ids(self, group_id: int) -> set:
        """获取已存在的文章ID集合"""
        self.cursor.execute('SELECT topic_id FROM topic_details WHERE group_id = ?', (group_id,))
        return {row[0] for row in self.cursor.fetchall()}
    
    # ==================== 数据清理 ====================
    
    def clear_all_data(self, group_id: int) -> Dict[str, int]:
        """清空指定群组的所有专栏数据"""
        stats = {
            'columns_deleted': 0,
            'topics_deleted': 0,
            'details_deleted': 0,
            'images_deleted': 0,
            'files_deleted': 0,
            'videos_deleted': 0,
            'comments_deleted': 0,
            'users_deleted': 0
        }
        
        try:
            # 获取该群组的所有topic_id
            self.cursor.execute('SELECT topic_id FROM topic_details WHERE group_id = ?', (group_id,))
            topic_ids = [row[0] for row in self.cursor.fetchall()]
            
            if topic_ids:
                placeholders = ','.join('?' * len(topic_ids))
                
                # 删除评论
                self.cursor.execute(f'DELETE FROM comments WHERE topic_id IN ({placeholders})', topic_ids)
                stats['comments_deleted'] = self.cursor.rowcount
                
                # 删除视频
                self.cursor.execute(f'DELETE FROM videos WHERE topic_id IN ({placeholders})', topic_ids)
                stats['videos_deleted'] = self.cursor.rowcount
                
                # 删除文件
                self.cursor.execute(f'DELETE FROM files WHERE topic_id IN ({placeholders})', topic_ids)
                stats['files_deleted'] = self.cursor.rowcount
                
                # 删除图片
                self.cursor.execute(f'DELETE FROM images WHERE topic_id IN ({placeholders})', topic_ids)
                stats['images_deleted'] = self.cursor.rowcount
                
                # 删除topic_owners
                self.cursor.execute(f'DELETE FROM topic_owners WHERE topic_id IN ({placeholders})', topic_ids)
            
            # 删除文章详情
            self.cursor.execute('DELETE FROM topic_details WHERE group_id = ?', (group_id,))
            stats['details_deleted'] = self.cursor.rowcount
            
            # 删除专栏文章
            self.cursor.execute('DELETE FROM column_topics WHERE group_id = ?', (group_id,))
            stats['topics_deleted'] = self.cursor.rowcount
            
            # 删除专栏目录
            self.cursor.execute('DELETE FROM columns WHERE group_id = ?', (group_id,))
            stats['columns_deleted'] = self.cursor.rowcount
            
            # 删除采集日志
            self.cursor.execute('DELETE FROM crawl_log WHERE group_id = ?', (group_id,))
            
            self.conn.commit()
            print(f"✅ 清空专栏数据完成: {stats}")
            return stats
            
        except Exception as e:
            self.conn.rollback()
            print(f"❌ 清空数据失败: {e}")
            raise
    
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()


def main():
    """测试专栏数据库"""
    db = ZSXQColumnsDatabase('test_columns.db')
    print("📊 专栏数据库测试完成")
    db.close()


if __name__ == "__main__":
    main()

