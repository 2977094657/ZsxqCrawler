#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
from typing import Dict, Any, Optional, List


class ZSXQDatabase:
    """知识星球数据库管理器"""
    
    def __init__(self, db_path: str = "zsxq_interactive.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self._init_database()
    
    def _init_database(self):
        """初始化数据库表结构"""
        
        # 群组表 - 适配现有结构
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS groups (
                group_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                type TEXT,
                background_url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 用户表 - 适配现有结构
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                alias TEXT,
                avatar_url TEXT,
                location TEXT,
                description TEXT,
                ai_comment_url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # 话题表 - 适配现有结构
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS topics (
                topic_id INTEGER PRIMARY KEY,
                group_id INTEGER NOT NULL,
                type TEXT NOT NULL,
                title TEXT,
                create_time TEXT,
                digested BOOLEAN DEFAULT FALSE,
                sticky BOOLEAN DEFAULT FALSE,
                likes_count INTEGER DEFAULT 0,
                tourist_likes_count INTEGER DEFAULT 0,
                rewards_count INTEGER DEFAULT 0,
                comments_count INTEGER DEFAULT 0,
                reading_count INTEGER DEFAULT 0,
                readers_count INTEGER DEFAULT 0,
                answered BOOLEAN DEFAULT FALSE,
                silenced BOOLEAN DEFAULT FALSE,
                annotation TEXT,
                user_liked BOOLEAN DEFAULT FALSE,
                user_subscribed BOOLEAN DEFAULT FALSE,
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (group_id) REFERENCES groups (group_id)
            )
        ''')
        
        # 话题内容表（talks）
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS talks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic_id INTEGER,
                owner_user_id INTEGER,
                text TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (topic_id) REFERENCES topics (topic_id),
                FOREIGN KEY (owner_user_id) REFERENCES users (user_id)
            )
        ''')
        
        # 文章表
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic_id INTEGER,
                title TEXT,
                article_id TEXT,
                article_url TEXT,
                inline_article_url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (topic_id) REFERENCES topics (topic_id)
            )
        ''')
        
        # 图片表
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS images (
                image_id INTEGER PRIMARY KEY,
                topic_id INTEGER,
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (topic_id) REFERENCES topics (topic_id)
            )
        ''')
        
        # 点赞表
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS likes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic_id INTEGER,
                user_id INTEGER,
                create_time TEXT,
                imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (topic_id) REFERENCES topics (topic_id),
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        ''')
        
        # 表情点赞表
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS like_emojis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic_id INTEGER,
                emoji_key TEXT,
                likes_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (topic_id) REFERENCES topics (topic_id)
            )
        ''')
        
        # 用户表情点赞表
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_liked_emojis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic_id INTEGER,
                emoji_key TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (topic_id) REFERENCES topics (topic_id)
            )
        ''')
        
        # 评论表
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS comments (
                comment_id INTEGER PRIMARY KEY,
                topic_id INTEGER,
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
                FOREIGN KEY (topic_id) REFERENCES topics (topic_id),
                FOREIGN KEY (owner_user_id) REFERENCES users (user_id),
                FOREIGN KEY (parent_comment_id) REFERENCES comments (comment_id),
                FOREIGN KEY (repliee_user_id) REFERENCES users (user_id)
            )
        ''')
        
        # 问题表
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS questions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic_id INTEGER,
                owner_user_id INTEGER,
                questionee_user_id INTEGER,
                text TEXT,
                expired BOOLEAN DEFAULT FALSE,
                anonymous BOOLEAN DEFAULT FALSE,
                owner_questions_count INTEGER,
                owner_join_time TEXT,
                owner_status TEXT,
                owner_location TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (topic_id) REFERENCES topics (topic_id),
                FOREIGN KEY (owner_user_id) REFERENCES users (user_id),
                FOREIGN KEY (questionee_user_id) REFERENCES users (user_id)
            )
        ''')
        
        # 回答表
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS answers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic_id INTEGER,
                owner_user_id INTEGER,
                text TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (topic_id) REFERENCES topics (topic_id),
                FOREIGN KEY (owner_user_id) REFERENCES users (user_id)
            )
        ''')
        
        # 标签表
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS tags (
                tag_id INTEGER PRIMARY KEY AUTOINCREMENT,
                group_id INTEGER NOT NULL,
                tag_name TEXT NOT NULL,
                hid TEXT,
                topic_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(group_id, tag_name),
                FOREIGN KEY (group_id) REFERENCES groups (group_id)
            )
        ''')
        
        # 话题标签关联表
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS topic_tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic_id INTEGER NOT NULL,
                tag_id INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(topic_id, tag_id),
                FOREIGN KEY (topic_id) REFERENCES topics (topic_id),
                FOREIGN KEY (tag_id) REFERENCES tags (tag_id)
            )
        ''')

        # 话题文件表 (talk.files数组)
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS topic_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic_id INTEGER,
                file_id INTEGER,
                name TEXT,
                hash TEXT,
                size INTEGER,
                duration INTEGER,
                download_count INTEGER,
                create_time TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (topic_id) REFERENCES topics (topic_id)
            )
        ''')

        self.conn.commit()
    
    def import_topic_data(self, topic_data: Dict[str, Any]) -> bool:
        """导入话题数据到数据库"""
        try:
            topic_id = topic_data.get('topic_id')
            group_info = topic_data.get('group', {})
            
            if not topic_id:
                return False
            

            
            # 导入群组信息
            if group_info:
                self._upsert_group(group_info)
            
            # 导入话题相关的所有用户信息
            self._import_all_users(topic_data)
            
            # 导入话题信息
            self._upsert_topic(topic_data)
            
            # 导入话题内容(talk)
            if 'talk' in topic_data and topic_data['talk']:
                self._upsert_talk(topic_id, topic_data['talk'])
            
            # 导入文章信息（如果话题类型是文章）
            self._import_articles(topic_id, topic_data)
            
            # 导入图片信息
            self._import_images(topic_id, topic_data)
            
            # 导入点赞信息
            self._import_likes(topic_id, topic_data)
            
            # 导入表情点赞信息
            self._import_like_emojis(topic_id, topic_data)
            
            # 导入用户表情点赞信息
            self._import_user_liked_emojis(topic_id, topic_data)
            
            # 导入评论信息
            if 'show_comments' in topic_data:
                self._import_comments(topic_id, topic_data['show_comments'])
            
            # 导入问题信息
            if 'question' in topic_data and topic_data['question']:
                self._upsert_question(topic_id, topic_data['question'])
            
            # 导入回答信息
            if 'answer' in topic_data and topic_data['answer']:
                self._upsert_answer(topic_id, topic_data['answer'])
            
            # 导入标签信息
            self._import_tags(topic_id, topic_data)

            # 导入文件信息
            if 'talk' in topic_data and topic_data['talk'] and 'files' in topic_data['talk']:
                self._import_files(topic_id, topic_data['talk']['files'])

            return True
            
        except Exception as e:
            print(f"❌ 导入话题数据失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def _upsert_group(self, group_data: Dict[str, Any]):
        """插入或更新群组信息"""
        group_id = group_data.get('group_id')
        if not group_id:
            return
        
        # 获取当前时间作为created_at（使用东八区时间格式）
        from datetime import datetime, timezone, timedelta
        beijing_tz = timezone(timedelta(hours=8))
        current_time = datetime.now(beijing_tz).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
        
        self.cursor.execute('''
            INSERT OR REPLACE INTO groups 
            (group_id, name, type, background_url, created_at)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            group_id,
            group_data.get('name', ''),
            group_data.get('type', ''),
            group_data.get('background_url', ''),
            current_time
        ))
    
    def _upsert_user(self, user_data: Dict[str, Any]):
        """插入或更新用户信息"""
        user_id = user_data.get('user_id')
        if not user_id:
            return
        
        # 获取当前时间作为created_at（使用东八区时间格式）
        from datetime import datetime, timezone, timedelta
        beijing_tz = timezone(timedelta(hours=8))
        current_time = datetime.now(beijing_tz).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
        
        self.cursor.execute('''
            INSERT OR REPLACE INTO users 
            (user_id, name, alias, avatar_url, location, description, ai_comment_url, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            user_id,
            user_data.get('name', ''),
            user_data.get('alias', ''),
            user_data.get('avatar_url', ''),
            user_data.get('location', ''),
            user_data.get('description', ''),
            user_data.get('ai_comment_url', ''),
            current_time
        ))
    
    def _upsert_topic(self, topic_data: Dict[str, Any]):
        """插入或更新话题信息"""
        topic_id = topic_data.get('topic_id')
        if not topic_id:
            return
        
        # 获取当前时间作为imported_at（使用东八区时间格式）
        from datetime import datetime, timezone, timedelta
        beijing_tz = timezone(timedelta(hours=8))
        current_time = datetime.now(beijing_tz).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
        
        self.cursor.execute('''
            INSERT OR REPLACE INTO topics 
            (topic_id, group_id, type, title, create_time, digested, sticky, 
             likes_count, tourist_likes_count, rewards_count, comments_count, 
             reading_count, readers_count, answered, silenced, annotation, 
             user_liked, user_subscribed, imported_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            topic_id,
            topic_data.get('group', {}).get('group_id', ''),
            topic_data.get('type', ''),
            topic_data.get('title', ''),
            topic_data.get('create_time', ''),
            topic_data.get('digested', False),
            topic_data.get('sticky', False),
            topic_data.get('likes_count', 0),
            topic_data.get('tourist_likes_count', 0),
            topic_data.get('rewards_count', 0),
            topic_data.get('comments_count', 0),
            topic_data.get('reading_count', 0),
            topic_data.get('readers_count', 0),
            topic_data.get('answered', False),
            topic_data.get('silenced', False),
            topic_data.get('annotation', ''),
            topic_data.get('user_liked', False),
            topic_data.get('user_subscribed', False),
            current_time
        ))
    
    def update_topic_stats(self, topic_data: Dict[str, Any]) -> bool:
        """仅更新话题的统计信息，不导入其他相关数据"""
        try:
            topic_id = topic_data.get('topic_id')
            if not topic_id:
                return False

            # 获取当前时间作为imported_at（使用东八区时间格式）
            from datetime import datetime, timezone, timedelta
            beijing_tz = timezone(timedelta(hours=8))
            current_time = datetime.now(beijing_tz).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'

            # 只更新统计相关字段，不更新内容字段
            self.cursor.execute('''
                UPDATE topics
                SET likes_count = ?, tourist_likes_count = ?, rewards_count = ?,
                    comments_count = ?, reading_count = ?, readers_count = ?,
                    digested = ?, sticky = ?, user_liked = ?, user_subscribed = ?,
                    imported_at = ?
                WHERE topic_id = ?
            ''', (
                topic_data.get('likes_count', 0),
                topic_data.get('tourist_likes_count', 0),
                topic_data.get('rewards_count', 0),
                topic_data.get('comments_count', 0),
                topic_data.get('reading_count', 0),
                topic_data.get('readers_count', 0),
                topic_data.get('digested', False),
                topic_data.get('sticky', False),
                topic_data.get('user_specific', {}).get('liked', False),
                topic_data.get('user_specific', {}).get('subscribed', False),
                current_time,
                topic_id
            ))

            # 检查是否有行被更新
            if self.cursor.rowcount > 0:
                return True
            else:
                print(f"警告：话题 {topic_id} 不存在，无法更新")
                return False

        except Exception as e:
            print(f"❌ 更新话题统计信息失败: {e}")
            import traceback
            traceback.print_exc()
            return False

    def get_database_stats(self) -> Dict[str, Any]:
        """获取数据库统计信息"""
        stats = {}

        tables = ['groups', 'users', 'topics', 'talks', 'articles', 'images',
                 'likes', 'like_emojis', 'user_liked_emojis', 'comments',
                 'questions', 'answers']

        for table in tables:
            try:
                self.cursor.execute(f'SELECT COUNT(*) FROM {table}')
                stats[table] = self.cursor.fetchone()[0]
            except Exception as e:
                print(f"获取表 {table} 统计信息失败: {e}")
                stats[table] = 0

        return stats
    
    def get_timestamp_range_info(self) -> Dict[str, Any]:
        """获取话题时间戳范围信息"""
        try:
            # 获取最新话题时间
            self.cursor.execute('''
                SELECT create_time FROM topics 
                WHERE create_time IS NOT NULL AND create_time != ''
                ORDER BY create_time DESC LIMIT 1
            ''')
            newest_result = self.cursor.fetchone()
            newest_time = newest_result[0] if newest_result else None
            
            # 获取最老话题时间
            self.cursor.execute('''
                SELECT create_time FROM topics 
                WHERE create_time IS NOT NULL AND create_time != ''
                ORDER BY create_time ASC LIMIT 1
            ''')
            oldest_result = self.cursor.fetchone()
            oldest_time = oldest_result[0] if oldest_result else None
            
            # 获取话题总数
            self.cursor.execute('SELECT COUNT(*) FROM topics')
            total_topics = self.cursor.fetchone()[0]
            
            # 判断是否有数据
            has_data = newest_time is not None and oldest_time is not None
            
            return {
                'newest_time': newest_time,
                'oldest_time': oldest_time,
                'newest_timestamp': newest_time,
                'oldest_timestamp': oldest_time,
                'total_topics': total_topics,
                'has_data': has_data
            }
            
        except Exception as e:
            print(f"获取时间戳范围信息失败: {e}")
            return {
                'newest_time': None,
                'oldest_time': None,
                'newest_timestamp': None,
                'oldest_timestamp': None,
                'total_topics': 0,
                'has_data': False
            }
    
    def get_oldest_topic_timestamp(self) -> Optional[str]:
        """获取数据库中最老的话题时间戳"""
        try:
            self.cursor.execute('''
                SELECT create_time FROM topics 
                WHERE create_time IS NOT NULL AND create_time != ''
                ORDER BY create_time ASC LIMIT 1
            ''')
            result = self.cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            print(f"获取最老话题时间戳失败: {e}")
            return None
    
    def get_newest_topic_timestamp(self) -> Optional[str]:
        """获取数据库中最新的话题时间戳"""
        try:
            self.cursor.execute('''
                SELECT create_time FROM topics 
                WHERE create_time IS NOT NULL AND create_time != ''
                ORDER BY create_time DESC LIMIT 1
            ''')
            result = self.cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            print(f"获取最新话题时间戳失败: {e}")
            return None
    
    def _import_all_users(self, topic_data: Dict[str, Any]):
        """导入话题相关的所有用户信息"""
        # 导入talk中的用户
        if 'talk' in topic_data and topic_data['talk'] and 'owner' in topic_data['talk']:
            self._upsert_user(topic_data['talk']['owner'])

        # 导入question中的用户
        if 'question' in topic_data and topic_data['question']:
            # 对于非匿名用户，导入提问者信息
            if 'owner' in topic_data['question'] and not topic_data['question'].get('anonymous', False):
                self._upsert_user(topic_data['question']['owner'])
            # 导入被提问者信息（无论是否匿名都有）
            if 'questionee' in topic_data['question']:
                self._upsert_user(topic_data['question']['questionee'])

        # 导入answer中的用户
        if 'answer' in topic_data and topic_data['answer'] and 'owner' in topic_data['answer']:
            self._upsert_user(topic_data['answer']['owner'])
        
        # 导入latest_likes中的用户
        if 'latest_likes' in topic_data:
            for like in topic_data['latest_likes']:
                if 'owner' in like:
                    self._upsert_user(like['owner'])
        
        # 导入comments中的用户
        if 'show_comments' in topic_data:
            for comment in topic_data['show_comments']:
                if 'owner' in comment:
                    self._upsert_user(comment['owner'])
                if 'repliee' in comment:
                    self._upsert_user(comment['repliee'])
    
    def _upsert_talk(self, topic_id: int, talk_data: Dict[str, Any]):
        """插入或更新话题内容"""
        if not talk_data:
            return
        
        owner_user_id = talk_data.get('owner', {}).get('user_id')
        if not owner_user_id:
            return
        
        # 获取当前时间作为created_at（使用东八区时间格式）
        from datetime import datetime, timezone, timedelta
        beijing_tz = timezone(timedelta(hours=8))
        current_time = datetime.now(beijing_tz).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
        
        self.cursor.execute('''
            INSERT OR REPLACE INTO talks 
            (topic_id, owner_user_id, text, created_at)
            VALUES (?, ?, ?, ?)
        ''', (
            topic_id,
            owner_user_id,
            talk_data.get('text', ''),
            current_time
        ))

    
    def _import_images(self, topic_id: int, topic_data: Dict[str, Any]):
        """导入图片信息"""
        images_to_import = []
        
        # 从talk中获取图片
        if 'talk' in topic_data and topic_data['talk'] and 'images' in topic_data['talk']:
            for img in topic_data['talk']['images']:
                images_to_import.append((img, None))  # (image_data, comment_id)
        
        # 从comments中获取图片
        if 'show_comments' in topic_data:
            for comment in topic_data['show_comments']:
                if 'images' in comment:
                    comment_id = comment.get('comment_id')
                    for img in comment['images']:
                        images_to_import.append((img, comment_id))
        
        # 导入所有图片
        for img_data, comment_id in images_to_import:
            self._upsert_image(topic_id, img_data, comment_id)
    
    def _upsert_image(self, topic_id: int, image_data: Dict[str, Any], comment_id: Optional[int] = None):
        """插入或更新图片信息"""
        image_id = image_data.get('image_id')
        if not image_id:
            return
        
        thumbnail = image_data.get('thumbnail', {})
        large = image_data.get('large', {})
        original = image_data.get('original', {})
        
        # 获取当前时间作为created_at（使用东八区时间格式）
        from datetime import datetime, timezone, timedelta
        beijing_tz = timezone(timedelta(hours=8))
        current_time = datetime.now(beijing_tz).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
        
        self.cursor.execute('''
            INSERT OR REPLACE INTO images 
            (image_id, topic_id, comment_id, type, thumbnail_url, thumbnail_width, thumbnail_height,
             large_url, large_width, large_height, original_url, original_width, original_height, original_size, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            image_id,
            topic_id,
            comment_id,
            image_data.get('type', ''),
            thumbnail.get('url', ''),
            thumbnail.get('width'),
            thumbnail.get('height'),
            large.get('url', ''),
            large.get('width'),
            large.get('height'),
            original.get('url', ''),
            original.get('width'),
            original.get('height'),
            original.get('size'),
            current_time
        ))

    
    def _import_likes(self, topic_id: int, topic_data: Dict[str, Any]):
        """导入点赞信息"""
        if 'latest_likes' not in topic_data:
            return
        
        for like in topic_data['latest_likes']:
            owner = like.get('owner', {})
            user_id = owner.get('user_id')
            if user_id:
                # 获取当前时间作为imported_at（使用东八区时间格式）
                from datetime import datetime, timezone, timedelta
                beijing_tz = timezone(timedelta(hours=8))
                current_time = datetime.now(beijing_tz).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
                
                self.cursor.execute('''
                    INSERT OR IGNORE INTO likes 
                    (topic_id, user_id, create_time, imported_at)
                    VALUES (?, ?, ?, ?)
                ''', (
                    topic_id,
                    user_id,
                    like.get('create_time', ''),
                    current_time
                ))
        
        if topic_data['latest_likes']:
            pass  # 数据已导入，无需额外日志

    def _import_like_emojis(self, topic_id: int, topic_data: Dict[str, Any]):
        """导入表情点赞信息"""
        if 'likes_detail' not in topic_data or 'emojis' not in topic_data['likes_detail']:
            return
        
        for emoji in topic_data['likes_detail']['emojis']:
            emoji_key = emoji.get('emoji_key')
            likes_count = emoji.get('likes_count', 0)
            if emoji_key:
                # 获取当前时间作为created_at（使用东八区时间格式）
                from datetime import datetime, timezone, timedelta
                beijing_tz = timezone(timedelta(hours=8))
                current_time = datetime.now(beijing_tz).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
                
                self.cursor.execute('''
                    INSERT OR REPLACE INTO like_emojis 
                    (topic_id, emoji_key, likes_count, created_at)
                    VALUES (?, ?, ?, ?)
                ''', (
                    topic_id,
                    emoji_key,
                    likes_count,
                    current_time
                ))
        
        if topic_data['likes_detail']['emojis']:
            pass  # 数据已导入，无需额外日志

    def _import_user_liked_emojis(self, topic_id: int, topic_data: Dict[str, Any]):
        """导入用户表情点赞信息"""
        if 'user_specific' not in topic_data or 'liked_emojis' not in topic_data['user_specific']:
            return
        
        for emoji_key in topic_data['user_specific']['liked_emojis']:
            if emoji_key:
                self.cursor.execute('''
                    INSERT OR IGNORE INTO user_liked_emojis 
                    (topic_id, emoji_key)
                    VALUES (?, ?)
                ''', (
                    topic_id,
                    emoji_key
                ))
        
        if topic_data['user_specific']['liked_emojis']:
            pass  # 数据已导入，无需额外日志

    def _import_comments(self, topic_id: int, comments: List[Dict[str, Any]]):
        """导入评论信息"""
        for comment in comments:
            self._upsert_comment(topic_id, comment)
            # 导入评论的图片
            if 'images' in comment and comment['images']:
                self._import_comment_images(topic_id, comment['comment_id'], comment['images'])

        if comments:
            pass  # 数据已导入，无需额外日志

    def import_additional_comments(self, topic_id: int, comments: List[Dict[str, Any]]):
        """导入额外获取的评论信息（来自评论API）"""
        if not comments:
            return

        print(f"📝 导入话题 {topic_id} 的 {len(comments)} 条额外评论...")

        for comment in comments:
            # 导入评论作者
            if 'owner' in comment and comment['owner']:
                self._upsert_user(comment['owner'])

            # 导入回复人（如果存在）
            if 'repliee' in comment and comment['repliee']:
                self._upsert_user(comment['repliee'])

            # 导入评论
            self._upsert_comment(topic_id, comment)

            # 导入评论的图片
            if 'images' in comment and comment['images']:
                self._import_comment_images(topic_id, comment['comment_id'], comment['images'])

        print(f"✅ 完成导入 {len(comments)} 条评论")

    def _upsert_comment(self, topic_id: int, comment_data: Dict[str, Any]):
        """插入或更新评论信息"""
        comment_id = comment_data.get('comment_id')
        if not comment_id:
            return
        
        owner_user_id = comment_data.get('owner', {}).get('user_id')
        repliee_user_id = comment_data.get('repliee', {}).get('user_id')
        
        # 获取当前时间作为imported_at（使用东八区时间格式）
        from datetime import datetime, timezone, timedelta
        beijing_tz = timezone(timedelta(hours=8))
        current_time = datetime.now(beijing_tz).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
        
        self.cursor.execute('''
            INSERT OR REPLACE INTO comments
            (comment_id, topic_id, owner_user_id, parent_comment_id, repliee_user_id,
             text, create_time, likes_count, rewards_count, replies_count, sticky, imported_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            comment_id,
            topic_id,
            owner_user_id,
            comment_data.get('parent_comment_id'),
            repliee_user_id,
            comment_data.get('text', ''),
            comment_data.get('create_time', ''),
            comment_data.get('likes_count', 0),
            comment_data.get('rewards_count', 0),
            comment_data.get('replies_count', 0),
            comment_data.get('sticky', False),
            current_time
        ))

    def _import_comment_images(self, topic_id: int, comment_id: int, images: List[Dict[str, Any]]):
        """导入评论的图片信息"""
        for image in images:
            if not image.get('image_id'):
                continue

            # 获取当前时间作为created_at（使用东八区时间格式）
            from datetime import datetime, timezone, timedelta
            beijing_tz = timezone(timedelta(hours=8))
            current_time = datetime.now(beijing_tz).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'

            self.cursor.execute('''
                INSERT OR REPLACE INTO images
                (image_id, topic_id, comment_id, type, thumbnail_url, thumbnail_width, thumbnail_height,
                 large_url, large_width, large_height, original_url, original_width, original_height,
                 original_size, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                image.get('image_id'),
                topic_id,
                comment_id,
                image.get('type', ''),
                image.get('thumbnail', {}).get('url', ''),
                image.get('thumbnail', {}).get('width', 0),
                image.get('thumbnail', {}).get('height', 0),
                image.get('large', {}).get('url', ''),
                image.get('large', {}).get('width', 0),
                image.get('large', {}).get('height', 0),
                image.get('original', {}).get('url', ''),
                image.get('original', {}).get('width', 0),
                image.get('original', {}).get('height', 0),
                image.get('original', {}).get('size', 0),
                current_time
            ))

    def _upsert_question(self, topic_id: int, question_data: Dict[str, Any]):
        """插入或更新问题信息"""
        owner_user_id = question_data.get('owner', {}).get('user_id')
        questionee_user_id = question_data.get('questionee', {}).get('user_id')
        is_anonymous = question_data.get('anonymous', False)

        # 对于匿名用户，owner_user_id 可能为 None，但仍需要存储问题信息
        # 只有在既没有 owner_user_id 又没有问题文本时才跳过
        if not owner_user_id and not question_data.get('text'):
            return

        owner_detail = question_data.get('owner_detail', {})

        # 获取当前时间作为created_at（使用东八区时间格式）
        from datetime import datetime, timezone, timedelta
        beijing_tz = timezone(timedelta(hours=8))
        current_time = datetime.now(beijing_tz).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'

        self.cursor.execute('''
            INSERT OR REPLACE INTO questions
            (topic_id, owner_user_id, questionee_user_id, text, expired, anonymous,
             owner_questions_count, owner_join_time, owner_status, owner_location, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            topic_id,
            owner_user_id,  # 对于匿名用户可能为 None
            questionee_user_id,
            question_data.get('text', ''),
            question_data.get('expired', False),
            is_anonymous,
            owner_detail.get('questions_count'),
            owner_detail.get('join_time', owner_detail.get('estimated_join_time', '')),  # 支持 estimated_join_time
            owner_detail.get('status', ''),
            question_data.get('owner_location', ''),
            current_time
        ))

    
    def _upsert_answer(self, topic_id: int, answer_data: Dict[str, Any]):
        """插入或更新回答信息"""
        owner_user_id = answer_data.get('owner', {}).get('user_id')
        
        if not owner_user_id:
            return
        
        # 获取当前时间作为created_at（使用东八区时间格式）
        from datetime import datetime, timezone, timedelta
        beijing_tz = timezone(timedelta(hours=8))
        current_time = datetime.now(beijing_tz).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
        
        self.cursor.execute('''
            INSERT OR REPLACE INTO answers 
            (topic_id, owner_user_id, text, created_at)
            VALUES (?, ?, ?, ?)
        ''', (
            topic_id,
            owner_user_id,
            answer_data.get('text', ''),
            current_time
        ))

    
    def _import_articles(self, topic_id: int, topic_data: Dict[str, Any]):
        """导入文章信息"""
        # 检查talk类型话题中的article字段
        if 'talk' in topic_data and topic_data['talk'] and 'article' in topic_data['talk']:
            article_data = topic_data['talk']['article']
            if article_data:
                self._upsert_article(topic_id, article_data)
                return
        
        # 检查顶层的article字段（如果存在）
        if 'article' in topic_data and topic_data['article']:
            article_data = topic_data['article']
            self._upsert_article(topic_id, article_data)
            return
        
        # 如果话题类型是article但没有article字段，从title等信息构建
        topic_type = topic_data.get('type', '')
        if topic_type == 'article' and topic_data.get('title'):
            article_data = {
                'title': topic_data.get('title', ''),
                'article_id': str(topic_id),  # 使用topic_id作为article_id
                'article_url': '',  # 暂时为空
                'inline_article_url': ''  # 暂时为空
            }
            self._upsert_article(topic_id, article_data)
    
    def _upsert_article(self, topic_id: int, article_data: Dict[str, Any]):
        """插入或更新文章信息"""
        title = article_data.get('title', '')
        article_id = article_data.get('article_id', '')
        
        if not title and not article_id:
            return
        
        # 获取话题的创建时间作为文章创建时间
        self.cursor.execute('''
            SELECT create_time FROM topics WHERE topic_id = ?
        ''', (topic_id,))
        result = self.cursor.fetchone()
        created_at = result[0] if result else ''
        
        self.cursor.execute('''
            INSERT OR REPLACE INTO articles
            (topic_id, title, article_id, article_url, inline_article_url, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            topic_id,
            title,
            article_id,
            article_data.get('article_url', ''),
            article_data.get('inline_article_url', ''),
            created_at
        ))

    def _import_files(self, topic_id: int, files_data: List[Dict[str, Any]]):
        """导入话题文件信息"""
        if not files_data:
            return

        for file_data in files_data:
            if not file_data.get('file_id'):
                continue

            # 获取当前时间作为created_at（使用东八区时间格式）
            from datetime import datetime, timezone, timedelta
            beijing_tz = timezone(timedelta(hours=8))
            current_time = datetime.now(beijing_tz).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'

            self.cursor.execute('''
                INSERT OR REPLACE INTO topic_files
                (topic_id, file_id, name, hash, size, duration, download_count, create_time, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                topic_id,
                file_data.get('file_id'),
                file_data.get('name', ''),
                file_data.get('hash', ''),
                file_data.get('size', 0),
                file_data.get('duration', 0),
                file_data.get('download_count', 0),
                file_data.get('create_time', ''),
                current_time
            ))


    def get_topic_detail(self, topic_id: int):
        """获取完整的话题详情"""
        try:
            # 1. 获取基本话题信息和群组信息
            self.cursor.execute('''
                SELECT
                    t.topic_id, t.type, t.title, t.create_time, t.digested, t.sticky,
                    t.likes_count, t.tourist_likes_count, t.rewards_count, t.comments_count,
                    t.reading_count, t.readers_count, t.answered, t.silenced, t.annotation,
                    t.user_liked, t.user_subscribed,
                    g.group_id, g.name as group_name, g.type as group_type, g.background_url
                FROM topics t
                LEFT JOIN groups g ON t.group_id = g.group_id
                WHERE t.topic_id = ?
            ''', (topic_id,))

            topic_row = self.cursor.fetchone()
            if not topic_row:
                return None

            # 构建基本话题信息
            topic_detail = {
                "topic_id": topic_row[0],
                "type": topic_row[1],
                "title": topic_row[2],
                "create_time": topic_row[3],
                "digested": bool(topic_row[4]),
                "sticky": bool(topic_row[5]),
                "likes_count": topic_row[6],
                "tourist_likes_count": topic_row[7],
                "rewards_count": topic_row[8],
                "comments_count": topic_row[9],
                "reading_count": topic_row[10],
                "readers_count": topic_row[11],
                "answered": bool(topic_row[12]),
                "silenced": bool(topic_row[13]),
                "annotation": topic_row[14],
                "group": {
                    "group_id": topic_row[17],
                    "name": topic_row[18],
                    "type": topic_row[19],
                    "background_url": topic_row[20]
                },
                "user_specific": {
                    "liked": bool(topic_row[15]),
                    "liked_emojis": [],
                    "subscribed": bool(topic_row[16])
                }
            }

            # 2. 获取话题内容（talk）
            self.cursor.execute('''
                SELECT
                    t.text,
                    u.user_id, u.name, u.alias, u.avatar_url, u.location, u.description
                FROM talks t
                LEFT JOIN users u ON t.owner_user_id = u.user_id
                WHERE t.topic_id = ?
                LIMIT 1
            ''', (topic_id,))

            talk_row = self.cursor.fetchone()
            if talk_row:
                talk_data = {
                    "text": talk_row[0],
                    "owner": {
                        "user_id": talk_row[1],
                        "name": talk_row[2],
                        "alias": talk_row[3],
                        "avatar_url": talk_row[4],
                        "location": talk_row[5],
                        "description": talk_row[6]
                    }
                }

                # 获取话题图片
                self.cursor.execute('''
                    SELECT
                        image_id, type, thumbnail_url, thumbnail_width, thumbnail_height,
                        large_url, large_width, large_height,
                        original_url, original_width, original_height, original_size
                    FROM images
                    WHERE topic_id = ? AND comment_id IS NULL
                    ORDER BY image_id
                ''', (topic_id,))

                images = []
                for img_row in self.cursor.fetchall():
                    images.append({
                        "image_id": img_row[0],
                        "type": img_row[1],
                        "thumbnail": {
                            "url": img_row[2],
                            "width": img_row[3],
                            "height": img_row[4]
                        },
                        "large": {
                            "url": img_row[5],
                            "width": img_row[6],
                            "height": img_row[7]
                        },
                        "original": {
                            "url": img_row[8],
                            "width": img_row[9],
                            "height": img_row[10],
                            "size": img_row[11]
                        }
                    })

                if images:
                    talk_data["images"] = images

                # 获取话题文件
                self.cursor.execute('''
                    SELECT
                        file_id, name, hash, size, duration, download_count, create_time
                    FROM topic_files
                    WHERE topic_id = ?
                    ORDER BY file_id
                ''', (topic_id,))

                files = []
                for file_row in self.cursor.fetchall():
                    files.append({
                        "file_id": file_row[0],
                        "name": file_row[1],
                        "hash": file_row[2],
                        "size": file_row[3],
                        "duration": file_row[4],
                        "download_count": file_row[5],
                        "create_time": file_row[6]
                    })

                if files:
                    talk_data["files"] = files

                topic_detail["talk"] = talk_data

            # 3. 获取最新点赞
            self.cursor.execute('''
                SELECT
                    l.create_time,
                    u.user_id, u.name, u.avatar_url
                FROM likes l
                LEFT JOIN users u ON l.user_id = u.user_id
                WHERE l.topic_id = ?
                ORDER BY l.create_time DESC
                LIMIT 5
            ''', (topic_id,))

            latest_likes = []
            for like_row in self.cursor.fetchall():
                latest_likes.append({
                    "create_time": like_row[0],
                    "owner": {
                        "user_id": like_row[1],
                        "name": like_row[2],
                        "avatar_url": like_row[3]
                    }
                })
            topic_detail["latest_likes"] = latest_likes

            # 4. 获取评论 - 不再限制为10条，返回所有评论
            self.cursor.execute('''
                SELECT
                    c.comment_id, c.text, c.create_time, c.likes_count, c.rewards_count, c.sticky,
                    c.parent_comment_id, c.replies_count,
                    u.user_id, u.name, u.alias, u.avatar_url, u.location, u.description,
                    r.user_id as repliee_user_id, r.name as repliee_name, r.avatar_url as repliee_avatar_url
                FROM comments c
                LEFT JOIN users u ON c.owner_user_id = u.user_id
                LEFT JOIN users r ON c.repliee_user_id = r.user_id
                WHERE c.topic_id = ?
                ORDER BY c.create_time ASC
            ''', (topic_id,))

            show_comments = []
            for comment_row in self.cursor.fetchall():
                comment_id = comment_row[0]
                comment_data = {
                    "comment_id": comment_id,
                    "text": comment_row[1],
                    "create_time": comment_row[2],
                    "likes_count": comment_row[3],
                    "rewards_count": comment_row[4],
                    "sticky": bool(comment_row[5]),
                    "parent_comment_id": comment_row[6],
                    "replies_count": comment_row[7],
                    "owner": {
                        "user_id": comment_row[8],
                        "name": comment_row[9],
                        "alias": comment_row[10],
                        "avatar_url": comment_row[11],
                        "location": comment_row[12],
                        "description": comment_row[13]
                    }
                }

                # 添加回复人信息（如果存在）
                if comment_row[14]:  # repliee_user_id
                    comment_data["repliee"] = {
                        "user_id": comment_row[14],
                        "name": comment_row[15],
                        "avatar_url": comment_row[16]
                    }

                # 获取评论的图片
                self.cursor.execute('''
                    SELECT
                        image_id, type, thumbnail_url, thumbnail_width, thumbnail_height,
                        large_url, large_width, large_height,
                        original_url, original_width, original_height, original_size
                    FROM images
                    WHERE comment_id = ?
                    ORDER BY image_id
                ''', (comment_id,))

                images = []
                for img_row in self.cursor.fetchall():
                    images.append({
                        "image_id": img_row[0],
                        "type": img_row[1],
                        "thumbnail": {
                            "url": img_row[2],
                            "width": img_row[3],
                            "height": img_row[4]
                        },
                        "large": {
                            "url": img_row[5],
                            "width": img_row[6],
                            "height": img_row[7]
                        },
                        "original": {
                            "url": img_row[8],
                            "width": img_row[9],
                            "height": img_row[10],
                            "size": img_row[11]
                        }
                    })

                if images:
                    comment_data["images"] = images

                show_comments.append(comment_data)
            topic_detail["show_comments"] = show_comments

            # 5. 获取点赞详情（表情）
            self.cursor.execute('''
                SELECT emoji_key, likes_count
                FROM like_emojis
                WHERE topic_id = ?
            ''', (topic_id,))

            emojis = []
            for emoji_row in self.cursor.fetchall():
                emojis.append({
                    "emoji_key": emoji_row[0],
                    "likes_count": emoji_row[1]
                })

            topic_detail["likes_detail"] = {
                "emojis": emojis
            }

            # 6. 获取问答数据（如果是问答类型话题）
            if topic_detail["type"] == "q&a":
                # 获取问题信息
                self.cursor.execute('''
                    SELECT
                        q.text, q.expired, q.anonymous, q.owner_questions_count,
                        q.owner_join_time, q.owner_status, q.owner_location,
                        owner.user_id as owner_user_id, owner.name as owner_name,
                        owner.alias as owner_alias, owner.avatar_url as owner_avatar_url,
                        owner.location as owner_location_detail, owner.description as owner_description,
                        questionee.user_id as questionee_user_id, questionee.name as questionee_name,
                        questionee.alias as questionee_alias, questionee.avatar_url as questionee_avatar_url,
                        questionee.location as questionee_location, questionee.description as questionee_description
                    FROM questions q
                    LEFT JOIN users owner ON q.owner_user_id = owner.user_id
                    LEFT JOIN users questionee ON q.questionee_user_id = questionee.user_id
                    WHERE q.topic_id = ?
                    LIMIT 1
                ''', (topic_id,))

                question_row = self.cursor.fetchone()
                if question_row:
                    question_data = {
                        "text": question_row[0],
                        "expired": bool(question_row[1]),
                        "anonymous": bool(question_row[2]),
                        "owner_detail": {
                            "questions_count": question_row[3],
                            "estimated_join_time": question_row[4],
                            "status": question_row[5]
                        },
                        "owner_location": question_row[6]
                    }

                    # 添加被提问者信息
                    if question_row[12]:  # questionee_user_id
                        question_data["questionee"] = {
                            "user_id": question_row[12],
                            "name": question_row[13],
                            "alias": question_row[14],
                            "avatar_url": question_row[15],
                            "location": question_row[16],
                            "description": question_row[17]
                        }

                    # 如果不是匿名且有提问者信息，添加提问者信息
                    if not question_data["anonymous"] and question_row[7]:  # owner_user_id
                        question_data["owner"] = {
                            "user_id": question_row[7],
                            "name": question_row[8],
                            "alias": question_row[9],
                            "avatar_url": question_row[10],
                            "location": question_row[11],
                            "description": question_row[11]
                        }

                    topic_detail["question"] = question_data

                # 获取回答信息
                self.cursor.execute('''
                    SELECT
                        a.text,
                        u.user_id, u.name, u.alias, u.avatar_url, u.location, u.description
                    FROM answers a
                    LEFT JOIN users u ON a.owner_user_id = u.user_id
                    WHERE a.topic_id = ?
                    LIMIT 1
                ''', (topic_id,))

                answer_row = self.cursor.fetchone()
                if answer_row:
                    answer_data = {
                        "text": answer_row[0],
                        "owner": {
                            "user_id": answer_row[1],
                            "name": answer_row[2],
                            "alias": answer_row[3],
                            "avatar_url": answer_row[4],
                            "location": answer_row[5],
                            "description": answer_row[6]
                        }
                    }
                    topic_detail["answer"] = answer_data

            return topic_detail

        except Exception as e:
            print(f"获取话题详情失败: {e}")
            return None
    
    def _import_tags(self, topic_id: int, topic_data: Dict[str, Any]):
        """从话题数据中提取并导入标签信息"""
        import re
        
        group_id = topic_data.get('group', {}).get('group_id')
        if not group_id:
            return
        
        # 收集所有可能包含标签的文本内容
        text_contents = []
        
        # 从talk内容中提取
        if 'talk' in topic_data and topic_data['talk'] and 'text' in topic_data['talk']:
            text_contents.append(topic_data['talk']['text'])
        
        # 从question内容中提取
        if 'question' in topic_data and topic_data['question'] and 'text' in topic_data['question']:
            text_contents.append(topic_data['question']['text'])
        
        # 从answer内容中提取
        if 'answer' in topic_data and topic_data['answer'] and 'text' in topic_data['answer']:
            text_contents.append(topic_data['answer']['text'])
        
        # 从评论中提取
        if 'show_comments' in topic_data:
            for comment in topic_data['show_comments']:
                if 'text' in comment:
                    text_contents.append(comment['text'])
        
        # 提取所有标签
        all_tags = set()
        for text in text_contents:
            if text:
                # 使用正则表达式提取标签 <e type="hashtag" hid="..." title="..." />
                tag_pattern = r'<e\s+type="hashtag"\s+hid="([^"]+)"\s+title="([^"]+)"\s*/>'
                matches = re.findall(tag_pattern, text)
                for hid, encoded_title in matches:
                    try:
                        # 解码标签名称
                        import urllib.parse
                        tag_name = urllib.parse.unquote(encoded_title)
                        # 移除可能的#符号
                        tag_name = tag_name.strip('#')
                        if tag_name:
                            all_tags.add((tag_name, hid))
                    except Exception as e:
                        print(f"解码标签失败: {e}")
        
        # 为每个标签创建或更新数据库记录
        for tag_name, hid in all_tags:
            tag_id = self._upsert_tag(group_id, tag_name, hid)
            if tag_id:
                self._link_topic_tag(topic_id, tag_id)
    
    def _upsert_tag(self, group_id: int, tag_name: str, hid: str = None) -> Optional[int]:
        """插入或更新标签信息"""
        try:
            # 检查标签是否已存在
            self.cursor.execute('''
                SELECT tag_id FROM tags WHERE group_id = ? AND tag_name = ?
            ''', (group_id, tag_name))
            
            result = self.cursor.fetchone()
            if result:
                tag_id = result[0]
                # 更新hid（如果提供了新的hid）
                if hid:
                    self.cursor.execute('''
                        UPDATE tags SET hid = ? WHERE tag_id = ?
                    ''', (hid, tag_id))
                return tag_id
            else:
                # 插入新标签
                from datetime import datetime, timezone, timedelta
                beijing_tz = timezone(timedelta(hours=8))
                current_time = datetime.now(beijing_tz).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
                
                self.cursor.execute('''
                    INSERT INTO tags (group_id, tag_name, hid, created_at)
                    VALUES (?, ?, ?, ?)
                ''', (group_id, tag_name, hid, current_time))
                
                return self.cursor.lastrowid
        except Exception as e:
            print(f"插入标签失败: {e}")
            return None
    
    def _link_topic_tag(self, topic_id: int, tag_id: int):
        """关联话题和标签"""
        try:
            from datetime import datetime, timezone, timedelta
            beijing_tz = timezone(timedelta(hours=8))
            current_time = datetime.now(beijing_tz).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
            
            self.cursor.execute('''
                INSERT OR IGNORE INTO topic_tags (topic_id, tag_id, created_at)
                VALUES (?, ?, ?)
            ''', (topic_id, tag_id, current_time))
            
            # 更新标签的话题计数
            self.cursor.execute('''
                UPDATE tags SET topic_count = (
                    SELECT COUNT(*) FROM topic_tags WHERE tag_id = ?
                ) WHERE tag_id = ?
            ''', (tag_id, tag_id))
            
        except Exception as e:
            print(f"关联话题标签失败: {e}")
    
    def get_tags_by_group(self, group_id: int) -> List[Dict[str, Any]]:
        """获取指定群组的所有标签"""
        try:
            self.cursor.execute('''
                SELECT tag_id, tag_name, hid, topic_count, created_at
                FROM tags
                WHERE group_id = ?
                ORDER BY topic_count DESC, tag_name ASC
            ''', (group_id,))
            
            tags = []
            for row in self.cursor.fetchall():
                tags.append({
                    'tag_id': row[0],
                    'tag_name': row[1],
                    'hid': row[2],
                    'topic_count': row[3],
                    'created_at': row[4]
                })
            
            return tags
        except Exception as e:
            print(f"获取标签列表失败: {e}")
            return []
    
    def get_topics_by_tag(self, tag_id: int, page: int = 1, per_page: int = 20) -> Dict[str, Any]:
        """根据标签获取话题列表"""
        try:
            offset = (page - 1) * per_page
            
            # 获取话题列表 - 包含所有详细信息，与get_group_topics保持一致
            self.cursor.execute('''
                SELECT
                    t.topic_id, t.title, t.create_time, t.likes_count, t.comments_count,
                    t.reading_count, t.type, t.digested, t.sticky,
                    q.text as question_text,
                    a.text as answer_text,
                    tk.text as talk_text,
                    u.user_id, u.name, u.avatar_url
                FROM topics t
                INNER JOIN topic_tags tt ON t.topic_id = tt.topic_id
                LEFT JOIN questions q ON t.topic_id = q.topic_id
                LEFT JOIN answers a ON t.topic_id = a.topic_id
                LEFT JOIN talks tk ON t.topic_id = tk.topic_id
                LEFT JOIN users u ON tk.owner_user_id = u.user_id
                WHERE tt.tag_id = ?
                ORDER BY t.create_time DESC
                LIMIT ? OFFSET ?
            ''', (tag_id, per_page, offset))
            
            topics = []
            for topic in self.cursor.fetchall():
                topic_data = {
                    "topic_id": topic[0],
                    "title": topic[1],
                    "create_time": topic[2],
                    "likes_count": topic[3],
                    "comments_count": topic[4],
                    "reading_count": topic[5],
                    "type": topic[6],
                    "digested": bool(topic[7]) if topic[7] is not None else False,
                    "sticky": bool(topic[8]) if topic[8] is not None else False
                }

                # 添加内容文本
                if topic[6] == 'q&a':
                    # 问答类型话题
                    topic_data['question_text'] = topic[9] if topic[9] else ''
                    topic_data['answer_text'] = topic[10] if topic[10] else ''
                else:
                    # 其他类型话题（talk、article等）
                    topic_data['talk_text'] = topic[11] if topic[11] else ''
                    if topic[12]:  # 有作者信息
                        topic_data['author'] = {
                            'user_id': topic[12],
                            'name': topic[13],
                            'avatar_url': topic[14]
                        }

                topics.append(topic_data)
            
            # 获取总数
            self.cursor.execute('''
                SELECT COUNT(*)
                FROM topic_tags
                WHERE tag_id = ?
            ''', (tag_id,))
            total = self.cursor.fetchone()[0]
            
            return {
                'topics': topics,
                'pagination': {
                    'page': page,
                    'per_page': per_page,
                    'total': total,
                    'pages': (total + per_page - 1) // per_page
                }
            }
        except Exception as e:
            print(f"根据标签获取话题失败: {e}")
            return {'topics': [], 'pagination': {'page': page, 'per_page': per_page, 'total': 0, 'pages': 0}}

    def close(self):
        """关闭数据库连接"""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()

    def __del__(self):
        """析构函数，确保数据库连接被关闭"""
        self.close()