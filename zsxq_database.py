#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sqlite3
from typing import Dict, Any, Optional, List


class ZSXQDatabase:
    """知识星球数据库管理器"""
    
    def __init__(self, db_path: str = "zsxq_interactive.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
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
        
        self.conn.commit()
    
    def import_topic_data(self, topic_data: Dict[str, Any]) -> bool:
        """导入话题数据到数据库"""
        try:
            topic_id = topic_data.get('topic_id')
            group_info = topic_data.get('group', {})
            
            if not topic_id:
                return False
            
            print(f"🔄 导入话题数据: topic_id={topic_id}")
            
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
            
            print(f"✅ 话题数据导入完成: topic_id={topic_id}")
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
            if 'owner' in topic_data['question']:
                self._upsert_user(topic_data['question']['owner'])
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
        print(f"   📝 导入talk数据: owner_id={owner_user_id}")
    
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
        print(f"   🖼️ 导入图片: image_id={image_id}")
    
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
            print(f"   👍 导入点赞数据: {len(topic_data['latest_likes'])}条")
    
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
            print(f"   😊 导入表情点赞数据: {len(topic_data['likes_detail']['emojis'])}条")
    
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
            print(f"   😍 导入用户表情点赞数据: {len(topic_data['user_specific']['liked_emojis'])}条")
    
    def _import_comments(self, topic_id: int, comments: List[Dict[str, Any]]):
        """导入评论信息"""
        for comment in comments:
            self._upsert_comment(topic_id, comment)
        
        if comments:
            print(f"   💬 导入评论数据: {len(comments)}条")
    
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
    
    def _upsert_question(self, topic_id: int, question_data: Dict[str, Any]):
        """插入或更新问题信息"""
        owner_user_id = question_data.get('owner', {}).get('user_id')
        questionee_user_id = question_data.get('questionee', {}).get('user_id')
        
        if not owner_user_id:
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
            owner_user_id,
            questionee_user_id,
            question_data.get('text', ''),
            question_data.get('expired', False),
            question_data.get('anonymous', False),
            owner_detail.get('questions_count'),
            owner_detail.get('join_time', ''),
            owner_detail.get('status', ''),
            question_data.get('owner_location', ''),
            current_time
        ))
        print(f"   ❓ 导入问题数据: owner_id={owner_user_id}")
    
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
        print(f"   💡 导入回答数据: owner_id={owner_user_id}")
    
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
        print(f"   📄 导入文章数据: title={title[:20]}...")
    
    def close(self):
        """关闭数据库连接"""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
    
    def __del__(self):
        """析构函数，确保数据库连接被关闭"""
        self.close()