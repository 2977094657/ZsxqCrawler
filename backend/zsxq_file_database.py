import sqlite3
from typing import Dict, List, Any, Optional


class ZSXQFileDatabase:
    """知识星球文件列表数据库管理工具 - 完全匹配API响应结构"""
    
    def __init__(self, db_path: str = "zsxq_files_complete.db"):
        """初始化数据库连接"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self.create_tables()
    
    def create_tables(self):
        """创建所有必需的数据表 - 完全匹配API响应结构"""
        
        # 1. API响应记录表
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS api_responses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            succeeded BOOLEAN,
            index_value TEXT,
            files_count INTEGER,
            request_url TEXT,
            request_params TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # 2. 文件主表 (files数组中的file对象)
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS files (
            file_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            hash TEXT,
            size INTEGER,
            duration INTEGER,
            download_count INTEGER,
            create_time TEXT,
            imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            download_status TEXT DEFAULT 'pending',
            local_path TEXT,
            download_time TIMESTAMP
        )
        ''')

        # 执行数据库迁移
        self._migrate_database()
        
        # 3. 群组表 (topic.group对象)
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS groups (
            group_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            type TEXT,
            background_url TEXT,
            imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # 4. 用户表 (所有用户信息的统一表)
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            alias TEXT,
            avatar_url TEXT,
            description TEXT,
            location TEXT,
            ai_comment_url TEXT,
            imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # 5. 话题主表 (topic对象)
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS topics (
            topic_id INTEGER PRIMARY KEY,
            group_id INTEGER,
            type TEXT NOT NULL,
            title TEXT,
            annotation TEXT,
            likes_count INTEGER DEFAULT 0,
            tourist_likes_count INTEGER DEFAULT 0,
            rewards_count INTEGER DEFAULT 0,
            comments_count INTEGER DEFAULT 0,
            reading_count INTEGER DEFAULT 0,
            readers_count INTEGER DEFAULT 0,
            digested BOOLEAN DEFAULT FALSE,
            sticky BOOLEAN DEFAULT FALSE,
            create_time TEXT,
            modify_time TEXT,
            user_liked BOOLEAN DEFAULT FALSE,
            user_subscribed BOOLEAN DEFAULT FALSE,
            imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (group_id) REFERENCES groups (group_id)
        )
        ''')
        
        # 6. 文件-话题关联表
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS file_topic_relations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER,
            topic_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (file_id) REFERENCES files (file_id),
            FOREIGN KEY (topic_id) REFERENCES topics (topic_id)
        )
        ''')
        
        # 7. 话题内容表 (talk对象)
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
        
        # 8. 图片表 (images数组)
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS images (
            image_id INTEGER PRIMARY KEY,
            topic_id INTEGER,
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
        
        # 9. 话题文件表 (talk.files数组)
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
        
        # 10. 最新点赞表 (latest_likes数组)
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS latest_likes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            topic_id INTEGER,
            owner_user_id INTEGER,
            create_time TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (topic_id) REFERENCES topics (topic_id),
            FOREIGN KEY (owner_user_id) REFERENCES users (user_id)
        )
        ''')
        
        # 11. 评论表 (show_comments数组)
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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (topic_id) REFERENCES topics (topic_id),
            FOREIGN KEY (owner_user_id) REFERENCES users (user_id),
            FOREIGN KEY (parent_comment_id) REFERENCES comments (comment_id),
            FOREIGN KEY (repliee_user_id) REFERENCES users (user_id)
        )
        ''')
        
        # 12. 点赞详情表情表 (likes_detail.emojis数组)
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
        
        # 13. 用户点赞表情表 (user_specific.liked_emojis数组)
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_liked_emojis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            topic_id INTEGER,
            emoji_key TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (topic_id) REFERENCES topics (topic_id)
        )
        ''')
        
        # 14. 栏目表 (columns数组)
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS columns (
            column_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        # 15. 话题-栏目关联表
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS topic_columns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            topic_id INTEGER,
            column_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (topic_id) REFERENCES topics (topic_id),
            FOREIGN KEY (column_id) REFERENCES columns (column_id)
        )
        ''')
        
        # 16. 解决方案表 (solution对象)
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS solutions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            topic_id INTEGER,
            task_id INTEGER,
            owner_user_id INTEGER,
            text TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (topic_id) REFERENCES topics (topic_id),
            FOREIGN KEY (owner_user_id) REFERENCES users (user_id)
        )
        ''')
        
        # 17. 解决方案文件表 (solution.files数组)
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS solution_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            solution_id INTEGER,
            file_id INTEGER,
            name TEXT,
            hash TEXT,
            size INTEGER,
            duration INTEGER,
            download_count INTEGER,
            create_time TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (solution_id) REFERENCES solutions (id)
        )
        ''')
        
        # 18. 收集日志表 (用于记录文件收集过程)
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS collection_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_time TEXT NOT NULL,
            end_time TEXT,
            total_files INTEGER DEFAULT 0,
            new_files INTEGER DEFAULT 0,
            status TEXT DEFAULT 'running',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        self.conn.commit()
        print("✅ 完整数据库表结构创建成功")
    
    def insert_user(self, user_data: Dict[str, Any]) -> Optional[int]:
        """插入或更新用户信息"""
        if not user_data or not user_data.get('user_id'):
            return None
            
        self.cursor.execute('''
        INSERT OR REPLACE INTO users 
        (user_id, name, alias, avatar_url, description, location, ai_comment_url)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            user_data.get('user_id'),
            user_data.get('name', ''),
            user_data.get('alias'),
            user_data.get('avatar_url'),
            user_data.get('description'),
            user_data.get('location'),
            user_data.get('ai_comment_url')
        ))
        return user_data.get('user_id')
    
    def insert_group(self, group_data: Dict[str, Any]) -> Optional[int]:
        """插入或更新群组信息"""
        if not group_data or not group_data.get('group_id'):
            return None
            
        self.cursor.execute('''
        INSERT OR REPLACE INTO groups 
        (group_id, name, type, background_url)
        VALUES (?, ?, ?, ?)
        ''', (
            group_data.get('group_id'),
            group_data.get('name', ''),
            group_data.get('type'),
            group_data.get('background_url')
        ))
        return group_data.get('group_id')
    
    def insert_file(self, file_data: Dict[str, Any]) -> Optional[int]:
        """插入或更新文件信息"""
        if not file_data or not file_data.get('file_id'):
            return None

        file_id = file_data.get('file_id')

        # 文件列表刷新只更新元数据，不覆盖已下载状态和本地路径。
        self.cursor.execute('''
        INSERT OR IGNORE INTO files
        (file_id, name, hash, size, duration, download_count, create_time)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            file_id,
            file_data.get('name', ''),
            file_data.get('hash'),
            file_data.get('size'),
            file_data.get('duration'),
            file_data.get('download_count'),
            file_data.get('create_time')
        ))

        self.cursor.execute('''
        UPDATE files
        SET name = ?,
            hash = ?,
            size = ?,
            duration = ?,
            download_count = ?,
            create_time = ?
        WHERE file_id = ?
        ''', (
            file_data.get('name', ''),
            file_data.get('hash'),
            file_data.get('size'),
            file_data.get('duration'),
            file_data.get('download_count'),
            file_data.get('create_time'),
            file_id
        ))
        return file_id
    
    def insert_topic(self, topic_data: Dict[str, Any]) -> Optional[int]:
        """插入或更新话题信息"""
        if not topic_data or not topic_data.get('topic_id'):
            return None
        
        # 处理用户特定信息
        user_specific = topic_data.get('user_specific', {})
        
        self.cursor.execute('''
        INSERT OR REPLACE INTO topics 
        (topic_id, group_id, type, title, annotation, likes_count, tourist_likes_count,
         rewards_count, comments_count, reading_count, readers_count, digested, sticky,
         create_time, modify_time, user_liked, user_subscribed)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            topic_data.get('topic_id'),
            topic_data.get('group', {}).get('group_id'),
            topic_data.get('type'),
            topic_data.get('title'),
            topic_data.get('annotation'),
            topic_data.get('likes_count', 0),
            topic_data.get('tourist_likes_count', 0),
            topic_data.get('rewards_count', 0),
            topic_data.get('comments_count', 0),
            topic_data.get('reading_count', 0),
            topic_data.get('readers_count', 0),
            topic_data.get('digested', False),
            topic_data.get('sticky', False),
            topic_data.get('create_time'),
            topic_data.get('modify_time'),  # 新增字段
            user_specific.get('liked', False),
            user_specific.get('subscribed', False)
        ))
        return topic_data.get('topic_id')
    
    def insert_talk(self, topic_id: int, talk_data: Dict[str, Any]):
        """插入话题内容"""
        if not talk_data:
            return
            
        owner = talk_data.get('owner', {})
        owner_id = self.insert_user(owner)
        
        self.cursor.execute('''
        INSERT OR IGNORE INTO talks (topic_id, owner_user_id, text)
        VALUES (?, ?, ?)
        ''', (topic_id, owner_id, talk_data.get('text', '')))
    
    def insert_images(self, topic_id: int, images_data: List[Dict[str, Any]]):
        """插入图片信息"""
        for image in images_data:
            if not image.get('image_id'):
                continue
                
            thumbnail = image.get('thumbnail', {})
            large = image.get('large', {})
            original = image.get('original', {})
            
            self.cursor.execute('''
            INSERT OR REPLACE INTO images 
            (image_id, topic_id, type, thumbnail_url, thumbnail_width, thumbnail_height,
             large_url, large_width, large_height, original_url, original_width, original_height, original_size)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                image.get('image_id'),
                topic_id,
                image.get('type'),
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
    
    def insert_topic_files(self, topic_id: int, files_data: List[Dict[str, Any]]):
        """插入话题关联的文件"""
        for file in files_data:
            if not file.get('file_id'):
                continue
                
            self.cursor.execute('''
            INSERT OR REPLACE INTO topic_files 
            (topic_id, file_id, name, hash, size, duration, download_count, create_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                topic_id,
                file.get('file_id'),
                file.get('name', ''),
                file.get('hash'),
                file.get('size'),
                file.get('duration'),
                file.get('download_count'),
                file.get('create_time')
            ))
    
    def insert_latest_likes(self, topic_id: int, likes_data: List[Dict[str, Any]]):
        """插入最新点赞记录"""
        for like in likes_data:
            owner = like.get('owner', {})
            owner_id = self.insert_user(owner)
            
            self.cursor.execute('''
            INSERT OR IGNORE INTO latest_likes (topic_id, owner_user_id, create_time)
            VALUES (?, ?, ?)
            ''', (topic_id, owner_id, like.get('create_time')))
    
    def insert_comments(self, topic_id: int, comments_data: List[Dict[str, Any]]):
        """插入评论信息"""
        for comment in comments_data:
            if not comment.get('comment_id'):
                continue
                
            owner = comment.get('owner', {})
            owner_id = self.insert_user(owner)
            
            repliee = comment.get('repliee', {})
            repliee_id = self.insert_user(repliee) if repliee else None
            
            self.cursor.execute('''
            INSERT OR REPLACE INTO comments 
            (comment_id, topic_id, owner_user_id, parent_comment_id, repliee_user_id,
             text, create_time, likes_count, rewards_count, replies_count, sticky)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                comment.get('comment_id'),
                topic_id,
                owner_id,
                comment.get('parent_comment_id'),
                repliee_id,
                comment.get('text', ''),
                comment.get('create_time'),
                comment.get('likes_count', 0),
                comment.get('rewards_count', 0),
                comment.get('replies_count', 0),
                comment.get('sticky', False)
            ))
    
    def insert_like_emojis(self, topic_id: int, likes_detail: Dict[str, Any]):
        """插入点赞表情详情"""
        emojis = likes_detail.get('emojis', [])
        for emoji in emojis:
            self.cursor.execute('''
            INSERT OR REPLACE INTO like_emojis (topic_id, emoji_key, likes_count)
            VALUES (?, ?, ?)
            ''', (topic_id, emoji.get('emoji_key'), emoji.get('likes_count', 0)))
    
    def insert_user_liked_emojis(self, topic_id: int, liked_emojis: List[str]):
        """插入用户点赞的表情"""
        for emoji_key in liked_emojis:
            self.cursor.execute('''
            INSERT OR IGNORE INTO user_liked_emojis (topic_id, emoji_key)
            VALUES (?, ?)
            ''', (topic_id, emoji_key))
    
    def insert_columns(self, topic_id: int, columns_data: List[Dict[str, Any]]):
        """插入栏目信息"""
        for column in columns_data:
            if not column.get('column_id'):
                continue
                
            # 插入栏目
            self.cursor.execute('''
            INSERT OR REPLACE INTO columns (column_id, name)
            VALUES (?, ?)
            ''', (column.get('column_id'), column.get('name', '')))
            
            # 插入话题-栏目关联
            self.cursor.execute('''
            INSERT OR IGNORE INTO topic_columns (topic_id, column_id)
            VALUES (?, ?)
            ''', (topic_id, column.get('column_id')))
    
    def insert_solution(self, topic_id: int, solution_data: Dict[str, Any]):
        """插入解决方案信息"""
        if not solution_data:
            return None
            
        owner = solution_data.get('owner', {})
        owner_id = self.insert_user(owner)
        
        self.cursor.execute('''
        INSERT OR REPLACE INTO solutions (topic_id, task_id, owner_user_id, text)
        VALUES (?, ?, ?, ?)
        ''', (
            topic_id,
            solution_data.get('task_id'),
            owner_id,
            solution_data.get('text', '')
        ))
        
        solution_id = self.cursor.lastrowid
        
        # 插入解决方案文件
        files = solution_data.get('files', [])
        for file in files:
            self.cursor.execute('''
            INSERT OR REPLACE INTO solution_files 
            (solution_id, file_id, name, hash, size, duration, download_count, create_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                solution_id,
                file.get('file_id'),
                file.get('name', ''),
                file.get('hash'),
                file.get('size'),
                file.get('duration'),
                file.get('download_count'),
                file.get('create_time')
            ))
        
        return solution_id
    
    def import_file_response(self, response_data: Dict[str, Any]) -> Dict[str, int]:
        """导入文件API响应数据"""
        stats = {
            'files': 0,
            'topics': 0,
            'users': 0,
            'groups': 0,
            'images': 0,
            'comments': 0,
            'likes': 0,
            'columns': 0,
            'solutions': 0
        }
        
        try:
            # 记录API响应
            files_data = response_data.get('resp_data', {}).get('files', [])
            self.cursor.execute('''
            INSERT INTO api_responses (succeeded, index_value, files_count)
            VALUES (?, ?, ?)
            ''', (
                response_data.get('succeeded', False),
                response_data.get('resp_data', {}).get('index'),
                len(files_data)
            ))
            
            # 处理每个文件和关联的话题
            for item in files_data:
                file_data = item.get('file', {})
                topic_data = item.get('topic', {})
                
                if not file_data.get('file_id') or not topic_data.get('topic_id'):
                    continue
                
                # 插入文件
                file_id = self.insert_file(file_data)
                if file_id:
                    stats['files'] += 1
                
                # 插入群组
                group_data = topic_data.get('group', {})
                if group_data:
                    group_id = self.insert_group(group_data)
                    if group_id:
                        stats['groups'] += 1
                
                # 插入话题
                topic_id = self.insert_topic(topic_data)
                if topic_id:
                    stats['topics'] += 1
                    
                    # 插入文件-话题关联
                    self.cursor.execute('''
                    INSERT OR IGNORE INTO file_topic_relations (file_id, topic_id)
                    VALUES (?, ?)
                    ''', (file_id, topic_id))
                    
                    # 处理talk信息
                    talk_data = topic_data.get('talk', {})
                    if talk_data:
                        self.insert_talk(topic_id, talk_data)
                        
                        # 处理talk中的图片
                        images = talk_data.get('images', [])
                        if images:
                            self.insert_images(topic_id, images)
                            stats['images'] += len(images)
                        
                        # 处理talk中的文件
                        topic_files = talk_data.get('files', [])
                        if topic_files:
                            self.insert_topic_files(topic_id, topic_files)
                    
                    # 处理最新点赞
                    latest_likes = topic_data.get('latest_likes', [])
                    if latest_likes:
                        self.insert_latest_likes(topic_id, latest_likes)
                        stats['likes'] += len(latest_likes)
                    
                    # 处理评论
                    comments = topic_data.get('show_comments', [])
                    if comments:
                        self.insert_comments(topic_id, comments)
                        stats['comments'] += len(comments)
                    
                    # 处理点赞详情
                    likes_detail = topic_data.get('likes_detail', {})
                    if likes_detail:
                        self.insert_like_emojis(topic_id, likes_detail)
                    
                    # 处理用户点赞表情
                    user_specific = topic_data.get('user_specific', {})
                    liked_emojis = user_specific.get('liked_emojis', [])
                    if liked_emojis:
                        self.insert_user_liked_emojis(topic_id, liked_emojis)
                    
                    # 处理栏目
                    columns = topic_data.get('columns', [])
                    if columns:
                        self.insert_columns(topic_id, columns)
                        stats['columns'] += len(columns)
                    
                    # 处理解决方案
                    solution = topic_data.get('solution', {})
                    if solution:
                        solution_id = self.insert_solution(topic_id, solution)
                        if solution_id:
                            stats['solutions'] += 1
            
            self.conn.commit()
            print(f"✅ 数据导入成功: {stats}")
            return stats
            
        except Exception as e:
            self.conn.rollback()
            print(f"❌ 数据导入失败: {e}")
            raise e
    
    def get_database_stats(self) -> Dict[str, Any]:
        """获取数据库统计信息"""
        stats = {}
        
        tables = [
            'files', 'groups', 'users', 'topics', 'talks', 'images', 
            'topic_files', 'latest_likes', 'comments', 'like_emojis',
            'user_liked_emojis', 'columns', 'topic_columns', 'solutions',
            'solution_files', 'file_topic_relations', 'api_responses', 'collection_log'
        ]
        
        for table in tables:
            self.cursor.execute(f"SELECT COUNT(*) FROM {table}")
            stats[table] = self.cursor.fetchone()[0]
        
        return stats

    def _migrate_database(self):
        """执行数据库迁移，添加新列"""
        migrations = [
            {
                'table': 'files',
                'column': 'download_status',
                'definition': 'TEXT DEFAULT "pending"'
            },
            {
                'table': 'files',
                'column': 'local_path',
                'definition': 'TEXT'
            },
            {
                'table': 'files',
                'column': 'download_time',
                'definition': 'TIMESTAMP'
            }
        ]

        for migration in migrations:
            try:
                # 检查列是否存在
                self.cursor.execute(f"PRAGMA table_info({migration['table']})")
                columns = [column[1] for column in self.cursor.fetchall()]

                if migration['column'] not in columns:
                    sql = f"ALTER TABLE {migration['table']} ADD COLUMN {migration['column']} {migration['definition']}"
                    self.cursor.execute(sql)
                    print(f"📊 添加列: {migration['table']}.{migration['column']}")
            except Exception as e:
                print(f"❌ 迁移失败: {migration['table']}.{migration['column']} - {e}")

        self.conn.commit()

    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()


def main():
    """测试数据库功能"""
    db = ZSXQFileDatabase()
    print("📊 数据库统计:")
    stats = db.get_database_stats()
    for table, count in stats.items():
        print(f"  {table}: {count}")
    db.close()


if __name__ == "__main__":
    main()
