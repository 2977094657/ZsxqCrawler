#!/usr/bin/env python3
"""
知识星球交互式数据采集器
支持多种爬取模式，增强反检测机制
"""

import requests
import time
import random
import json
from typing import Dict, Any, Optional, List
from zsxq_database import ZSXQDatabase
from zsxq_file_downloader import ZSXQFileDownloader
import os
try:
    import tomllib
except ImportError:
    try:
        import tomli as tomllib
    except ImportError:
        print("⚠️ 需要安装tomli库来解析TOML配置文件")
        print("💡 请运行: pip install tomli")
        tomllib = None


class ZSXQInteractiveCrawler:
    """知识星球交互式数据采集器"""
    
    def __init__(self, cookie: str, group_id: str, db_path: str = "zsxq_interactive.db"):
        self.cookie = self.clean_cookie(cookie)
        self.group_id = group_id
        self.db = ZSXQDatabase(db_path)
        self.session = requests.Session()
        
        # 文件下载器（懒加载）
        self.file_downloader = None
        
        # 基础API配置
        self.base_url = "https://api.zsxq.com"
        self.api_endpoint = f"/v2/groups/{group_id}/topics"
        
        # 反检测配置
        self.request_count = 0
        self.page_count = 0  # 成功处理的页面数
        self.last_request_time = 0
        self.min_delay = 2.0  # 最小延迟
        self.max_delay = 5.0  # 最大延迟
        self.long_delay_interval = 15  # 每15个页面进行长延迟
        self.debug_mode = False  # 调试模式
        self.timestamp_offset_ms = 1  # 时间戳减去的毫秒数
        
        print(f"🚀 知识星球交互式采集器初始化完成")
        print(f"📊 目标群组: {group_id}")
        print(f"💾 数据库: {db_path}")
        
        # 显示当前数据库状态
        self.show_database_status()
    
    def clean_cookie(self, cookie: str) -> str:
        """清理Cookie字符串，去除不合法字符
        
        Args:
            cookie (str): 原始Cookie字符串
        
        Returns:
            str: 清理后的Cookie字符串
        """
        try:
            # 如果是bytes类型，先解码
            if isinstance(cookie, bytes):
                cookie = cookie.decode('utf-8')
            
            # 去除多余的空格和换行符
            cookie = cookie.strip()
            
            # 如果有多行，只取第一行
            if '\n' in cookie:
                cookie = cookie.split('\n')[0]
            
            # 去除末尾的反斜杠
            cookie = cookie.rstrip('\\')
            
            # 去除可能的前缀b和引号
            if cookie.startswith("b'") and cookie.endswith("'"):
                cookie = cookie[2:-1]
            elif cookie.startswith('b"') and cookie.endswith('"'):
                cookie = cookie[2:-1]
            elif cookie.startswith("'") and cookie.endswith("'"):
                cookie = cookie[1:-1]
            elif cookie.startswith('"') and cookie.endswith('"'):
                cookie = cookie[1:-1]
            
            # 处理转义字符
            cookie = cookie.replace('\\n', '')
            cookie = cookie.replace('\\"', '"')
            cookie = cookie.replace("\\'", "'")
            
            # 确保分号后有空格
            cookie = '; '.join(part.strip() for part in cookie.split(';'))
            
            return cookie
        except Exception as e:
            print(f"Cookie清理失败: {e}")
            return cookie  # 返回原始值
    
    def get_file_downloader(self):
        """获取文件下载器（懒加载）"""
        if self.file_downloader is None:
            self.file_downloader = ZSXQFileDownloader(self.cookie, self.group_id)
        return self.file_downloader
    
    def show_database_status(self):
        """显示数据库当前状态"""
        stats = self.db.get_database_stats()
        total_topics = stats.get('topics', 0)
        total_users = stats.get('users', 0)
        total_comments = stats.get('comments', 0)
        
        print(f"\n📊 当前数据库状态:")
        print(f"   话题: {total_topics}, 用户: {total_users}, 评论: {total_comments}")
        
        # 显示时间戳范围信息
        if total_topics > 0:
            timestamp_info = self.db.get_timestamp_range_info()
            if timestamp_info['has_data']:
                print(f"   时间范围: {timestamp_info['oldest_timestamp']} ~ {timestamp_info['newest_timestamp']}")
            else:
                print(f"   ⚠️ 时间戳数据不完整")
    
    def get_stealth_headers(self) -> Dict[str, str]:
        """获取隐蔽性更强的请求头"""
        # 更多样化的User-Agent池
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        ]
        
        # 基础头部
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7",
            "Cache-Control": "no-cache",
            "Cookie": self.cookie,
            "Origin": "https://wx.zsxq.com",
            "Pragma": "no-cache",
            "Priority": "u=1, i",
            "Referer": "https://wx.zsxq.com/",
            "Sec-Ch-Ua": '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "User-Agent": random.choice(user_agents),
            "X-Aduid": "a3be07cd6-dd67-3912-0093-862d844e7fe",
            "X-Request-Id": f"dcc5cb6ab-1bc3-8273-cc26-{random.randint(100000000000, 999999999999)}",
            "X-Signature": "733fd672ddf6d4e367730d9622cdd1e28a4b6203",
            "X-Timestamp": str(int(time.time())),
            "X-Version": "2.77.0"
        }
        
        # 随机添加可选头部
        optional_headers = {
            "X-Requested-With": "XMLHttpRequest",
            "Sec-GPC": "1",
            "Upgrade-Insecure-Requests": "1"
        }
        
        for key, value in optional_headers.items():
            if random.random() > 0.4:  # 60%概率添加
                headers[key] = value
        
        return headers
    
    def smart_delay(self, is_historical: bool = False):
        """智能延迟机制 - 模拟人类行为（仅基础延迟）"""
        self.request_count += 1
        
        # 基础延迟时间
        if is_historical:
            delay = random.uniform(self.min_delay + 1.0, self.max_delay + 2.0)  # 历史爬取稍长
        else:
            delay = random.uniform(self.min_delay, self.max_delay)
        
        time.sleep(delay)
        self.last_request_time = time.time()
        
        # 调试信息
        if self.debug_mode:
            print(f"   ⏱️ 延迟: {delay:.2f}秒 (请求#{self.request_count})")
    
    def check_page_long_delay(self):
        """检查页面级长休眠：每15个页面进行长休眠"""
        self.page_count += 1
        
        if self.page_count % self.long_delay_interval == 0:
            import datetime
            
            long_delay = random.uniform(180, 300)  # 3-5分钟长休眠
            start_time = datetime.datetime.now()
            end_time = start_time + datetime.timedelta(seconds=long_delay)
            
            print(f"🛌 长休眠开始: {long_delay:.1f}秒 ({long_delay/60:.1f}分钟)")
            print(f"   已完成 {self.page_count} 个页面，进入长休眠模式...")
            print(f"   ⏰ 开始时间: {start_time.strftime('%H:%M:%S')}")
            print(f"   🕐 预计恢复: {end_time.strftime('%H:%M:%S')}")
            
            time.sleep(long_delay)
            
            actual_end_time = datetime.datetime.now()
            print(f"😴 长休眠结束，继续爬取...")
            print(f"   🕐 实际结束: {actual_end_time.strftime('%H:%M:%S')}")
            
            # 调试信息
            if self.debug_mode:
                actual_duration = (actual_end_time - start_time).total_seconds()
                print(f"   💤 长休眠完成: 预计{long_delay:.1f}秒，实际{actual_duration:.1f}秒 (页面#{self.page_count})")
    
    def fetch_topics_safe(self, scope: str = "all", count: int = 20, 
                         end_time: Optional[str] = None, is_historical: bool = False) -> Optional[Dict[str, Any]]:
        """安全的话题获取方法"""
        
        # 智能延迟
        self.smart_delay(is_historical)
        
        url = f"{self.base_url}{self.api_endpoint}"
        headers = self.get_stealth_headers()
        
        # 构建参数
        params = {
            "scope": scope,
            "count": str(count)
        }
        
        if end_time:
            params["end_time"] = end_time
        
        # 不添加额外参数，保持与官网请求一致
        # random_params = {
        #     "_t": str(int(time.time() * 1000)),
        #     "v": "1.0",
        #     "_r": str(random.randint(1000, 9999))
        # }
        # 
        # for key, value in random_params.items():
        #     if random.random() > 0.3:  # 70%概率添加
        #         params[key] = value
        
        # 构造完整URL用于显示
        from urllib.parse import urlencode
        full_url = f"{url}?{urlencode(params)}"
        
        print(f"🌐 安全请求 #{self.request_count}")
        print(f"   🎯 参数: scope={scope}, count={count}")
        if end_time:
            print(f"   📅 时间: {end_time}")
        print(f"   🔗 完整链接: {full_url}")
        
        # 调试模式输出详细信息
        if self.debug_mode:
            print(f"   🔍 调试模式:")
            print(f"   📍 基础URL: {url}")
            print(f"   📊 所有参数: {params}")
            print(f"   🔧 请求头: {json.dumps(headers, ensure_ascii=False, indent=4)}")
            print(f"   🍪 Cookie长度: {len(self.cookie)}字符")
            print(f"   ⏰ 当前时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            response = self.session.get(
                url, 
                headers=headers, 
                params=params, 
                timeout=45,  # 增加超时时间
                allow_redirects=True
            )
            
            print(f"   📊 状态: {response.status_code}, 大小: {len(response.content)}B")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    if data.get('succeeded'):
                        topics = data.get('resp_data', {}).get('topics', [])
                        print(f"   ✅ 获取成功: {len(topics)}个话题")
                        return data
                    else:
                        print(f"   ❌ API失败: {data.get('message', '未知错误')}")
                        print(f"   📋 完整响应: {json.dumps(data, ensure_ascii=False, indent=2)}")
                        return None
                except json.JSONDecodeError as e:
                    print(f"   ❌ JSON解析失败: {e}")
                    print(f"   📄 响应内容: {response.text[:500]}...")
                    print(f"   📋 响应头: {dict(response.headers)}")
                    return None
            else:
                print(f"   ❌ HTTP错误: {response.status_code}")
                print(f"   📄 响应内容: {response.text}")
                print(f"   📋 响应头: {dict(response.headers)}")
                if response.status_code == 429:
                    print("   🚨 触发频率限制，建议增加延迟时间")
                elif response.status_code == 403:
                    print("   🚨 访问被拒绝，可能需要更新Cookie或反检测策略")
                elif response.status_code == 401:
                    print("   🚨 认证失败，请检查Cookie是否过期")
                return None
                
        except requests.exceptions.Timeout as e:
            print(f"   ❌ 请求超时: {e}")
            print(f"   🔧 建议: 增加超时时间或检查网络连接")
            return None
        except requests.exceptions.ConnectionError as e:
            print(f"   ❌ 连接错误: {e}")
            print(f"   🔧 建议: 检查网络连接或DNS设置")
            return None
        except requests.exceptions.HTTPError as e:
            print(f"   ❌ HTTP协议错误: {e}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"   ❌ 请求异常: {e}")
            print(f"   🔧 异常类型: {type(e).__name__}")
            return None
    
    def store_batch_data(self, data: Dict[str, Any]) -> Dict[str, int]:
        """批量存储数据到数据库"""
        if not data or not data.get('succeeded'):
            return {'new_topics': 0, 'updated_topics': 0, 'errors': 0}
        
        topics = data.get('resp_data', {}).get('topics', [])
        if not topics:
            return {'new_topics': 0, 'updated_topics': 0, 'errors': 0}
        
        stats = {'new_topics': 0, 'updated_topics': 0, 'errors': 0}
        
        for topic_data in topics:
            try:
                topic_id = topic_data.get('topic_id')
                
                # 检查是否已存在
                self.db.cursor.execute('SELECT topic_id FROM topics WHERE topic_id = ?', (topic_id,))
                exists = self.db.cursor.fetchone()
                
                # 导入数据
                self.db.import_topic_data(topic_data)
                
                if exists:
                    stats['updated_topics'] += 1
                else:
                    stats['new_topics'] += 1
                
            except Exception as e:
                stats['errors'] += 1
                print(f"   ⚠️ 话题导入失败: {e}")
        
        # 提交事务
        self.db.conn.commit()
        return stats
    
    def crawl_latest(self, count: int = 20) -> Dict[str, int]:
        """爬取最新话题"""
        print(f"\n🆕 爬取最新 {count} 个话题...")
        
        data = self.fetch_topics_safe(scope="all", count=count)
        if data:
            stats = self.store_batch_data(data)
            print(f"💾 存储结果: 新增{stats['new_topics']}, 更新{stats['updated_topics']}")
            return stats
        else:
            print("❌ 获取失败")
            return {'new_topics': 0, 'updated_topics': 0, 'errors': 1}
    
    def crawl_historical(self, pages: int = 10, per_page: int = 20) -> Dict[str, int]:
        """爬取历史数据"""
        print(f"\n📚 爬取历史数据: {pages}页 x {per_page}条/页")
        
        total_stats = {'new_topics': 0, 'updated_topics': 0, 'errors': 0, 'pages': 0}
        end_time = None
        completed_pages = 0
        max_retries_per_page = 10  # 每页最大重试次数
        
        while completed_pages < pages:
            current_page = completed_pages + 1
            print(f"\n📄 页面 {current_page}/{pages}")
            retry_count = 0
            
            # 重试当前页直到成功或达到最大重试次数
            while retry_count < max_retries_per_page:
                if retry_count > 0:
                    print(f"   🔄 第{retry_count}次重试")
                
                # 获取数据
                if current_page == 1:
                    data = self.fetch_topics_safe(scope="all", count=per_page, is_historical=True)
                else:
                    data = self.fetch_topics_safe(scope="all", count=per_page, 
                                                end_time=end_time, is_historical=True)
                
                if data:
                    # 成功获取数据
                    topics = data.get('resp_data', {}).get('topics', [])
                    if not topics:
                        print(f"   📭 无更多数据，停止爬取")
                        return total_stats
                    
                    # 存储数据
                    page_stats = self.store_batch_data(data)
                    print(f"   💾 页面存储: 新增{page_stats['new_topics']}, 更新{page_stats['updated_topics']}")
                    
                    # 累计统计
                    total_stats['new_topics'] += page_stats['new_topics']
                    total_stats['updated_topics'] += page_stats['updated_topics']
                    total_stats['errors'] += page_stats['errors']
                    total_stats['pages'] += 1
                    completed_pages += 1
                    
                    # 调试：显示所有话题的时间戳
                    print(f"   🔍 调试信息:")
                    print(f"   📊 本页获取到 {len(topics)} 个话题")
                    for i, topic in enumerate(topics):
                        topic_time = topic.get('create_time', 'N/A')
                        topic_title = topic.get('title', '无标题')[:30]
                        print(f"   {i+1:2d}. {topic_time} - {topic_title}")
                    
                    # 准备下一页的时间戳
                    if topics:
                        original_time = topics[-1].get('create_time')
                        try:
                            from datetime import datetime, timedelta
                            dt = datetime.fromisoformat(original_time.replace('+0800', '+08:00'))
                            dt = dt - timedelta(milliseconds=self.timestamp_offset_ms)
                            end_time = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
                            print(f"   📅 原始时间戳: {original_time}")
                            print(f"   ⏭️ 下一页时间戳: {end_time} (减去{self.timestamp_offset_ms}毫秒)")
                        except Exception as e:
                            end_time = original_time
                            print(f"   ⚠️ 时间戳调整失败: {e}")
                            print(f"   ⏭️ 下一页时间戳: {end_time} (未调整)")
                    
                    # 检查是否已爬完
                    if len(topics) < per_page:
                        print(f"   📭 已爬取完毕 (返回{len(topics)}条)")
                        return total_stats
                    
                    # 成功，跳出重试循环
                    self.check_page_long_delay()  # 页面成功处理后进行长休眠检查
                    break
                else:
                    # 失败，增加重试计数和错误计数
                    retry_count += 1
                    total_stats['errors'] += 1
                    print(f"   ❌ 页面 {current_page} 获取失败 (重试{retry_count}/{max_retries_per_page})")
                    
                    # 调整时间戳用于重试
                    if end_time:
                        try:
                            from datetime import datetime, timedelta
                            dt = datetime.fromisoformat(end_time.replace('+0800', '+08:00'))
                            dt = dt - timedelta(milliseconds=self.timestamp_offset_ms)
                            end_time = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
                            print(f"   🔄 调整时间戳: {end_time} (再次减去{self.timestamp_offset_ms}毫秒)")
                        except Exception as e:
                            print(f"   ⚠️ 时间戳调整失败: {e}")
            
            # 如果重试次数用完仍然失败
            if retry_count >= max_retries_per_page:
                print(f"   🚫 页面 {current_page} 达到最大重试次数，跳过此页")
                # 如果有时间戳，尝试大幅度调整跳过问题区域
                if end_time:
                    try:
                        from datetime import datetime, timedelta
                        dt = datetime.fromisoformat(end_time.replace('+0800', '+08:00'))
                        # 大幅度跳过，减去1小时
                        dt = dt - timedelta(hours=1)
                        end_time = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
                        print(f"   ⏰ 大幅度跳过时间段: {end_time} (减去1小时)")
                    except Exception as e:
                        print(f"   ⚠️ 大幅度时间戳调整失败: {e}")
                completed_pages += 1  # 跳过这一页
        
        print(f"\n🏁 历史爬取完成:")
        print(f"   📄 成功页数: {total_stats['pages']}")
        print(f"   ✅ 新增话题: {total_stats['new_topics']}")
        print(f"   🔄 更新话题: {total_stats['updated_topics']}")
        if total_stats['errors'] > 0:
            print(f"   ❌ 总错误数: {total_stats['errors']}")
        
        return total_stats
    
    def crawl_all_historical(self, per_page: int = 20) -> Dict[str, int]:
        """获取所有历史数据：无限爬取直到没有数据（使用增量爬取逻辑）"""
        print(f"\n🌊 获取所有历史数据模式 (每页{per_page}条)")
        print(f"⚠️ 警告：此模式将持续爬取直到没有数据，可能需要很长时间")
        
        # 检查数据库状态，如果有数据则使用增量爬取逻辑
        timestamp_info = self.db.get_timestamp_range_info()
        start_end_time = None
        
        if timestamp_info['has_data']:
            oldest_timestamp = timestamp_info['oldest_timestamp']
            total_existing = timestamp_info['total_topics']
            
            print(f"📊 数据库现状:")
            print(f"   现有话题数: {total_existing}")
            print(f"   最老时间戳: {oldest_timestamp}")
            print(f"   最新时间戳: {timestamp_info['newest_timestamp']}")
            print(f"🎯 将从最老时间戳开始继续向历史爬取（增量模式）...")
            
            # 准备增量爬取的起始时间戳
            try:
                from datetime import datetime, timedelta
                dt = datetime.fromisoformat(oldest_timestamp.replace('+0800', '+08:00'))
                dt = dt - timedelta(milliseconds=self.timestamp_offset_ms)
                start_end_time = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
                print(f"🚀 增量爬取起始时间戳: {start_end_time}")
            except Exception as e:
                print(f"⚠️ 时间戳处理失败，使用原时间戳: {e}")
                start_end_time = oldest_timestamp
        else:
            print(f"📊 数据库为空，将从最新数据开始爬取")
        
        # 用户确认
        confirm = input("确认开始无限爬取？(y/N): ").lower().strip()
        if confirm != 'y':
            print("❌ 用户取消操作")
            return {'new_topics': 0, 'updated_topics': 0, 'errors': 0, 'pages': 0}
        
        print(f"🚀 开始无限历史爬取...")
        
        total_stats = {'new_topics': 0, 'updated_topics': 0, 'errors': 0, 'pages': 0}
        end_time = start_end_time  # 使用增量爬取的起始时间戳
        current_page = 0
        max_retries_per_page = 10
        consecutive_empty_pages = 0  # 连续空页面计数
        max_consecutive_empty = 3   # 最大连续空页面数
        
        while True:
            current_page += 1
            print(f"\n📄 页面 {current_page}")
            retry_count = 0
            page_success = False
            
            # 重试当前页直到成功或达到最大重试次数
            while retry_count < max_retries_per_page:
                if retry_count > 0:
                    print(f"   🔄 第{retry_count}次重试")
                
                # 获取数据 - 根据是否有起始时间戳决定请求方式
                if current_page == 1 and start_end_time is None:
                    # 数据库为空，从最新开始
                    data = self.fetch_topics_safe(scope="all", count=per_page, is_historical=True)
                else:
                    # 有数据或后续页面，使用 end_time 参数
                    data = self.fetch_topics_safe(scope="all", count=per_page, 
                                                end_time=end_time, is_historical=True)
                
                if data:
                    # 成功获取数据
                    topics = data.get('resp_data', {}).get('topics', [])
                    
                    if not topics:
                        consecutive_empty_pages += 1
                        print(f"   📭 第{consecutive_empty_pages}个空页面")
                        
                        if consecutive_empty_pages >= max_consecutive_empty:
                            print(f"   🏁 连续{max_consecutive_empty}个空页面，所有历史数据爬取完成")
                            print(f"\n🎉 无限爬取完成总结:")
                            print(f"   📄 总页数: {total_stats['pages']}")
                            print(f"   ✅ 新增话题: {total_stats['new_topics']}")
                            print(f"   🔄 更新话题: {total_stats['updated_topics']}")
                            if total_stats['errors'] > 0:
                                print(f"   ❌ 总错误数: {total_stats['errors']}")
                            
                            # 显示最终数据库状态
                            final_db_stats = self.db.get_timestamp_range_info()
                            if final_db_stats['has_data']:
                                print(f"\n📊 最终数据库状态:")
                                print(f"   话题总数: {final_db_stats['total_topics']}")
                                if timestamp_info['has_data']:
                                    print(f"   新增话题: {final_db_stats['total_topics'] - timestamp_info['total_topics']}")
                                print(f"   时间范围: {final_db_stats['oldest_timestamp']} ~ {final_db_stats['newest_timestamp']}")
                            
                            return total_stats
                        
                        # 空页面也算成功，避免无限重试
                        page_success = True
                        break
                    else:
                        consecutive_empty_pages = 0  # 重置连续空页面计数
                    
                    # 检查是否有新数据（避免重复爬取已有数据）
                    new_topics_count = 0
                    for topic in topics:
                        topic_id = topic.get('topic_id')
                        self.db.cursor.execute('SELECT topic_id FROM topics WHERE topic_id = ?', (topic_id,))
                        if not self.db.cursor.fetchone():
                            new_topics_count += 1
                    
                    # 存储数据
                    page_stats = self.store_batch_data(data)
                    print(f"   💾 页面存储: 新增{page_stats['new_topics']}, 更新{page_stats['updated_topics']}")
                    
                    # 累计统计
                    total_stats['new_topics'] += page_stats['new_topics']
                    total_stats['updated_topics'] += page_stats['updated_topics']
                    total_stats['errors'] += page_stats['errors']
                    total_stats['pages'] += 1
                    
                    # 显示进度信息
                    print(f"   📊 获取到 {len(topics)} 个话题，其中 {new_topics_count} 个为新话题")
                    print(f"   📈 累计: 新增{total_stats['new_topics']}, 更新{total_stats['updated_topics']}, 页数{total_stats['pages']}")
                    
                    # 调试：显示时间戳信息（简化版）
                    if topics:
                        first_time = topics[0].get('create_time', 'N/A')
                        last_time = topics[-1].get('create_time', 'N/A')
                        print(f"   ⏰ 时间范围: {first_time} ~ {last_time}")
                    
                    # 准备下一页的时间戳
                    if topics:
                        original_time = topics[-1].get('create_time')
                        try:
                            from datetime import datetime, timedelta
                            dt = datetime.fromisoformat(original_time.replace('+0800', '+08:00'))
                            dt = dt - timedelta(milliseconds=self.timestamp_offset_ms)
                            end_time = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
                        except Exception as e:
                            end_time = original_time
                            print(f"   ⚠️ 时间戳调整失败: {e}")
                    
                    # 检查是否返回数据量小于预期（可能接近底部）
                    if len(topics) < per_page:
                        print(f"   ⚠️ 返回数据量({len(topics)})小于预期({per_page})，可能接近历史底部")
                    
                    # 如果没有新话题且数据量不足，可能已达历史底部
                    if new_topics_count == 0 and len(topics) < per_page:
                        print(f"   📭 无新话题且数据量不足，可能已达历史底部")
                        return total_stats
                    
                    # 成功，跳出重试循环
                    page_success = True
                    break
                else:
                    # 失败，增加重试计数和错误计数
                    retry_count += 1
                    total_stats['errors'] += 1
                    print(f"   ❌ 页面 {current_page} 获取失败 (重试{retry_count}/{max_retries_per_page})")
                    
                    # 调整时间戳用于重试
                    if end_time:
                        try:
                            from datetime import datetime, timedelta
                            dt = datetime.fromisoformat(end_time.replace('+0800', '+08:00'))
                            dt = dt - timedelta(milliseconds=self.timestamp_offset_ms)
                            end_time = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
                        except Exception as e:
                            print(f"   ⚠️ 时间戳调整失败: {e}")
            
            # 如果重试次数用完仍然失败
            if not page_success:
                print(f"   🚫 页面 {current_page} 达到最大重试次数")
                # 大幅度跳过问题区域
                if end_time:
                    try:
                        from datetime import datetime, timedelta
                        dt = datetime.fromisoformat(end_time.replace('+0800', '+08:00'))
                        dt = dt - timedelta(hours=1)
                        end_time = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
                        print(f"   ⏰ 大幅度跳过时间段: {end_time} (减去1小时)")
                    except Exception as e:
                        print(f"   ⚠️ 大幅度时间戳调整失败: {e}")
                completed_pages += 1  # 跳过这一页
            else:
                # 页面成功处理后进行长休眠检查（基于页面数而非请求数）
                self.check_page_long_delay()
            
            # 每50页显示一次总体进度
            if current_page % 50 == 0:
                print(f"\n🎯 进度报告 (第{current_page}页):")
                print(f"   📊 累计新增: {total_stats['new_topics']}")
                print(f"   📊 累计更新: {total_stats['updated_topics']}")
                print(f"   📊 成功页数: {total_stats['pages']}")
                print(f"   📊 错误次数: {total_stats['errors']}")
                
                # 显示当前数据库状态
                current_db_stats = self.db.get_timestamp_range_info()
                if current_db_stats['has_data']:
                    print(f"   📊 数据库状态: {current_db_stats['total_topics']}个话题")
                    print(f"   📊 时间范围: {current_db_stats['oldest_timestamp']} ~ {current_db_stats['newest_timestamp']}")
        
        # 这里理论上不会到达，因为在循环内会return
        return total_stats
    
    def crawl_incremental(self, pages: int = 10, per_page: int = 20) -> Dict[str, int]:
        """增量爬取：基于数据库最老时间戳继续向历史爬取"""
        print(f"\n📈 增量爬取模式: {pages}页 x {per_page}条/页")
        
        # 获取数据库时间戳范围信息
        timestamp_info = self.db.get_timestamp_range_info()
        
        if not timestamp_info['has_data']:
            print("❌ 数据库中没有话题数据，请先进行历史爬取")
            return {'new_topics': 0, 'updated_topics': 0, 'errors': 1}
        
        oldest_timestamp = timestamp_info['oldest_timestamp']
        total_existing = timestamp_info['total_topics']
        
        print(f"📊 数据库状态:")
        print(f"   现有话题数: {total_existing}")
        print(f"   最老时间戳: {oldest_timestamp}")
        print(f"   最新时间戳: {timestamp_info['newest_timestamp']}")
        print(f"🎯 将从最老时间戳开始继续向历史爬取...")
        
        # 准备增量爬取的起始时间戳（在最老时间戳基础上减去偏移量）
        try:
            from datetime import datetime, timedelta
            dt = datetime.fromisoformat(oldest_timestamp.replace('+0800', '+08:00'))
            dt = dt - timedelta(milliseconds=self.timestamp_offset_ms)
            start_end_time = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
            print(f"🚀 增量爬取起始时间戳: {start_end_time}")
        except Exception as e:
            print(f"⚠️ 时间戳处理失败，使用原时间戳: {e}")
            start_end_time = oldest_timestamp
        
        # 执行增量爬取
        total_stats = {'new_topics': 0, 'updated_topics': 0, 'errors': 0, 'pages': 0}
        end_time = start_end_time
        completed_pages = 0
        max_retries_per_page = 10
        
        while completed_pages < pages:
            current_page = completed_pages + 1
            print(f"\n📄 增量页面 {current_page}/{pages}")
            retry_count = 0
            
            # 重试当前页直到成功或达到最大重试次数
            while retry_count < max_retries_per_page:
                if retry_count > 0:
                    print(f"   🔄 第{retry_count}次重试")
                
                # 获取数据 - 总是使用 end_time 参数
                data = self.fetch_topics_safe(scope="all", count=per_page, 
                                            end_time=end_time, is_historical=True)
                
                if data:
                    # 成功获取数据
                    topics = data.get('resp_data', {}).get('topics', [])
                    if not topics:
                        print(f"   📭 无更多历史数据，增量爬取完成")
                        return total_stats
                    
                    # 检查是否有新数据（避免重复爬取已有数据）
                    new_topics_count = 0
                    for topic in topics:
                        topic_id = topic.get('topic_id')
                        self.db.cursor.execute('SELECT topic_id FROM topics WHERE topic_id = ?', (topic_id,))
                        if not self.db.cursor.fetchone():
                            new_topics_count += 1
                    
                    print(f"   📊 获取到 {len(topics)} 个话题，其中 {new_topics_count} 个为新话题")
                    
                    # 如果没有新话题且当前页话题数少于预期，可能已到达历史底部
                    if new_topics_count == 0 and len(topics) < per_page:
                        print(f"   📭 无新话题且数据量不足，可能已达历史底部")
                        return total_stats
                    
                    # 存储数据
                    page_stats = self.store_batch_data(data)
                    print(f"   💾 页面存储: 新增{page_stats['new_topics']}, 更新{page_stats['updated_topics']}")
                    
                    # 累计统计
                    total_stats['new_topics'] += page_stats['new_topics']
                    total_stats['updated_topics'] += page_stats['updated_topics']
                    total_stats['errors'] += page_stats['errors']
                    total_stats['pages'] += 1
                    completed_pages += 1
                    
                    # 调试：显示话题时间戳信息
                    if self.debug_mode:
                        print(f"   🔍 调试信息:")
                        print(f"   📊 本页获取到 {len(topics)} 个话题")
                        for i, topic in enumerate(topics):
                            topic_time = topic.get('create_time', 'N/A')
                            topic_title = topic.get('title', '无标题')[:30]
                            print(f"   {i+1:2d}. {topic_time} - {topic_title}")
                    
                    # 准备下一页的时间戳
                    if topics:
                        original_time = topics[-1].get('create_time')
                        try:
                            dt = datetime.fromisoformat(original_time.replace('+0800', '+08:00'))
                            dt = dt - timedelta(milliseconds=self.timestamp_offset_ms)
                            end_time = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
                            print(f"   ⏭️ 下一页时间戳: {end_time}")
                        except Exception as e:
                            end_time = original_time
                            print(f"   ⚠️ 时间戳调整失败: {e}")
                    
                    # 成功，跳出重试循环
                    self.check_page_long_delay()  # 页面成功处理后进行长休眠检查
                    break
                else:
                    # 失败，增加重试计数和错误计数
                    retry_count += 1
                    total_stats['errors'] += 1
                    print(f"   ❌ 页面 {current_page} 获取失败 (重试{retry_count}/{max_retries_per_page})")
                    
                    # 调整时间戳用于重试
                    if end_time:
                        try:
                            dt = datetime.fromisoformat(end_time.replace('+0800', '+08:00'))
                            dt = dt - timedelta(milliseconds=self.timestamp_offset_ms)
                            end_time = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
                            print(f"   🔄 调整时间戳: {end_time}")
                        except Exception as e:
                            print(f"   ⚠️ 时间戳调整失败: {e}")
            
            # 如果重试次数用完仍然失败
            if retry_count >= max_retries_per_page:
                print(f"   🚫 页面 {current_page} 达到最大重试次数，跳过此页")
                # 大幅度跳过问题区域
                if end_time:
                    try:
                        dt = datetime.fromisoformat(end_time.replace('+0800', '+08:00'))
                        dt = dt - timedelta(hours=1)
                        end_time = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
                        print(f"   ⏰ 大幅度跳过时间段: {end_time} (减去1小时)")
                    except Exception as e:
                        print(f"   ⚠️ 大幅度时间戳调整失败: {e}")
                completed_pages += 1  # 跳过这一页
        
        print(f"\n🏁 增量爬取完成:")
        print(f"   📄 成功页数: {total_stats['pages']}")
        print(f"   ✅ 新增话题: {total_stats['new_topics']}")
        print(f"   🔄 更新话题: {total_stats['updated_topics']}")
        if total_stats['errors'] > 0:
            print(f"   ❌ 总错误数: {total_stats['errors']}")
        
        # 显示更新后的数据库状态
        updated_info = self.db.get_timestamp_range_info()
        print(f"\n📊 更新后数据库状态:")
        print(f"   话题总数: {updated_info['total_topics']} (+{updated_info['total_topics'] - total_existing})")
        print(f"   时间范围: {updated_info['oldest_timestamp']} ~ {updated_info['newest_timestamp']}")
        
        return total_stats
    
    def crawl_latest_until_complete(self, per_page: int = 20) -> Dict[str, int]:
        """获取最新记录：智能增量更新，爬取到与数据库完全衔接为止"""
        print(f"\n🔄 获取最新记录模式 (每页{per_page}条)")
        print(f"💡 智能逻辑：检查最新话题，如有新内容则向后爬取直到与数据库完全衔接")
        
        # 检查数据库状态
        timestamp_info = self.db.get_timestamp_range_info()
        if not timestamp_info['has_data']:
            print("❌ 数据库为空，请先进行历史数据爬取")
            return {'new_topics': 0, 'updated_topics': 0, 'errors': 0, 'pages': 0}
        
        print(f"📊 数据库状态:")
        print(f"   现有话题数: {timestamp_info['total_topics']}")
        print(f"   最新时间戳: {timestamp_info['newest_timestamp']}")
        
        total_stats = {'new_topics': 0, 'updated_topics': 0, 'errors': 0, 'pages': 0}
        end_time = None  # 从最新开始
        current_page = 0
        max_retries_per_page = 10
        
        while True:
            current_page += 1
            print(f"\n📄 检查页面 {current_page}")
            retry_count = 0
            page_success = False
            
            # 重试当前页直到成功或达到最大重试次数
            while retry_count < max_retries_per_page:
                if retry_count > 0:
                    print(f"   🔄 第{retry_count}次重试")
                
                # 获取数据
                if current_page == 1:
                    # 第一页：获取最新话题
                    data = self.fetch_topics_safe(scope="all", count=per_page, is_historical=False)
                else:
                    # 后续页面：使用 end_time 向历史爬取
                    data = self.fetch_topics_safe(scope="all", count=per_page, 
                                                end_time=end_time, is_historical=True)
                
                if data:
                    # 成功获取数据
                    topics = data.get('resp_data', {}).get('topics', [])
                    
                    if not topics:
                        print(f"   📭 无更多数据，获取完成")
                        break
                    
                    # 检查这一页的话题是否在数据库中全部存在
                    existing_count = 0
                    new_topics_list = []
                    
                    for topic in topics:
                        topic_id = topic.get('topic_id')
                        self.db.cursor.execute('SELECT topic_id FROM topics WHERE topic_id = ?', (topic_id,))
                        if self.db.cursor.fetchone():
                            existing_count += 1
                        else:
                            new_topics_list.append(topic)
                    
                    print(f"   📊 页面分析: {len(topics)}个话题，{existing_count}个已存在，{len(new_topics_list)}个新话题")
                    
                    # 判断是否需要停止
                    if existing_count == len(topics):
                        # 整页话题全部存在于数据库中
                        print(f"   ✅ 整页话题全部存在于数据库，增量更新完成")
                        print(f"\n🎉 获取最新记录完成总结:")
                        print(f"   📄 检查页数: {total_stats['pages']}")
                        print(f"   ✅ 新增话题: {total_stats['new_topics']}")
                        print(f"   🔄 更新话题: {total_stats['updated_topics']}")
                        if total_stats['errors'] > 0:
                            print(f"   ❌ 总错误数: {total_stats['errors']}")
                        
                        # 显示更新后的数据库状态
                        final_db_stats = self.db.get_timestamp_range_info()
                        if final_db_stats['has_data']:
                            print(f"\n📊 数据库最终状态:")
                            print(f"   话题总数: {final_db_stats['total_topics']} (+{final_db_stats['total_topics'] - timestamp_info['total_topics']})")
                            print(f"   时间范围: {final_db_stats['oldest_timestamp']} ~ {final_db_stats['newest_timestamp']}")
                        
                        return total_stats
                    
                    elif existing_count == 0:
                        # 整页话题都是新的，全部存储
                        page_stats = self.store_batch_data(data)
                        print(f"   💾 整页存储: 新增{page_stats['new_topics']}, 更新{page_stats['updated_topics']}")
                    
                    else:
                        # 部分话题是新的，只存储新话题
                        print(f"   💾 部分存储: 只处理{len(new_topics_list)}个新话题")
                        new_topics_count = 0
                        updated_topics_count = 0
                        
                        for new_topic in new_topics_list:
                            try:
                                topic_id = new_topic.get('topic_id')
                                # 检查是否已存在（双重检查）
                                self.db.cursor.execute('SELECT topic_id FROM topics WHERE topic_id = ?', (topic_id,))
                                exists = self.db.cursor.fetchone()
                                
                                # 导入数据
                                self.db.import_topic_data(new_topic)
                                
                                if exists:
                                    updated_topics_count += 1
                                else:
                                    new_topics_count += 1
                                    
                            except Exception as e:
                                print(f"   ⚠️ 话题导入失败: {e}")
                        
                        # 提交事务
                        self.db.conn.commit()
                        print(f"   💾 新话题存储: 新增{new_topics_count}, 更新{updated_topics_count}")
                        
                        # 更新统计
                        total_stats['new_topics'] += new_topics_count
                        total_stats['updated_topics'] += updated_topics_count
                    
                    # 累计统计（如果是整页存储）
                    if existing_count == 0:
                        total_stats['new_topics'] += page_stats['new_topics']
                        total_stats['updated_topics'] += page_stats['updated_topics']
                        total_stats['errors'] += page_stats['errors']
                    
                    total_stats['pages'] += 1
                    
                    # 显示当前进度
                    print(f"   📈 累计: 新增{total_stats['new_topics']}, 更新{total_stats['updated_topics']}, 页数{total_stats['pages']}")
                    
                    # 显示时间戳信息
                    if topics:
                        first_time = topics[0].get('create_time', 'N/A')
                        last_time = topics[-1].get('create_time', 'N/A')
                        print(f"   ⏰ 时间范围: {first_time} ~ {last_time}")
                    
                    # 准备下一页的时间戳
                    if topics:
                        original_time = topics[-1].get('create_time')
                        try:
                            from datetime import datetime, timedelta
                            dt = datetime.fromisoformat(original_time.replace('+0800', '+08:00'))
                            dt = dt - timedelta(milliseconds=self.timestamp_offset_ms)
                            end_time = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
                        except Exception as e:
                            end_time = original_time
                            print(f"   ⚠️ 时间戳调整失败: {e}")
                    
                    # 成功，跳出重试循环
                    page_success = True
                    break
                else:
                    # 失败，增加重试计数和错误计数
                    retry_count += 1
                    total_stats['errors'] += 1
                    print(f"   ❌ 页面 {current_page} 获取失败 (重试{retry_count}/{max_retries_per_page})")
                    
                    # 调整时间戳用于重试
                    if end_time:
                        try:
                            from datetime import datetime, timedelta
                            dt = datetime.fromisoformat(end_time.replace('+0800', '+08:00'))
                            dt = dt - timedelta(milliseconds=self.timestamp_offset_ms)
                            end_time = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
                        except Exception as e:
                            print(f"   ⚠️ 时间戳调整失败: {e}")
            
            # 如果重试次数用完仍然失败
            if not page_success:
                print(f"   🚫 页面 {current_page} 达到最大重试次数，停止获取")
                break
            else:
                # 页面成功处理后进行长休眠检查（基于页面数而非请求数）
                self.check_page_long_delay()
        
        return total_stats
    
    def show_menu(self):
        """显示交互菜单"""
        print(f"\n{'='*60}")
        print("🕷️ 知识星球交互式数据采集器")
        print("="*60)
        print("📝 话题采集功能:")
        print("1. 获取所有历史数据 (无限爬取) - 适合：全量归档，从最老数据无限挖掘")
        print("2. 增量爬取历史 (基于数据库最老时间戳) - 适合：精确补充历史，有目标的回填")
        print("3. 获取最新记录 (智能增量更新) - 适合：日常维护，自动检测并只爬新内容")
        print("")
        print("📥 文件下载功能:")
        print("4. 增量收集文件列表 - 适合：从数据库最老时间戳继续收集更早文件")
        print("5. 查看文件数据库统计 - 适合：查看收集的文件信息和下载状态")
        print("6. 按下载次数下载文件 - 适合：自动收集热门文件并按下载次数排序下载")
        print("7. 按时间顺序下载文件 - 适合：自动收集文件列表并按时间顺序下载")
        print("8. 文件下载设置 - 适合：调整下载间隔和休眠参数")
        print("")
        print("⚙️ 系统功能:")
        print("9. 查看话题数据库统计 - 适合：监控话题数据状态，了解当前数据范围")
        print("10. 调整反检测设置 - 适合：优化爬取速度，应对不同网络环境")
        print(f"11. 时间戳设置 (当前: 减去{self.timestamp_offset_ms}毫秒) - 适合：解决时间点冲突，精确控制分页")
        print(f"12. 调试模式 (当前: {'开启' if self.debug_mode else '关闭'}) - 适合：排查问题，查看详细请求信息")
        print("13. 退出程序")
        print("="*60)
    
    def adjust_stealth_settings(self):
        """调整反检测设置"""
        print(f"\n🔧 当前反检测设置:")
        print(f"   最小延迟: {self.min_delay}秒")
        print(f"   最大延迟: {self.max_delay}秒")
        print(f"   长延迟间隔: 每{self.long_delay_interval}个页面")
        print(f"   长休眠时间: 3-5分钟 (180-300秒)")
        print(f"💡 说明: 长休眠基于成功处理的页面数，而非请求数，更加合理稳定")
        
        try:
            new_min = float(input(f"新的最小延迟 (当前{self.min_delay}): ") or self.min_delay)
            new_max = float(input(f"新的最大延迟 (当前{self.max_delay}): ") or self.max_delay)
            new_interval = int(input(f"长延迟间隔 (当前每{self.long_delay_interval}页): ") or self.long_delay_interval)
            
            self.min_delay = max(new_min, 1.0)  # 最小1秒
            self.max_delay = max(new_max, self.min_delay + 1.0)
            self.long_delay_interval = max(new_interval, 5)
            
            print(f"✅ 设置已更新")
            print(f"💡 长休眠时间固定为3-5分钟，有助于更好地模拟人类行为")
            print(f"🎯 长休眠触发：每成功处理{self.long_delay_interval}个页面进行一次长休眠")
            
        except ValueError:
            print("❌ 输入无效，保持原设置")
    
    def adjust_timestamp_settings(self):
        """调整时间戳设置"""
        print(f"\n⏰ 当前时间戳设置:")
        print(f"   减去毫秒数: {self.timestamp_offset_ms}毫秒")
        print(f"\n💡 说明:")
        print(f"   - 减去1毫秒: 标准设置，与官网一致")
        print(f"   - 减去2-3毫秒: 可能避开某些问题时间点")
        print(f"   - 减去5-10毫秒: 更大的容错范围")
        
        try:
            new_offset = int(input(f"新的毫秒偏移量 (当前{self.timestamp_offset_ms}): ") or self.timestamp_offset_ms)
            
            if new_offset < 0:
                print("❌ 毫秒偏移量不能为负数")
                return
            
            self.timestamp_offset_ms = new_offset
            print(f"✅ 时间戳设置已更新: 减去{self.timestamp_offset_ms}毫秒")
            
        except ValueError:
            print("❌ 输入无效，保持原设置")
    
    def run_interactive(self):
        """运行交互式界面"""
        try:
            while True:
                self.show_menu()
                choice = input("\n请选择 (1-13): ").strip()
                
                if choice == "1":
                    per_page = int(input("每页数量 (默认20): ") or "20")
                    self.crawl_all_historical(per_page)
                    
                elif choice == "2":
                    pages = int(input("爬取页数 (默认10): ") or "10")
                    per_page = int(input("每页数量 (默认20): ") or "20")
                    self.crawl_incremental(pages, per_page)
                    
                elif choice == "3":
                    per_page = int(input("每页数量 (默认20): ") or "20")
                    self.crawl_latest_until_complete(per_page)
                    
                elif choice == "4":
                    # 增量收集文件列表
                    downloader = self.get_file_downloader()
                    downloader.collect_incremental_files()
                    
                elif choice == "5":
                    # 查看文件数据库统计
                    downloader = self.get_file_downloader()
                    downloader.show_database_stats()
                    
                elif choice == "6":
                    # 按下载次数下载文件 (集成收集和下载)
                    downloader = self.get_file_downloader()
                    
                    # 检查数据库是否已有文件数据
                    stats = downloader.file_db.get_database_stats()
                    existing_files = stats.get('files', 0)
                    
                    if existing_files > 0:
                        print(f"📊 数据库中已有 {existing_files} 个文件记录")
                        collect_confirm = input("是否重新收集文件列表? (y/n, 默认n直接下载): ").strip().lower()
                        if collect_confirm != 'y':
                            print("⚡ 直接使用现有数据进行下载...")
                        else:
                            print("🔄 按下载次数重新收集文件列表...")
                            downloader.collect_all_files_to_database()
                    else:
                        print("🔄 按下载次数收集热门文件列表...")
                        downloader.collect_all_files_to_database()
                    
                    # 自动开始下载
                    print("\n🚀 自动开始下载文件...")
                    user_input = input("最大下载文件数 (默认无限，输入数字限制): ").strip()
                    if user_input and user_input.isdigit():
                        max_files = int(user_input)
                    else:
                        max_files = None
                    downloader.download_files_from_database(max_files=max_files, status_filter='pending')
                    
                elif choice == "7":
                    # 按时间顺序下载文件 (集成收集和下载)
                    downloader = self.get_file_downloader()
                    
                    # 检查数据库是否已有文件数据
                    stats = downloader.file_db.get_database_stats()
                    existing_files = stats.get('files', 0)
                    
                    if existing_files > 0:
                        print(f"📊 数据库中已有 {existing_files} 个文件记录")
                        collect_confirm = input("是否重新收集文件列表? (y/n, 默认n直接下载): ").strip().lower()
                        if collect_confirm != 'y':
                            print("⚡ 直接使用现有数据进行下载...")
                        else:
                            print("🔄 按时间排序重新收集文件列表...")
                            downloader.collect_files_by_time()
                    else:
                        print("🔄 按时间排序收集文件列表...")
                        downloader.collect_files_by_time()
                    
                    # 自动开始下载
                    print("\n🚀 自动开始下载文件...")
                    user_input = input("最大下载文件数 (默认无限，输入数字限制): ").strip()
                    if user_input and user_input.isdigit():
                        max_files = int(user_input)
                    else:
                        max_files = None
                    downloader.download_files_from_database(max_files=max_files, status_filter='pending')
                    
                elif choice == "8":
                    # 文件下载设置
                    downloader = self.get_file_downloader()
                    downloader.adjust_settings()
                    
                elif choice == "9":
                    # 查看话题数据库统计
                    self.show_database_status()
                    stats = self.db.get_database_stats()
                    print("\n📊 详细统计:")
                    for table, count in stats.items():
                        print(f"   {table}: {count}")
                    
                elif choice == "10":
                    self.adjust_stealth_settings()
                
                elif choice == "11":
                    self.adjust_timestamp_settings()
                    
                elif choice == "12":
                    self.debug_mode = not self.debug_mode
                    status = "开启" if self.debug_mode else "关闭"
                    print(f"🔍 调试模式已{status}")
                    if self.debug_mode:
                        print("⚠️ 调试模式会输出详细的请求信息，包括完整的失败响应")
                    
                elif choice == "13":
                    print("👋 退出程序")
                    break
                    
                else:
                    print("❌ 无效选择")
                
                input("\n按回车键继续...")
                
        except KeyboardInterrupt:
            print("\n⏹️ 用户中断")
        except Exception as e:
            print(f"❌ 程序异常: {e}")
        finally:
            self.close()
    
    def close(self):
        """关闭资源"""
        self.db.close()
        print("🔒 数据库连接已关闭")


def load_config():
    """加载TOML配置文件"""
    if tomllib is None:
        return None
        
    config_file = "config.toml"
    if not os.path.exists(config_file):
        print("⚠️ 未找到config.toml配置文件，请先创建并配置")
        print("💡 可以复制config.toml.example为config.toml并修改")
        return None
    
    try:
        with open(config_file, 'rb') as f:
            config = tomllib.load(f)
        
        print("✅ 已从config.toml加载配置")
        return config
    except Exception as e:
        print(f"❌ 加载配置文件出错: {e}")
        return None

def main():
    """主函数"""
    # 加载配置信息
    config = load_config()
    if not config:
        return
    
    # 从TOML配置中获取值
    auth_config = config.get('auth', {})
    db_config = config.get('database', {})
    
    COOKIE = auth_config.get('cookie', 'your_cookie_here')
    GROUP_ID = auth_config.get('group_id', 'your_group_id_here')
    DB_PATH = db_config.get('path', 'zsxq_interactive.db')
    
    # 检查配置是否已修改
    if COOKIE == "your_cookie_here" or GROUP_ID == "your_group_id_here":
        print("⚠️ 请先在config.toml中配置您的cookie和group_id")
        return
    
    # 创建交互式爬虫
    crawler = ZSXQInteractiveCrawler(COOKIE, GROUP_ID, DB_PATH)
    
    # 运行交互界面
    crawler.run_interactive()


if __name__ == "__main__":
    main()