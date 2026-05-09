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
from .zsxq_database import ZSXQDatabase
from .zsxq_file_downloader import ZSXQFileDownloader
from .db_path_manager import get_db_path_manager
from .logger_config import log_error, log_info, log_warning
import os
import argparse
try:
    import tomllib
except ImportError:
    try:
        import tomli as tomllib
    except ImportError:
        log_warning("⚠️ 需要安装tomli库来解析TOML配置文件")
        log_warning("💡 请运行: pip install tomli")
        tomllib = None


class ZSXQInteractiveCrawler:
    """知识星球交互式数据采集器"""
    
    def __init__(self, cookie: str, group_id: str, db_path: str = None, log_callback=None):
        self.cookie = self.clean_cookie(cookie)
        self.group_id = group_id
        self.log_callback = log_callback  # 日志回调函数
        self.stop_flag = False  # 停止标志
        self.stop_check_func = None  # 停止检查函数

        # 使用路径管理器获取数据库路径
        path_manager = get_db_path_manager()
        if db_path is None:
            db_path = path_manager.get_topics_db_path(group_id)

        self.db_path = db_path  # 保存数据库路径
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

        # 可配置的间隔参数（用于API调用时覆盖默认值）
        self.use_custom_intervals = False
        self.custom_min_delay = None
        self.custom_max_delay = None
        self.custom_long_delay_min = None
        self.custom_long_delay_max = None
        self.custom_pages_per_batch = None

        self.log(f"🚀 知识星球交互式采集器初始化完成")
        self.log(f"📊 目标群组: {group_id}")
        self.log(f"💾 数据库: {db_path}")

        # 显示当前数据库状态
        self.show_database_status()

    def set_custom_intervals(self, crawl_interval_min=None, crawl_interval_max=None,
                           long_sleep_interval_min=None, long_sleep_interval_max=None,
                           pages_per_batch=None):
        """设置自定义间隔参数"""
        if any([crawl_interval_min, crawl_interval_max, long_sleep_interval_min,
                long_sleep_interval_max, pages_per_batch]):
            self.use_custom_intervals = True
            self.custom_min_delay = crawl_interval_min
            self.custom_max_delay = crawl_interval_max
            self.custom_long_delay_min = long_sleep_interval_min
            self.custom_long_delay_max = long_sleep_interval_max
            self.custom_pages_per_batch = pages_per_batch

            self.log(f"🔧 使用自定义间隔设置:")
            if crawl_interval_min and crawl_interval_max:
                self.log(f"   页面间隔: {crawl_interval_min}-{crawl_interval_max}秒")
            if long_sleep_interval_min and long_sleep_interval_max:
                self.log(f"   长休眠: {long_sleep_interval_min}-{long_sleep_interval_max}秒")
            if pages_per_batch:
                self.log(f"   批次大小: {pages_per_batch}页")
        else:
            self.use_custom_intervals = False
            self.log(f"🔧 使用默认间隔设置")

    def log(self, message: str):
        """统一的日志输出方法"""
        message = str(message)
        if self.log_callback:
            self.log_callback(message)  # 同时推送到前端
        else:
            log_info(message)

    def set_stop_flag(self):
        """设置停止标志"""
        self.stop_flag = True
        self.log("🛑 收到停止信号，任务将在下一个检查点停止")

    def is_stopped(self):
        """检查是否被停止"""
        # 首先检查本地停止标志
        if self.stop_flag:
            return True
        # 然后检查外部停止检查函数
        if self.stop_check_func and self.stop_check_func():
            self.stop_flag = True  # 同步本地标志
            return True
        return False

    def _interruptible_sleep(self, duration: float):
        """可中断的睡眠，每0.1秒检查一次停止标志"""
        start_time = time.time()
        while time.time() - start_time < duration:
            if self.is_stopped():
                return
            time.sleep(0.1)  # 短暂睡眠，允许快速响应停止信号
    
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
            self.log(f"Cookie清理失败: {e}")
            return cookie  # 返回原始值
    
    def get_file_downloader(self):
        """获取文件下载器（懒加载）"""
        if self.file_downloader is None:
            # 使用路径管理器获取文件数据库路径
            path_manager = get_db_path_manager()
            files_db_path = path_manager.get_files_db_path(self.group_id)
            self.file_downloader = ZSXQFileDownloader(self.cookie, self.group_id, files_db_path)
        return self.file_downloader
    
    def show_database_status(self):
        """显示数据库当前状态"""
        stats = self.db.get_database_stats()
        total_topics = stats.get('topics', 0)
        total_users = stats.get('users', 0)
        total_comments = stats.get('comments', 0)
        
        self.log(f"\n📊 当前数据库状态:")
        self.log(f"   话题: {total_topics}, 用户: {total_users}, 评论: {total_comments}")
        
        # 显示时间戳范围信息
        if total_topics > 0:
            timestamp_info = self.db.get_timestamp_range_info()
            if timestamp_info['has_data']:
                self.log(f"   时间范围: {timestamp_info['oldest_timestamp']} ~ {timestamp_info['newest_timestamp']}")
            else:
                self.log(f"   ⚠️ 时间戳数据不完整")
    
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
        if self.use_custom_intervals and self.custom_min_delay and self.custom_max_delay:
            # 使用自定义间隔
            min_delay = self.custom_min_delay
            max_delay = self.custom_max_delay
            if is_historical:
                delay = random.uniform(min_delay, max_delay + 1.0)  # 历史爬取稍长
            else:
                delay = random.uniform(min_delay, max_delay)
            self.log(f"⏱️ 页面间隔: {delay:.2f}秒 [自定义范围: {min_delay}-{max_delay}秒]")
        else:
            # 使用默认间隔
            if is_historical:
                delay = random.uniform(self.min_delay + 1.0, self.max_delay + 2.0)  # 历史爬取稍长
            else:
                delay = random.uniform(self.min_delay, self.max_delay)
            if self.debug_mode:
                self.log(f"   ⏱️ 延迟: {delay:.2f}秒 (请求#{self.request_count})")

        # 可中断的延迟
        self._interruptible_sleep(delay)
        self.last_request_time = time.time()
    
    def check_page_long_delay(self):
        """检查页面级长休眠：根据配置进行长休眠"""
        self.page_count += 1

        # 确定长休眠间隔
        if self.use_custom_intervals and self.custom_pages_per_batch:
            interval = self.custom_pages_per_batch
        else:
            interval = self.long_delay_interval

        if self.page_count % interval == 0:
            import datetime

            # 确定长休眠时间
            if self.use_custom_intervals and self.custom_long_delay_min and self.custom_long_delay_max:
                long_delay = random.uniform(self.custom_long_delay_min, self.custom_long_delay_max)
                self.log(f"🛌 长休眠开始: {long_delay:.1f}秒 ({long_delay/60:.1f}分钟) [自定义范围: {self.custom_long_delay_min/60:.1f}-{self.custom_long_delay_max/60:.1f}分钟]")
            else:
                long_delay = random.uniform(180, 300)  # 3-5分钟长休眠
                self.log(f"🛌 长休眠开始: {long_delay:.1f}秒 ({long_delay/60:.1f}分钟) [默认范围: 3-5分钟]")

            start_time = datetime.datetime.now()
            end_time = start_time + datetime.timedelta(seconds=long_delay)

            self.log(f"   已完成 {self.page_count} 个页面，进入长休眠模式...")
            self.log(f"   ⏰ 开始时间: {start_time.strftime('%H:%M:%S')}")
            self.log(f"   🕐 预计恢复: {end_time.strftime('%H:%M:%S')}")

            # 可中断的长延迟
            self._interruptible_sleep(long_delay)

            actual_end_time = datetime.datetime.now()
            self.log(f"😴 长休眠结束，继续爬取...")
            self.log(f"   🕐 实际结束: {actual_end_time.strftime('%H:%M:%S')}")

            # 调试信息
            if self.debug_mode:
                actual_duration = (actual_end_time - start_time).total_seconds()
                self.log(f"   💤 长休眠完成: 预计{long_delay:.1f}秒，实际{actual_duration:.1f}秒 (页面#{self.page_count})")

    def fetch_comments_safe(self, topic_id: int, begin_time: str = None, count: int = 30, max_retries: int = 10) -> Optional[Dict[str, Any]]:
        """安全获取话题评论，包含重试机制处理反爬"""
        for retry in range(max_retries):
            try:
                # 构建评论API URL
                url = f"https://api.zsxq.com/v2/topics/{topic_id}/comments"
                params = {
                    'sort': 'asc',
                    'count': count,
                    'with_sticky': 'true'
                }

                if begin_time:
                    params['begin_time'] = begin_time

                # 使用与主要API相同的隐蔽性请求头，包含完整的认证信息
                headers = self.get_stealth_headers()

                # 调试模式输出详细信息
                if self.debug_mode and retry == 0:  # 只在第一次尝试时输出
                    from urllib.parse import urlencode
                    full_url = f"{url}?{urlencode(params)}"
                    self.log(f"🔍 评论API调试信息:")
                    self.log(f"   🔗 完整URL: {full_url}")
                    self.log(f"   📊 参数: {params}")
                    self.log(f"   🔧 关键认证头:")
                    self.log(f"      X-Signature: {headers.get('X-Signature', 'N/A')}")
                    self.log(f"      X-Timestamp: {headers.get('X-Timestamp', 'N/A')}")
                    self.log(f"      X-Request-Id: {headers.get('X-Request-Id', 'N/A')}")
                    self.log(f"      X-Aduid: {headers.get('X-Aduid', 'N/A')}")

                # 发送请求
                response = self.session.get(url, params=params, headers=headers, timeout=30)

                if response.status_code == 200:
                    data = response.json()
                    if data.get('succeeded'):
                        if retry > 0:
                            self.log(f"✅ 评论API重试成功 (第{retry+1}次尝试)")
                        return data
                    else:
                        error_code = data.get('code')
                        error_msg = data.get('error', '未知错误')

                        # 检查是否是反爬错误码1059
                        if error_code == 1059:
                            if retry < max_retries - 1:
                                # 智能等待时间策略：前几次短等待，后面逐渐增加
                                if retry < 3:
                                    wait_time = 2  # 前3次等待2秒
                                elif retry < 6:
                                    wait_time = 5  # 第4-6次等待5秒
                                else:
                                    wait_time = 10  # 第7-10次等待10秒

                                self.log(f"⚠️ 遇到反爬机制 (错误码1059)，等待{wait_time}秒后重试 (第{retry+1}/{max_retries}次)")
                                time.sleep(wait_time)
                                continue
                            else:
                                self.log(f"❌ 评论API重试{max_retries}次后仍失败: 错误码{error_code} - {error_msg}")
                                return None
                        else:
                            self.log(f"❌ 评论API返回失败: 错误码{error_code} - {error_msg}")
                            return None
                else:
                    # 详细的错误日志
                    self.log(f"❌ 评论API请求失败: {response.status_code}")
                    self.log(f"🔗 请求URL: {response.url}")
                    self.log(f"📋 响应内容: {response.text[:500]}...")
                    return None

            except Exception as e:
                if retry < max_retries - 1:
                    # 使用与1059错误相同的等待策略
                    if retry < 3:
                        wait_time = 2
                    elif retry < 6:
                        wait_time = 5
                    else:
                        wait_time = 10

                    self.log(f"❌ 获取评论异常: {str(e)}，等待{wait_time}秒后重试 (第{retry+1}/{max_retries}次)")
                    time.sleep(wait_time)
                    continue
                else:
                    self.log(f"❌ 获取评论异常，重试{max_retries}次后仍失败: {str(e)}")
                    return None

        return None

    def fetch_all_comments(self, topic_id: int, comments_count: int) -> List[Dict[str, Any]]:
        """获取话题的所有评论（如果评论数量大于8）"""
        if comments_count <= 8:
            return []  # 不需要额外获取

        self.log(f"📝 话题 {topic_id} 有 {comments_count} 条评论，开始获取完整评论列表...")

        all_comments = []
        begin_time = None
        page = 1

        while True:
            # 检查停止标志
            if self.is_stopped():
                self.log("🛑 评论获取已停止")
                break

            self.log(f"   📄 获取第 {page} 页评论...")

            # 获取当前页评论
            data = self.fetch_comments_safe(topic_id, begin_time, count=30)
            if not data:
                self.log(f"   ❌ 第 {page} 页获取失败，可能是权限问题，跳过此话题")
                break

            comments = data.get('resp_data', {}).get('comments', [])
            if not comments:
                self.log(f"   📭 第 {page} 页无评论，停止获取")
                break

            self.log(f"   ✅ 第 {page} 页获取到 {len(comments)} 条评论")

            # 处理评论数据，包括回复评论
            for comment in comments:
                all_comments.append(comment)

                # 处理回复评论
                if 'replied_comments' in comment and comment['replied_comments']:
                    for reply in comment['replied_comments']:
                        all_comments.append(reply)

            # 如果返回的评论数量少于30，说明已经是最后一页
            if len(comments) < 30:
                self.log(f"   🏁 已获取完所有评论，共 {len(all_comments)} 条")
                break

            # 准备下一页的 begin_time（最后一条评论的时间 + 1毫秒）
            last_comment = comments[-1]
            last_time = last_comment.get('create_time')
            if last_time:
                begin_time = self._increment_time(last_time)
                self.log(f"   ⏭️ 下一页起始时间: {begin_time}")
            else:
                self.log("   ❌ 无法获取最后评论时间，停止获取")
                break

            page += 1

            # 添加延迟避免请求过快
            time.sleep(1)

        return all_comments

    def _increment_time(self, time_str: str) -> str:
        """将时间字符串增加1毫秒"""
        try:
            from datetime import datetime, timedelta
            import re

            # 解析时间字符串，例如: "2025-07-03T12:54:05.849+0800"
            # 提取毫秒部分
            match = re.match(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.(\d{3})(\+\d{4})', time_str)
            if match:
                base_time = match.group(1)
                milliseconds = int(match.group(2))
                timezone = match.group(3)

                # 增加1毫秒
                milliseconds += 1
                if milliseconds >= 1000:
                    # 需要进位到秒
                    dt = datetime.strptime(base_time, '%Y-%m-%dT%H:%M:%S')
                    dt += timedelta(seconds=1)
                    base_time = dt.strftime('%Y-%m-%dT%H:%M:%S')
                    milliseconds = 0

                return f"{base_time}.{milliseconds:03d}{timezone}"
            else:
                # 如果格式不匹配，直接返回原时间
                return time_str

        except Exception as e:
            self.log(f"❌ 时间增量失败: {e}")
            return time_str

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
        
        self.log(f"🌐 安全请求 #{self.request_count}")
        self.log(f"   🎯 参数: scope={scope}, count={count}")
        if end_time:
            self.log(f"   📅 时间: {end_time}")
        self.log(f"   🔗 完整链接: {full_url}")
        
        # 调试模式输出详细信息
        if self.debug_mode:
            self.log(f"   🔍 调试模式:")
            self.log(f"   📍 基础URL: {url}")
            self.log(f"   📊 所有参数: {params}")
            self.log(f"   🔧 请求头: {json.dumps(headers, ensure_ascii=False, indent=4)}")
            self.log(f"   🍪 Cookie长度: {len(self.cookie)}字符")
            self.log(f"   ⏰ 当前时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 在发起请求前检查停止标志
        if self.is_stopped():
            # 停止时不再打印日志，直接返回
            return None

        try:
            response = self.session.get(
                url,
                headers=headers,
                params=params,
                timeout=10,  # 降低超时时间以便快速响应停止信号
                allow_redirects=True
            )
            
            self.log(f"   📊 状态: {response.status_code}, 大小: {len(response.content)}B")

            # 请求完成后立即检查停止标志
            if self.is_stopped():
                return None

            if response.status_code == 200:
                try:
                    # 在处理响应前检查停止标志
                    if self.is_stopped():
                        self.log("🛑 响应处理前检测到停止信号")
                        return None

                    data = response.json()
                    if data.get('succeeded'):
                        topics = data.get('resp_data', {}).get('topics', [])
                        self.log(f"   ✅ 获取成功: {len(topics)}个话题")
                        return data
                    else:
                        error_code = data.get('code')
                        error_message = data.get('error', data.get('message', '未知错误'))

                        # 检查是否是会员过期错误
                        if error_code == 14210:
                            self.log(f"   ❌ 会员已过期: {error_message}")
                            self.log(f"   📋 完整响应: {json.dumps(data, ensure_ascii=False, indent=2)}")
                            # 设置过期标志，让调用方知道这是过期错误
                            return {"expired": True, "code": error_code, "message": error_message}
                        else:
                            self.log(f"   ❌ API失败: {error_message}")
                            self.log(f"   📋 完整响应: {json.dumps(data, ensure_ascii=False, indent=2)}")
                            return None
                except json.JSONDecodeError as e:
                    self.log(f"   ❌ JSON解析失败: {e}")
                    self.log(f"   📄 响应内容: {response.text[:500]}...")
                    self.log(f"   📋 响应头: {dict(response.headers)}")
                    return None
            else:
                self.log(f"   ❌ HTTP错误: {response.status_code}")
                self.log(f"   📄 响应内容: {response.text}")
                self.log(f"   📋 响应头: {dict(response.headers)}")
                if response.status_code == 429:
                    self.log("   🚨 触发频率限制，建议增加延迟时间")
                elif response.status_code == 403:
                    self.log("   🚨 访问被拒绝，可能需要更新Cookie或反检测策略")
                elif response.status_code == 401:
                    self.log("   🚨 认证失败，请检查Cookie是否过期")
                return None
                
        except requests.exceptions.Timeout as e:
            self.log(f"   ❌ 请求超时: {e}")
            self.log(f"   🔧 建议: 增加超时时间或检查网络连接")
            return None
        except requests.exceptions.ConnectionError as e:
            self.log(f"   ❌ 连接错误: {e}")
            self.log(f"   🔧 建议: 检查网络连接或DNS设置")
            return None
        except requests.exceptions.HTTPError as e:
            self.log(f"   ❌ HTTP协议错误: {e}")
            return None
        except requests.exceptions.RequestException as e:
            self.log(f"   ❌ 请求异常: {e}")
            self.log(f"   🔧 异常类型: {type(e).__name__}")
            return None
    
    def store_batch_data(self, data: Dict[str, Any]) -> Dict[str, int]:
        """批量存储数据到数据库"""
        # 在数据存储前检查停止标志
        if self.is_stopped():
            self.log("🛑 数据存储前检测到停止信号")
            return {'new_topics': 0, 'updated_topics': 0, 'errors': 0}

        if not data or not data.get('succeeded'):
            return {'new_topics': 0, 'updated_topics': 0, 'errors': 0}

        topics = data.get('resp_data', {}).get('topics', [])
        if not topics:
            return {'new_topics': 0, 'updated_topics': 0, 'errors': 0}

        stats = {'new_topics': 0, 'updated_topics': 0, 'errors': 0}

        for topic_data in topics:
            # 在处理每个话题前检查停止标志
            if self.is_stopped():
                self.log("🛑 话题处理过程中检测到停止信号")
                break

            try:
                topic_id = topic_data.get('topic_id')

                # 检查是否已存在
                self.db.cursor.execute('SELECT topic_id FROM topics WHERE topic_id = ?', (topic_id,))
                exists = self.db.cursor.fetchone()

                # 导入数据
                self.db.import_topic_data(topic_data)

                # 检查是否需要获取更多评论
                comments_count = topic_data.get('comments_count', 0)
                if comments_count > 8:
                    self.log(f"📝 话题 {topic_id} 有 {comments_count} 条评论，尝试获取完整评论列表...")
                    try:
                        additional_comments = self.fetch_all_comments(topic_id, comments_count)
                        if additional_comments:
                            self.db.import_additional_comments(topic_id, additional_comments)
                            self.log(f"✅ 成功获取并导入 {len(additional_comments)} 条额外评论")
                        else:
                            self.log(f"ℹ️ 话题 {topic_id} 无法获取更多评论，可能是权限限制")
                    except Exception as e:
                        self.log(f"⚠️ 话题 {topic_id} 获取评论时出错: {e}")
                        # 不影响话题本身的导入

                if exists:
                    stats['updated_topics'] += 1
                else:
                    stats['new_topics'] += 1

            except Exception as e:
                stats['errors'] += 1
                self.log(f"   ⚠️ 话题导入失败: {e}")
        
        # 提交事务
        self.db.conn.commit()
        return stats
    
    def crawl_latest(self, count: int = 20) -> Dict[str, int]:
        """爬取最新话题"""
        self.log(f"\n🆕 爬取最新 {count} 个话题...")
        
        data = self.fetch_topics_safe(scope="all", count=count)
        if data:
            stats = self.store_batch_data(data)
            self.log(f"💾 存储结果: 新增{stats['new_topics']}, 更新{stats['updated_topics']}")
            return stats
        else:
            self.log("❌ 获取失败")
            return {'new_topics': 0, 'updated_topics': 0, 'errors': 1}
    
    def crawl_historical(self, pages: int = 10, per_page: int = 20) -> Dict[str, int]:
        """爬取历史数据"""
        self.log(f"\n📚 爬取历史数据: {pages}页 x {per_page}条/页")
        
        total_stats = {'new_topics': 0, 'updated_topics': 0, 'errors': 0, 'pages': 0}
        end_time = None
        completed_pages = 0
        max_retries_per_page = 10  # 每页最大重试次数
        
        while completed_pages < pages:
            # 检查停止标志
            if self.is_stopped():
                self.log("🛑 任务已停止")
                break

            current_page = completed_pages + 1
            self.log(f"\n📄 页面 {current_page}/{pages}")
            retry_count = 0
            
            # 重试当前页直到成功或达到最大重试次数
            while retry_count < max_retries_per_page:
                # 在重试循环中也检查停止标志
                if self.is_stopped():
                    return total_stats

                if retry_count > 0:
                    self.log(f"   🔄 第{retry_count}次重试")
                
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
                        self.log(f"   📭 无更多数据，停止爬取")
                        return total_stats
                    
                    # 存储数据
                    page_stats = self.store_batch_data(data)
                    self.log(f"   💾 页面存储: 新增{page_stats['new_topics']}, 更新{page_stats['updated_topics']}")
                    
                    # 累计统计
                    total_stats['new_topics'] += page_stats['new_topics']
                    total_stats['updated_topics'] += page_stats['updated_topics']
                    total_stats['errors'] += page_stats['errors']
                    total_stats['pages'] += 1
                    completed_pages += 1
                    
                    # 调试：显示所有话题的时间戳（只在调试模式下）
                    if self.debug_mode:
                        self.log(f"   🔍 调试信息:")
                        self.log(f"   📊 本页获取到 {len(topics)} 个话题")
                        for i, topic in enumerate(topics):
                            topic_time = topic.get('create_time', 'N/A')
                            topic_title = topic.get('title', '无标题')[:30]
                            self.log(f"   {i+1:2d}. {topic_time} - {topic_title}")
                    
                    # 准备下一页的时间戳
                    if topics:
                        original_time = topics[-1].get('create_time')
                        try:
                            from datetime import datetime, timedelta
                            dt = datetime.fromisoformat(original_time.replace('+0800', '+08:00'))
                            dt = dt - timedelta(milliseconds=self.timestamp_offset_ms)
                            end_time = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
                            self.log(f"   📅 原始时间戳: {original_time}")
                            self.log(f"   ⏭️ 下一页时间戳: {end_time} (减去{self.timestamp_offset_ms}毫秒)")
                        except Exception as e:
                            end_time = original_time
                            self.log(f"   ⚠️ 时间戳调整失败: {e}")
                            self.log(f"   ⏭️ 下一页时间戳: {end_time} (未调整)")
                    
                    # 检查是否已爬完
                    if len(topics) < per_page:
                        self.log(f"   📭 已爬取完毕 (返回{len(topics)}条)")
                        return total_stats
                    
                    # 成功，跳出重试循环
                    self.check_page_long_delay()  # 页面成功处理后进行长休眠检查
                    break
                else:
                    # 失败，增加重试计数和错误计数
                    retry_count += 1
                    total_stats['errors'] += 1
                    self.log(f"   ❌ 页面 {current_page} 获取失败 (重试{retry_count}/{max_retries_per_page})")
                    
                    # 调整时间戳用于重试
                    if end_time:
                        try:
                            from datetime import datetime, timedelta
                            dt = datetime.fromisoformat(end_time.replace('+0800', '+08:00'))
                            dt = dt - timedelta(milliseconds=self.timestamp_offset_ms)
                            end_time = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
                            self.log(f"   🔄 调整时间戳: {end_time} (再次减去{self.timestamp_offset_ms}毫秒)")
                        except Exception as e:
                            self.log(f"   ⚠️ 时间戳调整失败: {e}")
            
            # 如果重试次数用完仍然失败
            if retry_count >= max_retries_per_page:
                self.log(f"   🚫 页面 {current_page} 达到最大重试次数，跳过此页")
                # 如果有时间戳，尝试大幅度调整跳过问题区域
                if end_time:
                    try:
                        from datetime import datetime, timedelta
                        dt = datetime.fromisoformat(end_time.replace('+0800', '+08:00'))
                        # 大幅度跳过，减去1小时
                        dt = dt - timedelta(hours=1)
                        end_time = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
                        self.log(f"   ⏰ 大幅度跳过时间段: {end_time} (减去1小时)")
                    except Exception as e:
                        self.log(f"   ⚠️ 大幅度时间戳调整失败: {e}")
                completed_pages += 1  # 跳过这一页
        
        self.log(f"\n🏁 历史爬取完成:")
        self.log(f"   📄 成功页数: {total_stats['pages']}")
        self.log(f"   ✅ 新增话题: {total_stats['new_topics']}")
        self.log(f"   🔄 更新话题: {total_stats['updated_topics']}")
        if total_stats['errors'] > 0:
            self.log(f"   ❌ 总错误数: {total_stats['errors']}")
        
        return total_stats
    
    def crawl_all_historical(self, per_page: int = 20, auto_confirm: bool = False) -> Dict[str, int]:
        """获取所有历史数据：无限爬取直到没有数据（使用增量爬取逻辑）"""
        self.log(f"\n🌊 获取所有历史数据模式 (每页{per_page}条)")
        self.log(f"⚠️ 警告：此模式将持续爬取直到没有数据，可能需要很长时间")
        
        # 检查数据库状态，如果有数据则使用增量爬取逻辑
        timestamp_info = self.db.get_timestamp_range_info()
        start_end_time = None
        
        if timestamp_info['has_data']:
            oldest_timestamp = timestamp_info['oldest_timestamp']
            total_existing = timestamp_info['total_topics']
            
            self.log(f"📊 数据库现状:")
            self.log(f"   现有话题数: {total_existing}")
            self.log(f"   最老时间戳: {oldest_timestamp}")
            self.log(f"   最新时间戳: {timestamp_info['newest_timestamp']}")
            self.log(f"🎯 将从最老时间戳开始继续向历史爬取（增量模式）...")
            
            # 准备增量爬取的起始时间戳
            try:
                from datetime import datetime, timedelta
                dt = datetime.fromisoformat(oldest_timestamp.replace('+0800', '+08:00'))
                dt = dt - timedelta(milliseconds=self.timestamp_offset_ms)
                start_end_time = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
                self.log(f"🚀 增量爬取起始时间戳: {start_end_time}")
            except Exception as e:
                self.log(f"⚠️ 时间戳处理失败，使用原时间戳: {e}")
                start_end_time = oldest_timestamp
        else:
            self.log(f"📊 数据库为空，将从最新数据开始爬取")

        # 用户确认（Web API调用时自动确认）
        if not auto_confirm:
            confirm = input("确认开始无限爬取？(y/N): ").lower().strip()
            if confirm != 'y':
                self.log("❌ 用户取消操作")
                return {'new_topics': 0, 'updated_topics': 0, 'errors': 0, 'pages': 0}

        self.log(f"🚀 开始无限历史爬取...")
        
        total_stats = {'new_topics': 0, 'updated_topics': 0, 'errors': 0, 'pages': 0}
        end_time = start_end_time  # 使用增量爬取的起始时间戳
        current_page = 0
        max_retries_per_page = 10
        consecutive_empty_pages = 0  # 连续空页面计数
        max_consecutive_empty = 3   # 最大连续空页面数
        
        while True:
            # 检查停止标志
            if self.is_stopped():
                self.log("🛑 任务已停止")
                break

            current_page += 1
            self.log(f"\n📄 页面 {current_page}")
            retry_count = 0
            page_success = False

            # 重试当前页直到成功或达到最大重试次数
            while retry_count < max_retries_per_page:
                # 在重试循环中也检查停止标志
                if self.is_stopped():
                    return total_stats
                if retry_count > 0:
                    self.log(f"   🔄 第{retry_count}次重试")
                
                # 获取数据 - 根据是否有起始时间戳决定请求方式
                if current_page == 1 and start_end_time is None:
                    # 数据库为空，从最新开始
                    data = self.fetch_topics_safe(scope="all", count=per_page, is_historical=True)
                else:
                    # 有数据或后续页面，使用 end_time 参数
                    data = self.fetch_topics_safe(scope="all", count=per_page,
                                                end_time=end_time, is_historical=True)

                # 检查是否是会员过期错误
                if data and data.get('expired'):
                    self.log(f"   ❌ 会员已过期，停止爬取")
                    return data  # 直接返回过期信息

                if data:
                    # 成功获取数据
                    topics = data.get('resp_data', {}).get('topics', [])
                    
                    if not topics:
                        consecutive_empty_pages += 1
                        self.log(f"   📭 第{consecutive_empty_pages}个空页面")
                        
                        if consecutive_empty_pages >= max_consecutive_empty:
                            self.log(f"   🏁 连续{max_consecutive_empty}个空页面，所有历史数据爬取完成")
                            self.log(f"\n🎉 无限爬取完成总结:")
                            self.log(f"   📄 总页数: {total_stats['pages']}")
                            self.log(f"   ✅ 新增话题: {total_stats['new_topics']}")
                            self.log(f"   🔄 更新话题: {total_stats['updated_topics']}")
                            if total_stats['errors'] > 0:
                                self.log(f"   ❌ 总错误数: {total_stats['errors']}")
                            
                            # 显示最终数据库状态
                            final_db_stats = self.db.get_timestamp_range_info()
                            if final_db_stats['has_data']:
                                self.log(f"\n📊 最终数据库状态:")
                                self.log(f"   话题总数: {final_db_stats['total_topics']}")
                                if timestamp_info['has_data']:
                                    self.log(f"   新增话题: {final_db_stats['total_topics'] - timestamp_info['total_topics']}")
                                self.log(f"   时间范围: {final_db_stats['oldest_timestamp']} ~ {final_db_stats['newest_timestamp']}")
                            
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
                    self.log(f"   💾 页面存储: 新增{page_stats['new_topics']}, 更新{page_stats['updated_topics']}")
                    
                    # 累计统计
                    total_stats['new_topics'] += page_stats['new_topics']
                    total_stats['updated_topics'] += page_stats['updated_topics']
                    total_stats['errors'] += page_stats['errors']
                    total_stats['pages'] += 1
                    
                    # 显示进度信息
                    self.log(f"   📊 获取到 {len(topics)} 个话题，其中 {new_topics_count} 个为新话题")
                    self.log(f"   📈 累计: 新增{total_stats['new_topics']}, 更新{total_stats['updated_topics']}, 页数{total_stats['pages']}")
                    
                    # 调试：显示时间戳信息（简化版）
                    if topics:
                        first_time = topics[0].get('create_time', 'N/A')
                        last_time = topics[-1].get('create_time', 'N/A')
                        self.log(f"   ⏰ 时间范围: {first_time} ~ {last_time}")
                    
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
                            self.log(f"   ⚠️ 时间戳调整失败: {e}")
                    
                    # 检查是否返回数据量小于预期（可能接近底部）
                    if len(topics) < per_page:
                        self.log(f"   ⚠️ 返回数据量({len(topics)})小于预期({per_page})，可能接近历史底部")
                    
                    # 如果没有新话题且数据量不足，可能已达历史底部
                    if new_topics_count == 0 and len(topics) < per_page:
                        self.log(f"   📭 无新话题且数据量不足，可能已达历史底部")
                        return total_stats
                    
                    # 成功，跳出重试循环
                    page_success = True
                    break
                else:
                    # 失败，增加重试计数和错误计数
                    retry_count += 1
                    total_stats['errors'] += 1
                    self.log(f"   ❌ 页面 {current_page} 获取失败 (重试{retry_count}/{max_retries_per_page})")
                    
                    # 调整时间戳用于重试
                    if end_time:
                        try:
                            from datetime import datetime, timedelta
                            dt = datetime.fromisoformat(end_time.replace('+0800', '+08:00'))
                            dt = dt - timedelta(milliseconds=self.timestamp_offset_ms)
                            end_time = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
                        except Exception as e:
                            self.log(f"   ⚠️ 时间戳调整失败: {e}")
            
            # 如果重试次数用完仍然失败
            if not page_success:
                self.log(f"   🚫 页面 {current_page} 达到最大重试次数")
                # 大幅度跳过问题区域
                if end_time:
                    try:
                        from datetime import datetime, timedelta
                        dt = datetime.fromisoformat(end_time.replace('+0800', '+08:00'))
                        dt = dt - timedelta(hours=1)
                        end_time = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
                        self.log(f"   ⏰ 大幅度跳过时间段: {end_time} (减去1小时)")
                    except Exception as e:
                        self.log(f"   ⚠️ 大幅度时间戳调整失败: {e}")
                completed_pages += 1  # 跳过这一页
            else:
                # 页面成功处理后进行长休眠检查（基于页面数而非请求数）
                self.check_page_long_delay()
            
            # 每50页显示一次总体进度
            if current_page % 50 == 0:
                self.log(f"\n🎯 进度报告 (第{current_page}页):")
                self.log(f"   📊 累计新增: {total_stats['new_topics']}")
                self.log(f"   📊 累计更新: {total_stats['updated_topics']}")
                self.log(f"   📊 成功页数: {total_stats['pages']}")
                self.log(f"   📊 错误次数: {total_stats['errors']}")
                
                # 显示当前数据库状态
                current_db_stats = self.db.get_timestamp_range_info()
                if current_db_stats['has_data']:
                    self.log(f"   📊 数据库状态: {current_db_stats['total_topics']}个话题")
                    self.log(f"   📊 时间范围: {current_db_stats['oldest_timestamp']} ~ {current_db_stats['newest_timestamp']}")
        
        # 这里理论上不会到达，因为在循环内会return
        return total_stats
    
    def crawl_incremental(self, pages: int = 10, per_page: int = 20) -> Dict[str, int]:
        """增量爬取：基于数据库最老时间戳继续向历史爬取"""
        self.log(f"\n📈 增量爬取模式: {pages}页 x {per_page}条/页")
        
        # 获取数据库时间戳范围信息
        timestamp_info = self.db.get_timestamp_range_info()
        
        if not timestamp_info['has_data']:
            self.log("❌ 数据库中没有话题数据，请先进行历史爬取")
            return {'new_topics': 0, 'updated_topics': 0, 'errors': 1}
        
        oldest_timestamp = timestamp_info['oldest_timestamp']
        total_existing = timestamp_info['total_topics']
        
        self.log(f"📊 数据库状态:")
        self.log(f"   现有话题数: {total_existing}")
        self.log(f"   最老时间戳: {oldest_timestamp}")
        self.log(f"   最新时间戳: {timestamp_info['newest_timestamp']}")
        self.log(f"🎯 将从最老时间戳开始继续向历史爬取...")
        
        # 准备增量爬取的起始时间戳（在最老时间戳基础上减去偏移量）
        try:
            from datetime import datetime, timedelta
            dt = datetime.fromisoformat(oldest_timestamp.replace('+0800', '+08:00'))
            dt = dt - timedelta(milliseconds=self.timestamp_offset_ms)
            start_end_time = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
            self.log(f"🚀 增量爬取起始时间戳: {start_end_time}")
        except Exception as e:
            self.log(f"⚠️ 时间戳处理失败，使用原时间戳: {e}")
            start_end_time = oldest_timestamp
        
        # 执行增量爬取
        total_stats = {'new_topics': 0, 'updated_topics': 0, 'errors': 0, 'pages': 0}
        end_time = start_end_time
        completed_pages = 0
        max_retries_per_page = 10
        
        while completed_pages < pages:
            current_page = completed_pages + 1
            self.log(f"\n📄 增量页面 {current_page}/{pages}")
            retry_count = 0
            
            # 重试当前页直到成功或达到最大重试次数
            while retry_count < max_retries_per_page:
                if retry_count > 0:
                    self.log(f"   🔄 第{retry_count}次重试")
                
                # 获取数据 - 总是使用 end_time 参数
                data = self.fetch_topics_safe(scope="all", count=per_page,
                                            end_time=end_time, is_historical=True)

                # 检查是否是会员过期错误
                if data and data.get('expired'):
                    self.log(f"   ❌ 会员已过期，停止爬取")
                    return data  # 直接返回过期信息

                if data:
                    # 成功获取数据
                    topics = data.get('resp_data', {}).get('topics', [])
                    if not topics:
                        self.log(f"   📭 无更多历史数据，增量爬取完成")
                        return total_stats
                    
                    # 检查是否有新数据（避免重复爬取已有数据）
                    new_topics_count = 0
                    for topic in topics:
                        topic_id = topic.get('topic_id')
                        self.db.cursor.execute('SELECT topic_id FROM topics WHERE topic_id = ?', (topic_id,))
                        if not self.db.cursor.fetchone():
                            new_topics_count += 1
                    
                    self.log(f"   📊 获取到 {len(topics)} 个话题，其中 {new_topics_count} 个为新话题")
                    
                    # 如果没有新话题且当前页话题数少于预期，可能已到达历史底部
                    if new_topics_count == 0 and len(topics) < per_page:
                        self.log(f"   📭 无新话题且数据量不足，可能已达历史底部")
                        return total_stats
                    
                    # 存储数据
                    page_stats = self.store_batch_data(data)
                    self.log(f"   💾 页面存储: 新增{page_stats['new_topics']}, 更新{page_stats['updated_topics']}")
                    
                    # 累计统计
                    total_stats['new_topics'] += page_stats['new_topics']
                    total_stats['updated_topics'] += page_stats['updated_topics']
                    total_stats['errors'] += page_stats['errors']
                    total_stats['pages'] += 1
                    completed_pages += 1
                    
                    # 调试：显示话题时间戳信息
                    if self.debug_mode:
                        self.log(f"   🔍 调试信息:")
                        self.log(f"   📊 本页获取到 {len(topics)} 个话题")
                        for i, topic in enumerate(topics):
                            topic_time = topic.get('create_time', 'N/A')
                            topic_title = topic.get('title', '无标题')[:30]
                            self.log(f"   {i+1:2d}. {topic_time} - {topic_title}")
                    
                    # 准备下一页的时间戳
                    if topics:
                        original_time = topics[-1].get('create_time')
                        try:
                            dt = datetime.fromisoformat(original_time.replace('+0800', '+08:00'))
                            dt = dt - timedelta(milliseconds=self.timestamp_offset_ms)
                            end_time = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
                            self.log(f"   ⏭️ 下一页时间戳: {end_time}")
                        except Exception as e:
                            end_time = original_time
                            self.log(f"   ⚠️ 时间戳调整失败: {e}")
                    
                    # 成功，跳出重试循环
                    self.check_page_long_delay()  # 页面成功处理后进行长休眠检查
                    break
                else:
                    # 失败，增加重试计数和错误计数
                    retry_count += 1
                    total_stats['errors'] += 1

                    # 如果任务已停止，不再打印错误信息和调整时间戳
                    if self.is_stopped():
                        return total_stats

                    self.log(f"   ❌ 页面 {current_page} 获取失败 (重试{retry_count}/{max_retries_per_page})")

                    # 调整时间戳用于重试
                    if end_time:
                        try:
                            dt = datetime.fromisoformat(end_time.replace('+0800', '+08:00'))
                            dt = dt - timedelta(milliseconds=self.timestamp_offset_ms)
                            end_time = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
                            self.log(f"   🔄 调整时间戳: {end_time}")
                        except Exception as e:
                            self.log(f"   ⚠️ 时间戳调整失败: {e}")
            
            # 如果重试次数用完仍然失败
            if retry_count >= max_retries_per_page:
                # 如果任务已停止，不再打印信息
                if self.is_stopped():
                    return total_stats

                self.log(f"   🚫 页面 {current_page} 达到最大重试次数，跳过此页")
                # 大幅度跳过问题区域
                if end_time:
                    try:
                        dt = datetime.fromisoformat(end_time.replace('+0800', '+08:00'))
                        dt = dt - timedelta(hours=1)
                        end_time = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
                        self.log(f"   ⏰ 大幅度跳过时间段: {end_time} (减去1小时)")
                    except Exception as e:
                        self.log(f"   ⚠️ 大幅度时间戳调整失败: {e}")
                completed_pages += 1  # 跳过这一页
        
        self.log(f"\n🏁 增量爬取完成:")
        self.log(f"   📄 成功页数: {total_stats['pages']}")
        self.log(f"   ✅ 新增话题: {total_stats['new_topics']}")
        self.log(f"   🔄 更新话题: {total_stats['updated_topics']}")
        if total_stats['errors'] > 0:
            self.log(f"   ❌ 总错误数: {total_stats['errors']}")
        
        # 显示更新后的数据库状态
        updated_info = self.db.get_timestamp_range_info()
        self.log(f"\n📊 更新后数据库状态:")
        self.log(f"   话题总数: {updated_info['total_topics']} (+{updated_info['total_topics'] - total_existing})")
        self.log(f"   时间范围: {updated_info['oldest_timestamp']} ~ {updated_info['newest_timestamp']}")
        
        return total_stats
    
    def crawl_latest_until_complete(self, per_page: int = 20) -> Dict[str, int]:
        """获取最新记录：智能增量更新，爬取到与数据库完全衔接为止"""
        self.log(f"\n🔄 获取最新记录模式 (每页{per_page}条)")
        self.log(f"💡 智能逻辑：检查最新话题，如有新内容则向后爬取直到与数据库完全衔接")
        
        # 检查数据库状态
        timestamp_info = self.db.get_timestamp_range_info()
        if not timestamp_info['has_data']:
            self.log("📊 数据库为空，将从最新开始爬取")
            # 空库场景：直接从最新开始增量，直到与已存数据衔接或无更多数据
        
        self.log(f"📊 数据库状态:")
        self.log(f"   现有话题数: {timestamp_info['total_topics']}")
        self.log(f"   最新时间戳: {timestamp_info['newest_timestamp']}")
        
        total_stats = {'new_topics': 0, 'updated_topics': 0, 'errors': 0, 'pages': 0}
        end_time = None  # 从最新开始
        current_page = 0
        max_retries_per_page = 10
        
        while True:
            # 检查停止标志
            if self.is_stopped():
                break

            current_page += 1
            self.log(f"\n📄 检查页面 {current_page}")
            retry_count = 0
            page_success = False

            # 重试当前页直到成功或达到最大重试次数
            while retry_count < max_retries_per_page:
                # 在重试循环中也检查停止标志
                if self.is_stopped():
                    return total_stats
                if retry_count > 0:
                    self.log(f"   🔄 第{retry_count}次重试")
                
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
                        self.log(f"   📭 无更多数据，获取完成")
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
                    
                    self.log(f"   📊 页面分析: {len(topics)}个话题，{existing_count}个已存在，{len(new_topics_list)}个新话题")
                    
                    # 判断是否需要停止
                    if existing_count == len(topics):
                        # 整页话题全部存在于数据库中
                        self.log(f"   ✅ 整页话题全部存在于数据库，增量更新完成")
                        self.log(f"\n🎉 获取最新记录完成总结:")
                        self.log(f"   📄 检查页数: {total_stats['pages']}")
                        self.log(f"   ✅ 新增话题: {total_stats['new_topics']}")
                        self.log(f"   🔄 更新话题: {total_stats['updated_topics']}")
                        if total_stats['errors'] > 0:
                            self.log(f"   ❌ 总错误数: {total_stats['errors']}")
                        
                        # 显示更新后的数据库状态
                        final_db_stats = self.db.get_timestamp_range_info()
                        if final_db_stats['has_data']:
                            self.log(f"\n📊 数据库最终状态:")
                            self.log(f"   话题总数: {final_db_stats['total_topics']} (+{final_db_stats['total_topics'] - timestamp_info['total_topics']})")
                            self.log(f"   时间范围: {final_db_stats['oldest_timestamp']} ~ {final_db_stats['newest_timestamp']}")
                        
                        return total_stats
                    
                    elif existing_count == 0:
                        # 整页话题都是新的，全部存储
                        page_stats = self.store_batch_data(data)
                        self.log(f"   💾 整页存储: 新增{page_stats['new_topics']}, 更新{page_stats['updated_topics']}")
                    
                    else:
                        # 部分话题是新的，只存储新话题
                        self.log(f"   💾 部分存储: 只处理{len(new_topics_list)}个新话题")
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
                                self.log(f"   ⚠️ 话题导入失败: {e}")
                        
                        # 提交事务
                        self.db.conn.commit()
                        self.log(f"   💾 新话题存储: 新增{new_topics_count}, 更新{updated_topics_count}")
                        
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
                    self.log(f"   📈 累计: 新增{total_stats['new_topics']}, 更新{total_stats['updated_topics']}, 页数{total_stats['pages']}")
                    
                    # 显示时间戳信息
                    if topics:
                        first_time = topics[0].get('create_time', 'N/A')
                        last_time = topics[-1].get('create_time', 'N/A')
                        self.log(f"   ⏰ 时间范围: {first_time} ~ {last_time}")
                    
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
                            self.log(f"   ⚠️ 时间戳调整失败: {e}")
                    
                    # 成功，跳出重试循环
                    page_success = True
                    break
                else:
                    # 失败，增加重试计数和错误计数
                    retry_count += 1
                    total_stats['errors'] += 1

                    # 如果任务已停止，不再打印错误信息和调整时间戳
                    if self.is_stopped():
                        return total_stats

                    self.log(f"   ❌ 页面 {current_page} 获取失败 (重试{retry_count}/{max_retries_per_page})")

                    # 调整时间戳用于重试
                    if end_time:
                        try:
                            from datetime import datetime, timedelta
                            dt = datetime.fromisoformat(end_time.replace('+0800', '+08:00'))
                            dt = dt - timedelta(milliseconds=self.timestamp_offset_ms)
                            end_time = dt.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+0800'
                        except Exception as e:
                            self.log(f"   ⚠️ 时间戳调整失败: {e}")
            
            # 如果重试次数用完仍然失败
            if not page_success:
                # 如果任务已停止，不再打印信息
                if self.is_stopped():
                    break
                self.log(f"   🚫 页面 {current_page} 达到最大重试次数，停止获取")
                break
            else:
                # 页面成功处理后进行长休眠检查（基于页面数而非请求数）
                self.check_page_long_delay()
        
        return total_stats
    
    def show_menu(self):
        """显示交互菜单"""
        self.log(f"\n{'='*60}")
        self.log("🕷️ 知识星球交互式数据采集器")
        self.log("="*60)
        self.log("📝 话题采集功能:")
        self.log("1. 获取所有历史数据 (无限爬取) - 适合：全量归档，从最老数据无限挖掘")
        self.log("2. 增量爬取历史 (基于数据库最老时间戳) - 适合：精确补充历史，有目标的回填")
        self.log("3. 获取最新记录 (智能增量更新) - 适合：日常维护，自动检测并只爬新内容")
        self.log("")
        self.log("📥 文件下载功能:")
        self.log("4. 增量收集文件列表 - 适合：从数据库最老时间戳继续收集更早文件")
        self.log("5. 查看文件数据库统计 - 适合：查看收集的文件信息和下载状态")
        self.log("6. 按下载次数下载文件 - 适合：自动收集热门文件并按下载次数排序下载")
        self.log("7. 按时间顺序下载文件 - 适合：自动收集文件列表并按时间顺序下载")
        self.log("8. 文件下载设置 - 适合：调整下载间隔和休眠参数")
        self.log("")
        self.log("⚙️ 系统功能:")
        self.log("9. 查看话题数据库统计 - 适合：监控话题数据状态，了解当前数据范围")
        self.log("10. 调整反检测设置 - 适合：优化爬取速度，应对不同网络环境")
        self.log(f"11. 时间戳设置 (当前: 减去{self.timestamp_offset_ms}毫秒) - 适合：解决时间点冲突，精确控制分页")
        self.log(f"12. 调试模式 (当前: {'开启' if self.debug_mode else '关闭'}) - 适合：排查问题，查看详细请求信息")
        self.log("13. 退出程序")
        self.log("="*60)
    
    def adjust_stealth_settings(self):
        """调整反检测设置"""
        self.log(f"\n🔧 当前反检测设置:")
        self.log(f"   最小延迟: {self.min_delay}秒")
        self.log(f"   最大延迟: {self.max_delay}秒")
        self.log(f"   长延迟间隔: 每{self.long_delay_interval}个页面")
        self.log(f"   长休眠时间: 3-5分钟 (180-300秒)")
        self.log(f"💡 说明: 长休眠基于成功处理的页面数，而非请求数，更加合理稳定")
        
        try:
            new_min = float(input(f"新的最小延迟 (当前{self.min_delay}): ") or self.min_delay)
            new_max = float(input(f"新的最大延迟 (当前{self.max_delay}): ") or self.max_delay)
            new_interval = int(input(f"长延迟间隔 (当前每{self.long_delay_interval}页): ") or self.long_delay_interval)
            
            self.min_delay = max(new_min, 1.0)  # 最小1秒
            self.max_delay = max(new_max, self.min_delay + 1.0)
            self.long_delay_interval = max(new_interval, 5)
            
            self.log(f"✅ 设置已更新")
            self.log(f"💡 长休眠时间固定为3-5分钟，有助于更好地模拟人类行为")
            self.log(f"🎯 长休眠触发：每成功处理{self.long_delay_interval}个页面进行一次长休眠")
            
        except ValueError:
            self.log("❌ 输入无效，保持原设置")
    
    def adjust_timestamp_settings(self):
        """调整时间戳设置"""
        self.log(f"\n⏰ 当前时间戳设置:")
        self.log(f"   减去毫秒数: {self.timestamp_offset_ms}毫秒")
        self.log(f"\n💡 说明:")
        self.log(f"   - 减去1毫秒: 标准设置，与官网一致")
        self.log(f"   - 减去2-3毫秒: 可能避开某些问题时间点")
        self.log(f"   - 减去5-10毫秒: 更大的容错范围")
        
        try:
            new_offset = int(input(f"新的毫秒偏移量 (当前{self.timestamp_offset_ms}): ") or self.timestamp_offset_ms)
            
            if new_offset < 0:
                self.log("❌ 毫秒偏移量不能为负数")
                return
            
            self.timestamp_offset_ms = new_offset
            self.log(f"✅ 时间戳设置已更新: 减去{self.timestamp_offset_ms}毫秒")
            
        except ValueError:
            self.log("❌ 输入无效，保持原设置")
    
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
                        self.log(f"📊 数据库中已有 {existing_files} 个文件记录")
                        collect_confirm = input("是否重新收集文件列表? (y/n, 默认n直接下载): ").strip().lower()
                        if collect_confirm != 'y':
                            self.log("⚡ 直接使用现有数据进行下载...")
                        else:
                            self.log("🔄 按下载次数重新收集文件列表...")
                            downloader.collect_all_files_to_database()
                    else:
                        self.log("🔄 按下载次数收集热门文件列表...")
                        downloader.collect_all_files_to_database()
                    
                    # 自动开始下载
                    self.log("\n🚀 自动开始下载文件...")
                    user_input = input("最大下载文件数 (默认无限，输入数字限制): ").strip()
                    if user_input and user_input.isdigit():
                        max_files = int(user_input)
                    else:
                        max_files = None
                    
                    days_input = input("只下载最近N天的文件 (默认无限，输入数字限制天数): ").strip()
                    if days_input and days_input.isdigit():
                        recent_days = int(days_input)
                    else:
                        recent_days = None
                    downloader.download_files_from_database(max_files=max_files, status_filter='pending', recent_days=recent_days, order_by="download_count DESC")
                    
                elif choice == "7":
                    # 按时间顺序下载文件 (集成收集和下载)
                    downloader = self.get_file_downloader()
                    
                    # 检查数据库是否已有文件数据
                    stats = downloader.file_db.get_database_stats()
                    existing_files = stats.get('files', 0)
                    
                    if existing_files > 0:
                        self.log(f"📊 数据库中已有 {existing_files} 个文件记录")
                        collect_confirm = input("是否重新收集文件列表? (y/n, 默认n直接下载): ").strip().lower()
                        if collect_confirm != 'y':
                            self.log("⚡ 直接使用现有数据进行下载...")
                        else:
                            self.log("🔄 按时间排序重新收集文件列表...")
                            downloader.collect_files_by_time()
                    else:
                        self.log("🔄 按时间排序收集文件列表...")
                        downloader.collect_files_by_time()
                    
                    # 自动开始下载
                    self.log("\n🚀 自动开始下载文件...")
                    user_input = input("最大下载文件数 (默认无限，输入数字限制): ").strip()
                    if user_input and user_input.isdigit():
                        max_files = int(user_input)
                    else:
                        max_files = None
                    
                    days_input = input("只下载最近N天的文件 (默认无限，输入数字限制天数): ").strip()
                    if days_input and days_input.isdigit():
                        recent_days = int(days_input)
                    else:
                        recent_days = None
                    
                    downloader.download_files_from_database(max_files=max_files, status_filter='pending', recent_days=recent_days, order_by="create_time DESC")
                    
                elif choice == "8":
                    # 文件下载设置
                    downloader = self.get_file_downloader()
                    downloader.adjust_settings()
                    
                elif choice == "9":
                    # 查看话题数据库统计
                    self.show_database_status()
                    stats = self.db.get_database_stats()
                    self.log("\n📊 详细统计:")
                    for table, count in stats.items():
                        self.log(f"   {table}: {count}")
                    
                elif choice == "10":
                    self.adjust_stealth_settings()
                
                elif choice == "11":
                    self.adjust_timestamp_settings()
                    
                elif choice == "12":
                    self.debug_mode = not self.debug_mode
                    status = "开启" if self.debug_mode else "关闭"
                    self.log(f"🔍 调试模式已{status}")
                    if self.debug_mode:
                        self.log("⚠️ 调试模式会输出详细的请求信息，包括完整的失败响应")
                    
                elif choice == "13":
                    self.log("👋 退出程序")
                    break
                    
                else:
                    self.log("❌ 无效选择")
                
                input("\n按回车键继续...")
                
        except KeyboardInterrupt:
            self.log("\n⏹️ 用户中断")
        except Exception as e:
            self.log(f"❌ 程序异常: {e}")
        finally:
            self.close()
    
    def close(self):
        """关闭资源"""
        self.db.close()
        self.log("🔒 数据库连接已关闭")


def load_config():
    """加载TOML配置文件"""
    if tomllib is None:
        return None

    # 尝试多个可能的配置文件路径
    config_paths = [
        "config.toml",           # 当前目录
        "../config.toml",        # 上级目录（从backend目录运行时）
        "../../config.toml"      # 上上级目录
    ]

    config_file = None
    for path in config_paths:
        if os.path.exists(path):
            config_file = path
            break

    if config_file is None:
        log_warning("⚠️ 未找到config.toml配置文件，请先创建并配置")
        log_warning("💡 可以复制config.toml.example为config.toml并修改")
        return None
    
    try:
        with open(config_file, 'rb') as f:
            config = tomllib.load(f)
        
        log_info("✅ 已从config.toml加载配置")
        return config
    except Exception as e:
        log_error(f"❌ 加载配置文件出错: {e}", exception=e)
        return None

def main():
    """主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='知识星球交互式数据采集器')
    parser.add_argument('-d', '--auto-download', action='store_true',
                        help='自动下载模式：按时间排序下载最近3天的文件，无需交互')
    args = parser.parse_args()
    
    # 加载配置信息
    config = load_config()
    if not config:
        return
    
    # 从TOML配置中获取值
    auth_config = config.get('auth', {})
    db_config = config.get('database', {})
    
    COOKIE = auth_config.get('cookie', 'your_cookie_here')
    GROUP_ID = auth_config.get('group_id', 'your_group_id_here')
    # 数据库路径改为可选；如未配置则由路径管理器自动管理
    DB_PATH = db_config.get('path') if isinstance(db_config, dict) else None
    
    # 检查配置是否已修改
    if COOKIE == "your_cookie_here" or not COOKIE:
        log_warning("⚠️ 请先在config.toml中配置您的 cookie")
        return
    if GROUP_ID == "your_group_id_here" or not GROUP_ID:
        log_warning("⚠️ 交互式命令行模式仍需手动指定单个群组ID，请在 config.toml 中添加 [auth].group_id")
        return
    
    # 创建交互式爬虫
    crawler = ZSXQInteractiveCrawler(COOKIE, GROUP_ID, DB_PATH)
    
    # 如果是自动下载模式
    if args.auto_download:
        log_info("🤖 自动下载模式：按时间排序下载最近3天的文件")
        log_info("=" * 60)
        
        # 获取文件下载器
        downloader = crawler.get_file_downloader()
        
        log_info("🔄 按时间排序收集文件列表...")
        downloader.collect_files_by_time()
        
        # 自动下载最近3天的文件
        log_info("\n🚀 开始下载最近3天的文件...")
        downloader.download_files_from_database(
            max_files=None,
            status_filter='pending',
            recent_days=3,
            order_by="create_time DESC"
        )
        
        log_info("\n✅ 自动下载任务完成！")
    else:
        # 运行交互界面
        crawler.run_interactive()


if __name__ == "__main__":
    main()
