#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
知识星球文件下载器
Author: AI Assistant
Date: 2024-12-19
Description: 专门用于下载知识星球文件的工具
"""

import datetime
import json
import os
import random
import time
from typing import Dict, Optional, Any

import requests

from zsxq_file_database import ZSXQFileDatabase


class ZSXQFileDownloader:
    """知识星球文件下载器"""
    
    def __init__(self, cookie: str, group_id: str, download_dir: str = "downloads", db_path: str = "zsxq_files_complete.db"):
        """
        初始化文件下载器
        
        Args:
            cookie: 登录凭证
            group_id: 星球ID
            download_dir: 下载目录
            db_path: 数据库文件路径
        """
        self.cookie = self.clean_cookie(cookie)
        self.group_id = group_id
        self.download_dir = download_dir
        self.db_path = db_path
        self.base_url = "https://api.zsxq.com"
        
        # 反检测设置
        self.min_delay = 2.0  # 最小延迟（秒）
        self.max_delay = 5.0  # 最大延迟（秒）
        self.download_interval_min = 60  # 下载间隔最小值（1分钟）
        self.download_interval_max = 180  # 下载间隔最大值（3分钟）
        self.long_delay_interval = 5  # 每N个文件进行长休眠
        self.long_delay_min = 180  # 长休眠最小值（3分钟）
        self.long_delay_max = 300  # 长休眠最大值（5分钟）
        
        # 统计
        self.request_count = 0
        self.download_count = 0
        self.debug_mode = False
        
        # 创建session
        self.session = requests.Session()
        
        # 确保下载目录存在
        os.makedirs(download_dir, exist_ok=True)
        print(f"📁 下载目录: {os.path.abspath(download_dir)}")
        
        # 使用完整的文件数据库
        self.file_db = ZSXQFileDatabase(db_path)
        print(f"📊 完整文件数据库初始化完成: {db_path}")
    
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
    
    def get_stealth_headers(self) -> Dict[str, str]:
        """获取反检测请求头（每次调用随机化）"""
        # 更丰富的User-Agent池
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:132.0) Gecko/20100101 Firefox/132.0"
        ]
        
        # 随机选择User-Agent
        selected_ua = random.choice(user_agents)
        
        # 根据User-Agent生成对应的Sec-Ch-Ua
        if "Chrome" in selected_ua:
            if "131.0.0.0" in selected_ua:
                sec_ch_ua = '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"'
            elif "130.0.0.0" in selected_ua:
                sec_ch_ua = '"Google Chrome";v="130", "Chromium";v="130", "Not?A_Brand";v="99"'
            elif "129.0.0.0" in selected_ua:
                sec_ch_ua = '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"'
            else:
                sec_ch_ua = '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"'
        else:
            sec_ch_ua = '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"'
        
        # 随机化其他头部
        accept_languages = [
            'zh-CN,zh;q=0.9,en;q=0.8',
            'zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7',
            'zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7',
            'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2'
        ]
        
        platforms = ['"Windows"', '"macOS"', '"Linux"']
        
        # 基础头部
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': random.choice(accept_languages),
            'Accept-Encoding': 'gzip, deflate, br',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Cookie': self.cookie,
            'Host': 'api.zsxq.com',
            'Origin': 'https://wx.zsxq.com',
            'Pragma': 'no-cache',
            'Referer': f'https://wx.zsxq.com/dweb2/index/group/{self.group_id}',
            'Sec-Ch-Ua': sec_ch_ua,
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': random.choice(platforms),
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'User-Agent': selected_ua
        }
        
        # 随机添加可选头部
        optional_headers = {
            'DNT': '1',
            'Sec-GPC': '1',
            'Upgrade-Insecure-Requests': '1',
            'X-Requested-With': 'XMLHttpRequest'
        }
        
        for key, value in optional_headers.items():
            if random.random() > 0.5:  # 50%概率添加
                headers[key] = value
        
        # 随机调整时间戳相关头部
        if random.random() > 0.7:  # 30%概率添加
            headers['X-Timestamp'] = str(int(time.time()) + random.randint(-30, 30))
        
        if random.random() > 0.6:  # 40%概率添加
            headers['X-Request-Id'] = f"req-{random.randint(100000000000, 999999999999)}"
        
        return headers
    
    def smart_delay(self):
        """智能延迟"""
        delay = random.uniform(self.min_delay, self.max_delay)
        if self.debug_mode:
            print(f"   ⏱️ 延迟 {delay:.1f}秒")
        time.sleep(delay)
    
    def download_delay(self):
        """下载间隔延迟（1-3分钟）"""
        delay = random.uniform(self.download_interval_min, self.download_interval_max)
        start_time = datetime.datetime.now()
        end_time = start_time + datetime.timedelta(seconds=delay)
        
        print(f"⏳ 下载间隔: {delay:.0f}秒 ({delay/60:.1f}分钟)")
        print(f"   ⏰ 开始时间: {start_time.strftime('%H:%M:%S')}")
        print(f"   🕐 预计恢复: {end_time.strftime('%H:%M:%S')}")
        
        time.sleep(delay)
        
        actual_end_time = datetime.datetime.now()
        print(f"   🕐 实际结束: {actual_end_time.strftime('%H:%M:%S')}")
    
    def check_long_delay(self):
        """检查是否需要长休眠"""
        if self.download_count > 0 and self.download_count % self.long_delay_interval == 0:
            delay = random.uniform(self.long_delay_min, self.long_delay_max)
            start_time = datetime.datetime.now()
            end_time = start_time + datetime.timedelta(seconds=delay)
            
            print(f"🛌 长休眠开始: {delay:.0f}秒 ({delay/60:.1f}分钟)")
            print(f"   已下载 {self.download_count} 个文件，进入长休眠模式...")
            print(f"   ⏰ 开始时间: {start_time.strftime('%H:%M:%S')}")
            print(f"   🕐 预计恢复: {end_time.strftime('%H:%M:%S')}")
            
            time.sleep(delay)
            
            actual_end_time = datetime.datetime.now()
            print(f"😴 长休眠结束，继续下载...")
            print(f"   🕐 实际结束: {actual_end_time.strftime('%H:%M:%S')}")
    
    def fetch_file_list(self, count: int = 20, index: Optional[str] = None, sort: str = "by_download_count") -> Optional[Dict[str, Any]]:
        """获取文件列表（带重试机制）"""
        url = f"{self.base_url}/v2/groups/{self.group_id}/files"
        max_retries = 10
        
        params = {
            "count": str(count),
            "sort": sort
        }
        
        if index:
            params["index"] = index
        
        print(f"🌐 获取文件列表")
        print(f"   📊 参数: count={count}, sort={sort}")
        if index:
            print(f"   📑 索引: {index}")
        print(f"   🌐 请求URL: {url}")
        
        for attempt in range(max_retries):
            if attempt > 0:
                # 重试延迟：15-30秒
                retry_delay = random.uniform(15, 30)
                print(f"   🔄 第{attempt}次重试，等待{retry_delay:.1f}秒...")
                time.sleep(retry_delay)
            
            # 每次重试都获取新的请求头（包含新的User-Agent等）
            self.smart_delay()
            self.request_count += 1
            headers = self.get_stealth_headers()
            
            if attempt > 0:
                print(f"   🔄 重试#{attempt}: 使用新的User-Agent: {headers.get('User-Agent', 'N/A')[:50]}...")
            
            try:
                response = self.session.get(url, headers=headers, params=params, timeout=30)
                
                print(f"   📊 响应状态: {response.status_code}")
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        
                        # 只在第一次尝试或最后一次失败时显示完整响应
                        if attempt == 0 or attempt == max_retries - 1 or data.get('succeeded'):
                            print(f"   📋 响应内容: {json.dumps(data, ensure_ascii=False, indent=2)}")
                        
                        if data.get('succeeded'):
                            files = data.get('resp_data', {}).get('files', [])
                            next_index = data.get('resp_data', {}).get('index')
                            if attempt > 0:
                                print(f"   ✅ 重试成功！第{attempt}次重试获取到文件列表")
                            else:
                                print(f"   ✅ 获取成功: {len(files)}个文件")
                            return data
                        else:
                            error_msg = data.get('message', data.get('error', '未知错误'))
                            error_code = data.get('code', 'N/A')
                            print(f"   ❌ API返回失败: {error_msg} (代码: {error_code})")
                            
                            # 检查是否是可重试的错误
                            if error_code in [1059, 500, 502, 503, 504]:  # 内部错误、服务器错误等
                                if attempt < max_retries - 1:
                                    print(f"   🔄 检测到可重试错误，准备重试...")
                                    continue
                            else:
                                print(f"   🚫 非可重试错误，停止重试")
                                return None
                                
                    except json.JSONDecodeError as e:
                        print(f"   ❌ JSON解析失败: {e}")
                        print(f"   📄 原始响应: {response.text[:500]}...")
                        if attempt < max_retries - 1:
                            print(f"   🔄 JSON解析失败，准备重试...")
                            continue
                        
                elif response.status_code in [429, 500, 502, 503, 504]:  # 频率限制或服务器错误
                    print(f"   ❌ HTTP错误: {response.status_code}")
                    print(f"   📄 响应内容: {response.text[:200]}...")
                    if attempt < max_retries - 1:
                        print(f"   🔄 服务器错误，准备重试...")
                        continue
                else:
                    print(f"   ❌ HTTP错误: {response.status_code}")
                    print(f"   📄 响应内容: {response.text[:200]}...")
                    print(f"   🚫 非可重试HTTP错误，停止重试")
                    return None
                    
            except Exception as e:
                print(f"   ❌ 请求异常: {e}")
                if attempt < max_retries - 1:
                    print(f"   🔄 请求异常，准备重试...")
                    continue
        
        print(f"   🚫 已重试{max_retries}次，全部失败")
        return None
    
    def get_download_url(self, file_id: int) -> Optional[str]:
        """获取文件下载链接（带重试机制）
        
        注意：file_id 参数在不同场景下含义不同：
        - 边获取边下载时：传入的是真实的 file_id
        - 从数据库下载时：传入的是 topic_id
        """
        url = f"{self.base_url}/v2/files/{file_id}/download_url"
        max_retries = 10
        
        print(f"   🔗 获取下载链接: ID={file_id}")
        print(f"   🌐 请求URL: {url}")
        
        for attempt in range(max_retries):
            if attempt > 0:
                # 重试延迟：15-30秒
                retry_delay = random.uniform(15, 30)
                print(f"   🔄 第{attempt}次重试，等待{retry_delay:.1f}秒...")
                time.sleep(retry_delay)
            
            # 每次重试都获取新的请求头（包含新的User-Agent等）
            self.smart_delay()
            self.request_count += 1
            headers = self.get_stealth_headers()
            
            if attempt > 0:
                print(f"   🔄 重试#{attempt}: 使用新的User-Agent: {headers.get('User-Agent', 'N/A')[:50]}...")
            
            try:
                response = self.session.get(url, headers=headers, timeout=30)
                
                print(f"   📊 响应状态: {response.status_code}")
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        
                        # 只在第一次尝试或最后一次失败时显示完整响应
                        if attempt == 0 or attempt == max_retries - 1 or data.get('succeeded'):
                            print(f"   📋 响应内容: {json.dumps(data, ensure_ascii=False, indent=2)}")
                        
                        if data.get('succeeded'):
                            download_url = data.get('resp_data', {}).get('download_url')
                            if download_url:
                                if attempt > 0:
                                    print(f"   ✅ 重试成功！第{attempt}次重试获取到下载链接")
                                else:
                                    print(f"   ✅ 获取下载链接成功")
                                return download_url
                            else:
                                print(f"   ❌ 响应中无下载链接字段")
                        else:
                            error_msg = data.get('message', data.get('error', '未知错误'))
                            error_code = data.get('code', 'N/A')
                            print(f"   ❌ API返回失败: {error_msg} (代码: {error_code})")
                            
                            # 检查是否是可重试的错误
                            if error_code in [1059, 500, 502, 503, 504]:  # 内部错误、服务器错误等
                                if attempt < max_retries - 1:
                                    print(f"   🔄 检测到可重试错误，准备重试...")
                                    continue
                            else:
                                print(f"   🚫 非可重试错误，停止重试")
                                return None
                                
                    except json.JSONDecodeError as e:
                        print(f"   ❌ JSON解析失败: {e}")
                        print(f"   📄 原始响应: {response.text[:500]}...")
                        if attempt < max_retries - 1:
                            print(f"   🔄 JSON解析失败，准备重试...")
                            continue
                        
                elif response.status_code in [429, 500, 502, 503, 504]:  # 频率限制或服务器错误
                    print(f"   ❌ HTTP错误: {response.status_code}")
                    print(f"   📄 响应内容: {response.text[:200]}...")
                    if attempt < max_retries - 1:
                        print(f"   🔄 服务器错误，准备重试...")
                        continue
                else:
                    print(f"   ❌ HTTP错误: {response.status_code}")
                    print(f"   📄 响应内容: {response.text[:200]}...")
                    print(f"   🚫 非可重试HTTP错误，停止重试")
                    return None
                    
            except Exception as e:
                print(f"   ❌ 请求异常: {e}")
                if attempt < max_retries - 1:
                    print(f"   🔄 请求异常，准备重试...")
                    continue
        
        print(f"   🚫 已重试{max_retries}次，全部失败")
        return None
    
    def download_file(self, file_info: Dict[str, Any]) -> bool:
        """下载单个文件"""
        file_data = file_info.get('file', {})
        file_id = file_data.get('id')
        file_name = file_data.get('name', 'Unknown')
        file_size = file_data.get('size', 0)
        download_count = file_data.get('download_count', 0)
        
        print(f"📥 准备下载文件:")
        print(f"   📄 名称: {file_name}")
        print(f"   📊 大小: {file_size:,} bytes ({file_size/1024/1024:.2f} MB)")
        print(f"   📈 下载次数: {download_count}")
        
        # 清理文件名（移除非法字符）
        safe_filename = "".join(c for c in file_name if c.isalnum() or c in '._-（）()[]{}')
        if not safe_filename:
            safe_filename = f"file_{file_id}"
        
        file_path = os.path.join(self.download_dir, safe_filename)
        
        # 🚀 优化：先检查本地文件，避免无意义的API请求
        if os.path.exists(file_path):
            existing_size = os.path.getsize(file_path)
            if existing_size == file_size:
                print(f"   ✅ 文件已存在且大小匹配，跳过下载")
                return "skipped"  # 返回特殊值表示跳过
            else:
                print(f"   ⚠️ 文件已存在但大小不匹配，重新下载")
        
        # 只有在需要下载时才获取下载链接
        download_url = self.get_download_url(file_id)
        if not download_url:
            print(f"   ❌ 无法获取下载链接")
            return False
        
        try:
            # 下载文件
            print(f"   🚀 开始下载...")
            response = self.session.get(download_url, timeout=300, stream=True)
            
            if response.status_code == 200:
                total_size = int(response.headers.get('content-length', 0))
                downloaded_size = 0
                
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded_size += len(chunk)
                            
                            # 显示进度（每10MB显示一次）
                            if downloaded_size % (10 * 1024 * 1024) == 0 or downloaded_size == total_size:
                                if total_size > 0:
                                    progress = (downloaded_size / total_size) * 100
                                    print(f"   📊 进度: {progress:.1f}% ({downloaded_size:,}/{total_size:,} bytes)")
                                else:
                                    print(f"   📊 已下载: {downloaded_size:,} bytes")
                
                # 验证文件大小
                final_size = os.path.getsize(file_path)
                if file_size > 0 and final_size != file_size:
                    print(f"   ⚠️ 文件大小不匹配: 预期{file_size:,}, 实际{final_size:,}")
                
                print(f"   ✅ 下载完成: {safe_filename}")
                print(f"   💾 保存路径: {file_path}")
                
                self.download_count += 1
                return True
            else:
                print(f"   ❌ 下载失败: HTTP {response.status_code}")
                return False
                
        except Exception as e:
            print(f"   ❌ 下载异常: {e}")
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"   🗑️ 删除不完整文件")
            return False
    
    def download_files_batch(self, max_files: Optional[int] = None, start_index: Optional[str] = None) -> Dict[str, int]:
        """批量下载文件"""
        if max_files is None:
            print(f"\n📥 开始无限下载文件 (直到没有更多文件)")
        else:
            print(f"\n📥 开始批量下载文件 (最多{max_files}个)")
        
        stats = {'total_files': 0, 'downloaded': 0, 'skipped': 0, 'failed': 0}
        current_index = start_index
        downloaded_in_batch = 0
        
        while max_files is None or downloaded_in_batch < max_files:
            # 获取文件列表
            data = self.fetch_file_list(count=20, index=current_index)
            if not data:
                print("❌ 获取文件列表失败")
                break
            
            files = data.get('resp_data', {}).get('files', [])
            next_index = data.get('resp_data', {}).get('index')
            
            if not files:
                print("📭 没有更多文件")
                break
            
            print(f"\n📋 当前批次: {len(files)} 个文件")
            
            for i, file_info in enumerate(files):
                if max_files is not None and downloaded_in_batch >= max_files:
                    break
                
                file_data = file_info.get('file', {})
                file_name = file_data.get('name', 'Unknown')
                
                if max_files is None:
                    print(f"\n【第{downloaded_in_batch + 1}个文件】{file_name}")
                else:
                    print(f"\n【{downloaded_in_batch + 1}/{max_files}】{file_name}")
                
                # 下载文件
                result = self.download_file(file_info)
                
                if result == "skipped":
                    stats['skipped'] += 1
                    print(f"   ⚠️ 文件已跳过，继续下一个")
                elif result:
                    stats['downloaded'] += 1
                    downloaded_in_batch += 1
                    
                    # 检查长休眠
                    self.check_long_delay()
                    
                    # 如果不是最后一个文件，进行下载间隔
                    has_more_in_batch = (i + 1) < len(files)
                    not_reached_limit = max_files is None or downloaded_in_batch < max_files
                    if has_more_in_batch and not_reached_limit:
                        self.download_delay()
                else:
                    stats['failed'] += 1
                
                stats['total_files'] += 1
            
            # 准备下一页
            should_continue = max_files is None or downloaded_in_batch < max_files
            if next_index and should_continue:
                current_index = next_index
                print(f"\n📄 准备获取下一页: {next_index}")
                time.sleep(2)  # 页面间短暂延迟
            else:
                break
        
        print(f"\n🎉 批量下载完成:")
        print(f"   📊 总文件数: {stats['total_files']}")
        print(f"   ✅ 下载成功: {stats['downloaded']}")
        print(f"   ⚠️ 跳过: {stats['skipped']}")
        print(f"   ❌ 失败: {stats['failed']}")
        
        return stats
    
    def show_file_list(self, count: int = 20, index: Optional[str] = None) -> Optional[str]:
        """显示文件列表"""
        data = self.fetch_file_list(count=count, index=index)
        if not data:
            return None
        
        files = data.get('resp_data', {}).get('files', [])
        next_index = data.get('resp_data', {}).get('index')
        
        print(f"\n📋 文件列表 ({len(files)} 个文件):")
        print("="*80)
        
        for i, file_info in enumerate(files, 1):
            file_data = file_info.get('file', {})
            topic_data = file_info.get('topic', {})
            
            file_name = file_data.get('name', 'Unknown')
            file_size = file_data.get('size', 0)
            download_count = file_data.get('download_count', 0)
            create_time = file_data.get('create_time', 'Unknown')
            
            topic_title = topic_data.get('talk', {}).get('text', '')[:50] if topic_data.get('talk') else ''
            
            print(f"{i:2d}. 📄 {file_name}")
            print(f"    📊 大小: {file_size:,} bytes ({file_size/1024/1024:.2f} MB)")
            print(f"    📈 下载: {download_count} 次")
            print(f"    ⏰ 时间: {create_time}")
            if topic_title:
                print(f"    💬 话题: {topic_title}...")
            print()
        
        if next_index:
            print(f"📑 下一页索引: {next_index}")
        else:
            print("📭 没有更多文件")
        
        return next_index
    
    def collect_all_files_to_database(self) -> Dict[str, int]:
        """收集所有文件信息到数据库"""
        print(f"\n📊 开始收集文件列表到数据库...")
        
        # 创建收集记录
        self.file_db.cursor.execute("INSERT INTO collection_log (start_time) VALUES (?)", 
                      (datetime.datetime.now().isoformat(),))
        log_id = self.file_db.cursor.lastrowid
        self.file_db.conn.commit()
        
        stats = {'total_files': 0, 'new_files': 0, 'skipped_files': 0}
        current_index = None
        page_count = 0
        
        try:
            while True:
                page_count += 1
                print(f"\n📄 收集第{page_count}页文件列表...")
                
                # 获取文件列表
                data = self.fetch_file_list(count=20, index=current_index)
                if not data:
                    print(f"❌ 第{page_count}页获取失败，收集过程中断")
                    print(f"💾 已成功收集前{page_count-1}页的数据")
                    break
                
                files = data.get('resp_data', {}).get('files', [])
                next_index = data.get('resp_data', {}).get('index')
                
                if not files:
                    print("📭 没有更多文件")
                    break
                
                print(f"   📋 当前页面: {len(files)} 个文件")
                
                # 使用完整数据库导入整个API响应
                try:
                    page_stats = self.file_db.import_file_response(data)
                    
                    stats['new_files'] += page_stats.get('files', 0)
                    stats['total_files'] += len(files)
                    
                    print(f"      ✅ 新增文件: {page_stats.get('files', 0)}")
                    print(f"      📊 其他数据: 话题+{page_stats.get('topics', 0)}, 用户+{page_stats.get('users', 0)}")
                    
                except Exception as e:
                    print(f"   ❌ 第{page_count}页存储失败: {e}")
                    continue
                
                print(f"   ✅ 第{page_count}页存储完成")
                
                # 准备下一页
                if next_index:
                    current_index = next_index
                    # 页面间短暂延迟
                    time.sleep(random.uniform(2, 5))
                else:
                    break
                    
        except KeyboardInterrupt:
            print(f"\n⏹️ 用户中断收集")
        except Exception as e:
            print(f"\n❌ 收集过程异常: {e}")
        
        # 更新收集记录
        self.file_db.cursor.execute('''
            UPDATE collection_log SET 
                end_time = ?, total_files = ?, new_files = ?, status = 'completed'
            WHERE id = ?
        ''', (datetime.datetime.now().isoformat(), stats['total_files'], 
              stats['new_files'], log_id))
        self.file_db.conn.commit()
        
        print(f"\n🎉 文件列表收集完成:")
        print(f"   📊 处理文件数: {stats['total_files']}")
        print(f"   ✅ 新增文件: {stats['new_files']}")
        print(f"   ⚠️ 跳过重复: {stats.get('skipped_files', 0)}")
        print(f"   📄 收集页数: {page_count}")
        
        return stats
    
    def get_database_time_range(self) -> Dict[str, Any]:
        """获取完整数据库中文件的时间范围信息"""
        # 使用新数据库检查是否有数据
        stats = self.file_db.get_database_stats()
        total_files = stats.get('files', 0)
        
        if total_files == 0:
            return {'has_data': False, 'total_files': 0}
        
        # 获取时间范围
        self.file_db.cursor.execute('''
            SELECT MIN(create_time) as oldest_time, 
                   MAX(create_time) as newest_time,
                   COUNT(*) as total_count
            FROM files 
            WHERE create_time IS NOT NULL AND create_time != ''
        ''')
        
        result = self.file_db.cursor.fetchone()
        
        return {
            'has_data': True,
            'total_files': total_files,
            'oldest_time': result[0] if result else None,
            'newest_time': result[1] if result else None,
            'time_based_count': result[2] if result else 0
        }
    
    def collect_files_by_time(self, sort: str = "by_create_time", start_time: Optional[str] = None) -> Dict[str, int]:
        """按时间顺序收集文件列表到数据库（使用完整的数据库结构）"""
        print(f"\n📊 开始按时间顺序收集文件列表到完整数据库...")
        print(f"   📅 排序方式: {sort}")
        if start_time:
            print(f"   ⏰ 起始时间: {start_time}")
        
        # 使用完整数据库的统计信息
        initial_stats = self.file_db.get_database_stats()
        initial_files = initial_stats.get('files', 0)
        print(f"   📊 数据库初始状态: {initial_files} 个文件")
        
        total_imported_stats = {
            'files': 0, 'topics': 0, 'users': 0, 'groups': 0,
            'images': 0, 'comments': 0, 'likes': 0, 'columns': 0, 'solutions': 0
        }
        current_index = start_time  # 使用时间戳作为index
        page_count = 0
        
        try:
            while True:
                page_count += 1
                print(f"\n📄 收集第{page_count}页文件列表...")
                
                # 获取文件列表（按时间排序）
                data = self.fetch_file_list(count=20, index=current_index, sort=sort)
                if not data:
                    print(f"❌ 第{page_count}页获取失败，收集过程中断")
                    print(f"💾 已成功收集前{page_count-1}页的数据")
                    break
                
                files = data.get('resp_data', {}).get('files', [])
                next_index = data.get('resp_data', {}).get('index')
                
                if not files:
                    print("📭 没有更多文件")
                    break
                
                print(f"   📋 当前页面: {len(files)} 个文件")
                
                # 使用完整数据库导入整个API响应
                try:
                    page_stats = self.file_db.import_file_response(data)
                    
                    # 累计统计
                    for key in total_imported_stats:
                        total_imported_stats[key] += page_stats.get(key, 0)
                    
                    print(f"   ✅ 第{page_count}页存储完成: 文件+{page_stats.get('files', 0)}, 话题+{page_stats.get('topics', 0)}")
                    print(f"      用户+{page_stats.get('users', 0)}, 群组+{page_stats.get('groups', 0)}, 图片+{page_stats.get('images', 0)}")
                    
                except Exception as e:
                    print(f"   ❌ 第{page_count}页存储失败: {e}")
                    continue
                
                # 准备下一页
                if next_index:
                    current_index = next_index
                    print(f"   ⏭️ 下一页时间戳: {current_index}")
                    # 页面间短暂延迟
                    time.sleep(random.uniform(2, 5))
                else:
                    print("📭 已到达最后一页")
                    break
                    
        except KeyboardInterrupt:
            print(f"\n⏹️ 用户中断收集")
        except Exception as e:
            print(f"\n❌ 收集过程异常: {e}")
        
        # 最终统计
        final_stats = self.file_db.get_database_stats()
        final_files = final_stats.get('files', 0)
        new_files = final_files - initial_files
        
        print(f"\n🎉 完整文件列表收集完成:")
        print(f"   📊 处理页数: {page_count}")
        print(f"   📁 新增文件: {new_files} (总计: {final_files})")
        print(f"   📋 累计导入统计:")
        for key, value in total_imported_stats.items():
            if value > 0:
                print(f"      {key}: +{value}")
        
        print(f"\n📊 当前数据库状态:")
        for table, count in final_stats.items():
            if count > 0:
                print(f"   {table}: {count}")
        
        return {
            'total_files': final_files,
            'new_files': new_files,
            'pages': page_count,
            **total_imported_stats
        }
    
    def collect_incremental_files(self) -> Dict[str, int]:
        """增量收集：从数据库最老时间戳开始继续收集"""
        print(f"\n🔄 开始增量文件收集...")
        
        # 获取数据库时间范围
        time_info = self.get_database_time_range()
        
        if not time_info['has_data']:
            print("📊 数据库为空，将进行全量收集")
            return self.collect_files_by_time()
        
        oldest_time = time_info['oldest_time']
        newest_time = time_info['newest_time']
        total_files = time_info['total_files']
        
        print(f"📊 数据库现状:")
        print(f"   现有文件数: {total_files}")
        print(f"   最老时间: {oldest_time}")
        print(f"   最新时间: {newest_time}")
        
        if not oldest_time:
            print("⚠️ 数据库中没有有效的时间信息，进行全量收集")
            return self.collect_files_by_time()
        
        # 从最老时间戳开始收集更早的文件
        print(f"🎯 将从最老时间戳开始收集更早的文件...")
        
        # 将时间戳转换为毫秒数用作index
        try:
            if '+' in oldest_time:
                # 处理带时区的时间戳
                from datetime import datetime
                dt = datetime.fromisoformat(oldest_time.replace('+0800', '+08:00'))
                timestamp_ms = int(dt.timestamp() * 1000)
            else:
                # 如果已经是毫秒时间戳
                timestamp_ms = int(oldest_time)
            
            start_index = str(timestamp_ms)
            print(f"🚀 增量收集起始时间戳: {start_index}")
            
            return self.collect_files_by_time(start_time=start_index)
            
        except Exception as e:
            print(f"⚠️ 时间戳处理失败: {e}")
            print("🔄 改为全量收集")
            return self.collect_files_by_time()
    
    def download_files_from_database(self, max_files: Optional[int] = None, status_filter: str = 'pending') -> Dict[str, int]:
        """从完整数据库下载文件（使用file_id字段）"""
        print(f"\n📥 开始从完整数据库下载文件...")
        if max_files:
            print(f"   🎯 下载限制: {max_files}个文件")
        print(f"   🔍 状态筛选: {status_filter} (注意: 新数据库暂无下载状态，默认为pending)")
        
        # 从完整数据库获取文件列表
        if max_files:
            self.file_db.cursor.execute('''
                SELECT file_id, name, size, download_count, create_time 
                FROM files 
                ORDER BY download_count DESC, size ASC 
                LIMIT ?
            ''', (max_files,))
        else:
            self.file_db.cursor.execute('''
                SELECT file_id, name, size, download_count, create_time 
                FROM files 
                ORDER BY download_count DESC, size ASC
            ''')
        
        files_to_download = self.file_db.cursor.fetchall()
        
        if not files_to_download:
            print(f"📭 数据库中没有文件可下载")
            return {'total_files': 0, 'downloaded': 0, 'skipped': 0, 'failed': 0}
        
        print(f"📋 找到 {len(files_to_download)} 个待下载文件")
        
        stats = {'total_files': len(files_to_download), 'downloaded': 0, 'skipped': 0, 'failed': 0}
        
        for i, (file_id, file_name, file_size, download_count, create_time) in enumerate(files_to_download, 1):
            try:
                print(f"\n【{i}/{len(files_to_download)}】{file_name}")
                print(f"   📊 文件ID: {file_id}, 大小: {file_size/1024:.1f}KB, 下载次数: {download_count}")
                
                # 构造文件信息结构（使用正确的file_id）
                file_info = {
                    'file': {
                        'id': file_id,  # 使用正确的file_id
                        'name': file_name,
                        'size': file_size,
                        'download_count': download_count
                    }
                }
                
                # 下载文件
                result = self.download_file(file_info)
                
                if result == "skipped":
                    stats['skipped'] += 1
                    print(f"   ⚠️ 文件已跳过")
                elif result:
                    stats['downloaded'] += 1
                    
                    # 检查长休眠
                    self.check_long_delay()
                    
                    # 如果不是最后一个文件，进行下载间隔
                    if i < len(files_to_download):
                        self.download_delay()
                else:
                    stats['failed'] += 1
                    print(f"   ❌ 下载失败")
                
            except KeyboardInterrupt:
                print(f"\n⏹️ 用户中断下载")
                break
            except Exception as e:
                print(f"   ❌ 处理文件异常: {e}")
                stats['failed'] += 1
                continue
        
        print(f"\n🎉 数据库下载完成:")
        print(f"   📊 总文件数: {stats['total_files']}")
        print(f"   ✅ 下载成功: {stats['downloaded']}")
        print(f"   ⚠️ 跳过: {stats['skipped']}")
        print(f"   ❌ 失败: {stats['failed']}")
        
        return stats
    
    def show_database_stats(self):
        """显示完整数据库统计信息"""
        print(f"\n📊 完整数据库统计信息:")
        print("="*60)
        print(f"📁 数据库文件: {self.db_path}")
        
        # 使用新数据库的统计方法
        stats = self.file_db.get_database_stats()
        
        # 主要数据统计
        total_files = stats.get('files', 0)
        total_topics = stats.get('topics', 0)
        total_users = stats.get('users', 0)
        total_groups = stats.get('groups', 0)
        
        print(f"📈 核心数据:")
        print(f"   📄 文件数量: {total_files:,}")
        print(f"   💬 话题数量: {total_topics:,}")
        print(f"   👥 用户数量: {total_users:,}")
        print(f"   🏠 群组数量: {total_groups:,}")
        
        # 文件大小统计
        self.file_db.cursor.execute("SELECT SUM(size) FROM files WHERE size IS NOT NULL")
        result = self.file_db.cursor.fetchone()
        total_size = result[0] if result and result[0] else 0
        
        if total_size > 0:
            print(f"💾 总文件大小: {total_size/1024/1024:.2f} MB")
        
        # 详细表统计
        print(f"\n📋 详细表统计:")
        for table_name, count in stats.items():
            if count > 0:
                # 添加表情符号
                emoji_map = {
                    'files': '📄', 'groups': '🏠', 'users': '👥', 'topics': '💬',
                    'talks': '💭', 'images': '🖼️', 'topic_files': '📎',
                    'latest_likes': '👍', 'comments': '💬', 'like_emojis': '😊',
                    'user_liked_emojis': '❤️', 'columns': '📚', 'topic_columns': '🔗',
                    'solutions': '💡', 'solution_files': '📋', 'file_topic_relations': '🔗',
                    'api_responses': '📡'
                }
                emoji = emoji_map.get(table_name, '📊')
                print(f"   {emoji} {table_name}: {count:,}")
        
        # 文件创建时间范围
        self.file_db.cursor.execute('''
            SELECT MIN(create_time), MAX(create_time), COUNT(*) 
            FROM files 
            WHERE create_time IS NOT NULL
        ''')
        time_result = self.file_db.cursor.fetchone()
        
        if time_result and time_result[2] > 0:
            min_time, max_time, time_count = time_result
            print(f"\n⏰ 文件时间范围:")
            print(f"   最早文件: {min_time}")
            print(f"   最新文件: {max_time}")
            print(f"   有时间信息的文件: {time_count:,}")
        
        # API响应统计
        self.file_db.cursor.execute('''
            SELECT succeeded, COUNT(*) 
            FROM api_responses 
            GROUP BY succeeded
        ''')
        api_stats = self.file_db.cursor.fetchall()
        
        if api_stats:
            print(f"\n📡 API响应统计:")
            for succeeded, count in api_stats:
                status = "成功" if succeeded else "失败"
                emoji = "✅" if succeeded else "❌"
                print(f"   {emoji} {status}: {count:,}")
        
        print("="*60)
    
    def adjust_settings(self):
        """调整下载设置"""
        print(f"\n🔧 当前下载设置:")
        print(f"   下载间隔: {self.download_interval_min}-{self.download_interval_max}秒 ({self.download_interval_min/60:.1f}-{self.download_interval_max/60:.1f}分钟)")
        print(f"   长休眠间隔: 每{self.long_delay_interval}个文件")
        print(f"   长休眠时间: {self.long_delay_min}-{self.long_delay_max}秒 ({self.long_delay_min/60:.1f}-{self.long_delay_max/60:.1f}分钟)")
        print(f"   下载目录: {self.download_dir}")
        
        try:
            new_interval = int(input(f"长休眠间隔 (当前每{self.long_delay_interval}个文件): ") or self.long_delay_interval)
            new_dir = input(f"下载目录 (当前: {self.download_dir}): ").strip() or self.download_dir
            
            self.long_delay_interval = max(new_interval, 1)
            
            if new_dir != self.download_dir:
                self.download_dir = new_dir
                os.makedirs(new_dir, exist_ok=True)
                print(f"📁 下载目录已更新: {os.path.abspath(new_dir)}")
            
            print(f"✅ 设置已更新")
            
        except ValueError:
            print("❌ 输入无效，保持原设置")
    
    def close(self):
        """关闭资源"""
        if hasattr(self, 'file_db') and self.file_db:
            self.file_db.close()
            print("🔒 文件数据库连接已关闭") 