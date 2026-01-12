"""
企业微信Webhook推送模块
用于在爬取到新内容后推送通知
"""
import requests
import json
import re
from typing import Dict, List, Optional
from html.parser import HTMLParser


class HTMLTagRemover(HTMLParser):
    """HTML标签清理器"""
    def __init__(self):
        super().__init__()
        self.result = []
    
    def handle_data(self, data):
        self.result.append(data)
    
    def get_text(self):
        return ''.join(self.result)


def clean_html_tags(text: str) -> str:
    """
    清理HTML标签，返回纯文本
    
    Args:
        text: 包含HTML标签的文本
        
    Returns:
        清理后的纯文本
    """
    if not text:
        return ""
    
    # 方法1: 使用HTMLParser清理
    try:
        parser = HTMLTagRemover()
        parser.feed(text)
        text = parser.get_text()
    except:
        # 如果HTMLParser失败，使用正则表达式
        pass
    
    # 方法2: 使用正则表达式清理剩余的HTML标签
    # 匹配所有HTML标签: <tag> 或 <tag />
    text = re.sub(r'<[^>]+>', '', text)
    
    # 清理HTML实体（如 &nbsp; 等）
    html_entities = {
        '&nbsp;': ' ',
        '&lt;': '<',
        '&gt;': '>',
        '&amp;': '&',
        '&quot;': '"',
        '&apos;': "'"
    }
    for entity, char in html_entities.items():
        text = text.replace(entity, char)
    
    # 清理URL编码的字符（如 %EF%BC%8C）
    try:
        # 解析URL编码
        from urllib.parse import unquote
        text = unquote(text)
    except:
        pass
    
    # 清理多余的空白字符
    text = re.sub(r' +', ' ', text)
    text = text.strip()
    
    return text


class WeComWebhook:
    """企业微信机器人Webhook推送类"""
    
    def __init__(self, webhook_url: str, enabled: bool = True):
        """
        初始化企业微信Webhook
        
        Args:
            webhook_url: 企业微信机器人webhook地址
            enabled: 是否启用推送
        """
        self.webhook_url = webhook_url
        self.enabled = enabled
        self.session = requests.Session()
    
    def send_text(self, content: str, mentioned_list: Optional[List[str]] = None) -> bool:
        """
        发送文本消息
        
        Args:
            content: 消息内容
            mentioned_list: @的用户列表（手机号），可选
            
        Returns:
            是否发送成功
        """
        if not self.enabled:
            return False
        
        if not self.webhook_url:
            print("⚠️ 企业微信webhook地址未配置")
            return False
        
        try:
            data = {
                "msgtype": "text",
                "text": {
                    "content": content
                }
            }
            
            if mentioned_list:
                data["text"]["mentioned_list"] = mentioned_list
            
            response = self.session.post(
                self.webhook_url,
                json=data,
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            
            result = response.json()
            if result.get("errcode") == 0:
                print("✅ 企业微信消息发送成功")
                return True
            else:
                print(f"❌ 企业微信消息发送失败: {result.get('errmsg')}")
                return False
                
        except Exception as e:
            print(f"❌ 企业微信消息发送异常: {e}")
            return False
    
    def send_markdown(self, content: str) -> bool:
        """
        发送markdown格式消息
        
        Args:
            content: markdown格式的内容
            
        Returns:
            是否发送成功
        """
        if not self.enabled:
            return False
        
        if not self.webhook_url:
            print("⚠️ 企业微信webhook地址未配置")
            return False
        
        try:
            data = {
                "msgtype": "markdown",
                "markdown": {
                    "content": content
                }
            }
            
            response = self.session.post(
                self.webhook_url,
                json=data,
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            
            result = response.json()
            if result.get("errcode") == 0:
                print("✅ 企业微信消息发送成功")
                return True
            else:
                print(f"❌ 企业微信消息发送失败: {result.get('errmsg')}")
                return False
                
        except Exception as e:
            print(f"❌ 企业微信消息发送异常: {e}")
            return False
    
    def send_new_topics_notification(self, new_topics: List[Dict], stats: Dict) -> bool:
        """
        发送新话题推送通知
        
        Args:
            new_topics: 新话题列表
            stats: 统计信息
            
        Returns:
            是否发送成功
        """
        if not self.enabled or not new_topics:
            return False
        
        # ✅ 限制最多推送10个话题
        topics_to_send = new_topics[:10]
        if len(new_topics) > 10:
            print(f"⚠️ 话题数量超过10个，只推送前10个（总共{len(new_topics)}个）")
        
        success_count = 0
        
        # 添加前10个新话题的预览，为每个话题单独推送
        for i, topic in enumerate(topics_to_send, 1):
            try:
                title = topic.get('title', '无标题')
                # 内容在 talk.text 字段中
                talk = topic.get('talk', {})
                content = talk.get('text', '无内容')
                create_time = topic.get('create_time', '未知时间')
                # 作者信息在 talk.owner.name 字段中
                owner = talk.get('owner', {})
                author_name = owner.get('name', '六便士')
                
                # ✅ 清理HTML标签
                title = clean_html_tags(title)
                content = clean_html_tags(content)
        
                # 构建markdown消息
                lines = [
                    "# 📣 大佳新内容通知",
                    "",
                    f"## {title}",
                    "",
                    f"👤 作者: {author_name}",
                    f"⏰ 时间: {create_time}",
                    "",
                    f"📄 内容:",
                    f"{content}",
                    "",
                    "---",
                    f"*🤖 本内容由六便士整理推送 - 第{i}/{len(new_topics)}条*"
                ]
            
                markdown_content = "\n".join(lines)
                
                # 发送消息
                if self.send_markdown(markdown_content):
                    success_count += 1
                    print(f"✅ 第{i}/{len(new_topics)}条推送成功")
                else:
                    print(f"❌ 第{i}/{len(new_topics)}条推送失败")
                
                # 如果不是最后一条，延迟1秒避免频率限制
                if i < len(new_topics):
                    import time
                    time.sleep(3)
            
            except Exception as e:
                print(f"❌ 第{i}条推送异常: {e}")
        
        # ✅ 删除总结打印
        return success_count == len(topics_to_send)
