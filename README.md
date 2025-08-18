<div align="center">
  <img src="images/_Image.png" alt="知识星球数据采集器" width="200">
  <h1>🌟 知识星球数据采集器</h1>
  <p>知识星球内容爬取与文件下载工具，支持话题采集、文件批量下载等功能</p>
  
  [![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/downloads/)
  [![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
  [![Platform](https://img.shields.io/badge/Platform-Windows%20%7C%20Linux%20%7C%20macOS-lightgrey.svg)]()
  
  <img src="images/info.png" alt="群组详情页面" height="400">
</div>

## ✨ 项目特性

- 🎯 **智能采集**: 支持全量、增量、智能更新等多种采集模式
- 📁 **文件管理**: 自动下载和管理知识星球中的文件资源，支持直接下载
- 💻 **命令行界面**: 提供强大的交互式命令行工具
- 🌐 **Web界面**: 现代化的React前端界面，操作更直观

## 🖼️ 界面展示

### Web界面

<div align="center">
  <img src="images/home.png" alt="首页界面" height="400">
  <p><em>首页 - 群组选择和概览</em></p>
</div>

<div align="center">
  <img src="images/config.png" alt="配置页面" height="400">
  <p><em>配置页面 - 爬取间隔设置</em></p>
</div>

<div align="center">
  <img src="images/log.png" alt="日志页面" height="400">
  <p><em>日志页面 - 实时任务执行日志</em></p>
</div>

## 🚀 快速开始

### 1. 安装部署

```bash
# 1. 克隆项目
git clone https://github.com/2977094657/ZsxqCrawler.git
cd ZsxqCrawler

# 2. 安装uv包管理器（推荐）
pip install uv

# 3. 安装依赖
uv sync
```

### 2. 获取认证信息

在使用工具前，需要获取知识星球的Cookie和群组ID：

1. **获取Cookie**:
   - 使用浏览器登录知识星球
   - 按 `F12` 打开开发者工具
   - 切换到 `Network` 标签
   - 刷新页面，找到任意API请求
   - 复制请求头中的 `Cookie` 值

2. **获取群组ID**:
   - 访问目标知识星球页面
   - URL格式：`https://wx.zsxq.com/group/{群组ID}`
   - 从URL中提取群组ID

3. **首次使用**：
   - 编辑 `config.toml` 文件，填入您的配置信息
   - 或者启动Web界面后按照提示进行配置

### 3. 运行应用

#### 方式一：Web界面（推荐）

```bash
# 1. 启动后端API服务
uv run main.py

# 2. 启动前端服务（新开终端窗口）
cd frontend
npm run dev
```

然后访问：
- 🌐 **Web界面**: http://localhost:3000
- 📖 **API文档**: http://localhost:8000/docs

#### 方式二：命令行工具

```bash
# 运行交互式命令行工具
uv run zsxq_interactive_crawler.py
```

<div align="center">
  <img src="images/QQ20250703-170055.png" alt="命令行界面" height="400">
  <p><em>命令行界面 - 交互式操作控制台</em></p>
</div>

## 🤝 贡献指南

欢迎提交Issue和Pull Request！

## 📄 许可证

本项目采用 [MIT License](LICENSE) 开源协议。

## ⚠️ 免责声明

本工具仅供学习和研究使用，请遵守知识星球的服务条款和相关法律法规。使用本工具产生的任何后果由使用者自行承担。

---

<div align="center">
  <p>如果这个项目对你有帮助，请给个 ⭐ Star 支持一下！</p>
</div>