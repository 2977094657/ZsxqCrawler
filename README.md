<div align="center">
  <img src="images/_Image.png" alt="知识星球数据采集器" width="200">
  <h1>知识星球数据采集器</h1>
  <p>知识星球内容爬取与文件下载工具，支持话题采集、文件批量下载等功能</p>
  <p>如需定制功能，请联系 QQ：2977094657</p>
  
  [![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/downloads/) [![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE) [![Platform](https://img.shields.io/badge/Platform-Windows | Linux | macOS-lightgrey.svg)]()
  
  <img src="images/info.png" alt="群组详情页面" height="400">
</div>

## 项目特性

- **智能采集**: 支持全量、增量、智能更新等多种采集模式
- **文件管理**: 自动下载和管理知识星球中的文件资源，支持直接下载
- **导入导出**: 支持单社群或全部数据打包备份、导入预览、冲突检查与离线浏览
- **命令行界面**: 提供交互式命令行工具
- **Web 界面**: 现代化的 React 前端界面，操作直观

## 界面展示

### Web 界面

<div align="center">
  <img src="images/home.png" alt="首页界面" height="400">
  <p><em>首页 - 群组选择和概览</em></p>
</div>

| 配置页面 | 日志页面 |
| --- | --- |
| <img src="images/config.png" alt="配置页面" height="300"> | <img src="images/log.png" alt="日志页面" height="300"> |
| <em>配置页面 - 爬取间隔设置</em> | <em>日志页面 - 实时任务执行日志</em> |

<div align="center">
  <img src="images/column.png" alt="专栏文章页面" height="400">
  <p><em>专栏文章页面 - 专栏目录浏览、文章内容展示与视频下载</em></p>
</div>

<div align="center">
  <img src="images/import.png" alt="导入数据包页面" height="400">
  <p><em>导入数据包 - 导出信息预览、社群列表和冲突检查</em></p>
</div>

## 快速开始

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

在使用工具前，需要获取知识星球的 **Cookie**（无需再手动填写群组ID）：

1. **获取Cookie**:
   - 使用浏览器登录知识星球
   - 按 `F12` 打开开发者工具
   - 切换到 `Network` 标签
   - 刷新页面，找到任意API请求
   - 复制请求头中的 `Cookie` 值

2. **首次使用**：
   - 启动 Web 界面后，在“配置认证信息/账号管理”中粘贴 Cookie 完成登录
   - 后端会根据该账号自动获取您加入的全部星球，前端选择不同星球时会将对应的群组ID动态传入后端进行抓取

### 3. 运行应用

#### 方式一：Web界面（推荐）

```bash
# 1. 启动后端API服务
uv run main.py

# 2. 启动前端服务（新开终端窗口）
cd frontend
npm run dev
```

如果前后端不在同一台机器/容器中，前端默认请求 `http://localhost:8208` 会导致 `Failed to fetch`，请在 `frontend/.env.local` 中配置后端地址（示例）：

```bash
NEXT_PUBLIC_API_BASE_URL=http://192.168.x.x:8208
```

然后访问：
- **Web 界面**: http://localhost:3060
- **API 文档**: http://localhost:8208/docs

#### 方式二：命令行工具

```bash
# 运行交互式命令行工具
uv run -m backend.zsxq_interactive_crawler
```

<div align="center">
  <img src="images/QQ20250703-170055.png" alt="命令行界面" height="400">
  <p><em>命令行界面 - 交互式操作控制台</em></p>
</div>

## 数据存储与下载路径

默认情况下，所有数据都会保存到**项目根目录**下的 `output/databases` 目录中（项目根目录即与 `config.toml` 同级的目录），不同群组会按照 `group_id` 分目录存放。

- **话题 / 文章内容数据库**: `output/databases/{group_id}/zsxq_topics_{group_id}.db`  
  - 保存所有话题、文章正文、评论等结构化数据（Web 界面展示内容都来自这里）。
- **文件列表数据库**: `output/databases/{group_id}/zsxq_files_{group_id}.db`  
  - 保存文件元数据（文件名、大小、下载次数等），用于文件面板和下载任务管理。
- **已下载附件 / 文件**: `output/databases/{group_id}/downloads/`  
  - 通过 Web 界面或命令行触发的文件下载，实际都会保存在这里。  
  - 例如当前示例配置中，群组 `88851415151812` 的文件路径为：`output/databases/88851415151812/downloads/`。
- **图片缓存（可安全删除）**: `output/databases/{group_id}/images/`  
  - 用于话题图片预览的本地缓存，如被删除，后续访问时会自动重新生成。


## 项目结构

```text
.
├── backend/                 # FastAPI 后端、爬虫核心、数据库与导出工具
├── frontend/                # Next.js Web 前端
├── scripts/                 # 一次性迁移和维护脚本
├── docs/                    # 迁移说明等补充文档
├── images/                  # README 和界面展示截图
├── output/                  # 运行时数据目录（默认忽略提交）
├── main.py                  # 后端兼容启动入口，实际应用位于 backend.main
├── pyproject.toml           # Python 项目配置
└── README.md
```

后端业务代码已统一收敛到 `backend/` 包中。为了兼容旧启动方式，根目录保留 `main.py`，仍可使用 `uv run main.py` 启动服务。

## 数据导出与导入

Web 首页支持按社群或整体备份本地数据：

- **单社群导出**: 每个社群卡片上的“导出”按钮会将该社群本地文件夹打包为 zip。
- **全部导出**: 首页顶部“全部导出”按钮会将项目根目录下的 `output` 文件夹整体打包为 zip。
- **导入**: 首页顶部“导入”按钮选择 zip 后，会先读取压缩包根目录的 `manifest.json` 并弹窗展示导出时间、数据大小和社群列表，确认后再导入。
- **导入预览统计**: 社群卡片底部依次展示成员数、话题数和文件数；话题/文件数量会优先根据导入包内 SQLite 数据库重新计算，避免旧导出包的 `manifest.json` 统计缺失时显示为 0。
- **离线浏览**: 导入后的社群数据可在未登录、未配置 Cookie 的情况下进入社群浏览本地话题、标签和文件列表；采集、刷新、下载等联网操作仍需要配置 Cookie。
- **冲突策略**: 如果导入包中的社群本地文件夹已存在，系统会拒绝导入并提示先删除已有本地数据，不会覆盖现有数据。

导出的 zip 根目录会包含 `manifest.json`，用于记录：

- 导出类型（单社群或全部 `output`）
- 导出时间
- 导出数据大小
- 社群 ID、名称、类型、封面、群主、统计信息等元数据

## 贡献指南

欢迎提交Issue和Pull Request！

## 许可证

本项目采用 [MIT License](LICENSE) 开源协议。

## 免责声明

本工具仅供学习和研究使用，请遵守知识星球的服务条款和相关法律法规。使用本工具产生的任何后果由使用者自行承担。

---

<div align="center">
  <p>如果这个项目对你有帮助，请给个 Star 支持一下。</p>
</div>
