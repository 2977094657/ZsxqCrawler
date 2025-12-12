# 账号系统迁移到SQL - 完成说明

## 完成的工作

### 1. 创建了SQL账号管理器
- 文件：`accounts_sql_manager.py`
- 功能：
  - 使用SQLite存储账号信息
  - 支持多账号管理
  - 支持默认账号设置
  - 支持群组账号分配
  - 自动迁移现有数据库表结构

### 2. 更新了后端API
- 文件：`main.py`
- 修改内容：
  - 移除了对`accounts_manager`的依赖，改用`accounts_sql_manager`
  - 移除了虚拟"default"账号的逻辑
  - 更新了所有账号相关API使用SQL管理器
  - 错误信息改为英文

### 3. 更新了前端界面
- 文件：`frontend/src/components/AccountPanel.tsx`
- 修改内容：
  - 移除了对`id === 'default'`账号的特殊限制
  - 现在所有账号都可以正常删除（包括默认账号）

### 4. 创建了迁移脚本
- 文件：`migrate_accounts_to_sql.py`
- 功能：
  - 将`accounts.json`中的账号数据迁移到SQL数据库
  - 迁移群组账号映射关系
  - 自动备份JSON文件

## 问题修复

### 默认账号无法删除的问题
**原因**：
- 之前的实现中，当`accounts.json`为空时，系统会返回一个虚拟的"default"账号，它对应`config.toml`中的cookie配置
- 前端通过检查`acc.id !== 'default'`来禁用删除按钮

**解决方案**：
- 移除了虚拟"default"账号的概念
- 所有账号都存储在SQL数据库中
- 现在所有账号都可以被删除（包括默认账号）

## 使用说明

### 首次启动
1. 启动后端服务：
   ```bash
   cd D:\abc\PycharmProjects\ZsxqCrawler
   uv run main.py
   ```

2. 访问账号管理页面：
   ```
   http://localhost:3060/accounts
   ```

3. 添加账号：
   - 点击"添加账号"按钮
   - 输入账号名称（可选）
   - 粘贴Cookie
   - 选择是否设为默认账号
   - 点击"添加账号"

### 如果有现有的accounts.json数据需要迁移

如果你的`accounts.json`中有账号数据需要迁移到SQL，运行迁移脚本：

```bash
cd D:\abc\PycharmProjects\ZsxqCrawler
python migrate_accounts_to_sql.py
```

迁移脚本会：
1. 读取`accounts.json`中的账号数据
2. 将账号数据导入到SQL数据库
3. 迁移群组账号映射关系
4. 自动备份`accounts.json`为`accounts.json.backup`

## 数据存储位置

- **SQL数据库**：`output/databases/zsxq_config.db`
- **账号表**：`accounts`
- **群组映射表**：`group_account_map`

## 注意事项

1. **旧的accounts.json文件**：
   - 迁移后，旧的`accounts.json`文件将不再使用
   - 建议保留备份文件以防万一

2. **默认账号**：
   - 第一个添加的账号会自动设为默认账号
   - 可以通过"设为默认"按钮更改默认账号
   - 删除默认账号后，第一个剩余账号会自动成为新的默认账号

3. **账号删除**：
   - 删除账号会同时清理该账号的所有群组映射
   - 删除操作不可撤销，请谨慎操作

## 测试建议

1. 测试添加账号功能
2. 测试设置默认账号
3. 测试删除账号（包括默认账号）
4. 测试刷新账号信息
5. 测试群组账号分配

## 技术细节

### 数据库表结构

#### accounts表
```sql
CREATE TABLE accounts (
    id TEXT PRIMARY KEY,
    name TEXT,
    cookie TEXT NOT NULL,
    is_default INTEGER DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT
)
```

#### group_account_map表
```sql
CREATE TABLE group_account_map (
    group_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    assigned_at TEXT NOT NULL,
    FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
)
```

### API端点

- `GET /api/accounts` - 获取所有账号列表
- `POST /api/accounts` - 创建新账号
- `DELETE /api/accounts/{account_id}` - 删除账号
- `POST /api/accounts/{account_id}/default` - 设置默认账号
- `POST /api/groups/{group_id}/assign-account` - 分配群组账号
- `GET /api/groups/{group_id}/account` - 获取群组使用的账号
- `GET /api/accounts/{account_id}/self` - 获取账号自我信息
- `POST /api/accounts/{account_id}/self/refresh` - 刷新账号自我信息

## 故障排除

### 如果遇到数据库错误
1. 检查数据库文件权限
2. 确保`output/databases`目录存在
3. 尝试删除`zsxq_config.db`文件重新初始化

### 如果账号显示异常
1. 清空浏览器缓存
2. 重启后端服务
3. 检查后端日志输出

---

完成时间：2025-12-12
