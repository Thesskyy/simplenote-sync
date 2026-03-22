# simplenote-sync learn-core

这是一个 Simplenote 到 Notion 的单向同步服务。程序通过 Simperium 实时接收 Simplenote 事件，写入本地 SQLite 队列，再异步推送到 Notion。

适合场景：
- 日常笔记在 Simplenote 编辑
- 需要在 Notion 中检索、归档、二次处理
- 需要可恢复、可观测、可长期运行的同步流程

## 1. 功能概览

- 实时接收 Simplenote 更新事件
- 本地 SQLite 持久化状态与事件日志
- 按 Simplenote ID 幂等写入 Notion（避免重复建页）
- 支持删除同步（映射到 Notion 的 Deleted 复选框）
- 支持双写模式：属性预览 + 页面正文块
- 失败自动重试（指数退避），达到上限后进入 GAVE_UP
- 版本号闸门（仅接收更高版本，过滤旧事件）
- 可选健康检查接口与周期指标日志
- 断联后具备自动重连能力

## 2. 运行前准备

### 2.1 软件要求

- Node.js 18+
- 可访问 Notion API 的网络环境
- Simperium App ID 与 Token

### 2.2 Notion 权限要求

你需要一个 Notion Integration Token，并且该 integration 已被邀请到目标数据库所在页面。

## 3. Notion 数据库字段要求

目标 Notion 数据库（或 Data Source）至少包含以下字段：

- 标题字段（Title，名称任意）
- Simplenote ID（rich_text）
- Content Preview（rich_text）
- Deleted（checkbox）
- Tags（multi_select）
- Created Time（date）
- Synced At（date）

说明：
- Simplenote ID 用于幂等匹配同一条笔记。
- Content Preview 用于数据库列表快速查看内容。
- 当启用 USE_PAGE_BLOCKS=true 时，完整正文会写入页面 blocks（更适合正文阅读）。

## 4. 安装与启动

### 4.1 安装依赖

```bash
npm install
```

### 4.2 配置环境变量

在项目根目录创建或编辑 .env，最小必填如下：

```dotenv
SIMPERIUM_APP_ID=你的_simperium_app_id
SIMPERIUM_TOKEN=你的_simperium_token
NOTION_TOKEN=你的_notion_integration_token
NOTION_DATABASE_ID=你的_notion_database_id
```

建议完整配置示例：

```dotenv
# 必填
SIMPERIUM_APP_ID=...
SIMPERIUM_TOKEN=...
NOTION_TOKEN=...
NOTION_DATABASE_ID=...

# 可选，仅用于日志展示
SN_EMAIL=

# 防抖窗口
QUIET_WINDOW_MS=15000
DELETE_QUIET_WINDOW_MS=5000

# 调度周期
DISPATCH_INTERVAL_MS=2000

# 重试策略
RETRY_BASE_MS=2000
RETRY_MAX_MS=300000
MAX_RETRY_COUNT=8

# Notion 正文块同步
USE_PAGE_BLOCKS=false

# 事件日志与存储保护
SOURCE_EVENT_INFO_EVERY=20
MAX_RAW_EVENT_BYTES=65536

# 事件保留策略
NOTE_EVENTS_RETENTION_DAYS=30
NOTE_EVENTS_PURGE_INTERVAL_MS=3600000

# 观测能力
METRICS_LOG_ENABLED=true
METRICS_LOG_INTERVAL_MS=60000
HEALTH_PORT=0

# 未授权恢复策略
UNAUTHORIZED_EXIT_DELAY_MS=3000
```

### 4.3 启动服务

```bash
npm start
```

### 4.4 运行测试

```bash
npm test
```

## 5. 同步流程说明

整体流程：
1. 接收 Simplenote 事件
2. 按版本号校验是否为新事件
3. 写入 notes 与 note_events
4. 进入稳定等待窗口（quiet window）
5. 调度器批量挑选可同步数据推送到 Notion
6. 成功标记 SYNCED/DELETED，失败标记 FAILED 并退避重试

关键策略：
- 版本闸门：只接受 incoming_version > current_version
- 防抖：稳定窗口内重复更新会刷新稳定时间，减少抖动写入
- 幂等：通过 Simplenote ID 查找并更新同一 Notion 页面

## 6. 如何判断同步稳定

可以从日志和数据库两方面判断：

### 6.1 日志侧

稳定时你会看到：
- Simperium connected
- Simperium ready, listening note updates...
- synced to notion: <note_id>（普通更新）
- marked deleted in notion: <note_id>（删除同步）

如果异常频繁，会看到：
- sync failed
- sync gave up after max retries

### 6.2 数据侧

notes.status 常见状态：
- DIRTY：待同步
- SYNCING：同步中
- SYNCED：已同步
- FAILED：失败待重试
- GAVE_UP：重试次数耗尽
- DELETED：删除状态已同步

稳定的判断标准：
- 大部分记录停留在 SYNCED/DELETED
- FAILED 不持续增长
- 没有新增 GAVE_UP

## 7. 延迟如何计算

单条更新的理论延迟约为：

普通更新：
- QUIET_WINDOW_MS + DISPATCH_INTERVAL_MS + Notion API 往返耗时

删除更新：
- DELETE_QUIET_WINDOW_MS + DISPATCH_INTERVAL_MS + Notion API 往返耗时

注意：
- 如果同一条笔记在静默窗口内被连续编辑，计时会从最后一次编辑重新开始。

## 8. 重试与失败策略

当 Notion 写入失败时：
- 状态置为 FAILED
- retry_count + 1
- 下次重试时间按照指数退避计算

默认退避序列（base=2s）：
- 2s, 4s, 8s, 16s ... 直到 RETRY_MAX_MS 上限

当 retry_count 达到 MAX_RETRY_COUNT：
- 状态转为 GAVE_UP
- 不再参与正常重试（需人工介入）

## 9. 健康检查与指标

### 9.1 健康检查

当 HEALTH_PORT > 0 时，服务会启用：
- GET /healthz

### 9.2 指标日志

当 METRICS_LOG_ENABLED=true 时，会定期输出调度指标，包括：
- loops
- selected
- skippedByLock
- synced
- deleted
- failed
- gaveUp

## 10. 生产部署（Ubuntu + PM2）

项目提供一键脚本：

```bash
chmod +x scripts/setup-pm2.sh
./scripts/setup-pm2.sh
```

脚本会完成：
- 安装依赖
- 启动 PM2 进程
- 配置开机自启
- 配置日志轮转

可选覆盖参数：

```bash
APP_NAME=simplenote-sync MAX_MEMORY=700M ./scripts/setup-pm2.sh
```

## 11. 常见问题排查

### 11.1 有入站日志但 Notion 没更新

优先检查：
- Notion 字段是否齐全且类型正确
- Integration 是否有数据库权限
- note 是否处于 DIRTY/FAILED 且 stable_after 未到
- 是否出现 sync failed / gave up

### 11.2 断网后没有继续同步

检查日志是否出现以下链路：
- Simperium client error
- Simperium reconnect scheduled
- Simperium reconnect attempt
- Simperium connected

若长时间没有 connected，建议结合 PM2 进程守护并检查网络策略。

### 11.3 为什么有些旧事件被忽略

这是版本闸门生效的正常现象：旧版本或重复版本不会覆盖新状态，用于防止乱序消息污染。

## 12. 安全建议

- 不要把 .env 提交到 Git 仓库。
- 建议定期轮换 NOTION_TOKEN 与 SIMPERIUM_TOKEN。
- 服务器上使用最小权限原则，仅开放必要网络访问。

## 13. 快速自检清单

启动后按以下顺序确认：
1. 日志出现 Simperium connected 与 ready
2. 修改一条 Simplenote 笔记
3. 观察日志出现 synced to notion
4. 在 Notion 数据库确认字段更新
5. 若 USE_PAGE_BLOCKS=true，再检查页面正文块是否更新

完成以上 5 步，说明主链路工作正常。
