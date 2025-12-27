## zao-bot（Telegram 群组签到/签退 + 成就 + 排行榜）

这是一个用于 **Telegram 群组** 的签到/签退 bot：

- **签到/签退**：`/zao`（签到）、`/wan`（签退）
- **清醒时长查询**：`/awake`（支持“回复某人消息后查询 TA”）
- **清醒排行榜**：`/rank`（今日/总榜，支持全局）
- **成就系统**：`/ach`（成就查询，支持全局与回复查询）、`/achrank`（成就排行榜）
- **数据库**：SQLite（默认在 `./data/zao_bot.sqlite3`，WAL 模式提升并发）

---

### 业务日规则（很重要）

本 bot 的“今日”不是按 0 点切换，而是按 **凌晨 04:00 ~ 次日 04:00**：

- 例如：周二 03:30 的 `/wan` 仍会计入“周一”的那一天
- 周二 04:10 之后才算“新的一天”

该规则影响：**每日一次限制、今日榜、今日最早、今日第 N 个签到、成就归档**。

---

### 快速开始

### 1) 安装依赖

项目依赖见 `pyproject.toml`（主要是 `python-telegram-bot`）。

### 2) 配置（推荐用环境变量）

必须：

- **BOT_TOKEN**：从 `@BotFather` 获取

可选：

- **DB_PATH**：SQLite 文件路径（默认 `./data/zao_bot.sqlite3`）
- **TZ**：时区（默认 `Asia/Shanghai`）
- **LOG_LEVEL**：日志级别（默认 `INFO`）
- **ZAO_CONFIG**：配置文件路径（默认读取 `./config.toml`，若存在）
- **ZAO_MESSAGES**：回复文案模板路径（默认读取 `./messages.toml`，若存在）

SQLite（并发/恢复相关）：

- **SQLITE_JOURNAL_MODE**：默认 `WAL`
- **SQLITE_SYNCHRONOUS**：默认 `NORMAL`（更稳可设 `FULL`）
- **SQLITE_BUSY_TIMEOUT_MS**：默认 `5000`
- **SQLITE_WAL_AUTOCHECKPOINT**：默认 `1000`

示例：

```bash
export BOT_TOKEN="123456:xxxx"
export TZ="Asia/Shanghai"
export DB_PATH="$PWD/data/zao_bot.sqlite3"
export LOG_LEVEL="INFO"
python main.py
```

#### 使用 `config.toml`

你也可以复制 `config.example.toml` 并编辑为 `config.toml`。注意：**建议不要把真实 token 提交到仓库**（项目里通常会把 `config.toml` 加入 `.gitignore`）。

```bash
cp config.example.toml config.toml
export BOT_TOKEN="123456:xxxx"  # 环境变量会覆盖 config.toml
python main.py
```

#### 自定义回复文案（模板）

直接编辑 `messages.toml`，或复制一份并用 `ZAO_MESSAGES` 指定路径。文案支持 `str.format` 占位符（如 `{name}`、`{time}`、`{awake}`、`{n}`）。

```bash
export ZAO_MESSAGES="$PWD/messages.toml"
python main.py
```

---

### 群里怎么用（指令大全）

#### 签到/签退

- **签到**：`/zao`
  - 成功后会提示“**你是今日第 N 个签到**”
  - 如果今天已完成一次签到+签退，会提示“今天已经结束，请休息吧”
- **签退**：`/wan`

#### 清醒时长

- **查自己**：`/awake`
- **查别人**：回复某人的消息后发送 `/awake`

#### 清醒排行榜

- **今日榜**：`/rank`
- **总榜**：`/rank all`
- **全局今日榜（跨群）**：`/rank global`
- **全局总榜（跨群）**：`/rank all global`

#### 成就

目前内置成就：

- **今日最早**：当天最早 `/zao` 的人（次数可累计）
- **连续今日最早 7 天**：连续 7 天都是今日最早（会在 7/14/21... 天触发）
- **准点下班**：本次清醒时长 \(8小时±1分钟\)
- **辛苦的一天**：本次清醒时长 \(>12小时\)

成就查询：

- **查自己（本群）**：`/ach`
- **查自己（全局）**：`/ach global`
- **查别人**：回复某人消息后 `/ach` 或 `/ach global`

成就排行榜：

- **本群**：`/achrank daily|streak|ontime|longday`
- **全局（跨群）**：`/achrank global daily|streak|ontime|longday`

---

### 数据结构（SQLite）

主要表：

- `users`：用户信息
- `chats`：群/会话信息
- `sessions`：签到记录（含 `session_day`，用于 04:00 切换的“今日”口径）
- `daily_earliest` / `streaks` / `achievement_events` / `achievement_stats`：成就相关

数据库文件默认在 `./data/` 目录，避免污染项目根目录；启动时会自动创建目录。

---

### SQLite 并发与恢复（WAL）

- **并发性能**：默认启用 **WAL**，读写并发更好，并配合 `busy_timeout` 降低 `database is locked`。
- **恢复机制**：WAL 模式下会产生 `*.sqlite3-wal` / `*.sqlite3-shm`。异常退出后，下次打开会自动回放 WAL 完成恢复。
  - 重要：运行中复制数据库时，不要只拷贝 `.sqlite3` 而丢掉 `-wal/-shm`。

#### 运维脚本（备份/检查/Checkpoint）

项目提供 `db_admin.py`：

```bash
python db_admin.py backup
python db_admin.py integrity_check
python db_admin.py checkpoint --mode FULL
```

---

### 常见问题（FAQ）

- **为什么今天不是 0 点切换？**
  - 本项目按“凌晨 4 点”作为一天边界，适合夜猫子场景（凌晨 4 点前签退仍算前一天）。
- **为什么 `config.toml`/数据库不建议提交到 git？**
  - `config.toml` 可能含 token；SQLite 是运行时数据，建议放 `data/` 并加入 `.gitignore`。

