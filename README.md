## zao-bot（Telegram 群组签到/签退）

支持在群组里记录每个人的 **签到 `/zao`**、**签退 `/wan`**，并提供：

- **清醒时长查询**：`/awake`（也可“回复某人的消息”后发送 `/awake` 查询 TA）
- **排行榜**：`/rank`（默认今日榜），`/rank all`（总榜）
- **数据库**：使用 SQLite（默认生成 `./data/zao_bot.sqlite3`）

### 1) 安装依赖

推荐用 `uv` 或 `pip` 安装本项目依赖（`pyproject.toml` 已声明 `python-telegram-bot`）。

### 2) 配置环境变量

- **BOT_TOKEN**：必填，去 `@BotFather` 创建 bot 获取
- **DB_PATH**：可选，SQLite 文件路径（默认 `./data/zao_bot.sqlite3`）
- **TZ**：可选，时区（默认 `Asia/Shanghai`）
- **LOG_LEVEL**：可选，日志级别（默认 `INFO`）
- **ZAO_CONFIG**：可选，配置文件路径（默认读取 `./config.toml`，若存在）
- **ZAO_MESSAGES**：可选，回复文案模板路径（默认读取 `./messages.toml`，若存在）
- **SQLITE_JOURNAL_MODE**：可选，SQLite 日志模式（默认 `WAL`）
- **SQLITE_SYNCHRONOUS**：可选，SQLite 同步级别（默认 `NORMAL`；更稳可用 `FULL`）
- **SQLITE_BUSY_TIMEOUT_MS**：可选，锁等待毫秒（默认 `5000`）
- **SQLITE_WAL_AUTOCHECKPOINT**：可选，WAL 自动 checkpoint 页数（默认 `1000`）

示例：

```bash
export BOT_TOKEN="123456:xxxx"
export TZ="Asia/Shanghai"
export DB_PATH="$PWD/data/zao_bot.sqlite3"
export LOG_LEVEL="INFO"
```

也可以用 `config.toml`（参考 `config.example.toml`），环境变量会覆盖配置文件：

```bash
cp config.example.toml config.toml
export BOT_TOKEN="123456:xxxx"
python main.py
```

也可以自定义回复文案（直接编辑 `messages.toml`，或复制一份并用 `ZAO_MESSAGES` 指定路径）：

```bash
export BOT_TOKEN="123456:xxxx"
python main.py
```

### 3) 运行

```bash
python main.py
```

### 5) SQLite 并发与恢复（WAL）

- **并发性能**：项目默认启用 **WAL**，读写并发更好，且减少“database is locked”概率（配合 busy_timeout）。
- **恢复机制**：WAL 模式下，会在同目录生成 `*.sqlite3-wal` / `*.sqlite3-shm`。异常退出后，下次打开数据库会自动回放 WAL 完成恢复。
- **建议备份**：使用 `db_admin.py` 在线备份：

```bash
python db_admin.py backup
python db_admin.py integrity_check
python db_admin.py checkpoint --mode FULL
```

### 4) 群里怎么用

- **签到**：`/zao`
- **签退**：`/wan`
- **查询清醒时长**：`/awake`
  - 回复某人的消息再发 `/awake`：查询 TA 的清醒时长
- **排行榜**：`/rank`（今日），`/rank all`（总榜）

### 说明与约束

- 每个用户在同一个群里 **同一时间只能有一条“未签退”记录**（避免重复签到刷数据）。
- `/rank` 统计“清醒时长”（签到到签退的时长；如果未签退，会按当前时间计算到现在）。

