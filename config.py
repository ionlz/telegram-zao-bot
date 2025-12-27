from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class Settings:
    bot_token: str | None
    tz_name: str
    db_path: str
    log_level: str
    config_path: str | None

    @property
    def tzinfo(self) -> ZoneInfo:
        return ZoneInfo(self.tz_name)


def _read_toml(path: str) -> dict[str, Any]:
    try:
        import tomllib  # py>=3.11
    except Exception:
        return {}

    try:
        with open(path, "rb") as f:
            data = tomllib.load(f)
    except FileNotFoundError:
        return {}
    except Exception:
        return {}

    if isinstance(data, dict):
        return data
    return {}


def load_settings() -> Settings:
    """
    优先级：环境变量 > config.toml > 默认值
    - ZAO_CONFIG：配置文件路径（默认 ./config.toml，如果存在就读取）
    - BOT_TOKEN：Telegram Bot Token（必填，运行时校验）
    - TZ：时区（默认 Asia/Shanghai）
    - DB_PATH：SQLite 路径（默认 ./data/zao_bot.sqlite3）
    - LOG_LEVEL：日志级别（默认 INFO）

    config.toml 支持的键（可选）：
    - bot_token
    - tz
    - db_path
    - log_level
    """
    default_config_path = str(Path.cwd() / "config.toml")
    cfg_path = os.getenv("ZAO_CONFIG", default_config_path)
    cfg = _read_toml(cfg_path) if cfg_path else {}

    # 默认值
    tz_name = str(cfg.get("tz") or "Asia/Shanghai")
    db_path = str(cfg.get("db_path") or (Path.cwd() / "data" / "zao_bot.sqlite3"))
    log_level = str(cfg.get("log_level") or "INFO")
    bot_token = cfg.get("bot_token")
    bot_token = str(bot_token) if bot_token else None

    # 环境变量覆盖
    tz_name = os.getenv("TZ", tz_name)
    db_path = os.getenv("DB_PATH", db_path)
    log_level = os.getenv("LOG_LEVEL", log_level)
    bot_token = os.getenv("BOT_TOKEN", bot_token or "")
    bot_token = bot_token.strip() or None

    # 如果默认路径不存在且用户没显式指定 ZAO_CONFIG，就不认为“使用了配置文件”
    effective_cfg_path: str | None = None
    if cfg_path and (os.path.exists(cfg_path) or os.getenv("ZAO_CONFIG")):
        effective_cfg_path = cfg_path

    return Settings(
        bot_token=bot_token,
        tz_name=tz_name,
        db_path=db_path,
        log_level=log_level,
        config_path=effective_cfg_path,
    )


