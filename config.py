from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class Settings:
    bot_token: str | None
    tz_name: str
    db_path: str
    database_url: str | None
    log_level: str
    proxy_url: str | None
    proxy_username: str | None
    proxy_password: str | None
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
    - ZAO_DATABASE_URL / DATABASE_URL：Postgres 连接串（例如 postgresql://user:pass@host:5432/dbname）
    - LOG_LEVEL：日志级别（默认 INFO）
    - ZAO_PROXY_URL：HTTP 代理（例如 http://127.0.0.1:7890），用于 Telegram 请求转发
    - ZAO_PROXY_USERNAME / ZAO_PROXY_PASSWORD：代理认证（可选）。也可以直接把账号密码写在 ZAO_PROXY_URL 里：
      http://user:pass@127.0.0.1:7890

    config.toml 支持的键（可选）：
    - bot_token
    - tz
    - db_path
    - database_url
    - log_level
    - proxy_url
    - proxy_username
    - proxy_password
    """
    default_config_path = str(Path.cwd() / "config.toml")
    cfg_path = os.getenv("ZAO_CONFIG", default_config_path)
    cfg = _read_toml(cfg_path) if cfg_path else {}

    # 默认值
    tz_name = str(cfg.get("tz") or "Asia/Shanghai")
    db_path = str(cfg.get("db_path") or (Path.cwd() / "data" / "zao_bot.sqlite3"))
    database_url = cfg.get("database_url")
    database_url = str(database_url).strip() if database_url else None
    log_level = str(cfg.get("log_level") or "INFO")
    proxy_url = cfg.get("proxy_url")
    proxy_url = str(proxy_url).strip() if proxy_url else None
    proxy_username = cfg.get("proxy_username")
    proxy_username = str(proxy_username).strip() if proxy_username else None
    proxy_password = cfg.get("proxy_password")
    proxy_password = str(proxy_password) if proxy_password else None
    bot_token = cfg.get("bot_token")
    bot_token = str(bot_token) if bot_token else None

    # 环境变量覆盖
    tz_name = os.getenv("TZ", tz_name)
    db_path = os.getenv("DB_PATH", db_path)
    database_url = os.getenv("ZAO_DATABASE_URL", os.getenv("DATABASE_URL", database_url or "")).strip() or None
    log_level = os.getenv("LOG_LEVEL", log_level)
    proxy_url = os.getenv("ZAO_PROXY_URL", proxy_url or "").strip() or None
    proxy_username = os.getenv("ZAO_PROXY_USERNAME", proxy_username or "").strip() or None
    # 密码不 strip，避免用户有意包含前后空格；这里只做空串判断
    proxy_password_env = os.getenv("ZAO_PROXY_PASSWORD", "")
    proxy_password = proxy_password_env if proxy_password_env != "" else proxy_password
    bot_token = os.getenv("BOT_TOKEN", bot_token or "")
    bot_token = bot_token.strip() or None

    # 如果 proxy_url 未包含认证信息且提供了 username/password，则拼接到 URL 里
    # 允许用户直接写 http://user:pass@host:port
    effective_proxy_url = proxy_url
    if effective_proxy_url and "@" not in effective_proxy_url and proxy_username and proxy_password is not None:
        # 仅支持形如 scheme://host:port 的 URL
        if "://" in effective_proxy_url:
            scheme, rest = effective_proxy_url.split("://", 1)
            u = quote(proxy_username, safe="")
            p = quote(proxy_password, safe="")
            effective_proxy_url = f"{scheme}://{u}:{p}@{rest}"

    # 如果默认路径不存在且用户没显式指定 ZAO_CONFIG，就不认为“使用了配置文件”
    effective_cfg_path: str | None = None
    if cfg_path and (os.path.exists(cfg_path) or os.getenv("ZAO_CONFIG")):
        effective_cfg_path = cfg_path

    return Settings(
        bot_token=bot_token,
        tz_name=tz_name,
        db_path=db_path,
        database_url=database_url,
        log_level=log_level,
        proxy_url=effective_proxy_url,
        proxy_username=proxy_username,
        proxy_password=proxy_password,
        config_path=effective_cfg_path,
    )


