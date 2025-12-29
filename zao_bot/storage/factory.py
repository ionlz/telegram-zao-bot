from __future__ import annotations

from pathlib import Path

from config import Settings
from zao_bot.storage.base import Storage
from zao_bot.storage.sqlalchemy_storage import SQLAlchemyStorage


def get_storage(settings: Settings) -> Storage:
    # 优先使用 Postgres（如果配置了连接串）
    if settings.database_url:
        url = settings.database_url.strip()
        if url.startswith("postgres://"):
            url = "postgresql://" + url[len("postgres://") :]
        if url.startswith("postgresql://") and "postgresql+" not in url:
            url = "postgresql+psycopg://" + url[len("postgresql://") :]
        return SQLAlchemyStorage(url=url)

    p = Path(settings.db_path).expanduser()
    if not p.is_absolute():
        p = (Path.cwd() / p).resolve()
    sqlite_url = f"sqlite+pysqlite:///{p.as_posix()}"
    return SQLAlchemyStorage(url=sqlite_url)


