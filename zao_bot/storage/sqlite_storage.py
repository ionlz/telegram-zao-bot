from __future__ import annotations

from datetime import datetime, timedelta

from zao_bot import db as sqlite_db
from zao_bot.storage.base import OpenSession, Storage


class SQLiteStorage(Storage):
    def __init__(self, *, db_path: str):
        self._db_path = db_path

    def init_db(self) -> None:
        sqlite_db.init_db(self._db_path)

    def upsert_user_and_chat(
        self,
        *,
        user_id: int,
        username: str | None,
        first_name: str | None,
        last_name: str | None,
        chat_id: int,
        chat_title: str | None,
        chat_type: str,
        updated_at: datetime,
    ) -> None:
        sqlite_db.upsert_user_and_chat(
            self._db_path,
            user_id=user_id,
            username=username,
            first_name=first_name,
            last_name=last_name,
            chat_id=chat_id,
            chat_title=chat_title,
            chat_type=chat_type,
            updated_at=updated_at,
        )

    def get_open_session(self, *, chat_id: int, user_id: int) -> OpenSession | None:
        osess = sqlite_db.get_open_session(self._db_path, chat_id=chat_id, user_id=user_id)
        if not osess:
            return None
        return OpenSession(session_id=osess.session_id, check_in=osess.check_in)

    def check_in(self, *, chat_id: int, user_id: int, ts: datetime) -> bool:
        return sqlite_db.check_in(self._db_path, chat_id=chat_id, user_id=user_id, ts=ts)

    def check_out(self, *, chat_id: int, user_id: int, ts: datetime) -> tuple[bool, timedelta | None, datetime | None, int | None]:
        return sqlite_db.check_out(self._db_path, chat_id=chat_id, user_id=user_id, ts=ts)

    def session_today_exists(self, *, chat_id: int, user_id: int, day: str) -> bool:
        return sqlite_db.session_today_exists(self._db_path, chat_id=chat_id, user_id=user_id, day=day)

    def session_today_completed(self, *, chat_id: int, user_id: int, day: str) -> bool:
        return sqlite_db.session_today_completed(self._db_path, chat_id=chat_id, user_id=user_id, day=day)

    def today_checkin_position(self, *, chat_id: int, session_id: int, check_in: datetime, day: str) -> int:
        return sqlite_db.today_checkin_position(self._db_path, chat_id=chat_id, session_id=session_id, check_in=check_in, day=day)

    def leaderboard(self, *, chat_id: int, mode: str, now: datetime) -> list[tuple[int, str, int]]:
        return sqlite_db.leaderboard(self._db_path, chat_id=chat_id, mode=mode, now=now)

    def leaderboard_global(self, *, mode: str, now: datetime) -> list[tuple[int, str, int]]:
        return sqlite_db.leaderboard_global(self._db_path, mode=mode, now=now)

    def set_daily_earliest(
        self,
        *,
        chat_id: int,
        day: str,
        user_id: int,
        session_id: int,
        check_in: datetime,
        created_at: datetime,
    ) -> bool:
        return sqlite_db.set_daily_earliest(
            self._db_path,
            chat_id=chat_id,
            day=day,
            user_id=user_id,
            session_id=session_id,
            check_in=check_in,
            created_at=created_at,
        )

    def update_streak(self, *, chat_id: int, user_id: int, key: str, day: str, created_at: datetime) -> int:
        return sqlite_db.update_streak(self._db_path, chat_id=chat_id, user_id=user_id, key=key, day=day, created_at=created_at)

    def get_streak(self, *, chat_id: int, user_id: int, key: str) -> int:
        return sqlite_db.get_streak(self._db_path, chat_id=chat_id, user_id=user_id, key=key)

    def get_streak_best_global(self, *, user_id: int, key: str) -> tuple[int, int | None, str | None]:
        return sqlite_db.get_streak_best_global(self._db_path, user_id=user_id, key=key)

    def award_achievement(
        self,
        *,
        chat_id: int,
        user_id: int,
        key: str,
        created_at: datetime,
        day: str | None = None,
        session_id: int | None = None,
    ) -> bool:
        return sqlite_db.award_achievement(
            self._db_path,
            chat_id=chat_id,
            user_id=user_id,
            key=key,
            created_at=created_at,
            day=day,
            session_id=session_id,
        )

    def get_achievement_stats(self, *, chat_id: int, user_id: int) -> list[tuple[str, int, str]]:
        return sqlite_db.get_achievement_stats(self._db_path, chat_id=chat_id, user_id=user_id)

    def get_achievement_stats_global(self, *, user_id: int) -> list[tuple[str, int, str]]:
        return sqlite_db.get_achievement_stats_global(self._db_path, user_id=user_id)

    def get_achievement_count(self, *, chat_id: int, user_id: int, key: str) -> int:
        return sqlite_db.get_achievement_count(self._db_path, chat_id=chat_id, user_id=user_id, key=key)

    def get_achievement_count_global(self, *, user_id: int, key: str) -> int:
        return sqlite_db.get_achievement_count_global(self._db_path, user_id=user_id, key=key)

    def achievement_rank_by_count(self, *, chat_id: int, key: str, limit: int = 20) -> list[tuple[int, str, int]]:
        return sqlite_db.achievement_rank_by_count(self._db_path, chat_id=chat_id, key=key, limit=limit)

    def achievement_rank_by_count_global(self, *, key: str, limit: int = 20) -> list[tuple[int, str, int]]:
        return sqlite_db.achievement_rank_by_count_global(self._db_path, key=key, limit=limit)

    def streak_rank(self, *, chat_id: int, key: str, limit: int = 20) -> list[tuple[int, str, int]]:
        return sqlite_db.streak_rank(self._db_path, chat_id=chat_id, key=key, limit=limit)

    def streak_rank_global(self, *, key: str, limit: int = 20) -> list[tuple[int, str, int, int | None, str | None]]:
        return sqlite_db.streak_rank_global(self._db_path, key=key, limit=limit)


