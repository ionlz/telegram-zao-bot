from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol


@dataclass(frozen=True)
class OpenSession:
    session_id: int
    check_in: datetime


class Storage(Protocol):
    # --- lifecycle ---
    def init_db(self) -> None: ...

    # --- users/chats ---
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
    ) -> None: ...

    # --- sessions ---
    def get_open_session(self, *, chat_id: int, user_id: int) -> OpenSession | None: ...
    def check_in(self, *, chat_id: int, user_id: int, ts: datetime) -> bool: ...
    def check_out(self, *, chat_id: int, user_id: int, ts: datetime) -> tuple[bool, timedelta | None, datetime | None, int | None]: ...
    def session_today_exists(self, *, chat_id: int, user_id: int, day: str) -> bool: ...
    def session_today_completed(self, *, chat_id: int, user_id: int, day: str) -> bool: ...
    def today_checkin_position(self, *, chat_id: int, session_id: int, check_in: datetime, day: str) -> int: ...

    # --- leaderboard ---
    def leaderboard(self, *, chat_id: int, mode: str, now: datetime) -> list[tuple[int, str, int]]: ...
    def leaderboard_global(self, *, mode: str, now: datetime) -> list[tuple[int, str, int]]: ...

    # --- achievements ---
    def set_daily_earliest(
        self,
        *,
        chat_id: int,
        day: str,
        user_id: int,
        session_id: int,
        check_in: datetime,
        created_at: datetime,
    ) -> bool: ...
    def update_streak(self, *, chat_id: int, user_id: int, key: str, day: str, created_at: datetime) -> int: ...
    def get_streak(self, *, chat_id: int, user_id: int, key: str) -> int: ...
    def get_streak_best_global(self, *, user_id: int, key: str) -> tuple[int, int | None, str | None]: ...

    def award_achievement(
        self,
        *,
        chat_id: int,
        user_id: int,
        key: str,
        created_at: datetime,
        day: str | None = None,
        session_id: int | None = None,
    ) -> bool: ...

    def get_achievement_stats(self, *, chat_id: int, user_id: int) -> list[tuple[str, int, str]]: ...
    def get_achievement_stats_global(self, *, user_id: int) -> list[tuple[str, int, str]]: ...
    def get_achievement_count(self, *, chat_id: int, user_id: int, key: str) -> int: ...
    def get_achievement_count_global(self, *, user_id: int, key: str) -> int: ...
    def achievement_rank_by_count(self, *, chat_id: int, key: str, limit: int = 20) -> list[tuple[int, str, int]]: ...
    def achievement_rank_by_count_global(self, *, key: str, limit: int = 20) -> list[tuple[int, str, int]]: ...
    def streak_rank(self, *, chat_id: int, key: str, limit: int = 20) -> list[tuple[int, str, int]]: ...
    def streak_rank_global(self, *, key: str, limit: int = 20) -> list[tuple[int, str, int, int | None, str | None]]: ...


