from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol


@dataclass(frozen=True)
class OpenSession:
    session_id: int
    check_in: datetime


@dataclass(frozen=True)
class RouletteGame:
    chat_id: int
    chambers: int
    bullet_position: int
    current_position: int
    created_by: int
    created_at: datetime


@dataclass(frozen=True)
class WakeReminder:
    id: int
    chat_id: int
    user_id: int
    wake_time: str
    next_trigger: datetime
    repeat: bool
    enabled: bool


@dataclass(frozen=True)
class RSPGame:
    id: int
    chat_id: int
    challenger_id: int
    opponent_id: int
    challenger_choice: str | None
    opponent_choice: str | None
    status: str  # 'pending', 'completed'
    winner_id: int | None  # NULL for draw
    message_id: int | None
    created_at: datetime


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
    def get_open_session(self, *, chat_id: int, user_id: int, day: str | None = None) -> OpenSession | None: ...
    def check_in(self, *, chat_id: int, user_id: int, ts: datetime) -> bool: ...
    def check_out(self, *, chat_id: int, user_id: int, ts: datetime) -> tuple[bool, timedelta | None, datetime | None, int | None]: ...
    def session_today_exists(self, *, chat_id: int, user_id: int, day: str) -> bool: ...
    def session_today_completed(self, *, chat_id: int, user_id: int, day: str) -> bool: ...
    def today_checkin_position(self, *, chat_id: int, session_id: int, check_in: datetime, day: str) -> int: ...
    def get_user_checkin_days(self, *, user_id: int, start_date: str, end_date: str) -> set[str]: ...

    # --- leaderboard ---
    def leaderboard(self, *, chat_id: int, mode: str, now: datetime) -> list[tuple[int, str, int]]: ...
    def leaderboard_global(self, *, mode: str, now: datetime) -> list[tuple[int, str, int]]: ...
    # 当前“未签退”的用户集合（用于榜单标记：🔥=未签退，💤=已签退）
    def open_user_ids(self, *, chat_id: int, day: str | None = None) -> set[int]: ...
    def open_user_ids_global(self, day: str | None = None) -> set[int]: ...

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

    # --- russian roulette ---
    def get_active_roulette(self, *, chat_id: int) -> RouletteGame | None: ...
    def create_roulette(
        self, *, chat_id: int, chambers: int, bullet_position: int, created_by: int, created_at: datetime
    ) -> None: ...
    def update_roulette_position(self, *, chat_id: int, position: int) -> None: ...
    def delete_roulette(self, *, chat_id: int) -> None: ...
    def record_roulette_attempt(
        self, *, chat_id: int, user_id: int, position: int, result: str, created_at: datetime
    ) -> None: ...

    # --- wake reminders ---
    def create_reminder(
        self, *, chat_id: int, user_id: int, wake_time: str, next_trigger: datetime, repeat: bool, created_at: datetime
    ) -> int: ...
    def get_pending_reminders(self, *, now: datetime) -> list[WakeReminder]: ...
    def get_user_reminders(self, *, chat_id: int, user_id: int) -> list[WakeReminder]: ...
    def update_reminder_next_trigger(self, *, reminder_id: int, next_trigger: datetime) -> None: ...
    def delete_reminder(self, *, reminder_id: int) -> None: ...
    def delete_user_reminders(self, *, chat_id: int, user_id: int) -> None: ...

    # --- rock paper scissors ---
    def create_rsp_game(
        self, *, chat_id: int, challenger_id: int, opponent_id: int, message_id: int | None, created_at: datetime
    ) -> int: ...
    def get_rsp_game(self, *, game_id: int) -> RSPGame | None: ...
    def get_pending_rsp_game(self, *, chat_id: int, user_id: int) -> RSPGame | None: ...
    def update_rsp_choice(self, *, game_id: int, user_id: int, choice: str) -> None: ...
    def complete_rsp_game(self, *, game_id: int, winner_id: int | None) -> None: ...
    def delete_rsp_game(self, *, game_id: int) -> None: ...
    def get_rsp_stats(self, *, chat_id: int, user_id: int) -> tuple[int, int, int, int]: ...  # (total, wins, losses, draws)
    def get_rsp_stats_global(self, *, user_id: int) -> tuple[int, int, int, int]: ...  # (total, wins, losses, draws)


