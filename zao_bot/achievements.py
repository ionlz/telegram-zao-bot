from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from zao_bot import db
from zao_bot.time_utils import day_key


ACH_DAILY_EARLIEST = "daily_earliest"
ACH_STREAK_EARLIEST_7 = "streak_earliest_7"
ACH_ONTIME_8H = "ontime_8h"
ACH_LONGDAY_12H = "longday_12h"


@dataclass(frozen=True)
class AchievementResult:
    unlocked: list[str]
    # 仅用于展示
    earliest_streak: int | None = None


def on_check_in(
    *,
    db_path: str,
    chat_id: int,
    user_id: int,
    session_id: int,
    check_in_ts,
    now_ts,
) -> AchievementResult:
    """
    在签到成功后调用。
    1) 每日最早：当日第一次写入 daily_earliest 的人获得（可累计次数）
    2) 连续最早：连续7天都是每日最早，触发一次（可在 7/14/21... 天继续触发）
    """
    unlocked: list[str] = []
    day = day_key(check_in_ts)

    # 今日最早（只会有一个人写入成功）
    is_earliest = db.set_daily_earliest(
        db_path,
        chat_id=chat_id,
        day=day,
        user_id=user_id,
        session_id=session_id,
        check_in=check_in_ts,
        created_at=now_ts,
    )
    earliest_streak: int | None = None
    if is_earliest:
        if db.award_achievement(db_path, chat_id=chat_id, user_id=user_id, key=ACH_DAILY_EARLIEST, created_at=now_ts, day=day):
            unlocked.append(ACH_DAILY_EARLIEST)

        earliest_streak = db.update_streak(db_path, chat_id=chat_id, user_id=user_id, key="earliest", day=day, created_at=now_ts)
        # 连续7天触发一次（7/14/21...）
        if earliest_streak > 0 and earliest_streak % 7 == 0:
            if db.award_achievement(
                db_path,
                chat_id=chat_id,
                user_id=user_id,
                key=ACH_STREAK_EARLIEST_7,
                created_at=now_ts,
                day=day,
            ):
                unlocked.append(ACH_STREAK_EARLIEST_7)

    return AchievementResult(unlocked=unlocked, earliest_streak=earliest_streak)


def on_check_out(
    *,
    db_path: str,
    chat_id: int,
    user_id: int,
    session_id: int,
    duration: timedelta,
    now_ts,
) -> AchievementResult:
    """
    在签退成功后调用。
    3) 8小时准点下班：awake 时间为 8h，误差 1 分钟（±60s）
    4) 辛苦的一天：awake 时间超过 12h
    """
    unlocked: list[str] = []
    day = day_key(now_ts)

    # 8h ± 1min
    if abs(duration - timedelta(hours=8)) <= timedelta(minutes=1):
        if db.award_achievement(
            db_path,
            chat_id=chat_id,
            user_id=user_id,
            key=ACH_ONTIME_8H,
            created_at=now_ts,
            day=day,
            session_id=session_id,
        ):
            unlocked.append(ACH_ONTIME_8H)

    # > 12h
    if duration > timedelta(hours=12):
        if db.award_achievement(
            db_path,
            chat_id=chat_id,
            user_id=user_id,
            key=ACH_LONGDAY_12H,
            created_at=now_ts,
            day=day,
            session_id=session_id,
        ):
            unlocked.append(ACH_LONGDAY_12H)

    return AchievementResult(unlocked=unlocked)

