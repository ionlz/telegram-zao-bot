from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

from zao_bot.storage.base import Storage
from zao_bot.time_utils import business_day_key


ACH_DAILY_EARLIEST = "daily_earliest"
ACH_STREAK_EARLIEST_7 = "streak_earliest_7"
ACH_ONTIME_8H = "ontime_8h"
ACH_LONGDAY_12H = "longday_12h"

# 成就分类：
# - 可重复获取：累计次数（获得成就）
# - 单次成就：只在一个群里首次达成时触发（解锁成就）
SINGLE_ACHIEVEMENTS: set[str] = {ACH_ONTIME_8H}


def is_single_achievement(key: str) -> bool:
    return key in SINGLE_ACHIEVEMENTS


@dataclass(frozen=True)
class AchievementResult:
    unlocked: list[str]
    # 仅用于展示
    earliest_streak: int | None = None


def on_check_in(
    *,
    storage: Storage,
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
    # 业务日：凌晨 4 点前仍算前一天
    day = business_day_key(check_in_ts, cutoff_hour=4)

    # 今日最早（只会有一个人写入成功）
    is_earliest = storage.set_daily_earliest(
        chat_id=chat_id,
        day=day,
        user_id=user_id,
        session_id=session_id,
        check_in=check_in_ts,
        created_at=now_ts,
    )
    earliest_streak: int | None = None
    if is_earliest:
        if storage.award_achievement(chat_id=chat_id, user_id=user_id, key=ACH_DAILY_EARLIEST, created_at=now_ts, day=day):
            unlocked.append(ACH_DAILY_EARLIEST)

        earliest_streak = storage.update_streak(chat_id=chat_id, user_id=user_id, key="earliest", day=day, created_at=now_ts)
        # 连续7天触发一次（7/14/21...）
        if earliest_streak > 0 and earliest_streak % 7 == 0:
            if storage.award_achievement(
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
    storage: Storage,
    chat_id: int,
    user_id: int,
    session_id: int,
    check_in_ts,
    duration: timedelta,
    now_ts,
) -> AchievementResult:
    """
    在签退成功后调用。
    3) 8小时准点下班：awake 时间为 8h，误差 1 分钟（±60s）
    4) 辛苦的一天：awake 时间超过 12h
    """
    unlocked: list[str] = []
    # 统一按“本次 session 的业务日”归档（凌晨 4 点前仍算前一天），避免跨天签退记到次日
    day = business_day_key(check_in_ts, cutoff_hour=4)
    # 仅工作日（周一~周五）触发
    try:
        is_weekday = date.fromisoformat(day).weekday() <= 4
    except Exception:
        is_weekday = True

    # 8h ± 1min
    if is_weekday and abs(duration - timedelta(hours=8)) <= timedelta(minutes=1):
        # 单次成就：每个群里只在首次达成时触发一次（之后不再累计）
        if storage.get_achievement_count(chat_id=chat_id, user_id=user_id, key=ACH_ONTIME_8H) <= 0:
            if storage.award_achievement(
                chat_id=chat_id,
                user_id=user_id,
                key=ACH_ONTIME_8H,
                created_at=now_ts,
                day=day,
                session_id=session_id,
            ):
                unlocked.append(ACH_ONTIME_8H)

    # > 12h
    if is_weekday and duration > timedelta(hours=12):
        if storage.award_achievement(
            chat_id=chat_id,
            user_id=user_id,
            key=ACH_LONGDAY_12H,
            created_at=now_ts,
            day=day,
            session_id=session_id,
        ):
            unlocked.append(ACH_LONGDAY_12H)

    return AchievementResult(unlocked=unlocked)

