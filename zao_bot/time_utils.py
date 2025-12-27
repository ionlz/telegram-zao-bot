from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


def now(tzinfo: ZoneInfo) -> datetime:
    # 统一用带时区的时间，存储 ISO-8601 字符串（含 offset）
    return datetime.now(tz=tzinfo)


def fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def fmt_td(td: timedelta) -> str:
    sec = int(td.total_seconds())
    if sec < 0:
        sec = 0
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    if h > 0:
        return f"{h}小时{m}分{s}秒"
    if m > 0:
        return f"{m}分{s}秒"
    return f"{s}秒"


def day_range(now: datetime) -> tuple[datetime, datetime]:
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return start, end


def day_key(dt: datetime) -> str:
    # 使用本地时区的日期作为“今天”键
    return dt.date().isoformat()


def business_day_range(now: datetime, *, cutoff_hour: int = 4) -> tuple[datetime, datetime]:
    """
    业务日范围：默认以凌晨 4 点作为一天的边界。
    例如：2025-01-02 03:59 仍属于 2025-01-01 的业务日。
    """
    cutoff = now.replace(hour=cutoff_hour, minute=0, second=0, microsecond=0)
    if now < cutoff:
        cutoff = cutoff - timedelta(days=1)
    return cutoff, cutoff + timedelta(days=1)


def business_day_key(dt: datetime, *, cutoff_hour: int = 4) -> str:
    """
    业务日 key（YYYY-MM-DD）：默认以凌晨 4 点作为一天的边界。
    """
    start, _end = business_day_range(dt, cutoff_hour=cutoff_hour)
    return start.date().isoformat()


