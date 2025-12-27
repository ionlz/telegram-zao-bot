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


