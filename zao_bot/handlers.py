from __future__ import annotations

import calendar
import random
from dataclasses import dataclass
from datetime import date, datetime, timezone
from datetime import timedelta

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, User
from telegram.ext import ContextTypes

from config import Settings
from zao_bot import achievements
from zao_bot.messages import MessageCatalog
from zao_bot.time_utils import business_day_key, fmt_dt, fmt_td, now as tz_now
from zao_bot.storage.base import Storage


def display_name(u: User) -> str:
    if u.username:
        return f"@{u.username}"
    name = " ".join([p for p in [u.first_name, u.last_name] if p])
    return name or str(u.id)


def target_user(update: Update) -> User | None:
    msg = update.effective_message
    if msg and msg.reply_to_message and msg.reply_to_message.from_user:
        return msg.reply_to_message.from_user
    return update.effective_user


@dataclass(frozen=True)
class HandlerDeps:
    settings: Settings
    messages: MessageCatalog
    storage: Storage


def event_time(update: Update, deps: HandlerDeps) -> datetime:
    """
    统一使用“用户消息发出时间”作为事件时间（而不是 bot 收到/处理时间）。
    Telegram 的 message.date 通常是 UTC 时间；这里会转换到配置的 TZ。
    """
    msg = update.effective_message
    if msg and getattr(msg, "date", None):
        dt: datetime = msg.date
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(deps.settings.tzinfo)
    return tz_now(deps.settings.tzinfo)


def _upsert(update: Update, deps: HandlerDeps) -> None:
    if not update.effective_user or not update.effective_chat:
        return
    u = update.effective_user
    c = update.effective_chat
    deps.storage.upsert_user_and_chat(
        user_id=u.id,
        username=u.username,
        first_name=u.first_name,
        last_name=u.last_name,
        chat_id=c.id,
        chat_title=getattr(c, "title", None),
        chat_type=c.type,
        updated_at=tz_now(deps.settings.tzinfo),
    )


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    deps: HandlerDeps = context.bot_data["deps"]
    _upsert(update, deps)
    await update.effective_message.reply_text(deps.messages.render("help"))


async def cmd_year(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    /year：返回当前年度的日期进度条（今年总天数 vs 今天是第几天）。
    """
    deps: HandlerDeps = context.bot_data["deps"]
    _upsert(update, deps)
    if not update.effective_message:
        return

    now = event_time(update, deps)
    today = now.date()
    y = today.year

    start = date(y, 1, 1)
    end = date(y + 1, 1, 1)
    total_days = (end - start).days
    day_no = (today - start).days + 1
    if total_days <= 0:
        total_days = 365
    if day_no < 1:
        day_no = 1
    if day_no > total_days:
        day_no = total_days

    ratio = day_no / total_days
    # 允许通过参数调更细：/year 48  (默认 20：更适配手机屏幕；范围限制避免太容易换行)
    bar_len = 20
    args = [a.strip() for a in (context.args or []) if a.strip()]
    if args:
        try:
            n = int(args[0])
            if 8 <= n <= 60:
                bar_len = n
        except ValueError:
            pass

    # 更细粒度的字符进度：每格 1/8（▏▎▍▌▋▊▉ + 满格用█）
    partial = ["", "▏", "▎", "▍", "▌", "▋", "▊", "▉"]
    full_char = "█"
    total_units = bar_len * 8
    filled_units = int(ratio * total_units)
    if filled_units < 0:
        filled_units = 0
    if filled_units > total_units:
        filled_units = total_units
    full_blocks, rem = divmod(filled_units, 8)
    bar = full_char * full_blocks
    if rem and len(bar) < bar_len:
        bar += partial[rem]
    bar = bar.ljust(bar_len, "　")
    bar = f"├{bar}┤"

    text = (
        f"{y}\n"
        f"{bar} {ratio * 100:.2f}%\n"
        f"{day_no}/{total_days} {today.isoformat()}"
    )
    await update.effective_message.reply_text(text)


async def cmd_zao(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    deps: HandlerDeps = context.bot_data["deps"]
    if not update.effective_chat or not update.effective_user:
        return
    _upsert(update, deps)
    now = event_time(update, deps)
    today_key = business_day_key(now, cutoff_hour=4)

    if deps.storage.session_today_completed(chat_id=update.effective_chat.id, user_id=update.effective_user.id, day=today_key):
        await update.effective_message.reply_text(
            deps.messages.render("day_ended", name=display_name(update.effective_user))
        )
        return

    ok = deps.storage.check_in(chat_id=update.effective_chat.id, user_id=update.effective_user.id, ts=now)
    if ok:
        # 签到成功 + 今日第N个签到
        open_sess = deps.storage.get_open_session(chat_id=update.effective_chat.id, user_id=update.effective_user.id, day=today_key)
        if open_sess:
            n = deps.storage.today_checkin_position(
                chat_id=update.effective_chat.id,
                session_id=open_sess.session_id,
                check_in=open_sess.check_in,
                day=today_key,
            )
            await update.effective_message.reply_text(
                deps.messages.render(
                    "checkin_ok_with_order",
                    name=display_name(update.effective_user),
                    time=fmt_dt(now),
                    n=n,
                )
            )

            # 成就：今日最早 / 连续最早（可单独发送）
            res = achievements.on_check_in(
                storage=deps.storage,
                chat_id=update.effective_chat.id,
                user_id=update.effective_user.id,
                session_id=open_sess.session_id,
                check_in_ts=open_sess.check_in,
                now_ts=now,
            )
            if res.unlocked:
                awarded = [k for k in res.unlocked if not achievements.is_single_achievement(k)]
                unlocked = [k for k in res.unlocked if achievements.is_single_achievement(k)]
                lines: list[str] = []
                if awarded:
                    names = [deps.messages.render(f"ach_name_{k}") for k in awarded]
                    # 兼容旧 messages.toml：没定义 ach_awarded 时退回 ach_unlocked
                    tpl = "ach_awarded" if "ach_awarded" in deps.messages.messages else "ach_unlocked"
                    lines.append(deps.messages.render(tpl, achievements="、".join(names)))
                if unlocked:
                    names = [deps.messages.render(f"ach_name_{k}") for k in unlocked]
                    lines.append(deps.messages.render("ach_unlocked", achievements="、".join(names)))
                await update.effective_message.reply_text("\n".join(lines))
        else:
            await update.effective_message.reply_text(
                deps.messages.render("checkin_ok", name=display_name(update.effective_user), time=fmt_dt(now))
            )
        return

    open_sess = deps.storage.get_open_session(chat_id=update.effective_chat.id, user_id=update.effective_user.id, day=today_key)
    if not open_sess:
        await update.effective_message.reply_text(deps.messages.render("checkin_inconsistent"))
        return

    await update.effective_message.reply_text(
        deps.messages.render(
            "checkin_already",
            name=display_name(update.effective_user),
            check_in=fmt_dt(open_sess.check_in),
            awake=fmt_td(now - open_sess.check_in),
        )
    )


async def cmd_wan(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    deps: HandlerDeps = context.bot_data["deps"]
    if not update.effective_chat or not update.effective_user:
        return
    _upsert(update, deps)
    now = event_time(update, deps)
    today_key = business_day_key(now, cutoff_hour=4)

    ok, dur, check_in_ts, session_id = deps.storage.check_out(
        chat_id=update.effective_chat.id,
        user_id=update.effective_user.id,
        ts=now,
    )
    if not ok or dur is None or check_in_ts is None or session_id is None:
        # 如果存在“跨业务日”的遗留未签退记录，按规则不允许用今天的 /wan 续接昨天
        any_open = deps.storage.get_open_session(chat_id=update.effective_chat.id, user_id=update.effective_user.id)
        if any_open:
            open_day = business_day_key(any_open.check_in, cutoff_hour=4)
            if open_day != today_key:
                await update.effective_message.reply_text(
                    deps.messages.render(
                        "checkout_cross_day",
                        name=display_name(update.effective_user),
                        day=open_day,
                    )
                )
                return
        if deps.storage.session_today_exists(chat_id=update.effective_chat.id, user_id=update.effective_user.id, day=today_key):
            await update.effective_message.reply_text(
                deps.messages.render("day_ended", name=display_name(update.effective_user))
            )
            return
        await update.effective_message.reply_text(
            deps.messages.render("checkout_not_checked_in", name=display_name(update.effective_user))
        )
        return

    await update.effective_message.reply_text(
        deps.messages.render(
            "checkout_ok",
            name=display_name(update.effective_user),
            time=fmt_dt(now),
            awake=fmt_td(dur),
            check_in=fmt_dt(check_in_ts),
        )
    )

    # 成就：准点下班 / 辛苦的一天
    res = achievements.on_check_out(
        storage=deps.storage,
        chat_id=update.effective_chat.id,
        user_id=update.effective_user.id,
        session_id=session_id,
        check_in_ts=check_in_ts,
        duration=dur,
        now_ts=now,
    )
    if res.unlocked:
        awarded = [k for k in res.unlocked if not achievements.is_single_achievement(k)]
        unlocked = [k for k in res.unlocked if achievements.is_single_achievement(k)]
        lines: list[str] = []
        if awarded:
            names = [deps.messages.render(f"ach_name_{k}") for k in awarded]
            tpl = "ach_awarded" if "ach_awarded" in deps.messages.messages else "ach_unlocked"
            lines.append(deps.messages.render(tpl, achievements="、".join(names)))
        if unlocked:
            names = [deps.messages.render(f"ach_name_{k}") for k in unlocked]
            lines.append(deps.messages.render("ach_unlocked", achievements="、".join(names)))
        await update.effective_message.reply_text("\n".join(lines))


async def cmd_awake(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    deps: HandlerDeps = context.bot_data["deps"]
    if not update.effective_chat:
        return
    _upsert(update, deps)

    u = target_user(update)
    if not u:
        return
    now = event_time(update, deps)
    today_key = business_day_key(now, cutoff_hour=4)
    open_sess = deps.storage.get_open_session(chat_id=update.effective_chat.id, user_id=u.id, day=today_key)
    if open_sess:
        await update.effective_message.reply_text(
            deps.messages.render(
                "awake_open",
                name=display_name(u),
                awake=fmt_td(now - open_sess.check_in),
                check_in=fmt_dt(open_sess.check_in),
            )
        )
        return
    await update.effective_message.reply_text(deps.messages.render("awake_none", name=display_name(u)))


async def cmd_rank(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    deps: HandlerDeps = context.bot_data["deps"]
    if not update.effective_chat:
        return
    _upsert(update, deps)

    mode = "today"
    is_global = False
    args = [a.strip().lower() for a in (context.args or []) if a.strip()]
    if "global" in args or "g" in args:
        is_global = True
        args = [a for a in args if a not in {"global", "g"}]
    if args:
        arg = args[0]
        if arg in {"all", "total", "overall"}:
            mode = "all"
        elif arg in {"today", "day", "daily"}:
            mode = "today"

    now = event_time(update, deps)
    today_key = business_day_key(now, cutoff_hour=4)
    rows = (
        deps.storage.leaderboard_global(mode=mode, now=now)
        if is_global
        else deps.storage.leaderboard(chat_id=update.effective_chat.id, mode=mode, now=now)
    )
    # 🔥/💤 标记也按业务日过滤，避免历史遗留未签退影响“今日”展示
    open_ids = (
        deps.storage.open_user_ids_global(day=today_key)
        if is_global
        else deps.storage.open_user_ids(chat_id=update.effective_chat.id, day=today_key)
    )
    if is_global:
        title = deps.messages.render("rank_title_today_global") if mode == "today" else deps.messages.render("rank_title_all_global")
    else:
        title = deps.messages.render("rank_title_today") if mode == "today" else deps.messages.render("rank_title_all")
    if not rows:
        await update.effective_message.reply_text(deps.messages.render("rank_no_data", title=title))
        return

    lines: list[str] = [deps.messages.render("rank_header", title=title, time=fmt_dt(now))]
    for i, (uid, name, sec) in enumerate(rows[:20], start=1):
        emoji = "🔥" if uid in open_ids else "💤"
        lines.append(
            deps.messages.render("rank_line", idx=i, name=name, awake=fmt_td(timedelta(seconds=sec)), emoji=emoji)
        )
    await update.effective_message.reply_text("\n".join(lines))


async def cmd_ach(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    deps: HandlerDeps = context.bot_data["deps"]
    if not update.effective_chat:
        return
    _upsert(update, deps)

    u = target_user(update)
    if not u:
        return

    args = [a.strip().lower() for a in (context.args or []) if a.strip()]
    is_global = ("global" in args) or ("g" in args)

    stats = (
        deps.storage.get_achievement_stats_global(user_id=u.id)
        if is_global
        else deps.storage.get_achievement_stats(chat_id=update.effective_chat.id, user_id=u.id)
    )
    total_earliest = (
        deps.storage.get_achievement_count_global(user_id=u.id, key=achievements.ACH_DAILY_EARLIEST)
        if is_global
        else deps.storage.get_achievement_count(
            chat_id=update.effective_chat.id,
            user_id=u.id,
            key=achievements.ACH_DAILY_EARLIEST,
        )
    )
    if is_global:
        streak, _cid, ctitle = deps.storage.get_streak_best_global(user_id=u.id, key="earliest")
    else:
        streak = deps.storage.get_streak(chat_id=update.effective_chat.id, user_id=u.id, key="earliest")
        ctitle = None

    lines: list[str] = [
        deps.messages.render("ach_header_global", name=display_name(u)) if is_global else deps.messages.render("ach_header", name=display_name(u))
    ]
    if stats:
        for key, count, _last in stats:
            lines.append(deps.messages.render("ach_line", ach=deps.messages.render(f"ach_name_{key}"), count=count))
    else:
        lines.append(deps.messages.render("ach_none"))

    if is_global:
        lines.append(deps.messages.render("ach_streak_earliest_global", streak=streak, total=total_earliest, chat=(ctitle or "-")))
    else:
        lines.append(deps.messages.render("ach_streak_earliest", streak=streak, total=total_earliest))
    await update.effective_message.reply_text("\n".join(lines))


async def cmd_achrank(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    deps: HandlerDeps = context.bot_data["deps"]
    if not update.effective_chat:
        return
    _upsert(update, deps)

    # 用法：
    # /achrank daily|streak|ontime|longday
    # /achrank global daily  (或 daily global)
    args = [a.strip().lower() for a in (context.args or []) if a.strip()]
    is_global = ("global" in args) or ("g" in args)
    args = [a for a in args if a not in {"global", "g"}]
    kind = (args[0] if args else "daily")
    if kind in {"daily", "earliest"}:
        title = deps.messages.render("ach_rank_title_daily_global") if is_global else deps.messages.render("ach_rank_title_daily")
        rows = (
            deps.storage.achievement_rank_by_count_global(key=achievements.ACH_DAILY_EARLIEST)
            if is_global
            else deps.storage.achievement_rank_by_count(chat_id=update.effective_chat.id, key=achievements.ACH_DAILY_EARLIEST)
        )
        lines = [title]
        for i, (_uid, name, count) in enumerate(rows, start=1):
            lines.append(deps.messages.render("ach_rank_line_count", idx=i, name=name, count=count))
        await update.effective_message.reply_text("\n".join(lines) if rows else deps.messages.render("ach_rank_empty"))
        return

    if kind in {"streak", "consecutive"}:
        title = deps.messages.render("ach_rank_title_streak_global") if is_global else deps.messages.render("ach_rank_title_streak")
        if is_global:
            rows = deps.storage.streak_rank_global(key="earliest")
        else:
            # 统一成 (uid,name,streak,chat_id,chat_title) 的结构
            local_rows = deps.storage.streak_rank(chat_id=update.effective_chat.id, key="earliest")
            rows = [(uid, name, streak, None, None) for (uid, name, streak) in local_rows]
        lines = [title]
        for i, (_uid, name, streak, _cid, ctitle) in enumerate(rows, start=1):
            if is_global:
                lines.append(deps.messages.render("ach_rank_line_streak_global", idx=i, name=name, streak=streak, chat=(ctitle or "-")))
            else:
                lines.append(deps.messages.render("ach_rank_line_streak", idx=i, name=name, streak=streak))
        await update.effective_message.reply_text("\n".join(lines) if rows else deps.messages.render("ach_rank_empty"))
        return

    if kind in {"ontime", "8h", "8"}:
        title = deps.messages.render("ach_rank_title_ontime_global") if is_global else deps.messages.render("ach_rank_title_ontime")
        rows = (
            deps.storage.achievement_rank_by_count_global(key=achievements.ACH_ONTIME_8H)
            if is_global
            else deps.storage.achievement_rank_by_count(chat_id=update.effective_chat.id, key=achievements.ACH_ONTIME_8H)
        )
        lines = [title]
        for i, (_uid, name, count) in enumerate(rows, start=1):
            lines.append(deps.messages.render("ach_rank_line_count", idx=i, name=name, count=count))
        await update.effective_message.reply_text("\n".join(lines) if rows else deps.messages.render("ach_rank_empty"))
        return

    if kind in {"longday", "12h", "12"}:
        title = deps.messages.render("ach_rank_title_longday_global") if is_global else deps.messages.render("ach_rank_title_longday")
        rows = (
            deps.storage.achievement_rank_by_count_global(key=achievements.ACH_LONGDAY_12H)
            if is_global
            else deps.storage.achievement_rank_by_count(chat_id=update.effective_chat.id, key=achievements.ACH_LONGDAY_12H)
        )
        lines = [title]
        for i, (_uid, name, count) in enumerate(rows, start=1):
            lines.append(deps.messages.render("ach_rank_line_count", idx=i, name=name, count=count))
        await update.effective_message.reply_text("\n".join(lines) if rows else deps.messages.render("ach_rank_empty"))
        return

    await update.effective_message.reply_text(deps.messages.render("ach_rank_help"))


def calculate_current_streak(storage: Storage, user_id: int, tz: timezone) -> int:
    """从今天倒推，计算连续签到天数"""
    today = business_day_key(datetime.now(tz=tz), cutoff_hour=4)
    # 获取最近365天的签到记录
    today_date = date.fromisoformat(today)
    start_date = (today_date - timedelta(days=365)).isoformat()
    checkin_days = storage.get_user_checkin_days(
        user_id=user_id, start_date=start_date, end_date=today
    )

    streak = 0
    current_day = today_date
    for _ in range(365):
        if current_day.isoformat() in checkin_days:
            streak += 1
            current_day -= timedelta(days=1)
        else:
            break
    return streak


def generate_heatmap(storage: Storage, user_id: int, year: int, month: int, tz: timezone) -> str:
    """生成用户的月度签到热力图"""
    # 获取当月的日期范围
    month_days = calendar.monthrange(year, month)[1]
    start_date = f"{year}-{month:02d}-01"
    end_date = f"{year}-{month:02d}-{month_days:02d}"

    # 获取签到日期集合
    checkin_days = storage.get_user_checkin_days(
        user_id=user_id, start_date=start_date, end_date=end_date
    )

    # 生成日历矩阵
    cal = calendar.monthcalendar(year, month)

    # 构建热力图（纯英文+字符，确保对齐）
    lines = [f"Check-in Heatmap: {year}-{month:02d}\n"]
    lines.append("Mon Tue Wed Thu Fri Sat Sun")

    for week_idx, week in enumerate(cal, start=1):
        line = ""
        for day in week:
            if day == 0:  # 空白日期
                line += "    "  # 4个空格对齐
            else:
                day_str = f"{year}-{month:02d}-{day:02d}"
                if day_str in checkin_days:
                    line += " ■  "  # 实心方块表示已签到
                else:
                    line += " □  "  # 空心方块表示未签到
        lines.append(line.rstrip())

    # 统计信息
    lines.append("")
    lines.append("■ Checked  □ Missed")

    # 计算连续签到天数
    streak = calculate_current_streak(storage, user_id, tz)
    total_days = len(checkin_days)

    lines.append(f"Streak: {streak} days")
    lines.append(f"Total: {total_days}/{month_days} days")

    return "\n".join(lines)


async def cmd_heatmap(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """显示用户的签到热力图"""
    deps: HandlerDeps = context.bot_data["deps"]
    if not update.effective_message or not update.effective_user:
        return

    # 支持查询别人的热力图（回复消息）
    target = target_user(update)
    if not target:
        return

    # 解析参数（可选：指定月份）
    args = context.args or []
    now = event_time(update, deps)
    year, month = now.year, now.month

    if args and len(args[0]) >= 7:  # YYYY-MM
        try:
            parts = args[0].split('-')
            year = int(parts[0])
            month = int(parts[1])
            if not (1 <= month <= 12):
                raise ValueError
        except (ValueError, IndexError):
            await update.effective_message.reply_text(
                "日期格式错误，请使用 YYYY-MM 格式（如 2026-01）"
            )
            return

    # 生成热力图
    heatmap_text = generate_heatmap(
        storage=deps.storage,
        user_id=target.id,
        year=year,
        month=month,
        tz=deps.settings.tzinfo,
    )

    await update.effective_message.reply_text(heatmap_text)


async def cmd_gun(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """俄罗斯轮盘游戏"""
    deps: HandlerDeps = context.bot_data["deps"]
    if not update.effective_chat or not update.effective_user or not update.effective_message:
        return

    _upsert(update, deps)
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    args = context.args or []

    # /gun n - 创建新游戏
    if args:
        try:
            chambers = int(args[0])
            if not (2 <= chambers <= 20):
                raise ValueError
        except (ValueError, IndexError):
            await update.effective_message.reply_text("弹槽数量必须是 2-20 之间的数字")
            return

        # 检查是否已有游戏
        existing = deps.storage.get_active_roulette(chat_id=chat_id)
        if existing:
            remaining = existing.chambers - existing.current_position
            await update.effective_message.reply_text(
                f"已经有一把枪在转了！\n剩余 {remaining} 发弹槽（1/{remaining} 概率中枪）"
            )
            return

        # 创建新游戏
        bullet_position = random.randint(1, chambers)
        deps.storage.create_roulette(
            chat_id=chat_id,
            chambers=chambers,
            bullet_position=bullet_position,
            created_by=user_id,
            created_at=event_time(update, deps),
        )

        await update.effective_message.reply_text(
            f"🔫 俄罗斯轮盘已装填！\n"
            f"弹槽: {chambers}发（1/{chambers} 概率中枪）\n"
            f"使用 /gun 扣动扳机\n"
            f"祝你好运~ 😈"
        )
        return

    # /gun - 扣动扳机
    game = deps.storage.get_active_roulette(chat_id=chat_id)
    if not game:
        await update.effective_message.reply_text("还没有装填弹药！\n使用 /gun 6 创建游戏")
        return

    # 扣动扳机
    new_position = game.current_position + 1
    is_shot = new_position == game.bullet_position

    # 记录尝试
    deps.storage.record_roulette_attempt(
        chat_id=chat_id,
        user_id=user_id,
        position=new_position,
        result="shot" if is_shot else "safe",
        created_at=event_time(update, deps),
    )

    if is_shot:
        # 中枪！游戏结束
        deps.storage.delete_roulette(chat_id=chat_id)
        await update.effective_message.reply_text(
            f"💥 BANG! {display_name(update.effective_user)} 中枪了！\n" f"游戏结束，使用 /gun n 重新开始"
        )
    else:
        # 安全
        remaining = game.chambers - new_position
        probability = f"1/{remaining}" if remaining > 0 else "?"

        deps.storage.update_roulette_position(chat_id=chat_id, position=new_position)

        await update.effective_message.reply_text(
            f"🔫 咔哒~ {display_name(update.effective_user)} 安全！\n" f"剩余弹槽: {remaining}发（{probability} 概率中枪）"
        )


async def cmd_wake(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """设置叫醒提醒"""
    deps: HandlerDeps = context.bot_data["deps"]
    if not update.effective_chat or not update.effective_user or not update.effective_message:
        return

    _upsert(update, deps)
    args = [a.strip() for a in (context.args or []) if a.strip()]

    # /wake list - 查看提醒列表
    if args and args[0] == "list":
        reminders = deps.storage.get_user_reminders(chat_id=update.effective_chat.id, user_id=update.effective_user.id)
        if not reminders:
            await update.effective_message.reply_text("你还没有设置提醒")
            return

        text = "⏰ 你的叫醒提醒:\n"
        for r in reminders:
            text += f"- {r.wake_time} {'(每天)' if r.repeat else ''}\n"
        await update.effective_message.reply_text(text)
        return

    # /wake cancel - 取消提醒
    if args and args[0] == "cancel":
        deps.storage.delete_user_reminders(chat_id=update.effective_chat.id, user_id=update.effective_user.id)
        await update.effective_message.reply_text("已取消所有提醒")
        return

    # /wake HH:MM - 设置提醒
    if not args:
        await update.effective_message.reply_text("用法: /wake 07:00 或 /wake list 或 /wake cancel")
        return

    # 解析时间
    time_str = args[0]
    try:
        hour, minute = map(int, time_str.split(':'))
        if not (0 <= hour < 24 and 0 <= minute < 60):
            raise ValueError
    except (ValueError, IndexError):
        await update.effective_message.reply_text("时间格式错误，请使用 HH:MM 格式（如 07:30）")
        return

    # 计算下次触发时间（明天的这个时间）
    now = event_time(update, deps)
    next_trigger = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if next_trigger <= now:
        next_trigger += timedelta(days=1)

    # 保存提醒
    deps.storage.create_reminder(
        chat_id=update.effective_chat.id,
        user_id=update.effective_user.id,
        wake_time=time_str,
        next_trigger=next_trigger,
        repeat=False,  # 默认一次性，未来可扩展
        created_at=now,
    )

    await update.effective_message.reply_text(f"⏰ 叫醒提醒已设置！\n明天 {time_str} 我会在这里@你~")


async def cmd_rsp(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """石头剪刀布游戏"""
    deps: HandlerDeps = context.bot_data["deps"]
    if not update.effective_chat or not update.effective_user or not update.effective_message:
        return

    _upsert(update, deps)

    # /rsp stats - 查看统计
    args = [a.strip().lower() for a in (context.args or []) if a.strip()]
    if args and args[0] in {"stats", "stat", "statistics"}:
        # 检查是否有 global 参数
        is_global = "global" in args or "g" in args

        # 检查是否查询别人的统计（回复消息）
        target = target_user(update)
        if not target:
            return

        if is_global:
            total, wins, losses, draws = deps.storage.get_rsp_stats_global(user_id=target.id)
            title = f"📊 {display_name(target)} 的全局石头剪刀布战绩"
        else:
            total, wins, losses, draws = deps.storage.get_rsp_stats(
                chat_id=update.effective_chat.id,
                user_id=target.id
            )
            title = f"📊 {display_name(target)} 在本群的石头剪刀布战绩"

        if total == 0:
            await update.effective_message.reply_text(f"{title}\n\n还没有游戏记录")
            return

        win_rate = (wins / total * 100) if total > 0 else 0
        stats_msg = (
            f"{title}\n\n"
            f"总场次: {total}\n"
            f"胜: {wins} ({win_rate:.1f}%)\n"
            f"负: {losses}\n"
            f"平: {draws}"
        )
        await update.effective_message.reply_text(stats_msg)
        return

    # 检查是否有待处理的游戏
    pending = deps.storage.get_pending_rsp_game(
        chat_id=update.effective_chat.id,
        user_id=update.effective_user.id
    )
    if pending:
        await update.effective_message.reply_text(
            "你还有一局未完成的游戏！请先完成当前游戏。"
        )
        return

    # 获取对手（必须 @ 某人或回复某人的消息）
    opponent = None
    if update.effective_message.reply_to_message and update.effective_message.reply_to_message.from_user:
        opponent = update.effective_message.reply_to_message.from_user
    elif update.effective_message.entities:
        # 检查是否有 @mention
        for entity in update.effective_message.entities:
            if entity.type == "mention":
                # 无法直接获取 user_id，需要用户回复消息方式
                pass
            elif entity.type == "text_mention" and entity.user:
                opponent = entity.user
                break

    if not opponent:
        await update.effective_message.reply_text(
            "请回复某人的消息或 @某人 来发起挑战！\n用法: /rsp @用户名"
        )
        return

    if opponent.id == update.effective_user.id:
        await update.effective_message.reply_text("不能和自己玩！")
        return

    if opponent.is_bot:
        await update.effective_message.reply_text("不能和机器人玩！")
        return

    # 检查对手是否有待处理的游戏
    opponent_pending = deps.storage.get_pending_rsp_game(
        chat_id=update.effective_chat.id,
        user_id=opponent.id
    )
    if opponent_pending:
        await update.effective_message.reply_text(
            f"{display_name(opponent)} 还有一局未完成的游戏！"
        )
        return

    # 创建游戏按钮
    keyboard = [
        [
            InlineKeyboardButton("✊ 石头", callback_data="rsp:rock"),
            InlineKeyboardButton("✋ 布", callback_data="rsp:paper"),
            InlineKeyboardButton("✌️ 剪刀", callback_data="rsp:scissors"),
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    # 发送游戏消息
    msg = await update.effective_message.reply_text(
        f"🎮 {display_name(update.effective_user)} 向 {display_name(opponent)} 发起了石头剪刀布挑战！\n\n"
        f"请双方点击下方按钮选择：",
        reply_markup=reply_markup
    )

    # 创建游戏记录
    deps.storage.create_rsp_game(
        chat_id=update.effective_chat.id,
        challenger_id=update.effective_user.id,
        opponent_id=opponent.id,
        message_id=msg.message_id,
        created_at=event_time(update, deps)
    )


async def rsp_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """处理石头剪刀布按钮点击"""
    deps: HandlerDeps = context.bot_data["deps"]
    query = update.callback_query
    if not query or not query.data or not query.message or not query.from_user:
        return

    await query.answer()

    # 解析 callback_data: "rsp:rock" / "rsp:paper" / "rsp:scissors"
    parts = query.data.split(":")
    if len(parts) != 2 or parts[0] != "rsp":
        return

    choice = parts[1]  # "rock", "paper", "scissors"
    if choice not in {"rock", "paper", "scissors"}:
        return

    # 查找游戏
    game = deps.storage.get_pending_rsp_game(
        chat_id=query.message.chat_id,
        user_id=query.from_user.id
    )

    if not game:
        await query.answer("找不到你的游戏记录！", show_alert=False)
        return

    # 检查是否是游戏参与者
    if query.from_user.id not in {game.challenger_id, game.opponent_id}:
        await query.answer("这不是你的游戏！", show_alert=False)
        return

    # 检查是否已经选择过
    is_challenger = query.from_user.id == game.challenger_id
    if is_challenger and game.challenger_choice:
        await query.answer("你已经做过选择了！", show_alert=False)
        return
    if not is_challenger and game.opponent_choice:
        await query.answer("你已经做过选择了！", show_alert=False)
        return

    # 保存选择
    deps.storage.update_rsp_choice(
        game_id=game.id,
        user_id=query.from_user.id,
        choice=choice
    )

    # 重新获取游戏状态
    game = deps.storage.get_rsp_game(game_id=game.id)
    if not game:
        return

    # 获取用户信息
    try:
        challenger = await context.bot.get_chat_member(game.chat_id, game.challenger_id)
        opponent = await context.bot.get_chat_member(game.chat_id, game.opponent_id)
        challenger_name = display_name(challenger.user)
        opponent_name = display_name(opponent.user)
    except Exception:
        challenger_name = str(game.challenger_id)
        opponent_name = str(game.opponent_id)

    # 检查是否双方都已选择
    if game.challenger_choice and game.opponent_choice:
        # 游戏结束，计算结果
        result = _determine_rsp_winner(game.challenger_choice, game.opponent_choice)

        # 格式化选择
        choice_emoji = {
            "rock": "✊ 石头",
            "paper": "✋ 布",
            "scissors": "✌️ 剪刀"
        }

        # 构建结果消息
        winner_id = None
        if result == "challenger":
            result_text = f"🎉 {challenger_name} 获胜！"
            winner_id = game.challenger_id
        elif result == "opponent":
            result_text = f"🎉 {opponent_name} 获胜！"
            winner_id = game.opponent_id
        else:
            result_text = "🤝 平局！"
            winner_id = None

        result_msg = (
            f"🎮 石头剪刀布结果：\n\n"
            f"{challenger_name}: {choice_emoji[game.challenger_choice]}\n"
            f"{opponent_name}: {choice_emoji[game.opponent_choice]}\n\n"
            f"{result_text}"
        )

        # 更新消息（移除按钮）
        await query.edit_message_text(result_msg)

        # 标记游戏完成并记录获胜者
        deps.storage.complete_rsp_game(game_id=game.id, winner_id=winner_id)
    else:
        # 还在等待另一方选择 - 更新消息显示进度
        if game.challenger_choice and not game.opponent_choice:
            waiting_msg = f"🎮 {challenger_name} 向 {opponent_name} 发起了石头剪刀布挑战！\n\n✅ {challenger_name} 已选择\n⏳ 等待 {opponent_name} 选择..."
        elif not game.challenger_choice and game.opponent_choice:
            waiting_msg = f"🎮 {challenger_name} 向 {opponent_name} 发起了石头剪刀布挑战！\n\n⏳ 等待 {challenger_name} 选择...\n✅ {opponent_name} 已选择"
        else:
            waiting_msg = f"🎮 {challenger_name} 向 {opponent_name} 发起了石头剪刀布挑战！\n\n请双方点击下方按钮选择："

        # 保留按钮，更新文本
        keyboard = [
            [
                InlineKeyboardButton("✊ 石头", callback_data="rsp:rock"),
                InlineKeyboardButton("✋ 布", callback_data="rsp:paper"),
                InlineKeyboardButton("✌️ 剪刀", callback_data="rsp:scissors"),
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(waiting_msg, reply_markup=reply_markup)
        await query.answer("你的选择已记录！", show_alert=False)


def _determine_rsp_winner(challenger_choice: str, opponent_choice: str) -> str:
    """判断胜负
    Returns: "challenger", "opponent", or "draw"
    """
    if challenger_choice == opponent_choice:
        return "draw"

    win_conditions = {
        "rock": "scissors",     # 石头赢剪刀
        "paper": "rock",        # 布赢石头
        "scissors": "paper"     # 剪刀赢布
    }

    if win_conditions.get(challenger_choice) == opponent_choice:
        return "challenger"
    return "opponent"


