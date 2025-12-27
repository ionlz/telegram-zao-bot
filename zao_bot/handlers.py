from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from telegram import Update, User
from telegram.ext import ContextTypes

from config import Settings
from zao_bot import achievements
from zao_bot import db
from zao_bot.messages import MessageCatalog
from zao_bot.time_utils import business_day_key, fmt_dt, fmt_td, now as tz_now


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


def _upsert(update: Update, deps: HandlerDeps) -> None:
    if not update.effective_user or not update.effective_chat:
        return
    u = update.effective_user
    c = update.effective_chat
    db.upsert_user_and_chat(
        deps.settings.db_path,
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


async def cmd_zao(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    deps: HandlerDeps = context.bot_data["deps"]
    if not update.effective_chat or not update.effective_user:
        return
    _upsert(update, deps)
    now = tz_now(deps.settings.tzinfo)
    today_key = business_day_key(now, cutoff_hour=4)

    if db.session_today_completed(deps.settings.db_path, chat_id=update.effective_chat.id, user_id=update.effective_user.id, day=today_key):
        await update.effective_message.reply_text(
            deps.messages.render("day_ended", name=display_name(update.effective_user))
        )
        return

    ok = db.check_in(deps.settings.db_path, chat_id=update.effective_chat.id, user_id=update.effective_user.id, ts=now)
    if ok:
        await update.effective_message.reply_text(
            deps.messages.render("checkin_ok", name=display_name(update.effective_user), time=fmt_dt(now))
        )
        open_sess = db.get_open_session(deps.settings.db_path, chat_id=update.effective_chat.id, user_id=update.effective_user.id)
        if open_sess:
            # 今日第 N 个签到
            n = db.today_checkin_position(
                deps.settings.db_path,
                chat_id=update.effective_chat.id,
                session_id=open_sess.session_id,
                check_in=open_sess.check_in,
                day=today_key,
            )
            await update.effective_message.reply_text(deps.messages.render("checkin_order_today", n=n))

        # 成就：今日最早 / 连续最早
        open_sess = db.get_open_session(deps.settings.db_path, chat_id=update.effective_chat.id, user_id=update.effective_user.id)
        if open_sess:
            res = achievements.on_check_in(
                db_path=deps.settings.db_path,
                chat_id=update.effective_chat.id,
                user_id=update.effective_user.id,
                session_id=open_sess.session_id,
                check_in_ts=open_sess.check_in,
                now_ts=now,
            )
            if res.unlocked:
                names = [deps.messages.render(f"ach_name_{k}") for k in res.unlocked]
                await update.effective_message.reply_text(
                    deps.messages.render("ach_unlocked", achievements="、".join(names))
                )
        return

    open_sess = db.get_open_session(deps.settings.db_path, chat_id=update.effective_chat.id, user_id=update.effective_user.id)
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
    now = tz_now(deps.settings.tzinfo)
    today_key = business_day_key(now, cutoff_hour=4)

    ok, dur, check_in_ts, session_id = db.check_out(
        deps.settings.db_path,
        chat_id=update.effective_chat.id,
        user_id=update.effective_user.id,
        ts=now,
    )
    if not ok or dur is None or check_in_ts is None or session_id is None:
        if db.session_today_exists(deps.settings.db_path, chat_id=update.effective_chat.id, user_id=update.effective_user.id, day=today_key):
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
        db_path=deps.settings.db_path,
        chat_id=update.effective_chat.id,
        user_id=update.effective_user.id,
        session_id=session_id,
        check_in_ts=check_in_ts,
        duration=dur,
        now_ts=now,
    )
    if res.unlocked:
        names = [deps.messages.render(f"ach_name_{k}") for k in res.unlocked]
        await update.effective_message.reply_text(deps.messages.render("ach_unlocked", achievements="、".join(names)))


async def cmd_awake(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    deps: HandlerDeps = context.bot_data["deps"]
    if not update.effective_chat:
        return
    _upsert(update, deps)

    u = target_user(update)
    if not u:
        return
    now = tz_now(deps.settings.tzinfo)
    open_sess = db.get_open_session(deps.settings.db_path, chat_id=update.effective_chat.id, user_id=u.id)
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

    now = tz_now(deps.settings.tzinfo)
    rows = (
        db.leaderboard_global(deps.settings.db_path, mode=mode, now=now)
        if is_global
        else db.leaderboard(deps.settings.db_path, chat_id=update.effective_chat.id, mode=mode, now=now)
    )
    if is_global:
        title = deps.messages.render("rank_title_today_global") if mode == "today" else deps.messages.render("rank_title_all_global")
    else:
        title = deps.messages.render("rank_title_today") if mode == "today" else deps.messages.render("rank_title_all")
    if not rows:
        await update.effective_message.reply_text(deps.messages.render("rank_no_data", title=title))
        return

    lines: list[str] = [deps.messages.render("rank_header", title=title, time=fmt_dt(now))]
    for i, (_uid, name, sec) in enumerate(rows[:20], start=1):
        lines.append(
            deps.messages.render("rank_line", idx=i, name=name, awake=fmt_td(timedelta(seconds=sec)))
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
        db.get_achievement_stats_global(deps.settings.db_path, user_id=u.id)
        if is_global
        else db.get_achievement_stats(deps.settings.db_path, chat_id=update.effective_chat.id, user_id=u.id)
    )
    total_earliest = (
        db.get_achievement_count_global(deps.settings.db_path, user_id=u.id, key=achievements.ACH_DAILY_EARLIEST)
        if is_global
        else db.get_achievement_count(
            deps.settings.db_path,
            chat_id=update.effective_chat.id,
            user_id=u.id,
            key=achievements.ACH_DAILY_EARLIEST,
        )
    )
    if is_global:
        streak, _cid, ctitle = db.get_streak_best_global(deps.settings.db_path, user_id=u.id, key="earliest")
    else:
        streak = db.get_streak(deps.settings.db_path, chat_id=update.effective_chat.id, user_id=u.id, key="earliest")
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
            db.achievement_rank_by_count_global(deps.settings.db_path, key=achievements.ACH_DAILY_EARLIEST)
            if is_global
            else db.achievement_rank_by_count(deps.settings.db_path, chat_id=update.effective_chat.id, key=achievements.ACH_DAILY_EARLIEST)
        )
        lines = [title]
        for i, (_uid, name, count) in enumerate(rows, start=1):
            lines.append(deps.messages.render("ach_rank_line_count", idx=i, name=name, count=count))
        await update.effective_message.reply_text("\n".join(lines) if rows else deps.messages.render("ach_rank_empty"))
        return

    if kind in {"streak", "consecutive"}:
        title = deps.messages.render("ach_rank_title_streak_global") if is_global else deps.messages.render("ach_rank_title_streak")
        if is_global:
            rows = db.streak_rank_global(deps.settings.db_path, key="earliest")
        else:
            # 统一成 (uid,name,streak,chat_id,chat_title) 的结构
            local_rows = db.streak_rank(deps.settings.db_path, chat_id=update.effective_chat.id, key="earliest")
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
            db.achievement_rank_by_count_global(deps.settings.db_path, key=achievements.ACH_ONTIME_8H)
            if is_global
            else db.achievement_rank_by_count(deps.settings.db_path, chat_id=update.effective_chat.id, key=achievements.ACH_ONTIME_8H)
        )
        lines = [title]
        for i, (_uid, name, count) in enumerate(rows, start=1):
            lines.append(deps.messages.render("ach_rank_line_count", idx=i, name=name, count=count))
        await update.effective_message.reply_text("\n".join(lines) if rows else deps.messages.render("ach_rank_empty"))
        return

    if kind in {"longday", "12h", "12"}:
        title = deps.messages.render("ach_rank_title_longday_global") if is_global else deps.messages.render("ach_rank_title_longday")
        rows = (
            db.achievement_rank_by_count_global(deps.settings.db_path, key=achievements.ACH_LONGDAY_12H)
            if is_global
            else db.achievement_rank_by_count(deps.settings.db_path, chat_id=update.effective_chat.id, key=achievements.ACH_LONGDAY_12H)
        )
        lines = [title]
        for i, (_uid, name, count) in enumerate(rows, start=1):
            lines.append(deps.messages.render("ach_rank_line_count", idx=i, name=name, count=count))
        await update.effective_message.reply_text("\n".join(lines) if rows else deps.messages.render("ach_rank_empty"))
        return

    await update.effective_message.reply_text(deps.messages.render("ach_rank_help"))


