from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from telegram import Update, User
from telegram.ext import ContextTypes

from config import Settings
from zao_bot import db
from zao_bot.messages import MessageCatalog
from zao_bot.time_utils import fmt_dt, fmt_td, now as tz_now


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

    if db.session_today_completed(deps.settings.db_path, chat_id=update.effective_chat.id, user_id=update.effective_user.id, now=now):
        await update.effective_message.reply_text(
            deps.messages.render("day_ended", name=display_name(update.effective_user))
        )
        return

    ok = db.check_in(deps.settings.db_path, chat_id=update.effective_chat.id, user_id=update.effective_user.id, ts=now)
    if ok:
        await update.effective_message.reply_text(
            deps.messages.render("checkin_ok", name=display_name(update.effective_user), time=fmt_dt(now))
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

    ok, dur, check_in_ts = db.check_out(deps.settings.db_path, chat_id=update.effective_chat.id, user_id=update.effective_user.id, ts=now)
    if not ok or dur is None or check_in_ts is None:
        if db.session_today_exists(deps.settings.db_path, chat_id=update.effective_chat.id, user_id=update.effective_user.id, now=now):
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
    if context.args:
        arg = context.args[0].strip().lower()
        if arg in {"all", "total", "overall"}:
            mode = "all"
        elif arg in {"today", "day", "daily"}:
            mode = "today"

    now = tz_now(deps.settings.tzinfo)
    rows = db.leaderboard(deps.settings.db_path, chat_id=update.effective_chat.id, mode=mode, now=now)
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


