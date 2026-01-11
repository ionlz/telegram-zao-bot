from __future__ import annotations

import logging
from pathlib import Path

from telegram import BotCommandScopeAllGroupChats, BotCommandScopeAllPrivateChats, BotCommandScopeDefault, Update
from telegram.ext import Application, CommandHandler, JobQueue
from telegram.request import HTTPXRequest

from config import load_settings
from zao_bot.handlers import HandlerDeps, cmd_ach, cmd_achrank, cmd_awake, cmd_gun, cmd_heatmap, cmd_rank, cmd_start, cmd_wake, cmd_wan, cmd_year, cmd_zao
from zao_bot.messages import MessageCatalog
from zao_bot.storage.factory import get_storage
from zao_bot.telegram_commands import default_bot_commands


LOG = logging.getLogger("zao-bot")


async def check_wake_reminders(context: ContextTypes.DEFAULT_TYPE) -> None:
    """定期检查是否有需要触发的提醒"""
    from datetime import datetime, timedelta

    deps: HandlerDeps = context.bot_data.get("deps")
    if not deps:
        return

    now = datetime.now(tz=deps.settings.tz)
    reminders = deps.storage.get_pending_reminders(now=now)

    for reminder in reminders:
        try:
            # 构建提醒消息
            user_mention = f'<a href="tg://user?id={reminder.user_id}">提醒</a>'
            message = f"☀️ {user_mention} 该起床啦！现在是 {now.strftime('%H:%M')}\n别忘了 /zao 签到开始新的一天~"

            await context.bot.send_message(
                chat_id=reminder.chat_id,
                text=message,
                parse_mode="HTML",
            )

            # 更新或删除提醒
            if reminder.repeat:
                # 重复提醒：计算下次触发时间（明天同一时间）
                next_trigger = reminder.next_trigger + timedelta(days=1)
                deps.storage.update_reminder_next_trigger(reminder_id=reminder.id, next_trigger=next_trigger)
            else:
                # 一次性提醒：删除
                deps.storage.delete_reminder(reminder_id=reminder.id)
        except Exception as e:
            # 记录错误但继续处理其他提醒
            LOG.exception(f"Wake reminder error for reminder_id={reminder.id}: {e}")


def build_app(
    token: str,
    *,
    proxy_url: str | None = None,
    auto_register_commands: bool = True,
) -> Application:
    async def _post_init(app: Application) -> None:
        if not auto_register_commands:
            return
        try:
            cmds = default_bot_commands()
            # Telegram 支持按 scope 设置不同的命令列表；这里为了在私聊/群组都稳定可见，设置三份。
            await app.bot.set_my_commands(cmds, scope=BotCommandScopeDefault())
            await app.bot.set_my_commands(cmds, scope=BotCommandScopeAllPrivateChats())
            await app.bot.set_my_commands(cmds, scope=BotCommandScopeAllGroupChats())
            LOG.info("已同步 Bot 命令到 BotFather（setMyCommands），共 %d 条", len(cmds))
        except Exception:
            # 命令同步失败不应导致 bot 无法启动（例如网络/代理问题）
            LOG.exception("同步 Bot 命令失败（setMyCommands），已忽略继续启动")

    builder = Application.builder().token(token).post_init(_post_init).job_queue(JobQueue())
    if proxy_url:
        # 同时用于 getUpdates 与其它 Bot API 请求（发消息/编辑消息等）
        # HTTPXRequest 使用 proxy 参数配置代理
        request = HTTPXRequest(proxy=proxy_url)
        builder = builder.request(request).get_updates_request(request)

    app = builder.build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_start))
    app.add_handler(CommandHandler("zao", cmd_zao))
    app.add_handler(CommandHandler("wan", cmd_wan))
    app.add_handler(CommandHandler("awake", cmd_awake))
    app.add_handler(CommandHandler("year", cmd_year))
    app.add_handler(CommandHandler("rank", cmd_rank))
    app.add_handler(CommandHandler("ach", cmd_ach))
    app.add_handler(CommandHandler("achievements", cmd_ach))
    app.add_handler(CommandHandler("achrank", cmd_achrank))
    app.add_handler(CommandHandler("heatmap", cmd_heatmap))
    app.add_handler(CommandHandler("gun", cmd_gun))
    app.add_handler(CommandHandler("wake", cmd_wake))

    # 添加定时任务：每分钟检查一次待触发的提醒
    if app.job_queue:
        app.job_queue.run_repeating(check_wake_reminders, interval=60, first=10)
        LOG.info("已启用 wake 提醒定时任务（每 60 秒检查一次）")
    else:
        LOG.warning("JobQueue 未启用，wake 提醒功能将不可用。提示：使用 Application.builder().job_queue(...) 启用")

    return app


def run() -> None:
    settings = load_settings()
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    msgs = MessageCatalog.load()

    storage = get_storage(settings)
    # SQLite 才需要本地目录
    if not settings.database_url:
        Path(settings.db_path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
    storage.init_db()

    if not settings.bot_token:
        raise SystemExit("缺少环境变量 BOT_TOKEN（从 @BotFather 获取）。")

    LOG.info(
        "DB=%s TZ=%s CONFIG=%s MESSAGES=%s PROXY=%s",
        (settings.database_url or settings.db_path),
        settings.tz_name,
        settings.config_path or "-",
        msgs.path or "-",
        ("set" if settings.proxy_url else "-"),
    )

    app = build_app(
        settings.bot_token,
        proxy_url=settings.proxy_url,
        auto_register_commands=settings.auto_register_commands,
    )
    app.bot_data["deps"] = HandlerDeps(settings=settings, messages=msgs, storage=storage)
    app.run_polling(allowed_updates=Update.ALL_TYPES)


