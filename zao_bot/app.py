from __future__ import annotations

import logging
from pathlib import Path

from telegram import BotCommandScopeAllGroupChats, BotCommandScopeAllPrivateChats, BotCommandScopeDefault, Update
from telegram.ext import Application, CommandHandler
from telegram.request import HTTPXRequest

from config import load_settings
from zao_bot.handlers import HandlerDeps, cmd_ach, cmd_achrank, cmd_awake, cmd_rank, cmd_start, cmd_wan, cmd_year, cmd_zao
from zao_bot.messages import MessageCatalog
from zao_bot.storage.factory import get_storage
from zao_bot.telegram_commands import default_bot_commands


LOG = logging.getLogger("zao-bot")


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

    builder = Application.builder().token(token).post_init(_post_init)
    if proxy_url:
        # 同时用于 getUpdates 与其它 Bot API 请求（发消息/编辑消息等）
        builder = builder.request(HTTPXRequest(proxy_url=proxy_url)).get_updates_request(HTTPXRequest(proxy_url=proxy_url))

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


