from __future__ import annotations

import logging
from pathlib import Path

from telegram import Update
from telegram.ext import Application, CommandHandler
from telegram.request import HTTPXRequest

from config import load_settings
from zao_bot.handlers import HandlerDeps, cmd_ach, cmd_achrank, cmd_awake, cmd_rank, cmd_start, cmd_wan, cmd_zao
from zao_bot.messages import MessageCatalog
from zao_bot.storage.factory import get_storage


LOG = logging.getLogger("zao-bot")


def build_app(token: str, *, proxy_url: str | None = None) -> Application:
    builder = Application.builder().token(token)
    if proxy_url:
        # 同时用于 getUpdates 与其它 Bot API 请求（发消息/编辑消息等）
        builder = builder.request(HTTPXRequest(proxy_url=proxy_url)).get_updates_request(HTTPXRequest(proxy_url=proxy_url))

    app = builder.build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_start))
    app.add_handler(CommandHandler("zao", cmd_zao))
    app.add_handler(CommandHandler("wan", cmd_wan))
    app.add_handler(CommandHandler("awake", cmd_awake))
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

    app = build_app(settings.bot_token, proxy_url=settings.proxy_url)
    app.bot_data["deps"] = HandlerDeps(settings=settings, messages=msgs, storage=storage)
    app.run_polling(allowed_updates=Update.ALL_TYPES)


