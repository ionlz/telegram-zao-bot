from __future__ import annotations

import logging
from pathlib import Path

from telegram import Update
from telegram.ext import Application, CommandHandler

from config import load_settings
from zao_bot import db
from zao_bot.handlers import HandlerDeps, cmd_ach, cmd_achrank, cmd_awake, cmd_rank, cmd_start, cmd_wan, cmd_zao
from zao_bot.messages import MessageCatalog


LOG = logging.getLogger("zao-bot")


def build_app(token: str) -> Application:
    app = Application.builder().token(token).build()
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

    # 确保 SQLite 目录存在（WAL/SHM 也会写在同目录）
    Path(settings.db_path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
    db.init_db(settings.db_path)

    if not settings.bot_token:
        raise SystemExit("缺少环境变量 BOT_TOKEN（从 @BotFather 获取）。")

    LOG.info(
        "DB=%s TZ=%s CONFIG=%s MESSAGES=%s",
        settings.db_path,
        settings.tz_name,
        settings.config_path or "-",
        msgs.path or "-",
    )

    app = build_app(settings.bot_token)
    app.bot_data["deps"] = HandlerDeps(settings=settings, messages=msgs)
    app.run_polling(allowed_updates=Update.ALL_TYPES)


