from __future__ import annotations

from telegram import BotCommand


def default_bot_commands() -> list[BotCommand]:
    """
    用于 Telegram 的 setMyCommands（等同 BotFather 的 /setcommands）。

    说明：
    - 这里只注册“主要命令”（会显示在客户端命令菜单里）。
    - 代码里仍然可以继续支持别名（例如 /achievements），但不一定要展示出来。
    """
    return [
        BotCommand("start", "显示帮助/指令说明"),
        BotCommand("help", "显示帮助/指令说明"),
        BotCommand("zao", "开始新的一天~"),
        BotCommand("wan", "准备休息吧~"),
        BotCommand("awake", "我还醒着吗?（可回复某人）"),
        BotCommand("year", "今年进度条（按当前日期）"),
        BotCommand("rank", "让我看看!（可加 all/global）"),
        BotCommand("ach", "应该得记下些什么（可加 global/可回复某人）"),
        BotCommand("achrank", "你们都咋样了（daily/streak/ontime/longday，可加 global）"),
    ]


