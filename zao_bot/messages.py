from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_MESSAGES: dict[str, str] = {
    "help": "📌 指令说明：\n/zao 签到\n/wan 签退\n/awake 查询清醒时长（可回复某人消息后查询 TA）\n/rank 今日排行榜（/rank all 总榜；加 global=全局，例如：/rank global 或 /rank all global）\n/ach 成就查询（可加 global；也可回复某人消息后 /ach 查询 TA）\n/achrank 成就排行榜（daily｜streak｜ontime｜longday；可加 global，例如：/achrank global daily）\n\n🕓 说明：本 bot 的“今日”按业务日计算：凌晨 04:00 ~ 次日 04:00。",
    "day_ended": "🛌 {name} 今天已经结束，请休息吧。",
    "checkin_ok": "🌅 {name} ✅ 签到成功：{time}",
    "checkin_ok_with_order": "🌅 {name} ✅ 签到成功：{time}（今日第 {n} 个）",
    "checkin_order_today": "📍 你是今日第 {n} 个签到的",
    "checkin_inconsistent": "⚠️ 你可能已经签到过了，但我没查到未签退记录；请稍后重试。",
    "checkin_already": "⏱️ {name} 你已经签到过了（{check_in}），已清醒 {awake}。",
    "checkout_ok": "🌙 {name} 💤 签退成功：{time}\n本次清醒：{awake}（从 {check_in} 开始）",
    "checkout_not_checked_in": "🙋 {name} 你还没有签到（/zao）哦。",
    "awake_open": "👀 {name} 当前已清醒 {awake}（签到时间：{check_in}）",
    "awake_none": "📭 {name} 当前没有未签退记录（可能已经签退 /wan）。",
    "rank_no_data": "📊 {title}：暂无数据。先 /zao 签到吧～",
    "rank_header": "📊 {title}（统计到 {time}）",
    "rank_title_today": "🏆 今日清醒排行榜",
    "rank_title_all": "🏆 总清醒排行榜",
    "rank_title_today_global": "🌐 今日清醒排行榜（全局）",
    "rank_title_all_global": "🌐 总清醒排行榜（全局）",
    "rank_line": "{idx}. {name} - {awake} 🔥",
    # --- Achievements ---
    "ach_unlocked": "🎉 解锁成就：{achievements}",
    "ach_name_daily_earliest": "🥇 今日最早",
    "ach_name_streak_earliest_7": "🔥 连续最早 7 天",
    "ach_name_ontime_8h": "⏰ 准点下班",
    "ach_name_longday_12h": "💪 辛苦的一天",
    "ach_header": "🏅 {name} 的成就",
    "ach_header_global": "🌐🏅 {name} 的成就（全局）",
    "ach_line": "- {ach} × {count}",
    "ach_none": "暂无成就记录，先 /zao 开始吧～",
    "ach_streak_earliest": "📈 当前“今日最早”连胜：{streak} 天｜累计最早：{total} 天",
    "ach_streak_earliest_global": "🌐📈 最强“今日最早”连胜：{streak} 天（来自：{chat}）｜累计最早：{total} 天",
    "ach_rank_help": "📊 用法：/achrank daily｜streak｜ontime｜longday",
    "ach_rank_empty": "📭 暂无排行榜数据。",
    "ach_rank_title_daily": "🥇 成就榜：今日最早（累计天数）",
    "ach_rank_title_streak": "🔥 成就榜：连续今日最早（当前连胜）",
    "ach_rank_title_ontime": "⏰ 成就榜：准点下班（累计次数）",
    "ach_rank_title_longday": "💪 成就榜：辛苦的一天（累计次数）",
    "ach_rank_title_daily_global": "🌐🥇 成就榜：今日最早（全局累计）",
    "ach_rank_title_streak_global": "🌐🔥 成就榜：连续今日最早（全局最强连胜）",
    "ach_rank_title_ontime_global": "🌐⏰ 成就榜：准点下班（全局累计）",
    "ach_rank_title_longday_global": "🌐💪 成就榜：辛苦的一天（全局累计）",
    "ach_rank_line_count": "{idx}. {name} - {count}",
    "ach_rank_line_streak": "{idx}. {name} - {streak} 天",
    "ach_rank_line_streak_global": "{idx}. {name} - {streak} 天（{chat}）",
}


def _read_toml(path: str) -> dict[str, Any]:
    try:
        import tomllib  # py>=3.11
    except Exception:
        return {}

    try:
        with open(path, "rb") as f:
            data = tomllib.load(f)
    except FileNotFoundError:
        return {}
    except Exception:
        return {}

    if isinstance(data, dict):
        return data
    return {}


@dataclass(frozen=True)
class MessageCatalog:
    messages: dict[str, str]
    path: str | None = None

    @staticmethod
    def load() -> "MessageCatalog":
        """
        优先级：ZAO_MESSAGES 指定的 toml > ./messages.toml > 默认文案

        messages.toml 为扁平 key-value，例如：
        help = "..."
        checkin_ok = "{name} ✅ 签到成功：{time}"
        """
        default_path = str(Path.cwd() / "messages.toml")
        path = os.getenv("ZAO_MESSAGES", default_path)
        data = _read_toml(path) if path else {}

        merged = dict(DEFAULT_MESSAGES)
        for k, v in data.items():
            if isinstance(k, str) and isinstance(v, str):
                merged[k] = v

        effective_path: str | None = None
        if path and (os.path.exists(path) or os.getenv("ZAO_MESSAGES")):
            effective_path = path

        return MessageCatalog(messages=merged, path=effective_path)

    def render(self, key: str, **kwargs: Any) -> str:
        tpl = self.messages.get(key) or DEFAULT_MESSAGES.get(key) or key
        try:
            return tpl.format(**kwargs)
        except Exception:
            # 模板里占位符错误时，退回原字符串，避免 bot 崩溃
            return tpl


