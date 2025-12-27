from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_MESSAGES: dict[str, str] = {
    "help": "📌 用法：/zao 签到，/wan 签退，/awake 查询清醒时长（可回复他人消息使用），/rank 今日排行榜 或 /rank all 总榜。",
    "day_ended": "🛌 {name} 今天已经结束，请休息吧。",
    "checkin_ok": "🌅 {name} ✅ 签到成功：{time}",
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
    "rank_line": "{idx}. {name} - {awake} 🔥",
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


