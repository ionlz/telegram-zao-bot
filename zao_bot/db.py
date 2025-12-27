from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta

from zao_bot.time_utils import day_range


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db(db_path: str) -> None:
    with connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
              user_id INTEGER PRIMARY KEY,
              username TEXT,
              first_name TEXT,
              last_name TEXT,
              updated_at TEXT NOT NULL
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chats (
              chat_id INTEGER PRIMARY KEY,
              title TEXT,
              chat_type TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              chat_id INTEGER NOT NULL,
              user_id INTEGER NOT NULL,
              check_in TEXT NOT NULL,
              check_out TEXT,
              FOREIGN KEY(chat_id) REFERENCES chats(chat_id),
              FOREIGN KEY(user_id) REFERENCES users(user_id)
            );
            """
        )
        # 每个(群,人)只允许存在一条未签退记录
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_open_session
            ON sessions(chat_id, user_id)
            WHERE check_out IS NULL;
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_sessions_chat_checkin
            ON sessions(chat_id, check_in);
            """
        )


def upsert_user_and_chat(
    db_path: str,
    *,
    user_id: int,
    username: str | None,
    first_name: str | None,
    last_name: str | None,
    chat_id: int,
    chat_title: str | None,
    chat_type: str,
    updated_at: datetime,
) -> None:
    now = updated_at.isoformat()
    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO users(user_id, username, first_name, last_name, updated_at)
            VALUES(?,?,?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET
              username=excluded.username,
              first_name=excluded.first_name,
              last_name=excluded.last_name,
              updated_at=excluded.updated_at;
            """,
            (user_id, username, first_name, last_name, now),
        )
        conn.execute(
            """
            INSERT INTO chats(chat_id, title, chat_type, updated_at)
            VALUES(?,?,?,?)
            ON CONFLICT(chat_id) DO UPDATE SET
              title=excluded.title,
              chat_type=excluded.chat_type,
              updated_at=excluded.updated_at;
            """,
            (chat_id, chat_title, chat_type, now),
        )


@dataclass(frozen=True)
class OpenSession:
    session_id: int
    check_in: datetime


def get_open_session(db_path: str, *, chat_id: int, user_id: int) -> OpenSession | None:
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT id, check_in
            FROM sessions
            WHERE chat_id=? AND user_id=? AND check_out IS NULL
            ORDER BY id DESC
            LIMIT 1;
            """,
            (chat_id, user_id),
        ).fetchone()
    if not row:
        return None
    return OpenSession(session_id=int(row["id"]), check_in=datetime.fromisoformat(row["check_in"]))


def check_in(db_path: str, *, chat_id: int, user_id: int, ts: datetime) -> bool:
    try:
        with connect(db_path) as conn:
            conn.execute(
                "INSERT INTO sessions(chat_id, user_id, check_in, check_out) VALUES(?,?,?,NULL);",
                (chat_id, user_id, ts.isoformat()),
            )
        return True
    except sqlite3.IntegrityError:
        return False


def check_out(
    db_path: str, *, chat_id: int, user_id: int, ts: datetime
) -> tuple[bool, timedelta | None, datetime | None]:
    open_sess = get_open_session(db_path, chat_id=chat_id, user_id=user_id)
    if not open_sess:
        return False, None, None
    if ts < open_sess.check_in:
        ts = open_sess.check_in
    with connect(db_path) as conn:
        conn.execute("UPDATE sessions SET check_out=? WHERE id=?;", (ts.isoformat(), open_sess.session_id))
    return True, ts - open_sess.check_in, open_sess.check_in


def session_today_exists(db_path: str, *, chat_id: int, user_id: int, now: datetime) -> bool:
    start, end = day_range(now)
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT 1
            FROM sessions
            WHERE chat_id=? AND user_id=? AND check_in >= ? AND check_in < ?
            LIMIT 1;
            """,
            (chat_id, user_id, start.isoformat(), end.isoformat()),
        ).fetchone()
    return row is not None


def session_today_completed(db_path: str, *, chat_id: int, user_id: int, now: datetime) -> bool:
    start, end = day_range(now)
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT 1
            FROM sessions
            WHERE chat_id=? AND user_id=? AND check_in >= ? AND check_in < ? AND check_out IS NOT NULL
            LIMIT 1;
            """,
            (chat_id, user_id, start.isoformat(), end.isoformat()),
        ).fetchone()
    return row is not None


def leaderboard(db_path: str, *, chat_id: int, mode: str, now: datetime) -> list[tuple[int, str, int]]:
    where = ""
    params: list[object] = [chat_id]
    if mode == "today":
        start, end = day_range(now)
        where = "AND s.check_in >= ? AND s.check_in < ?"
        params.extend([start.isoformat(), end.isoformat()])

    with connect(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT
              u.user_id AS user_id,
              COALESCE(u.username, (u.first_name || ' ' || COALESCE(u.last_name,''))) AS name,
              SUM(
                CASE
                  WHEN s.check_out IS NULL THEN
                    CAST((julianday(?) - julianday(s.check_in)) * 86400 AS INTEGER)
                  ELSE
                    CAST((julianday(s.check_out) - julianday(s.check_in)) * 86400 AS INTEGER)
                END
              ) AS seconds
            FROM sessions s
            JOIN users u ON u.user_id = s.user_id
            WHERE s.chat_id = ?
            {where}
            GROUP BY u.user_id
            ORDER BY seconds DESC;
            """,
            (now.isoformat(), *params),
        ).fetchall()

    out: list[tuple[int, str, int]] = []
    for r in rows:
        sec = int(r["seconds"] or 0)
        name = (r["name"] or str(r["user_id"])).strip()
        if name and " " not in name and not name.isdigit() and not name.startswith("@"):
            name = f"@{name}"
        out.append((int(r["user_id"]), name, sec))
    return out


