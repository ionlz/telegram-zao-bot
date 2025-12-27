from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta

from zao_bot.time_utils import business_day_key, business_day_range, day_range


def connect(db_path: str) -> sqlite3.Connection:
    # check_same_thread=False 便于在不同线程中使用连接（PTB 默认在 event loop 里，但这里更稳妥）
    conn = sqlite3.connect(db_path, timeout=5, check_same_thread=False)
    conn.row_factory = sqlite3.Row

    # --- 并发与可靠性 ---
    # WAL：读写并发更好；崩溃恢复时会自动回放 -wal 文件
    conn.execute(f"PRAGMA journal_mode={os.getenv('SQLITE_JOURNAL_MODE', 'WAL')};")
    # NORMAL：WAL 下常见推荐值，性能/可靠性平衡；需要更强保证可改为 FULL
    conn.execute(f"PRAGMA synchronous={os.getenv('SQLITE_SYNCHRONOUS', 'NORMAL')};")
    # 遇到写锁等待更久，减少 “database is locked”
    conn.execute(f"PRAGMA busy_timeout={int(os.getenv('SQLITE_BUSY_TIMEOUT_MS', '5000'))};")
    # 自动 checkpoint 频率（单位：page），控制 -wal 增长；可按需调整
    conn.execute(f"PRAGMA wal_autocheckpoint={int(os.getenv('SQLITE_WAL_AUTOCHECKPOINT', '1000'))};")
    # 临时表走内存，减少 IO
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def wal_checkpoint(db_path: str, *, mode: str = "PASSIVE") -> tuple[int, int, int]:
    """
    手动触发 WAL checkpoint，返回 (busy, log, checkpointed)
    mode: PASSIVE | FULL | RESTART | TRUNCATE
    """
    mode_u = mode.upper()
    if mode_u not in {"PASSIVE", "FULL", "RESTART", "TRUNCATE"}:
        mode_u = "PASSIVE"
    with connect(db_path) as conn:
        row = conn.execute(f"PRAGMA wal_checkpoint({mode_u});").fetchone()
    if not row:
        return (0, 0, 0)
    return (int(row[0]), int(row[1]), int(row[2]))


def integrity_check(db_path: str) -> list[str]:
    """
    返回 integrity_check 结果；正常情况下是 ["ok"]。
    """
    with connect(db_path) as conn:
        rows = conn.execute("PRAGMA integrity_check;").fetchall()
    return [str(r[0]) for r in rows] if rows else ["ok"]


def backup_to(db_path: str, *, backup_path: str) -> None:
    """
    在线备份：把当前数据库备份到 backup_path（推荐定期做）。
    """
    src = connect(db_path)
    try:
        dst = sqlite3.connect(backup_path)
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()


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
              session_day TEXT,             -- YYYY-MM-DD（业务日：默认凌晨4点切换）
              check_in TEXT NOT NULL,
              check_out TEXT,
              FOREIGN KEY(chat_id) REFERENCES chats(chat_id),
              FOREIGN KEY(user_id) REFERENCES users(user_id)
            );
            """
        )
        # 旧库迁移：补齐 session_day 并回填
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(sessions);").fetchall()}
        if "session_day" not in cols:
            conn.execute("ALTER TABLE sessions ADD COLUMN session_day TEXT;")
            rows = conn.execute("SELECT id, check_in FROM sessions WHERE session_day IS NULL OR session_day='';").fetchall()
            for r in rows:
                try:
                    dt = datetime.fromisoformat(str(r["check_in"]))
                    sday = business_day_key(dt, cutoff_hour=4)
                except Exception:
                    sday = None
                if sday:
                    conn.execute("UPDATE sessions SET session_day=? WHERE id=?;", (sday, int(r["id"])))
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
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_sessions_chat_day
            ON sessions(chat_id, session_day);
            """
        )

        # --- Achievements ---
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_earliest (
              chat_id INTEGER NOT NULL,
              day TEXT NOT NULL,               -- YYYY-MM-DD (按 TZ)
              user_id INTEGER NOT NULL,
              session_id INTEGER NOT NULL,
              check_in TEXT NOT NULL,
              created_at TEXT NOT NULL,
              PRIMARY KEY(chat_id, day),
              FOREIGN KEY(chat_id) REFERENCES chats(chat_id),
              FOREIGN KEY(user_id) REFERENCES users(user_id),
              FOREIGN KEY(session_id) REFERENCES sessions(id)
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS streaks (
              chat_id INTEGER NOT NULL,
              user_id INTEGER NOT NULL,
              key TEXT NOT NULL,              -- e.g. earliest
              last_day TEXT NOT NULL,         -- YYYY-MM-DD
              streak INTEGER NOT NULL,
              updated_at TEXT NOT NULL,
              PRIMARY KEY(chat_id, user_id, key),
              FOREIGN KEY(chat_id) REFERENCES chats(chat_id),
              FOREIGN KEY(user_id) REFERENCES users(user_id)
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS achievement_events (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              chat_id INTEGER NOT NULL,
              user_id INTEGER NOT NULL,
              key TEXT NOT NULL,              -- daily_earliest | streak_earliest_7 | ontime_8h | longday_12h
              day TEXT,                       -- YYYY-MM-DD（可空）
              session_id INTEGER,             -- 关联某次签到记录（可空）
              created_at TEXT NOT NULL,
              FOREIGN KEY(chat_id) REFERENCES chats(chat_id),
              FOREIGN KEY(user_id) REFERENCES users(user_id),
              FOREIGN KEY(session_id) REFERENCES sessions(id)
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS achievement_stats (
              chat_id INTEGER NOT NULL,
              user_id INTEGER NOT NULL,
              key TEXT NOT NULL,
              count INTEGER NOT NULL,
              last_awarded_at TEXT NOT NULL,
              PRIMARY KEY(chat_id, user_id, key),
              FOREIGN KEY(chat_id) REFERENCES chats(chat_id),
              FOREIGN KEY(user_id) REFERENCES users(user_id)
            );
            """
        )

        # 唯一性约束（用部分索引区分不同成就的“去重维度”）
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_ae_daily_unique
            ON achievement_events(chat_id, key, day)
            WHERE key='daily_earliest';
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_ae_streak7_unique
            ON achievement_events(chat_id, user_id, key, day)
            WHERE key='streak_earliest_7';
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_ae_session_unique
            ON achievement_events(chat_id, user_id, key, session_id)
            WHERE key IN ('ontime_8h','longday_12h');
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
            sday = business_day_key(ts, cutoff_hour=4)
            conn.execute(
                "INSERT INTO sessions(chat_id, user_id, session_day, check_in, check_out) VALUES(?,?,?,?,NULL);",
                (chat_id, user_id, sday, ts.isoformat()),
            )
        return True
    except sqlite3.IntegrityError:
        return False


def check_out(
    db_path: str, *, chat_id: int, user_id: int, ts: datetime
) -> tuple[bool, timedelta | None, datetime | None, int | None]:
    open_sess = get_open_session(db_path, chat_id=chat_id, user_id=user_id)
    if not open_sess:
        return False, None, None, None
    if ts < open_sess.check_in:
        ts = open_sess.check_in
    with connect(db_path) as conn:
        conn.execute("UPDATE sessions SET check_out=? WHERE id=?;", (ts.isoformat(), open_sess.session_id))
    return True, ts - open_sess.check_in, open_sess.check_in, open_sess.session_id


def set_daily_earliest(
    db_path: str,
    *,
    chat_id: int,
    day: str,
    user_id: int,
    session_id: int,
    check_in: datetime,
    created_at: datetime,
) -> bool:
    """
    记录某群某天的最早签到者（只会成功一次）。
    返回 True 表示本次写入成功（即你是当天最早）。
    """
    try:
        with connect(db_path) as conn:
            conn.execute(
                """
                INSERT INTO daily_earliest(chat_id, day, user_id, session_id, check_in, created_at)
                VALUES(?,?,?,?,?,?);
                """,
                (chat_id, day, user_id, session_id, check_in.isoformat(), created_at.isoformat()),
            )
        return True
    except sqlite3.IntegrityError:
        return False


def update_streak(
    db_path: str,
    *,
    chat_id: int,
    user_id: int,
    key: str,
    day: str,
    created_at: datetime,
) -> int:
    """
    更新连胜，返回更新后的 streak 值。
    规则：如果 day 是 last_day+1，则 streak+1；否则 streak=1。
    """
    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT last_day, streak FROM streaks WHERE chat_id=? AND user_id=? AND key=?;",
            (chat_id, user_id, key),
        ).fetchone()

        if row:
            last_day = str(row["last_day"])
            prev = int(row["streak"])
            # day/last_day 格式均为 YYYY-MM-DD
            try:
                last_dt = datetime.fromisoformat(last_day)
            except Exception:
                last_dt = None
            try:
                cur_dt = datetime.fromisoformat(day)
            except Exception:
                cur_dt = None

            new_streak = 1
            if last_dt and cur_dt and (cur_dt.date() - last_dt.date()).days == 1:
                new_streak = prev + 1

            conn.execute(
                """
                UPDATE streaks
                SET last_day=?, streak=?, updated_at=?
                WHERE chat_id=? AND user_id=? AND key=?;
                """,
                (day, new_streak, created_at.isoformat(), chat_id, user_id, key),
            )
            return new_streak

        conn.execute(
            """
            INSERT INTO streaks(chat_id, user_id, key, last_day, streak, updated_at)
            VALUES(?,?,?,?,?,?);
            """,
            (chat_id, user_id, key, day, 1, created_at.isoformat()),
        )
        return 1


def get_streak(db_path: str, *, chat_id: int, user_id: int, key: str) -> int:
    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT streak FROM streaks WHERE chat_id=? AND user_id=? AND key=?;",
            (chat_id, user_id, key),
        ).fetchone()
    return int(row["streak"]) if row else 0


def award_achievement(
    db_path: str,
    *,
    chat_id: int,
    user_id: int,
    key: str,
    created_at: datetime,
    day: str | None = None,
    session_id: int | None = None,
) -> bool:
    """
    写入成就事件 + 统计计数（带去重约束）。
    返回 True 表示这次确实“新解锁/新累计”了一次。
    """
    try:
        with connect(db_path) as conn:
            conn.execute(
                """
                INSERT INTO achievement_events(chat_id, user_id, key, day, session_id, created_at)
                VALUES(?,?,?,?,?,?);
                """,
                (chat_id, user_id, key, day, session_id, created_at.isoformat()),
            )
            conn.execute(
                """
                INSERT INTO achievement_stats(chat_id, user_id, key, count, last_awarded_at)
                VALUES(?,?,?,?,?)
                ON CONFLICT(chat_id, user_id, key) DO UPDATE SET
                  count = count + 1,
                  last_awarded_at = excluded.last_awarded_at;
                """,
                (chat_id, user_id, key, 1, created_at.isoformat()),
            )
        return True
    except sqlite3.IntegrityError:
        return False


def get_achievement_stats(db_path: str, *, chat_id: int, user_id: int) -> list[tuple[str, int, str]]:
    """
    返回 (key, count, last_awarded_at) 列表
    """
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT key, count, last_awarded_at
            FROM achievement_stats
            WHERE chat_id=? AND user_id=?
            ORDER BY count DESC, key ASC;
            """,
            (chat_id, user_id),
        ).fetchall()
    return [(str(r["key"]), int(r["count"]), str(r["last_awarded_at"])) for r in rows]


def get_achievement_stats_global(db_path: str, *, user_id: int) -> list[tuple[str, int, str]]:
    """
    全局（跨所有 chat）统计：返回 (key, count_sum, last_awarded_at_max)
    """
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT key, SUM(count) AS count, MAX(last_awarded_at) AS last_awarded_at
            FROM achievement_stats
            WHERE user_id=?
            GROUP BY key
            ORDER BY count DESC, key ASC;
            """,
            (user_id,),
        ).fetchall()
    return [(str(r["key"]), int(r["count"]), str(r["last_awarded_at"])) for r in rows]


def get_achievement_count(db_path: str, *, chat_id: int, user_id: int, key: str) -> int:
    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT count FROM achievement_stats WHERE chat_id=? AND user_id=? AND key=?;",
            (chat_id, user_id, key),
        ).fetchone()
    return int(row["count"]) if row else 0


def get_achievement_count_global(db_path: str, *, user_id: int, key: str) -> int:
    with connect(db_path) as conn:
        row = conn.execute(
            "SELECT COALESCE(SUM(count),0) AS count FROM achievement_stats WHERE user_id=? AND key=?;",
            (user_id, key),
        ).fetchone()
    return int(row["count"]) if row else 0


def achievement_rank_by_count(
    db_path: str, *, chat_id: int, key: str, limit: int = 20
) -> list[tuple[int, str, int]]:
    """
    成就排行榜（按 achievement_stats.count）
    返回 (user_id, display_name, count)
    """
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT
              u.user_id AS user_id,
              COALESCE(u.username, (u.first_name || ' ' || COALESCE(u.last_name,''))) AS name,
              s.count AS count
            FROM achievement_stats s
            JOIN users u ON u.user_id = s.user_id
            WHERE s.chat_id=? AND s.key=?
            ORDER BY s.count DESC, u.user_id ASC
            LIMIT ?;
            """,
            (chat_id, key, limit),
        ).fetchall()
    out: list[tuple[int, str, int]] = []
    for r in rows:
        name = (r["name"] or str(r["user_id"])).strip()
        if name and " " not in name and not name.isdigit() and not name.startswith("@"):
            name = f"@{name}"
        out.append((int(r["user_id"]), name, int(r["count"])))
    return out


def achievement_rank_by_count_global(db_path: str, *, key: str, limit: int = 20) -> list[tuple[int, str, int]]:
    """
    全局（跨所有 chat）成就排行榜：返回 (user_id, display_name, count_sum)
    """
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT
              u.user_id AS user_id,
              COALESCE(u.username, (u.first_name || ' ' || COALESCE(u.last_name,''))) AS name,
              SUM(s.count) AS count
            FROM achievement_stats s
            JOIN users u ON u.user_id = s.user_id
            WHERE s.key=?
            GROUP BY u.user_id
            ORDER BY count DESC, u.user_id ASC
            LIMIT ?;
            """,
            (key, limit),
        ).fetchall()
    out: list[tuple[int, str, int]] = []
    for r in rows:
        name = (r["name"] or str(r["user_id"])).strip()
        if name and " " not in name and not name.isdigit() and not name.startswith("@"):
            name = f"@{name}"
        out.append((int(r["user_id"]), name, int(r["count"])))
    return out


def streak_rank(
    db_path: str, *, chat_id: int, key: str, limit: int = 20
) -> list[tuple[int, str, int]]:
    """
    连胜排行榜（按 streaks.streak）
    返回 (user_id, display_name, streak)
    """
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT
              u.user_id AS user_id,
              COALESCE(u.username, (u.first_name || ' ' || COALESCE(u.last_name,''))) AS name,
              st.streak AS streak
            FROM streaks st
            JOIN users u ON u.user_id = st.user_id
            WHERE st.chat_id=? AND st.key=?
            ORDER BY st.streak DESC, u.user_id ASC
            LIMIT ?;
            """,
            (chat_id, key, limit),
        ).fetchall()
    out: list[tuple[int, str, int]] = []
    for r in rows:
        name = (r["name"] or str(r["user_id"])).strip()
        if name and " " not in name and not name.isdigit() and not name.startswith("@"):
            name = f"@{name}"
        out.append((int(r["user_id"]), name, int(r["streak"])))
    return out


def streak_rank_global(
    db_path: str, *, key: str, limit: int = 20
) -> list[tuple[int, str, int, int | None, str | None]]:
    """
    全局（跨所有 chat）连胜排行榜：取每个用户的最大 streak
    返回 (user_id, display_name, streak, chat_id, chat_title)
    """
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            WITH ranked AS (
              SELECT
                st.user_id,
                st.chat_id,
                st.streak,
                ROW_NUMBER() OVER (PARTITION BY st.user_id ORDER BY st.streak DESC, st.chat_id ASC) AS rn
              FROM streaks st
              WHERE st.key=?
            )
            SELECT
              u.user_id AS user_id,
              COALESCE(u.username, (u.first_name || ' ' || COALESCE(u.last_name,''))) AS name,
              r.streak AS streak,
              r.chat_id AS chat_id,
              c.title AS chat_title
            FROM ranked r
            JOIN users u ON u.user_id = r.user_id
            LEFT JOIN chats c ON c.chat_id = r.chat_id
            WHERE r.rn=1
            ORDER BY r.streak DESC, u.user_id ASC
            LIMIT ?;
            """,
            (key, limit),
        ).fetchall()
    out: list[tuple[int, str, int, int | None, str | None]] = []
    for r in rows:
        name = (r["name"] or str(r["user_id"])).strip()
        if name and " " not in name and not name.isdigit() and not name.startswith("@"):
            name = f"@{name}"
        out.append(
            (
                int(r["user_id"]),
                name,
                int(r["streak"] or 0),
                (int(r["chat_id"]) if r["chat_id"] is not None else None),
                (str(r["chat_title"]) if r["chat_title"] is not None else None),
            )
        )
    return out


def get_streak_best_global(db_path: str, *, user_id: int, key: str) -> tuple[int, int | None, str | None]:
    """
    全局（跨所有 chat）取该用户最大 streak，返回 (streak, chat_id, chat_title)
    """
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT st.streak AS streak, st.chat_id AS chat_id, c.title AS chat_title
            FROM streaks st
            LEFT JOIN chats c ON c.chat_id = st.chat_id
            WHERE st.user_id=? AND st.key=?
            ORDER BY st.streak DESC, st.chat_id ASC
            LIMIT 1;
            """,
            (user_id, key),
        ).fetchone()
    if not row:
        return (0, None, None)
    return (
        int(row["streak"] or 0),
        (int(row["chat_id"]) if row["chat_id"] is not None else None),
        (str(row["chat_title"]) if row["chat_title"] is not None else None),
    )


def session_today_exists(db_path: str, *, chat_id: int, user_id: int, day: str) -> bool:
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT 1
            FROM sessions
            WHERE chat_id=? AND user_id=? AND session_day=?
            LIMIT 1;
            """,
            (chat_id, user_id, day),
        ).fetchone()
    return row is not None


def session_today_completed(db_path: str, *, chat_id: int, user_id: int, day: str) -> bool:
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT 1
            FROM sessions
            WHERE chat_id=? AND user_id=? AND session_day=? AND check_out IS NOT NULL
            LIMIT 1;
            """,
            (chat_id, user_id, day),
        ).fetchone()
    return row is not None


def today_checkin_position(db_path: str, *, chat_id: int, session_id: int, check_in: datetime, day: str) -> int:
    """
    返回该 session 在“本群今日签到”中的名次（从 1 开始）。
    规则：按 check_in 时间升序；同一时间按 id 升序。
    """
    with connect(db_path) as conn:
        row = conn.execute(
            """
            SELECT COUNT(1) AS n
            FROM sessions
            WHERE chat_id=?
              AND session_day=?
              AND (
                julianday(check_in) < julianday(?)
                OR (check_in = ? AND id <= ?)
              );
            """,
            (chat_id, day, check_in.isoformat(), check_in.isoformat(), session_id),
        ).fetchone()
    n = int(row["n"]) if row else 0
    return n if n > 0 else 1


def leaderboard(db_path: str, *, chat_id: int, mode: str, now: datetime) -> list[tuple[int, str, int]]:
    where = ""
    params: list[object] = [chat_id]
    if mode == "today":
        day = business_day_key(now, cutoff_hour=4)
        where = "AND s.session_day = ?"
        params.append(day)

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


def leaderboard_global(db_path: str, *, mode: str, now: datetime) -> list[tuple[int, str, int]]:
    """
    全局（跨所有 chat）清醒时长排行榜
    返回 (user_id, display_name, seconds)
    """
    where = ""
    params: list[object] = []
    if mode == "today":
        day = business_day_key(now, cutoff_hour=4)
        where = "AND s.session_day = ?"
        params.append(day)

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
            WHERE 1=1
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


