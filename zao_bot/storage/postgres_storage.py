from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

import psycopg
from psycopg.errors import UniqueViolation

from zao_bot.storage.base import OpenSession, Storage
from zao_bot.time_utils import business_day_key


def _display_name_from_row(name: str | None, user_id: int) -> str:
    nm = (name or str(user_id)).strip()
    if nm and " " not in nm and not nm.isdigit() and not nm.startswith("@"):
        nm = f"@{nm}"
    return nm


@dataclass(frozen=True)
class PostgresStorage(Storage):
    dsn: str

    def _connect(self) -> psycopg.Connection[Any]:
        # autocommit=False：显式事务更安全（with 会自动提交/回滚）
        return psycopg.connect(self.dsn, autocommit=False)

    def init_db(self) -> None:
        with self._connect() as conn, conn.cursor() as cur:
            # 基础表
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                  user_id BIGINT PRIMARY KEY,
                  username TEXT,
                  first_name TEXT,
                  last_name TEXT,
                  updated_at TIMESTAMPTZ NOT NULL
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS chats (
                  chat_id BIGINT PRIMARY KEY,
                  title TEXT,
                  chat_type TEXT NOT NULL,
                  updated_at TIMESTAMPTZ NOT NULL
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS sessions (
                  id BIGSERIAL PRIMARY KEY,
                  chat_id BIGINT NOT NULL REFERENCES chats(chat_id),
                  user_id BIGINT NOT NULL REFERENCES users(user_id),
                  session_day TEXT,
                  check_in TIMESTAMPTZ NOT NULL,
                  check_out TIMESTAMPTZ
                );
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_sessions_chat_checkin ON sessions(chat_id, check_in);"
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_sessions_chat_day ON sessions(chat_id, session_day);"
            )
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_open_session
                ON sessions(chat_id, user_id)
                WHERE check_out IS NULL;
                """
            )

            # Achievements tables
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS daily_earliest (
                  chat_id BIGINT NOT NULL REFERENCES chats(chat_id),
                  day TEXT NOT NULL,
                  user_id BIGINT NOT NULL REFERENCES users(user_id),
                  session_id BIGINT NOT NULL REFERENCES sessions(id),
                  check_in TIMESTAMPTZ NOT NULL,
                  created_at TIMESTAMPTZ NOT NULL,
                  PRIMARY KEY(chat_id, day)
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS streaks (
                  chat_id BIGINT NOT NULL REFERENCES chats(chat_id),
                  user_id BIGINT NOT NULL REFERENCES users(user_id),
                  key TEXT NOT NULL,
                  last_day TEXT NOT NULL,
                  streak INTEGER NOT NULL,
                  updated_at TIMESTAMPTZ NOT NULL,
                  PRIMARY KEY(chat_id, user_id, key)
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS achievement_events (
                  id BIGSERIAL PRIMARY KEY,
                  chat_id BIGINT NOT NULL REFERENCES chats(chat_id),
                  user_id BIGINT NOT NULL REFERENCES users(user_id),
                  key TEXT NOT NULL,
                  day TEXT,
                  session_id BIGINT REFERENCES sessions(id),
                  created_at TIMESTAMPTZ NOT NULL
                );
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS achievement_stats (
                  chat_id BIGINT NOT NULL REFERENCES chats(chat_id),
                  user_id BIGINT NOT NULL REFERENCES users(user_id),
                  key TEXT NOT NULL,
                  count INTEGER NOT NULL,
                  last_awarded_at TIMESTAMPTZ NOT NULL,
                  PRIMARY KEY(chat_id, user_id, key)
                );
                """
            )
            # 去重约束（partial unique）
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_ae_daily_unique
                ON achievement_events(chat_id, key, day)
                WHERE key='daily_earliest';
                """
            )
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_ae_streak7_unique
                ON achievement_events(chat_id, user_id, key, day)
                WHERE key='streak_earliest_7';
                """
            )
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_ae_session_unique
                ON achievement_events(chat_id, user_id, key, session_id)
                WHERE key IN ('ontime_8h','longday_12h');
                """
            )
            conn.commit()

    def upsert_user_and_chat(
        self,
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
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users(user_id, username, first_name, last_name, updated_at)
                VALUES(%s,%s,%s,%s,%s)
                ON CONFLICT (user_id) DO UPDATE SET
                  username=EXCLUDED.username,
                  first_name=EXCLUDED.first_name,
                  last_name=EXCLUDED.last_name,
                  updated_at=EXCLUDED.updated_at;
                """,
                (user_id, username, first_name, last_name, updated_at),
            )
            cur.execute(
                """
                INSERT INTO chats(chat_id, title, chat_type, updated_at)
                VALUES(%s,%s,%s,%s)
                ON CONFLICT (chat_id) DO UPDATE SET
                  title=EXCLUDED.title,
                  chat_type=EXCLUDED.chat_type,
                  updated_at=EXCLUDED.updated_at;
                """,
                (chat_id, chat_title, chat_type, updated_at),
            )
            conn.commit()

    def get_open_session(self, *, chat_id: int, user_id: int, day: str | None = None) -> OpenSession | None:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, check_in
                FROM sessions
                WHERE chat_id=%s AND user_id=%s AND check_out IS NULL
                  AND (%s IS NULL OR session_day = %s)
                ORDER BY id DESC
                LIMIT 1;
                """,
                (chat_id, user_id, day, day),
            )
            row = cur.fetchone()
        if not row:
            return None
        return OpenSession(session_id=int(row[0]), check_in=row[1])

    def check_in(self, *, chat_id: int, user_id: int, ts: datetime) -> bool:
        # 与 SQLite 口径一致：业务日（凌晨 4 点切换）
        session_day = business_day_key(ts, cutoff_hour=4)
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO sessions(chat_id, user_id, session_day, check_in, check_out)
                    VALUES(%s,%s,%s,%s,NULL);
                    """,
                    (chat_id, user_id, session_day, ts),
                )
                conn.commit()
            return True
        except UniqueViolation:
            return False

    def check_out(self, *, chat_id: int, user_id: int, ts: datetime) -> tuple[bool, timedelta | None, datetime | None, int | None]:
        day = business_day_key(ts, cutoff_hour=4)
        osess = self.get_open_session(chat_id=chat_id, user_id=user_id, day=day)
        if not osess:
            return False, None, None, None
        check_in_ts = osess.check_in
        if ts < check_in_ts:
            ts = check_in_ts
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("UPDATE sessions SET check_out=%s WHERE id=%s;", (ts, osess.session_id))
            conn.commit()
        return True, ts - check_in_ts, check_in_ts, osess.session_id

    def session_today_exists(self, *, chat_id: int, user_id: int, day: str) -> bool:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM sessions WHERE chat_id=%s AND user_id=%s AND session_day=%s LIMIT 1;",
                (chat_id, user_id, day),
            )
            return cur.fetchone() is not None

    def session_today_completed(self, *, chat_id: int, user_id: int, day: str) -> bool:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM sessions WHERE chat_id=%s AND user_id=%s AND session_day=%s AND check_out IS NOT NULL LIMIT 1;",
                (chat_id, user_id, day),
            )
            return cur.fetchone() is not None

    def today_checkin_position(self, *, chat_id: int, session_id: int, check_in: datetime, day: str) -> int:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(1) AS n
                FROM sessions
                WHERE chat_id=%s
                  AND session_day=%s
                  AND (check_in < %s OR (check_in=%s AND id <= %s));
                """,
                (chat_id, day, check_in, check_in, session_id),
            )
            row = cur.fetchone()
        n = int(row[0]) if row else 0
        return n if n > 0 else 1

    def leaderboard(self, *, chat_id: int, mode: str, now: datetime) -> list[tuple[int, str, int]]:
        params: list[Any] = [now, chat_id]
        where = ""
        if mode == "today":
            # 与 SQLite 口径一致：业务日（凌晨 4 点切换）
            where = "AND s.session_day = %s"
            params.append(business_day_key(now, cutoff_hour=4))
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                  u.user_id AS user_id,
                  COALESCE(u.username, CONCAT_WS(' ', u.first_name, u.last_name)) AS name,
                  SUM(EXTRACT(EPOCH FROM (COALESCE(s.check_out, %s) - s.check_in)))::bigint AS seconds
                FROM sessions s
                JOIN users u ON u.user_id = s.user_id
                WHERE s.chat_id = %s
                {where}
                GROUP BY u.user_id
                ORDER BY seconds DESC;
                """,
                tuple(params),
            )
            rows = cur.fetchall()
        out: list[tuple[int, str, int]] = []
        for user_id, name, seconds in rows:
            out.append((int(user_id), _display_name_from_row(name, int(user_id)), int(seconds or 0)))
        return out

    def leaderboard_global(self, *, mode: str, now: datetime) -> list[tuple[int, str, int]]:
        params: list[Any] = [now]
        where = ""
        if mode == "today":
            where = "AND s.session_day = %s"
            params.append(business_day_key(now, cutoff_hour=4))
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                  u.user_id AS user_id,
                  COALESCE(u.username, CONCAT_WS(' ', u.first_name, u.last_name)) AS name,
                  SUM(EXTRACT(EPOCH FROM (COALESCE(s.check_out, %s) - s.check_in)))::bigint AS seconds
                FROM sessions s
                JOIN users u ON u.user_id = s.user_id
                WHERE 1=1
                {where}
                GROUP BY u.user_id
                ORDER BY seconds DESC;
                """,
                tuple(params),
            )
            rows = cur.fetchall()
        out: list[tuple[int, str, int]] = []
        for user_id, name, seconds in rows:
            out.append((int(user_id), _display_name_from_row(name, int(user_id)), int(seconds or 0)))
        return out

    def open_user_ids(self, *, chat_id: int, day: str | None = None) -> set[int]:
        with self._connect() as conn, conn.cursor() as cur:
            if day:
                cur.execute(
                    """
                    SELECT DISTINCT user_id
                    FROM sessions
                    WHERE chat_id=%s AND check_out IS NULL AND session_day=%s;
                    """,
                    (chat_id, day),
                )
            else:
                cur.execute(
                    """
                    SELECT DISTINCT user_id
                    FROM sessions
                    WHERE chat_id=%s AND check_out IS NULL;
                    """,
                    (chat_id,),
                )
            rows = cur.fetchall()
        return {int(r[0]) for r in rows}

    def open_user_ids_global(self, day: str | None = None) -> set[int]:
        with self._connect() as conn, conn.cursor() as cur:
            if day:
                cur.execute(
                    """
                    SELECT DISTINCT user_id
                    FROM sessions
                    WHERE check_out IS NULL AND session_day=%s;
                    """,
                    (day,),
                )
            else:
                cur.execute(
                    """
                    SELECT DISTINCT user_id
                    FROM sessions
                    WHERE check_out IS NULL;
                    """
                )
            rows = cur.fetchall()
        return {int(r[0]) for r in rows}

    def set_daily_earliest(
        self,
        *,
        chat_id: int,
        day: str,
        user_id: int,
        session_id: int,
        check_in: datetime,
        created_at: datetime,
    ) -> bool:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO daily_earliest(chat_id, day, user_id, session_id, check_in, created_at)
                    VALUES(%s,%s,%s,%s,%s,%s);
                    """,
                    (chat_id, day, user_id, session_id, check_in, created_at),
                )
                conn.commit()
            return True
        except UniqueViolation:
            return False

    def update_streak(self, *, chat_id: int, user_id: int, key: str, day: str, created_at: datetime) -> int:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT last_day, streak FROM streaks WHERE chat_id=%s AND user_id=%s AND key=%s;",
                (chat_id, user_id, key),
            )
            row = cur.fetchone()
            if row:
                last_day, prev = str(row[0]), int(row[1])
                try:
                    new_streak = prev + 1 if (date.fromisoformat(day) - date.fromisoformat(last_day)).days == 1 else 1
                except Exception:
                    new_streak = 1
                cur.execute(
                    """
                    UPDATE streaks
                    SET last_day=%s, streak=%s, updated_at=%s
                    WHERE chat_id=%s AND user_id=%s AND key=%s;
                    """,
                    (day, new_streak, created_at, chat_id, user_id, key),
                )
                conn.commit()
                return new_streak
            cur.execute(
                """
                INSERT INTO streaks(chat_id, user_id, key, last_day, streak, updated_at)
                VALUES(%s,%s,%s,%s,%s,%s);
                """,
                (chat_id, user_id, key, day, 1, created_at),
            )
            conn.commit()
            return 1

    def get_streak(self, *, chat_id: int, user_id: int, key: str) -> int:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute("SELECT streak FROM streaks WHERE chat_id=%s AND user_id=%s AND key=%s;", (chat_id, user_id, key))
            row = cur.fetchone()
        return int(row[0]) if row else 0

    def get_streak_best_global(self, *, user_id: int, key: str) -> tuple[int, int | None, str | None]:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT st.streak AS streak, st.chat_id AS chat_id, c.title AS chat_title
                FROM streaks st
                LEFT JOIN chats c ON c.chat_id = st.chat_id
                WHERE st.user_id=%s AND st.key=%s
                ORDER BY st.streak DESC, st.chat_id ASC
                LIMIT 1;
                """,
                (user_id, key),
            )
            row = cur.fetchone()
        if not row:
            return (0, None, None)
        return (int(row[0] or 0), int(row[1]) if row[1] is not None else None, str(row[2]) if row[2] is not None else None)

    def award_achievement(
        self,
        *,
        chat_id: int,
        user_id: int,
        key: str,
        created_at: datetime,
        day: str | None = None,
        session_id: int | None = None,
    ) -> bool:
        try:
            with self._connect() as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO achievement_events(chat_id, user_id, key, day, session_id, created_at)
                    VALUES(%s,%s,%s,%s,%s,%s);
                    """,
                    (chat_id, user_id, key, day, session_id, created_at),
                )
                cur.execute(
                    """
                    INSERT INTO achievement_stats(chat_id, user_id, key, count, last_awarded_at)
                    VALUES(%s,%s,%s,1,%s)
                    ON CONFLICT (chat_id, user_id, key) DO UPDATE SET
                      count = achievement_stats.count + 1,
                      last_awarded_at = EXCLUDED.last_awarded_at;
                    """,
                    (chat_id, user_id, key, created_at),
                )
                conn.commit()
            return True
        except UniqueViolation:
            return False

    def get_achievement_stats(self, *, chat_id: int, user_id: int) -> list[tuple[str, int, str]]:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT key, count, last_awarded_at
                FROM achievement_stats
                WHERE chat_id=%s AND user_id=%s
                ORDER BY count DESC, key ASC;
                """,
                (chat_id, user_id),
            )
            rows = cur.fetchall()
        return [(str(k), int(c), str(t)) for (k, c, t) in rows]

    def get_achievement_stats_global(self, *, user_id: int) -> list[tuple[str, int, str]]:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT key, SUM(count) AS count, MAX(last_awarded_at) AS last_awarded_at
                FROM achievement_stats
                WHERE user_id=%s
                GROUP BY key
                ORDER BY count DESC, key ASC;
                """,
                (user_id,),
            )
            rows = cur.fetchall()
        return [(str(k), int(c), str(t)) for (k, c, t) in rows]

    def get_achievement_count(self, *, chat_id: int, user_id: int, key: str) -> int:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT count FROM achievement_stats WHERE chat_id=%s AND user_id=%s AND key=%s;",
                (chat_id, user_id, key),
            )
            row = cur.fetchone()
        return int(row[0]) if row else 0

    def get_achievement_count_global(self, *, user_id: int, key: str) -> int:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(SUM(count),0) FROM achievement_stats WHERE user_id=%s AND key=%s;",
                (user_id, key),
            )
            row = cur.fetchone()
        return int(row[0]) if row else 0

    def achievement_rank_by_count(self, *, chat_id: int, key: str, limit: int = 20) -> list[tuple[int, str, int]]:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  u.user_id AS user_id,
                  COALESCE(u.username, CONCAT_WS(' ', u.first_name, u.last_name)) AS name,
                  s.count AS count
                FROM achievement_stats s
                JOIN users u ON u.user_id = s.user_id
                WHERE s.chat_id=%s AND s.key=%s
                ORDER BY s.count DESC, u.user_id ASC
                LIMIT %s;
                """,
                (chat_id, key, limit),
            )
            rows = cur.fetchall()
        return [(int(uid), _display_name_from_row(name, int(uid)), int(cnt)) for (uid, name, cnt) in rows]

    def achievement_rank_by_count_global(self, *, key: str, limit: int = 20) -> list[tuple[int, str, int]]:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  u.user_id AS user_id,
                  COALESCE(u.username, CONCAT_WS(' ', u.first_name, u.last_name)) AS name,
                  SUM(s.count) AS count
                FROM achievement_stats s
                JOIN users u ON u.user_id = s.user_id
                WHERE s.key=%s
                GROUP BY u.user_id
                ORDER BY count DESC, u.user_id ASC
                LIMIT %s;
                """,
                (key, limit),
            )
            rows = cur.fetchall()
        return [(int(uid), _display_name_from_row(name, int(uid)), int(cnt)) for (uid, name, cnt) in rows]

    def streak_rank(self, *, chat_id: int, key: str, limit: int = 20) -> list[tuple[int, str, int]]:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  u.user_id AS user_id,
                  COALESCE(u.username, CONCAT_WS(' ', u.first_name, u.last_name)) AS name,
                  st.streak AS streak
                FROM streaks st
                JOIN users u ON u.user_id = st.user_id
                WHERE st.chat_id=%s AND st.key=%s
                ORDER BY st.streak DESC, u.user_id ASC
                LIMIT %s;
                """,
                (chat_id, key, limit),
            )
            rows = cur.fetchall()
        return [(int(uid), _display_name_from_row(name, int(uid)), int(st)) for (uid, name, st) in rows]

    def streak_rank_global(self, *, key: str, limit: int = 20) -> list[tuple[int, str, int, int | None, str | None]]:
        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                WITH ranked AS (
                  SELECT
                    st.user_id,
                    st.chat_id,
                    st.streak,
                    ROW_NUMBER() OVER (PARTITION BY st.user_id ORDER BY st.streak DESC, st.chat_id ASC) AS rn
                  FROM streaks st
                  WHERE st.key=%s
                )
                SELECT
                  u.user_id AS user_id,
                  COALESCE(u.username, CONCAT_WS(' ', u.first_name, u.last_name)) AS name,
                  r.streak AS streak,
                  r.chat_id AS chat_id,
                  c.title AS chat_title
                FROM ranked r
                JOIN users u ON u.user_id = r.user_id
                LEFT JOIN chats c ON c.chat_id = r.chat_id
                WHERE r.rn=1
                ORDER BY r.streak DESC, u.user_id ASC
                LIMIT %s;
                """,
                (key, limit),
            )
            rows = cur.fetchall()
        out: list[tuple[int, str, int, int | None, str | None]] = []
        for uid, name, streak, cid, ctitle in rows:
            out.append((int(uid), _display_name_from_row(name, int(uid)), int(streak), int(cid) if cid is not None else None, str(ctitle) if ctitle is not None else None))
        return out


