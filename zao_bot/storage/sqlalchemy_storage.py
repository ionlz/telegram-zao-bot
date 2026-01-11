from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

from zao_bot.storage.base import OpenSession, Storage
from zao_bot.time_utils import business_day_key


def _display_name(name: str | None, user_id: int) -> str:
    nm = (name or str(user_id)).strip()
    if nm and " " not in nm and not nm.isdigit() and not nm.startswith("@"):
        nm = f"@{nm}"
    return nm


@dataclass(frozen=True)
class SQLAlchemyStorage(Storage):
    url: str

    def _parse_dt(self, v: Any) -> datetime:
        if isinstance(v, datetime):
            return v
        return datetime.fromisoformat(str(v))

    def __post_init__(self) -> None:
        object.__setattr__(self, "_engine", self._make_engine(self.url))

    @property
    def engine(self) -> Engine:
        return getattr(self, "_engine")

    def _make_engine(self, url: str) -> Engine:
        if url.startswith("sqlite"):
            engine = create_engine(
                url,
                future=True,
                pool_pre_ping=True,
                connect_args={"check_same_thread": False},
            )

            @event.listens_for(engine, "connect")
            def _sqlite_pragmas(dbapi_conn, _conn_record) -> None:  # type: ignore[no-redef]
                cur = dbapi_conn.cursor()
                try:
                    cur.execute(f"PRAGMA journal_mode={os.getenv('SQLITE_JOURNAL_MODE', 'WAL')};")
                    cur.execute(f"PRAGMA synchronous={os.getenv('SQLITE_SYNCHRONOUS', 'NORMAL')};")
                    cur.execute(f"PRAGMA busy_timeout={int(os.getenv('SQLITE_BUSY_TIMEOUT_MS', '5000'))};")
                    cur.execute(f"PRAGMA wal_autocheckpoint={int(os.getenv('SQLITE_WAL_AUTOCHECKPOINT', '1000'))};")
                    cur.execute("PRAGMA temp_store=MEMORY;")
                    cur.execute("PRAGMA foreign_keys=ON;")
                finally:
                    cur.close()

            return engine

        return create_engine(url, future=True, pool_pre_ping=True)

    # --- schema ---
    def init_db(self) -> None:
        dialect = self.engine.dialect.name
        with self.engine.begin() as conn:
            if dialect == "postgresql":
                conn.execute(
                    text(
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
                )
                conn.execute(
                    text(
                        """
                        CREATE TABLE IF NOT EXISTS chats (
                          chat_id BIGINT PRIMARY KEY,
                          title TEXT,
                          chat_type TEXT NOT NULL,
                          updated_at TIMESTAMPTZ NOT NULL
                        );
                        """
                    )
                )
                conn.execute(
                    text(
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
                )
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_sessions_chat_checkin ON sessions(chat_id, check_in);"))
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_sessions_chat_day ON sessions(chat_id, session_day);"))
                # 迁移：旧版本使用“每人仅允许一条未签退记录”，会导致跨业务日无法再 /zao
                conn.execute(text("DROP INDEX IF EXISTS idx_open_session;"))
                # 新口径：每人每天（业务日）只允许一条 session；允许跨天存在历史未签退记录
                conn.execute(
                    text(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_user_day
                        ON sessions(chat_id, user_id, session_day);
                        """
                    )
                )
            else:
                # sqlite / others
                conn.execute(
                    text(
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
                )
                conn.execute(
                    text(
                        """
                        CREATE TABLE IF NOT EXISTS chats (
                          chat_id INTEGER PRIMARY KEY,
                          title TEXT,
                          chat_type TEXT NOT NULL,
                          updated_at TEXT NOT NULL
                        );
                        """
                    )
                )
                conn.execute(
                    text(
                        """
                        CREATE TABLE IF NOT EXISTS sessions (
                          id INTEGER PRIMARY KEY AUTOINCREMENT,
                          chat_id INTEGER NOT NULL,
                          user_id INTEGER NOT NULL,
                          session_day TEXT,
                          check_in TEXT NOT NULL,
                          check_out TEXT,
                          FOREIGN KEY(chat_id) REFERENCES chats(chat_id),
                          FOREIGN KEY(user_id) REFERENCES users(user_id)
                        );
                        """
                    )
                )
                # 迁移：移除旧的“唯一未签退”索引，改为“每日唯一 session”
                conn.execute(text("DROP INDEX IF EXISTS idx_open_session;"))
                conn.execute(
                    text(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_user_day
                        ON sessions(chat_id, user_id, session_day);
                        """
                    )
                )
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_sessions_chat_checkin ON sessions(chat_id, check_in);"))
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_sessions_chat_day ON sessions(chat_id, session_day);"))

                # 旧库迁移：回填 session_day
                cols = [r[1] for r in conn.execute(text("PRAGMA table_info(sessions);")).fetchall()]
                if "session_day" not in cols:
                    conn.execute(text("ALTER TABLE sessions ADD COLUMN session_day TEXT;"))
                rows = conn.execute(
                    text("SELECT id, check_in FROM sessions WHERE session_day IS NULL OR session_day='';")
                ).fetchall()
                for sid, check_in_s in rows:
                    try:
                        dt = datetime.fromisoformat(str(check_in_s))
                        sday = business_day_key(dt, cutoff_hour=4)
                    except Exception:
                        sday = None
                    if sday:
                        conn.execute(text("UPDATE sessions SET session_day=:d WHERE id=:id;"), {"d": sday, "id": int(sid)})

            # achievements schema (same for sqlite/pg, types differ but acceptable)
            if dialect == "postgresql":
                conn.execute(
                    text(
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
                )
                conn.execute(
                    text(
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
                )
                conn.execute(
                    text(
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
                )
                conn.execute(
                    text(
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
                )
            else:
                conn.execute(
                    text(
                        """
                        CREATE TABLE IF NOT EXISTS daily_earliest (
                          chat_id INTEGER NOT NULL,
                          day TEXT NOT NULL,
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
                )
                conn.execute(
                    text(
                        """
                        CREATE TABLE IF NOT EXISTS streaks (
                          chat_id INTEGER NOT NULL,
                          user_id INTEGER NOT NULL,
                          key TEXT NOT NULL,
                          last_day TEXT NOT NULL,
                          streak INTEGER NOT NULL,
                          updated_at TEXT NOT NULL,
                          PRIMARY KEY(chat_id, user_id, key),
                          FOREIGN KEY(chat_id) REFERENCES chats(chat_id),
                          FOREIGN KEY(user_id) REFERENCES users(user_id)
                        );
                        """
                    )
                )
                conn.execute(
                    text(
                        """
                        CREATE TABLE IF NOT EXISTS achievement_events (
                          id INTEGER PRIMARY KEY AUTOINCREMENT,
                          chat_id INTEGER NOT NULL,
                          user_id INTEGER NOT NULL,
                          key TEXT NOT NULL,
                          day TEXT,
                          session_id INTEGER,
                          created_at TEXT NOT NULL,
                          FOREIGN KEY(chat_id) REFERENCES chats(chat_id),
                          FOREIGN KEY(user_id) REFERENCES users(user_id),
                          FOREIGN KEY(session_id) REFERENCES sessions(id)
                        );
                        """
                    )
                )
                conn.execute(
                    text(
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
                )

            # russian roulette tables
            if dialect == "postgresql":
                conn.execute(
                    text(
                        """
                        CREATE TABLE IF NOT EXISTS russian_roulette (
                          chat_id BIGINT PRIMARY KEY REFERENCES chats(chat_id),
                          chambers INT NOT NULL,
                          bullet_position INT NOT NULL,
                          current_position INT NOT NULL,
                          created_by BIGINT NOT NULL REFERENCES users(user_id),
                          created_at TIMESTAMPTZ NOT NULL
                        );
                        """
                    )
                )
                conn.execute(
                    text(
                        """
                        CREATE TABLE IF NOT EXISTS roulette_attempts (
                          id BIGSERIAL PRIMARY KEY,
                          chat_id BIGINT NOT NULL REFERENCES chats(chat_id),
                          user_id BIGINT NOT NULL REFERENCES users(user_id),
                          position INT NOT NULL,
                          result TEXT NOT NULL,
                          created_at TIMESTAMPTZ NOT NULL
                        );
                        """
                    )
                )
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_roulette_attempts ON roulette_attempts(chat_id, created_at);"))
            else:
                conn.execute(
                    text(
                        """
                        CREATE TABLE IF NOT EXISTS russian_roulette (
                          chat_id INTEGER PRIMARY KEY,
                          chambers INTEGER NOT NULL,
                          bullet_position INTEGER NOT NULL,
                          current_position INTEGER NOT NULL,
                          created_by INTEGER NOT NULL,
                          created_at TEXT NOT NULL,
                          FOREIGN KEY(chat_id) REFERENCES chats(chat_id),
                          FOREIGN KEY(created_by) REFERENCES users(user_id)
                        );
                        """
                    )
                )
                conn.execute(
                    text(
                        """
                        CREATE TABLE IF NOT EXISTS roulette_attempts (
                          id INTEGER PRIMARY KEY AUTOINCREMENT,
                          chat_id INTEGER NOT NULL,
                          user_id INTEGER NOT NULL,
                          position INTEGER NOT NULL,
                          result TEXT NOT NULL,
                          created_at TEXT NOT NULL,
                          FOREIGN KEY(chat_id) REFERENCES chats(chat_id),
                          FOREIGN KEY(user_id) REFERENCES users(user_id)
                        );
                        """
                    )
                )
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_roulette_attempts ON roulette_attempts(chat_id, created_at);"))

            # wake reminders tables
            if dialect == "postgresql":
                conn.execute(
                    text(
                        """
                        CREATE TABLE IF NOT EXISTS wake_reminders (
                          id BIGSERIAL PRIMARY KEY,
                          chat_id BIGINT NOT NULL REFERENCES chats(chat_id),
                          user_id BIGINT NOT NULL REFERENCES users(user_id),
                          wake_time TEXT NOT NULL,
                          next_trigger TIMESTAMPTZ NOT NULL,
                          repeat BOOLEAN DEFAULT false,
                          enabled BOOLEAN DEFAULT true,
                          created_at TIMESTAMPTZ NOT NULL
                        );
                        """
                    )
                )
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_wake_next_trigger ON wake_reminders(next_trigger, enabled);"))
            else:
                conn.execute(
                    text(
                        """
                        CREATE TABLE IF NOT EXISTS wake_reminders (
                          id INTEGER PRIMARY KEY AUTOINCREMENT,
                          chat_id INTEGER NOT NULL,
                          user_id INTEGER NOT NULL,
                          wake_time TEXT NOT NULL,
                          next_trigger TEXT NOT NULL,
                          repeat INTEGER DEFAULT 0,
                          enabled INTEGER DEFAULT 1,
                          created_at TEXT NOT NULL,
                          FOREIGN KEY(chat_id) REFERENCES chats(chat_id),
                          FOREIGN KEY(user_id) REFERENCES users(user_id)
                        );
                        """
                    )
                )
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_wake_next_trigger ON wake_reminders(next_trigger, enabled);"))

            # partial unique indexes
            conn.execute(
                text(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_ae_daily_unique
                    ON achievement_events(chat_id, key, day)
                    WHERE key='daily_earliest';
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_ae_streak7_unique
                    ON achievement_events(chat_id, user_id, key, day)
                    WHERE key='streak_earliest_7';
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_ae_session_unique
                    ON achievement_events(chat_id, user_id, key, session_id)
                    WHERE key IN ('ontime_8h','longday_12h');
                    """
                )
            )

    # --- users/chats ---
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
        dialect = self.engine.dialect.name
        if dialect != "postgresql":
            updated_at_val: Any = updated_at.isoformat()
        else:
            updated_at_val = updated_at
        with self.engine.begin() as conn:
            if dialect == "postgresql":
                conn.execute(
                    text(
                        """
                        INSERT INTO users(user_id, username, first_name, last_name, updated_at)
                        VALUES(:uid,:un,:fn,:ln,:ua)
                        ON CONFLICT (user_id) DO UPDATE SET
                          username=EXCLUDED.username,
                          first_name=EXCLUDED.first_name,
                          last_name=EXCLUDED.last_name,
                          updated_at=EXCLUDED.updated_at;
                        """
                    ),
                    {"uid": user_id, "un": username, "fn": first_name, "ln": last_name, "ua": updated_at_val},
                )
                conn.execute(
                    text(
                        """
                        INSERT INTO chats(chat_id, title, chat_type, updated_at)
                        VALUES(:cid,:t,:ct,:ua)
                        ON CONFLICT (chat_id) DO UPDATE SET
                          title=EXCLUDED.title,
                          chat_type=EXCLUDED.chat_type,
                          updated_at=EXCLUDED.updated_at;
                        """
                    ),
                    {"cid": chat_id, "t": chat_title, "ct": chat_type, "ua": updated_at_val},
                )
            else:
                conn.execute(
                    text(
                        """
                        INSERT INTO users(user_id, username, first_name, last_name, updated_at)
                        VALUES(:uid,:un,:fn,:ln,:ua)
                        ON CONFLICT(user_id) DO UPDATE SET
                          username=excluded.username,
                          first_name=excluded.first_name,
                          last_name=excluded.last_name,
                          updated_at=excluded.updated_at;
                        """
                    ),
                    {"uid": user_id, "un": username, "fn": first_name, "ln": last_name, "ua": updated_at_val},
                )
                conn.execute(
                    text(
                        """
                        INSERT INTO chats(chat_id, title, chat_type, updated_at)
                        VALUES(:cid,:t,:ct,:ua)
                        ON CONFLICT(chat_id) DO UPDATE SET
                          title=excluded.title,
                          chat_type=excluded.chat_type,
                          updated_at=excluded.updated_at;
                        """
                    ),
                    {"cid": chat_id, "t": chat_title, "ct": chat_type, "ua": updated_at_val},
                )

    # --- sessions ---
    def get_open_session(self, *, chat_id: int, user_id: int, day: str | None = None) -> OpenSession | None:
        with self.engine.connect() as conn:
            r = conn.execute(
                text(
                    """
                    SELECT id, check_in
                    FROM sessions
                    WHERE chat_id=:cid AND user_id=:uid AND check_out IS NULL
                      AND (:day IS NULL OR session_day = :day)
                    ORDER BY id DESC
                    LIMIT 1;
                    """
                ),
                {"cid": chat_id, "uid": user_id, "day": day},
            ).fetchone()
        if not r:
            return None
        check_in_dt = self._parse_dt(r[1])
        return OpenSession(session_id=int(r[0]), check_in=check_in_dt)

    def check_in(self, *, chat_id: int, user_id: int, ts: datetime) -> bool:
        dialect = self.engine.dialect.name
        session_day = business_day_key(ts, cutoff_hour=4)
        check_in_val: Any = ts if dialect == "postgresql" else ts.isoformat()
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO sessions(chat_id, user_id, session_day, check_in, check_out)
                        VALUES(:cid,:uid,:day,:ci,NULL);
                        """
                    ),
                    {"cid": chat_id, "uid": user_id, "day": session_day, "ci": check_in_val},
                )
            return True
        except IntegrityError:
            # 失败的典型原因：同一业务日重复签到（idx_sessions_user_day）
            return False

    def check_out(self, *, chat_id: int, user_id: int, ts: datetime) -> tuple[bool, timedelta | None, datetime | None, int | None]:
        # 只允许签退“当前业务日”的 open session，避免跨日续接旧 /zao
        day = business_day_key(ts, cutoff_hour=4)
        osess = self.get_open_session(chat_id=chat_id, user_id=user_id, day=day)
        if not osess:
            return False, None, None, None
        check_in_ts = osess.check_in
        if ts < check_in_ts:
            ts = check_in_ts
        dialect = self.engine.dialect.name
        check_out_val: Any = ts if dialect == "postgresql" else ts.isoformat()
        with self.engine.begin() as conn:
            conn.execute(text("UPDATE sessions SET check_out=:co WHERE id=:id;"), {"co": check_out_val, "id": osess.session_id})
        return True, ts - check_in_ts, check_in_ts, osess.session_id

    def session_today_exists(self, *, chat_id: int, user_id: int, day: str) -> bool:
        with self.engine.connect() as conn:
            r = conn.execute(
                text("SELECT 1 FROM sessions WHERE chat_id=:cid AND user_id=:uid AND session_day=:d LIMIT 1;"),
                {"cid": chat_id, "uid": user_id, "d": day},
            ).fetchone()
        return r is not None

    def session_today_completed(self, *, chat_id: int, user_id: int, day: str) -> bool:
        with self.engine.connect() as conn:
            r = conn.execute(
                text(
                    "SELECT 1 FROM sessions WHERE chat_id=:cid AND user_id=:uid AND session_day=:d AND check_out IS NOT NULL LIMIT 1;"
                ),
                {"cid": chat_id, "uid": user_id, "d": day},
            ).fetchone()
        return r is not None

    def today_checkin_position(self, *, chat_id: int, session_id: int, check_in: datetime, day: str) -> int:
        dialect = self.engine.dialect.name
        ci_val: Any = check_in if dialect == "postgresql" else check_in.isoformat()
        with self.engine.connect() as conn:
            r = conn.execute(
                text(
                    """
                    SELECT COUNT(1) AS n
                    FROM sessions
                    WHERE chat_id=:cid
                      AND session_day=:d
                      AND (check_in < :ci OR (check_in=:ci AND id <= :id));
                    """
                ),
                {"cid": chat_id, "d": day, "ci": ci_val, "id": session_id},
            ).fetchone()
        n = int(r[0]) if r else 0
        return n if n > 0 else 1

    def get_user_checkin_days(self, *, user_id: int, start_date: str, end_date: str) -> set[str]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT DISTINCT session_day
                    FROM sessions
                    WHERE user_id=:uid
                      AND session_day IS NOT NULL
                      AND session_day BETWEEN :start AND :end
                    ORDER BY session_day;
                    """
                ),
                {"uid": user_id, "start": start_date, "end": end_date},
            ).fetchall()
        return {str(r[0]) for r in rows if r[0]}

    # --- leaderboard ---
    def leaderboard(self, *, chat_id: int, mode: str, now: datetime) -> list[tuple[int, str, int]]:
        dialect = self.engine.dialect.name
        if dialect == "postgresql":
            if mode == "today":
                params: dict[str, Any] = {"now": now, "cid": chat_id, "d": business_day_key(now, cutoff_hour=4)}
                where = "AND s.session_day = :d"
                seconds_expr = "EXTRACT(EPOCH FROM (COALESCE(s.check_out, :now) - s.check_in))"
                extra_where = ""
            else:
                # 总榜：不把历史未签退记录按 now 无限累加（仅统计已签退的 session）
                params = {"cid": chat_id}
                where = ""
                seconds_expr = "EXTRACT(EPOCH FROM (s.check_out - s.check_in))"
                extra_where = "AND s.check_out IS NOT NULL"
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(
                        f"""
                        SELECT
                          u.user_id AS user_id,
                          COALESCE(u.username, CONCAT_WS(' ', u.first_name, u.last_name)) AS name,
                          SUM({seconds_expr})::bigint AS seconds
                        FROM sessions s
                        JOIN users u ON u.user_id = s.user_id
                        WHERE s.chat_id = :cid
                        {extra_where}
                        {where}
                        GROUP BY u.user_id
                        ORDER BY seconds DESC;
                        """
                    ),
                    params,
                ).fetchall()
        else:
            if mode == "today":
                now_val = now.isoformat()
                params2: dict[str, Any] = {"now": now_val, "cid": chat_id, "d": business_day_key(now, cutoff_hour=4)}
                where2 = "AND s.session_day = :d"
                seconds_expr2 = """
                          SUM(
                            CASE
                              WHEN s.check_out IS NULL THEN
                                CAST((julianday(:now) - julianday(s.check_in)) * 86400 AS INTEGER)
                              ELSE
                                CAST((julianday(s.check_out) - julianday(s.check_in)) * 86400 AS INTEGER)
                            END
                          ) AS seconds
                """
                extra_where2 = ""
            else:
                # 总榜：仅统计已签退的 session，避免未签退记录无限增长
                params2 = {"cid": chat_id}
                where2 = ""
                seconds_expr2 = "SUM(CAST((julianday(s.check_out) - julianday(s.check_in)) * 86400 AS INTEGER)) AS seconds"
                extra_where2 = "AND s.check_out IS NOT NULL"
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(
                        f"""
                        SELECT
                          u.user_id AS user_id,
                          COALESCE(u.username, (u.first_name || ' ' || COALESCE(u.last_name,''))) AS name,
                          {seconds_expr2}
                        FROM sessions s
                        JOIN users u ON u.user_id = s.user_id
                        WHERE s.chat_id = :cid
                        {extra_where2}
                        {where2}
                        GROUP BY u.user_id
                        ORDER BY seconds DESC;
                        """
                    ),
                    params2,
                ).fetchall()
        out: list[tuple[int, str, int]] = []
        for r in rows:
            out.append((int(r[0]), _display_name(r[1], int(r[0])), int(r[2] or 0)))
        return out

    def leaderboard_global(self, *, mode: str, now: datetime) -> list[tuple[int, str, int]]:
        dialect = self.engine.dialect.name
        if dialect == "postgresql":
            if mode == "today":
                params: dict[str, Any] = {"now": now, "d": business_day_key(now, cutoff_hour=4)}
                where = "AND s.session_day = :d"
                seconds_expr = "EXTRACT(EPOCH FROM (COALESCE(s.check_out, :now) - s.check_in))"
                extra_where = ""
            else:
                params = {}
                where = ""
                seconds_expr = "EXTRACT(EPOCH FROM (s.check_out - s.check_in))"
                extra_where = "AND s.check_out IS NOT NULL"
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(
                        f"""
                        SELECT
                          u.user_id AS user_id,
                          COALESCE(u.username, CONCAT_WS(' ', u.first_name, u.last_name)) AS name,
                          SUM({seconds_expr})::bigint AS seconds
                        FROM sessions s
                        JOIN users u ON u.user_id = s.user_id
                        WHERE 1=1
                        {extra_where}
                        {where}
                        GROUP BY u.user_id
                        ORDER BY seconds DESC;
                        """
                    ),
                    params,
                ).fetchall()
        else:
            if mode == "today":
                now_val = now.isoformat()
                params2: dict[str, Any] = {"now": now_val, "d": business_day_key(now, cutoff_hour=4)}
                where2 = "AND s.session_day = :d"
                seconds_expr2 = """
                          SUM(
                            CASE
                              WHEN s.check_out IS NULL THEN
                                CAST((julianday(:now) - julianday(s.check_in)) * 86400 AS INTEGER)
                              ELSE
                                CAST((julianday(s.check_out) - julianday(s.check_in)) * 86400 AS INTEGER)
                            END
                          ) AS seconds
                """
                extra_where2 = ""
            else:
                params2 = {}
                where2 = ""
                seconds_expr2 = "SUM(CAST((julianday(s.check_out) - julianday(s.check_in)) * 86400 AS INTEGER)) AS seconds"
                extra_where2 = "AND s.check_out IS NOT NULL"
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(
                        f"""
                        SELECT
                          u.user_id AS user_id,
                          COALESCE(u.username, (u.first_name || ' ' || COALESCE(u.last_name,''))) AS name,
                          {seconds_expr2}
                        FROM sessions s
                        JOIN users u ON u.user_id = s.user_id
                        WHERE 1=1
                        {extra_where2}
                        {where2}
                        GROUP BY u.user_id
                        ORDER BY seconds DESC;
                        """
                    ),
                    params2,
                ).fetchall()
        out: list[tuple[int, str, int]] = []
        for r in rows:
            out.append((int(r[0]), _display_name(r[1], int(r[0])), int(r[2] or 0)))
        return out

    def open_user_ids(self, *, chat_id: int, day: str | None = None) -> set[int]:
        with self.engine.connect() as conn:
            if day:
                rows = conn.execute(
                    text(
                        """
                        SELECT DISTINCT user_id
                        FROM sessions
                        WHERE chat_id=:cid AND check_out IS NULL AND session_day=:d;
                        """
                    ),
                    {"cid": chat_id, "d": day},
                ).fetchall()
            else:
                rows = conn.execute(
                    text("SELECT DISTINCT user_id FROM sessions WHERE chat_id=:cid AND check_out IS NULL;"),
                    {"cid": chat_id},
                ).fetchall()
        return {int(r[0]) for r in rows}

    def open_user_ids_global(self, day: str | None = None) -> set[int]:
        with self.engine.connect() as conn:
            if day:
                rows = conn.execute(
                    text("SELECT DISTINCT user_id FROM sessions WHERE check_out IS NULL AND session_day=:d;"),
                    {"d": day},
                ).fetchall()
            else:
                rows = conn.execute(text("SELECT DISTINCT user_id FROM sessions WHERE check_out IS NULL;")).fetchall()
        return {int(r[0]) for r in rows}

    # --- achievements ---
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
        dialect = self.engine.dialect.name
        ci_val: Any = check_in if dialect == "postgresql" else check_in.isoformat()
        ca_val: Any = created_at if dialect == "postgresql" else created_at.isoformat()
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO daily_earliest(chat_id, day, user_id, session_id, check_in, created_at)
                        VALUES(:cid,:d,:uid,:sid,:ci,:ca);
                        """
                    ),
                    {"cid": chat_id, "d": day, "uid": user_id, "sid": session_id, "ci": ci_val, "ca": ca_val},
                )
            return True
        except IntegrityError:
            return False

    def update_streak(self, *, chat_id: int, user_id: int, key: str, day: str, created_at: datetime) -> int:
        ca_val: Any = created_at if self.engine.dialect.name == "postgresql" else created_at.isoformat()
        with self.engine.begin() as conn:
            row = conn.execute(
                text("SELECT last_day, streak FROM streaks WHERE chat_id=:cid AND user_id=:uid AND key=:k;"),
                {"cid": chat_id, "uid": user_id, "k": key},
            ).fetchone()
            if row:
                last_day, prev = str(row[0]), int(row[1])
                try:
                    new_streak = prev + 1 if (date.fromisoformat(day) - date.fromisoformat(last_day)).days == 1 else 1
                except Exception:
                    new_streak = 1
                conn.execute(
                    text(
                        """
                        UPDATE streaks
                        SET last_day=:d, streak=:s, updated_at=:ua
                        WHERE chat_id=:cid AND user_id=:uid AND key=:k;
                        """
                    ),
                    {"d": day, "s": new_streak, "ua": ca_val, "cid": chat_id, "uid": user_id, "k": key},
                )
                return new_streak

            conn.execute(
                text(
                    """
                    INSERT INTO streaks(chat_id, user_id, key, last_day, streak, updated_at)
                    VALUES(:cid,:uid,:k,:d,1,:ua);
                    """
                ),
                {"cid": chat_id, "uid": user_id, "k": key, "d": day, "ua": ca_val},
            )
            return 1

    def get_streak(self, *, chat_id: int, user_id: int, key: str) -> int:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT streak FROM streaks WHERE chat_id=:cid AND user_id=:uid AND key=:k;"),
                {"cid": chat_id, "uid": user_id, "k": key},
            ).fetchone()
        return int(row[0]) if row else 0

    def get_streak_best_global(self, *, user_id: int, key: str) -> tuple[int, int | None, str | None]:
        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT st.streak AS streak, st.chat_id AS chat_id, c.title AS chat_title
                    FROM streaks st
                    LEFT JOIN chats c ON c.chat_id = st.chat_id
                    WHERE st.user_id=:uid AND st.key=:k
                    ORDER BY st.streak DESC, st.chat_id ASC
                    LIMIT 1;
                    """
                ),
                {"uid": user_id, "k": key},
            ).fetchone()
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
        dialect = self.engine.dialect.name
        ca_val: Any = created_at if dialect == "postgresql" else created_at.isoformat()
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        INSERT INTO achievement_events(chat_id, user_id, key, day, session_id, created_at)
                        VALUES(:cid,:uid,:k,:d,:sid,:ca);
                        """
                    ),
                    {"cid": chat_id, "uid": user_id, "k": key, "d": day, "sid": session_id, "ca": ca_val},
                )
                if dialect == "postgresql":
                    conn.execute(
                        text(
                            """
                            INSERT INTO achievement_stats(chat_id, user_id, key, count, last_awarded_at)
                            VALUES(:cid,:uid,:k,1,:ca)
                            ON CONFLICT (chat_id, user_id, key) DO UPDATE SET
                              count = achievement_stats.count + 1,
                              last_awarded_at = EXCLUDED.last_awarded_at;
                            """
                        ),
                        {"cid": chat_id, "uid": user_id, "k": key, "ca": ca_val},
                    )
                else:
                    conn.execute(
                        text(
                            """
                            INSERT INTO achievement_stats(chat_id, user_id, key, count, last_awarded_at)
                            VALUES(:cid,:uid,:k,1,:ca)
                            ON CONFLICT(chat_id, user_id, key) DO UPDATE SET
                              count = count + 1,
                              last_awarded_at = excluded.last_awarded_at;
                            """
                        ),
                        {"cid": chat_id, "uid": user_id, "k": key, "ca": ca_val},
                    )
            return True
        except IntegrityError:
            return False

    def get_achievement_stats(self, *, chat_id: int, user_id: int) -> list[tuple[str, int, str]]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT key, count, last_awarded_at
                    FROM achievement_stats
                    WHERE chat_id=:cid AND user_id=:uid
                    ORDER BY count DESC, key ASC;
                    """
                ),
                {"cid": chat_id, "uid": user_id},
            ).fetchall()
        return [(str(k), int(c), str(t)) for (k, c, t) in rows]

    def get_achievement_stats_global(self, *, user_id: int) -> list[tuple[str, int, str]]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT key, SUM(count) AS count, MAX(last_awarded_at) AS last_awarded_at
                    FROM achievement_stats
                    WHERE user_id=:uid
                    GROUP BY key
                    ORDER BY count DESC, key ASC;
                    """
                ),
                {"uid": user_id},
            ).fetchall()
        return [(str(k), int(c), str(t)) for (k, c, t) in rows]

    def get_achievement_count(self, *, chat_id: int, user_id: int, key: str) -> int:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT count FROM achievement_stats WHERE chat_id=:cid AND user_id=:uid AND key=:k;"),
                {"cid": chat_id, "uid": user_id, "k": key},
            ).fetchone()
        return int(row[0]) if row else 0

    def get_achievement_count_global(self, *, user_id: int, key: str) -> int:
        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT COALESCE(SUM(count),0) FROM achievement_stats WHERE user_id=:uid AND key=:k;"),
                {"uid": user_id, "k": key},
            ).fetchone()
        return int(row[0]) if row else 0

    def achievement_rank_by_count(self, *, chat_id: int, key: str, limit: int = 20) -> list[tuple[int, str, int]]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT
                      u.user_id AS user_id,
                      COALESCE(u.username, (u.first_name || ' ' || COALESCE(u.last_name,''))) AS name,
                      s.count AS count
                    FROM achievement_stats s
                    JOIN users u ON u.user_id = s.user_id
                    WHERE s.chat_id=:cid AND s.key=:k
                    ORDER BY s.count DESC, u.user_id ASC
                    LIMIT :lim;
                    """
                ),
                {"cid": chat_id, "k": key, "lim": limit},
            ).fetchall()
        return [(int(uid), _display_name(name, int(uid)), int(cnt)) for (uid, name, cnt) in rows]

    def achievement_rank_by_count_global(self, *, key: str, limit: int = 20) -> list[tuple[int, str, int]]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT
                      u.user_id AS user_id,
                      COALESCE(u.username, (u.first_name || ' ' || COALESCE(u.last_name,''))) AS name,
                      SUM(s.count) AS count
                    FROM achievement_stats s
                    JOIN users u ON u.user_id = s.user_id
                    WHERE s.key=:k
                    GROUP BY u.user_id
                    ORDER BY count DESC, u.user_id ASC
                    LIMIT :lim;
                    """
                ),
                {"k": key, "lim": limit},
            ).fetchall()
        return [(int(uid), _display_name(name, int(uid)), int(cnt)) for (uid, name, cnt) in rows]

    def streak_rank(self, *, chat_id: int, key: str, limit: int = 20) -> list[tuple[int, str, int]]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT
                      u.user_id AS user_id,
                      COALESCE(u.username, (u.first_name || ' ' || COALESCE(u.last_name,''))) AS name,
                      st.streak AS streak
                    FROM streaks st
                    JOIN users u ON u.user_id = st.user_id
                    WHERE st.chat_id=:cid AND st.key=:k
                    ORDER BY st.streak DESC, u.user_id ASC
                    LIMIT :lim;
                    """
                ),
                {"cid": chat_id, "k": key, "lim": limit},
            ).fetchall()
        return [(int(uid), _display_name(name, int(uid)), int(st)) for (uid, name, st) in rows]

    def streak_rank_global(self, *, key: str, limit: int = 20) -> list[tuple[int, str, int, int | None, str | None]]:
        # sqlite <3.25 lacks window functions; our app supports modern sqlite generally, but keep query compatible by using window function only on pg
        if self.engine.dialect.name != "postgresql":
            # best-effort: take max streak per user but without chat title
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(
                        """
                        SELECT
                          u.user_id AS user_id,
                          COALESCE(u.username, (u.first_name || ' ' || COALESCE(u.last_name,''))) AS name,
                          MAX(st.streak) AS streak
                        FROM streaks st
                        JOIN users u ON u.user_id = st.user_id
                        WHERE st.key=:k
                        GROUP BY u.user_id
                        ORDER BY streak DESC, u.user_id ASC
                        LIMIT :lim;
                        """
                    ),
                    {"k": key, "lim": limit},
                ).fetchall()
            return [(int(uid), _display_name(name, int(uid)), int(st), None, None) for (uid, name, st) in rows]

        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    WITH ranked AS (
                      SELECT
                        st.user_id,
                        st.chat_id,
                        st.streak,
                        ROW_NUMBER() OVER (PARTITION BY st.user_id ORDER BY st.streak DESC, st.chat_id ASC) AS rn
                      FROM streaks st
                      WHERE st.key=:k
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
                    LIMIT :lim;
                    """
                ),
                {"k": key, "lim": limit},
            ).fetchall()
        return [
            (int(uid), _display_name(str(name), int(uid)), int(streak), int(cid) if cid is not None else None, str(ctitle) if ctitle is not None else None)
            for (uid, name, streak, cid, ctitle) in rows
        ]

    # --- russian roulette ---
    def get_active_roulette(self, *, chat_id: int) -> RouletteGame | None:
        from zao_bot.storage.base import RouletteGame

        with self.engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT chat_id, chambers, bullet_position, current_position, created_by, created_at
                    FROM russian_roulette
                    WHERE chat_id=:cid;
                    """
                ),
                {"cid": chat_id},
            ).fetchone()
        if not row:
            return None
        return RouletteGame(
            chat_id=int(row[0]),
            chambers=int(row[1]),
            bullet_position=int(row[2]),
            current_position=int(row[3]),
            created_by=int(row[4]),
            created_at=self._parse_dt(row[5]),
        )

    def create_roulette(
        self, *, chat_id: int, chambers: int, bullet_position: int, created_by: int, created_at: datetime
    ) -> None:
        dialect = self.engine.dialect.name
        ca_val: Any = created_at if dialect == "postgresql" else created_at.isoformat()
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO russian_roulette(chat_id, chambers, bullet_position, current_position, created_by, created_at)
                    VALUES(:cid,:ch,:bp,0,:cb,:ca);
                    """
                ),
                {"cid": chat_id, "ch": chambers, "bp": bullet_position, "cb": created_by, "ca": ca_val},
            )

    def update_roulette_position(self, *, chat_id: int, position: int) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text("UPDATE russian_roulette SET current_position=:pos WHERE chat_id=:cid;"),
                {"pos": position, "cid": chat_id},
            )

    def delete_roulette(self, *, chat_id: int) -> None:
        with self.engine.begin() as conn:
            conn.execute(text("DELETE FROM russian_roulette WHERE chat_id=:cid;"), {"cid": chat_id})

    def record_roulette_attempt(
        self, *, chat_id: int, user_id: int, position: int, result: str, created_at: datetime
    ) -> None:
        dialect = self.engine.dialect.name
        ca_val: Any = created_at if dialect == "postgresql" else created_at.isoformat()
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO roulette_attempts(chat_id, user_id, position, result, created_at)
                    VALUES(:cid,:uid,:pos,:res,:ca);
                    """
                ),
                {"cid": chat_id, "uid": user_id, "pos": position, "res": result, "ca": ca_val},
            )

    # --- wake reminders ---
    def create_reminder(
        self, *, chat_id: int, user_id: int, wake_time: str, next_trigger: datetime, repeat: bool, created_at: datetime
    ) -> int:
        dialect = self.engine.dialect.name
        nt_val: Any = next_trigger if dialect == "postgresql" else next_trigger.isoformat()
        ca_val: Any = created_at if dialect == "postgresql" else created_at.isoformat()
        repeat_val: Any = repeat if dialect == "postgresql" else (1 if repeat else 0)
        with self.engine.begin() as conn:
            result = conn.execute(
                text(
                    """
                    INSERT INTO wake_reminders(chat_id, user_id, wake_time, next_trigger, repeat, enabled, created_at)
                    VALUES(:cid,:uid,:wt,:nt,:rep,true,:ca)
                    RETURNING id;
                    """ if dialect == "postgresql" else """
                    INSERT INTO wake_reminders(chat_id, user_id, wake_time, next_trigger, repeat, enabled, created_at)
                    VALUES(:cid,:uid,:wt,:nt,:rep,1,:ca);
                    """
                ),
                {"cid": chat_id, "uid": user_id, "wt": wake_time, "nt": nt_val, "rep": repeat_val, "ca": ca_val},
            )
            if dialect == "postgresql":
                return int(result.fetchone()[0])  # type: ignore
            return int(result.lastrowid)  # type: ignore

    def get_pending_reminders(self, *, now: datetime) -> list[WakeReminder]:
        from zao_bot.storage.base import WakeReminder

        dialect = self.engine.dialect.name
        now_val: Any = now if dialect == "postgresql" else now.isoformat()
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT id, chat_id, user_id, wake_time, next_trigger, repeat, enabled
                    FROM wake_reminders
                    WHERE enabled=:enabled AND next_trigger <= :now
                    ORDER BY next_trigger;
                    """
                ),
                {"enabled": True if dialect == "postgresql" else 1, "now": now_val},
            ).fetchall()
        return [
            WakeReminder(
                id=int(r[0]),
                chat_id=int(r[1]),
                user_id=int(r[2]),
                wake_time=str(r[3]),
                next_trigger=self._parse_dt(r[4]),
                repeat=bool(r[5]) if dialect == "postgresql" else bool(int(r[5])),
                enabled=bool(r[6]) if dialect == "postgresql" else bool(int(r[6])),
            )
            for r in rows
        ]

    def get_user_reminders(self, *, chat_id: int, user_id: int) -> list[WakeReminder]:
        from zao_bot.storage.base import WakeReminder

        dialect = self.engine.dialect.name
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT id, chat_id, user_id, wake_time, next_trigger, repeat, enabled
                    FROM wake_reminders
                    WHERE chat_id=:cid AND user_id=:uid AND enabled=:enabled
                    ORDER BY wake_time;
                    """
                ),
                {"cid": chat_id, "uid": user_id, "enabled": True if dialect == "postgresql" else 1},
            ).fetchall()
        return [
            WakeReminder(
                id=int(r[0]),
                chat_id=int(r[1]),
                user_id=int(r[2]),
                wake_time=str(r[3]),
                next_trigger=self._parse_dt(r[4]),
                repeat=bool(r[5]) if dialect == "postgresql" else bool(int(r[5])),
                enabled=bool(r[6]) if dialect == "postgresql" else bool(int(r[6])),
            )
            for r in rows
        ]

    def update_reminder_next_trigger(self, *, reminder_id: int, next_trigger: datetime) -> None:
        dialect = self.engine.dialect.name
        nt_val: Any = next_trigger if dialect == "postgresql" else next_trigger.isoformat()
        with self.engine.begin() as conn:
            conn.execute(
                text("UPDATE wake_reminders SET next_trigger=:nt WHERE id=:id;"),
                {"nt": nt_val, "id": reminder_id},
            )

    def delete_reminder(self, *, reminder_id: int) -> None:
        with self.engine.begin() as conn:
            conn.execute(text("DELETE FROM wake_reminders WHERE id=:id;"), {"id": reminder_id})

    def delete_user_reminders(self, *, chat_id: int, user_id: int) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text("DELETE FROM wake_reminders WHERE chat_id=:cid AND user_id=:uid;"),
                {"cid": chat_id, "uid": user_id},
            )


