from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

from config import load_settings
from zao_bot import db


def main() -> None:
    settings = load_settings()
    parser = argparse.ArgumentParser(description="SQLite 管理工具（checkpoint / integrity_check / backup）")
    parser.add_argument("--db", default=settings.db_path, help="SQLite 路径（默认取 DB_PATH/config.toml）")

    sub = parser.add_subparsers(dest="cmd", required=True)

    p_ck = sub.add_parser("checkpoint", help="手动触发 WAL checkpoint")
    p_ck.add_argument("--mode", default="FULL", choices=["PASSIVE", "FULL", "RESTART", "TRUNCATE"])

    sub.add_parser("integrity_check", help="执行 PRAGMA integrity_check")

    p_bk = sub.add_parser("backup", help="在线备份数据库到指定文件")
    p_bk.add_argument("--out", default="", help="输出路径（默认 ./data/backup-YYYYmmdd-HHMMSS.sqlite3）")

    args = parser.parse_args()
    db_path = str(Path(args.db))

    if args.cmd == "checkpoint":
        busy, log, checkpointed = db.wal_checkpoint(db_path, mode=args.mode)
        print(f"checkpoint mode={args.mode} busy={busy} log={log} checkpointed={checkpointed}")
        return

    if args.cmd == "integrity_check":
        res = db.integrity_check(db_path)
        print("\n".join(res))
        return

    if args.cmd == "backup":
        out = args.out.strip()
        if not out:
            Path("data").mkdir(parents=True, exist_ok=True)
            out = f"data/backup-{datetime.now().strftime('%Y%m%d-%H%M%S')}.sqlite3"
        db.backup_to(db_path, backup_path=out)
        print(f"backup saved: {out}")
        return


if __name__ == "__main__":
    main()


