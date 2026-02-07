#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import os
import sys
from pathlib import Path

import psycopg
from dotenv import load_dotenv


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply SQL migrations to Postgres.")
    parser.add_argument(
        "--dir",
        default="db/migrations",
        help="Directory containing *.sql migration files.",
    )
    return parser.parse_args()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def ensure_migrations_table(conn: psycopg.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
          filename TEXT PRIMARY KEY,
          checksum TEXT NOT NULL,
          applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )


def main() -> int:
    load_dotenv()
    args = parse_args()

    migrations_dir = Path(args.dir)
    if not migrations_dir.exists():
        print(f"Migration directory not found: {migrations_dir}", file=sys.stderr)
        return 1

    dsn = os.getenv("POSTGRES_DSN")
    if not dsn:
        print("POSTGRES_DSN is required.", file=sys.stderr)
        return 1

    files = sorted(p for p in migrations_dir.glob("*.sql") if p.is_file())
    if not files:
        print(f"No migration files found in {migrations_dir}")
        return 0

    with psycopg.connect(dsn) as conn:
        conn.autocommit = False
        ensure_migrations_table(conn)

        applied = {
            row[0]: row[1]
            for row in conn.execute("SELECT filename, checksum FROM schema_migrations").fetchall()
        }

        for path in files:
            checksum = sha256_file(path)
            existing = applied.get(path.name)

            if existing and existing != checksum:
                print(
                    f"Checksum mismatch for already-applied migration {path.name}. "
                    "Refusing to continue.",
                    file=sys.stderr,
                )
                return 1

            if existing:
                print(f"skip {path.name}")
                continue

            sql = path.read_text(encoding="utf-8")
            print(f"apply {path.name}")
            conn.execute(sql)
            conn.execute(
                "INSERT INTO schema_migrations (filename, checksum) VALUES (%s, %s)",
                (path.name, checksum),
            )
            conn.commit()

    print("Migrations complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
