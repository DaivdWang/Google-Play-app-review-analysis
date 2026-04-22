from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


DEFAULT_SCHEMA_PATH = Path(__file__).with_name("schema.sql")


def initialize_database(db_path: Path, schema_path: Path = DEFAULT_SCHEMA_PATH) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    schema_sql = schema_path.read_text(encoding="utf-8")

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.executescript(schema_sql)
        conn.execute(
            """
            INSERT INTO data_sources (source_name, description)
            VALUES (?, ?)
            ON CONFLICT(source_name) DO NOTHING
            """,
            (
                "google_play",
                "Google Play Store public app metadata and review ingestion source",
            ),
        )
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize the SQLite database for app review ingestion.")
    parser.add_argument("--db", required=True, help="Path to the SQLite database file.")
    parser.add_argument(
        "--schema",
        default=str(DEFAULT_SCHEMA_PATH),
        help="Path to the schema.sql file. Defaults to the schema next to this script.",
    )
    args = parser.parse_args()

    initialize_database(Path(args.db), Path(args.schema))
    print(f"Initialized database at {args.db}")
