
import json
import sqlite3
from pathlib import Path


class PipelineDatabase:
    """SQLite storage for leads, the append-only event ledger and ingestion state."""

    def __init__(self, database_path=None):
        project_root = Path(__file__).resolve().parents[2]

        self.database_path = Path(
            database_path
            if database_path is not None
            else project_root / "solution" / "pipeline.db"
        )

        self.database_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        self._initialize()

    def connect(self):
        connection = sqlite3.connect(self.database_path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def _initialize(self):
        with self.connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS leads (
                    workflow_id TEXT NOT NULL,
                    phone_hash TEXT NOT NULL,
                    batch_id TEXT NOT NULL,
                    fields_json TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (workflow_id, phone_hash)
                );

                CREATE TABLE IF NOT EXISTS events (
                    event_id TEXT PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    workflow_id TEXT NOT NULL,
                    call_id TEXT,
                    occurred_at TEXT,
                    phone_hash TEXT,
                    payload_json TEXT NOT NULL,
                    source_file TEXT,
                    source_line INTEGER,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_events_call
                ON events (workflow_id, call_id);

                CREATE INDEX IF NOT EXISTS idx_events_phone
                ON events (workflow_id, phone_hash);

                CREATE TABLE IF NOT EXISTS processed_files (
                    file_path TEXT PRIMARY KEY,
                    checksum TEXT NOT NULL,
                    file_type TEXT NOT NULL,
                    processed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS rejections (
                    source_file TEXT NOT NULL,
                    workflow_id TEXT NOT NULL,
                    batch_id TEXT NOT NULL,
                    row_number INTEGER NOT NULL,
                    reason TEXT NOT NULL,
                    PRIMARY KEY (source_file, row_number)
                );

                CREATE TABLE IF NOT EXISTS quality_counters (
                    counter_name TEXT PRIMARY KEY,
                    counter_value INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS quality_groups (
                    group_name TEXT NOT NULL,
                    group_key TEXT NOT NULL,
                    counter_value INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (group_name, group_key)
                );
                """
            )

    @staticmethod
    def serialize_fields(fields):
        return json.dumps(
            fields,
            ensure_ascii=False,
            sort_keys=True,
        )

    @staticmethod
    def deserialize_fields(value):
        return json.loads(value)

    def is_file_processed(self, file_path, checksum):
        with self.connect() as connection:
            row = connection.execute(
                """
                SELECT checksum
                FROM processed_files
                WHERE file_path = ?
                """,
                (str(file_path),),
            ).fetchone()

        return row is not None and row["checksum"] == checksum

    def mark_file_processed(self, file_path, checksum, file_type):
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO processed_files (
                    file_path,
                    checksum,
                    file_type
                )
                VALUES (?, ?, ?)
                ON CONFLICT(file_path) DO UPDATE SET
                    checksum = excluded.checksum,
                    file_type = excluded.file_type,
                    processed_at = CURRENT_TIMESTAMP
                """,
                (str(file_path), checksum, file_type),
            )

    def upsert_lead(self, lead):
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO leads (
                    workflow_id,
                    phone_hash,
                    batch_id,
                    fields_json
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(workflow_id, phone_hash) DO UPDATE SET
                    fields_json = excluded.fields_json,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    lead["workflow_id"],
                    lead["phone_hash"],
                    lead["batch_id"],
                    self.serialize_fields(lead["fields"]),
                ),
            )

    def get_leads(self):
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT workflow_id, phone_hash, batch_id, fields_json
                FROM leads
                ORDER BY workflow_id, phone_hash
                """
            ).fetchall()

        return [
            {
                "workflow_id": row["workflow_id"],
                "phone_hash": row["phone_hash"],
                "batch_id": row["batch_id"],
                "fields": self.deserialize_fields(row["fields_json"]),
            }
            for row in rows
        ]

    def insert_event(self, event):
        with self.connect() as connection:
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO events (
                    event_id,
                    event_type,
                    workflow_id,
                    call_id,
                    occurred_at,
                    phone_hash,
                    payload_json,
                    source_file,
                    source_line
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event["event_id"],
                    event["event_type"],
                    event["workflow_id"],
                    event.get("call_id"),
                    event.get("occurred_at"),
                    event.get("phone_hash"),
                    self.serialize_fields(event.get("payload", {})),
                    event.get("source_file"),
                    event.get("source_line"),
                ),
            )

            return cursor.rowcount == 1

    def get_events(self):
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT *
                FROM events
                ORDER BY rowid
                """
            ).fetchall()

        return [
            {
                "event_id": row["event_id"],
                "event_type": row["event_type"],
                "workflow_id": row["workflow_id"],
                "call_id": row["call_id"],
                "occurred_at": row["occurred_at"],
                "payload": self.deserialize_fields(row["payload_json"]),
                "source_file": row["source_file"],
                "source_line": row["source_line"],
                "phone_hash": row["phone_hash"],
            }
            for row in rows
        ]

    def save_rejection(self, rejection, source_file):
        with self.connect() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO rejections (
                    source_file,
                    workflow_id,
                    batch_id,
                    row_number,
                    reason
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    str(source_file),
                    rejection["workflow_id"],
                    rejection["batch_id"],
                    rejection["row_number"],
                    rejection["reason"],
                ),
            )

    def get_rejections(self):
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT workflow_id, batch_id, row_number, reason
                FROM rejections
                ORDER BY workflow_id, batch_id, row_number
                """
            ).fetchall()

        return [dict(row) for row in rows]

    def increment_counter(self, name, amount=1):
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO quality_counters (counter_name, counter_value)
                VALUES (?, ?)
                ON CONFLICT(counter_name) DO UPDATE SET
                    counter_value = counter_value + excluded.counter_value
                """,
                (name, amount),
            )

    def increment_group(self, group_name, group_key, amount=1):
        with self.connect() as connection:
            connection.execute(
                """
                INSERT INTO quality_groups (
                    group_name,
                    group_key,
                    counter_value
                )
                VALUES (?, ?, ?)
                ON CONFLICT(group_name, group_key) DO UPDATE SET
                    counter_value = counter_value + excluded.counter_value
                """,
                (group_name, group_key, amount),
            )

    def get_quality_counters(self):
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT counter_name, counter_value
                FROM quality_counters
                ORDER BY counter_name
                """
            ).fetchall()

        return {
            row["counter_name"]: row["counter_value"]
            for row in rows
        }

    def get_quality_groups(self, group_name):
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT group_key, counter_value
                FROM quality_groups
                WHERE group_name = ?
                ORDER BY group_key
                """,
                (group_name,),
            ).fetchall()

        return {
            row["group_key"]: row["counter_value"]
            for row in rows
        }
