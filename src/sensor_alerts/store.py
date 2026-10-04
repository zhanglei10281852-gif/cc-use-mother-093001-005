"""SQLite 持久化层：游标、设备生命周期、观测、未结告警与重算任务。

所有时间以 UTC ISO-8601 字符串存储；业务层负责加锁与事务边界。
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS devices (
    device_id  TEXT PRIMARY KEY,
    name       TEXT,
    metric     TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rule_versions (
    version    TEXT PRIMARY KEY,
    valid_from TEXT NOT NULL,
    body       TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS installations (
    installation_id TEXT PRIMARY KEY,
    device_id       TEXT NOT NULL,
    serial_no       TEXT NOT NULL,
    installed_at    TEXT NOT NULL,
    removed_at      TEXT,
    note            TEXT
);
CREATE TABLE IF NOT EXISTS calibrations (
    calibration_id  TEXT PRIMARY KEY,
    installation_id TEXT NOT NULL,
    device_id       TEXT NOT NULL,
    factor          REAL NOT NULL,
    calibrated_at   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS maintenance_windows (
    window_id TEXT PRIMARY KEY,
    device_id TEXT NOT NULL,
    start_at  TEXT NOT NULL,
    end_at    TEXT,
    reason    TEXT NOT NULL,
    closed_at TEXT
);
CREATE TABLE IF NOT EXISTS rejected_observations (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id    TEXT NOT NULL,
    serial_hint  TEXT,
    sequence     INTEGER,
    observed_at  TEXT NOT NULL,
    raw_value    REAL,
    ingested_at  TEXT NOT NULL,
    quality      TEXT NOT NULL,
    reason       TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS observations (
    device_id       TEXT NOT NULL,
    installation_id TEXT NOT NULL,
    sequence        INTEGER NOT NULL,
    observed_at     TEXT NOT NULL,
    ingested_at     TEXT NOT NULL,
    raw_value       REAL NOT NULL,
    adjusted_value  REAL NOT NULL,
    quality         TEXT NOT NULL,
    quality_reason  TEXT,
    calibration_id  TEXT,
    rule_version    TEXT NOT NULL,
    PRIMARY KEY (installation_id, sequence)
);
CREATE INDEX IF NOT EXISTS idx_obs_device_time
    ON observations (device_id, observed_at);
CREATE TABLE IF NOT EXISTS ingestion_cursors (
    device_id       TEXT NOT NULL,
    installation_id TEXT NOT NULL,
    last_sequence   INTEGER,
    last_observed_at TEXT,
    good_count      INTEGER NOT NULL DEFAULT 0,
    dup_count       INTEGER NOT NULL DEFAULT 0,
    reject_count    INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (device_id, installation_id)
);
CREATE TABLE IF NOT EXISTS device_state (
    device_id               TEXT PRIMARY KEY,
    last_good_observed_at   TEXT,
    last_good_sequence      INTEGER,
    current_installation_id TEXT,
    active_rule_version     TEXT
);
CREATE TABLE IF NOT EXISTS alerts (
    alert_id            TEXT PRIMARY KEY,
    device_id           TEXT NOT NULL,
    installation_id     TEXT NOT NULL,
    rule_group          TEXT NOT NULL,
    rule_id             TEXT NOT NULL,
    metric              TEXT NOT NULL,
    level               TEXT NOT NULL,
    state               TEXT NOT NULL,
    opened_at           TEXT NOT NULL,
    first_sequence      INTEGER NOT NULL,
    trigger_observed_at TEXT NOT NULL,
    trigger_raw         REAL,
    trigger_adjusted    REAL,
    rule_version        TEXT NOT NULL,
    supersedes_alert_id TEXT,
    created_by_job_id   TEXT,
    last_notified_at    TEXT,
    update_count        INTEGER NOT NULL DEFAULT 0,
    recover_count       INTEGER NOT NULL DEFAULT 0,
    resolved_at         TEXT
);
CREATE INDEX IF NOT EXISTS idx_alerts_install ON alerts (installation_id, state);
CREATE INDEX IF NOT EXISTS idx_alerts_device ON alerts (device_id, state);
CREATE TABLE IF NOT EXISTS alert_events (
    event_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_id             TEXT NOT NULL,
    event_type           TEXT NOT NULL,
    at                   TEXT NOT NULL,
    actor                TEXT,
    reason               TEXT NOT NULL,
    from_state           TEXT,
    to_state             TEXT,
    level                TEXT,
    observation_sequence INTEGER,
    observed_at          TEXT,
    value_adjusted       REAL,
    rule_version         TEXT,
    recompute_job_id     TEXT,
    detail               TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_alert ON alert_events (alert_id, event_id);
CREATE TABLE IF NOT EXISTS recompute_jobs (
    job_id           TEXT PRIMARY KEY,
    device_id        TEXT NOT NULL,
    rule_version     TEXT NOT NULL,
    from_observed_at TEXT,
    to_observed_at   TEXT,
    status           TEXT NOT NULL,
    reason           TEXT NOT NULL,
    created_at       TEXT NOT NULL,
    started_at       TEXT,
    finished_at      TEXT,
    checkpoint_at    TEXT,
    checkpoint_seq   INTEGER,
    error            TEXT
);
CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT NOT NULL);
"""


def to_dt(value: Any) -> datetime:
    """把输入统一成带时区的 UTC datetime。"""
    if value is None:
        raise ValueError("缺少时间字段")
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise ValueError(f"无法识别的时间格式: {value!r}")
    if dt.tzinfo is None:
        raise ValueError("时间必须带时区")
    return dt.astimezone(timezone.utc)


def iso(dt: datetime) -> str:
    return to_dt(dt).isoformat()


class Store:
    def __init__(self, path: str | Path = ":memory:"):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(
            self.path, check_same_thread=False, isolation_level=None
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.executescript(SCHEMA)

    def close(self) -> None:
        self.conn.close()

    # --- 基础辅助 ---
    def query(self, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
        return self.conn.execute(sql, tuple(params)).fetchall()

    def query_one(self, sql: str, params: Iterable[Any] = ()) -> Optional[sqlite3.Row]:
        return self.conn.execute(sql, tuple(params)).fetchone()

    def execute(self, sql: str, params: Iterable[Any] = ()) -> sqlite3.Cursor:
        return self.conn.execute(sql, tuple(params))

    def begin(self) -> None:
        self.conn.execute("BEGIN IMMEDIATE")

    def commit(self) -> None:
        self.conn.execute("COMMIT")

    def rollback(self) -> None:
        self.conn.execute("ROLLBACK")

    @staticmethod
    def row_to_dict(row: Optional[sqlite3.Row]) -> Optional[dict]:
        return dict(row) if row is not None else None

    def get_kv(self, key: str) -> Optional[str]:
        row = self.query_one("SELECT v FROM kv WHERE k=?", (key,))
        return row["v"] if row else None

    def set_kv(self, key: str, value: str) -> None:
        self.execute(
            "INSERT INTO kv(k, v) VALUES(?, ?) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (key, value),
        )

    def set_json(self, key: str, value: Any) -> None:
        self.set_kv(key, json.dumps(value, ensure_ascii=False, default=str))

    def get_json(self, key: str) -> Any:
        raw = self.get_kv(key)
        return json.loads(raw) if raw is not None else None
