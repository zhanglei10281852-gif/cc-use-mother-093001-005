"""SQLite 持久化：设备、安装、校准、健康、维护窗口、观测、桶、告警、重算任务。"""
from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS devices (
  device_id   TEXT PRIMARY KEY,
  metric_type TEXT NOT NULL,
  hard_min    REAL,
  hard_max    REAL,
  created_at  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS installs (
  install_id      TEXT PRIMARY KEY,
  device_id       TEXT NOT NULL,
  seq             INTEGER NOT NULL,          -- 同一设备的第几次安装（换表序号）
  installed_at    TEXT NOT NULL,
  removed_at      TEXT,
  remove_reason   TEXT,
  replacement_of  TEXT
);
CREATE INDEX IF NOT EXISTS idx_installs_device ON installs(device_id, installed_at);

CREATE TABLE IF NOT EXISTS calibrations (
  calibration_id TEXT PRIMARY KEY,
  install_id     TEXT NOT NULL,
  factor         REAL NOT NULL,
  effective_from TEXT NOT NULL,
  note           TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_cal_install ON calibrations(install_id, effective_from);

CREATE TABLE IF NOT EXISTS health_events (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  install_id     TEXT NOT NULL,
  status         TEXT NOT NULL,
  effective_from TEXT NOT NULL,
  note           TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_health_install ON health_events(install_id, effective_from);

CREATE TABLE IF NOT EXISTS maintenance_windows (
  window_id  TEXT PRIMARY KEY,
  install_id TEXT NOT NULL,
  start_at   TEXT NOT NULL,
  end_at     TEXT NOT NULL,
  reason     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_mw_install ON maintenance_windows(install_id, start_at, end_at);

CREATE TABLE IF NOT EXISTS rule_versions (
  rule_version INTEGER PRIMARY KEY AUTOINCREMENT,
  payload      TEXT NOT NULL,
  created_at   TEXT NOT NULL,
  note         TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS observations (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  install_id       TEXT NOT NULL,
  device_id        TEXT NOT NULL,
  sequence         INTEGER NOT NULL,
  observed_at      TEXT NOT NULL,
  raw_value        REAL NOT NULL,
  calibrated_value REAL NOT NULL,
  factor           REAL NOT NULL,
  calibration_id   TEXT,
  quality          TEXT NOT NULL,
  quality_reasons  TEXT NOT NULL,          -- JSON 数组
  late             INTEGER NOT NULL DEFAULT 0, -- 迟到：落桶时该桶已定稿
  bucket_start     TEXT,
  rule_version     INTEGER NOT NULL,
  processed_at     TEXT NOT NULL,
  UNIQUE(install_id, sequence)
);
CREATE INDEX IF NOT EXISTS idx_obs_time ON observations(install_id, observed_at);
CREATE INDEX IF NOT EXISTS idx_obs_bucket ON observations(install_id, bucket_start);

CREATE TABLE IF NOT EXISTS dedup_cursor (
  install_id       TEXT PRIMARY KEY,
  last_sequence    INTEGER NOT NULL,
  last_observed_at TEXT NOT NULL,
  updated_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS buckets (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  install_id       TEXT NOT NULL,
  bucket_start     TEXT NOT NULL,
  rule_version     INTEGER NOT NULL,
  good_count       INTEGER NOT NULL DEFAULT 0,
  suspect_count    INTEGER NOT NULL DEFAULT 0,
  bad_count        INTEGER NOT NULL DEFAULT 0,
  suppressed_count INTEGER NOT NULL DEFAULT 0,
  sum_value        REAL NOT NULL DEFAULT 0,
  min_value        REAL,
  max_value        REAL,
  trend_slope      REAL,
  direction        TEXT,                   -- high / low / NULL
  level            TEXT,                   -- warn / crit / NULL
  triggers         TEXT NOT NULL DEFAULT '[]',
  breach_hi_streak INTEGER NOT NULL DEFAULT 0,
  crit_hi_streak   INTEGER NOT NULL DEFAULT 0,
  breach_lo_streak INTEGER NOT NULL DEFAULT 0,
  crit_lo_streak   INTEGER NOT NULL DEFAULT 0,
  recover_hi       INTEGER NOT NULL DEFAULT 0,
  recover_lo       INTEGER NOT NULL DEFAULT 0,
  finalized        INTEGER NOT NULL DEFAULT 0,
  UNIQUE(install_id, bucket_start)
);

CREATE TABLE IF NOT EXISTS alerts (
  alert_id      TEXT PRIMARY KEY,
  install_id    TEXT NOT NULL,
  device_id     TEXT NOT NULL,
  metric_type   TEXT NOT NULL,
  rule_version  INTEGER NOT NULL,
  level         TEXT NOT NULL,
  status        TEXT NOT NULL,
  direction     TEXT NOT NULL,
  first_bucket  TEXT NOT NULL,
  latest_bucket TEXT NOT NULL,
  first_value   REAL,
  latest_value  REAL,
  threshold     REAL,
  root_alert_id TEXT,
  created_at    TEXT NOT NULL,
  updated_at    TEXT NOT NULL,
  acked_by      TEXT,
  acked_at      TEXT,
  assignee      TEXT,
  assigned_at   TEXT,
  resolved_at   TEXT
);
CREATE INDEX IF NOT EXISTS idx_alerts_install ON alerts(install_id, status);
CREATE INDEX IF NOT EXISTS idx_alerts_device ON alerts(device_id, status);

CREATE TABLE IF NOT EXISTS alert_events (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  alert_id    TEXT NOT NULL,
  event_type  TEXT NOT NULL,
  at          TEXT NOT NULL,
  actor       TEXT NOT NULL,
  reason      TEXT NOT NULL DEFAULT '',
  from_status TEXT,
  to_status   TEXT,
  from_level  TEXT,
  to_level    TEXT,
  detail      TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_events_alert ON alert_events(alert_id, id);

CREATE TABLE IF NOT EXISTS recalc_jobs (
  job_id           TEXT PRIMARY KEY,
  install_id       TEXT NOT NULL,
  rule_version     INTEGER NOT NULL,
  from_bucket      TEXT,
  status           TEXT NOT NULL,          -- pending / running / completed / failed
  phase            TEXT NOT NULL DEFAULT 'queued', -- queued / rebuilding / done
  created_at       TEXT NOT NULL,
  started_at       TEXT,
  completed_at     TEXT,
  cursor_bucket    TEXT,
  processed_buckets INTEGER NOT NULL DEFAULT 0,
  total_buckets    INTEGER NOT NULL DEFAULT 0,
  error            TEXT
);
CREATE TABLE IF NOT EXISTS recalc_saved_manual (
  job_id      TEXT NOT NULL,
  direction   TEXT NOT NULL,
  alert_id    TEXT NOT NULL,
  state_json  TEXT NOT NULL,
  events_json TEXT NOT NULL,
  PRIMARY KEY (job_id, direction)
);
"""


def to_iso(dt: datetime) -> str:
    """统一存成 UTC ISO 字符串。"""
    if dt.tzinfo is None:
        raise ValueError("时间必须带时区")
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def parse_iso(s: str) -> datetime:
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


class Storage:
    def __init__(self, path: str | Path = ":memory:"):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path, check_same_thread=False,
                                    isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self._wlock = threading.RLock()  # 多线程 HTTP 下串行化写事务
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.executescript("BEGIN;" + SCHEMA + "COMMIT;")

    @contextmanager
    def tx(self):
        """串行化的读写事务；隔离级 None 下手动 BEGIN/COMMIT。"""
        self._wlock.acquire()
        try:
            self.conn.execute("BEGIN")
            yield self.conn
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise
        finally:
            self._wlock.release()

    def query(self, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
        return list(self.conn.execute(sql, tuple(params)))

    def query_one(self, sql: str, params: Iterable[Any] = ()) -> Optional[sqlite3.Row]:
        return self.conn.execute(sql, tuple(params)).fetchone()

    def close(self) -> None:
        self.conn.close()
