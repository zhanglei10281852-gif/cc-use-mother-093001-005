"""主动预警核心服务。

职责：
- 设备安装 / 校准 / 维护窗口 / 健康状态的历史维护（按安装实例隔离序列）；
- 带游标观测的乱序去重、质量判定、校准调整、趋势聚合与多级告警；
- 告警合并 / 升级 / 确认 / 转派 / 解除 / 误报复核，全部带原因留痕；
- 固定规则版本：新版本只影响后续计算，历史结论只能由显式重算任务改写；
- 可断点续跑的重算任务；
- 单条告警从原始读数到最终状态的完整解释。
"""
from __future__ import annotations

import json
import threading
import uuid
from datetime import datetime, timedelta
from typing import Any, Optional

from .contracts import (
    AlertLevel,
    AlertState,
    HealthStatus,
    JobStatus,
    MetricKind,
    Observation,
    Quality,
)
from .rules import Ruleset, default_ruleset, utcnow
from .store import Store, iso, to_dt


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


LEVEL_RANK = {AlertLevel.WARNING: 1, AlertLevel.CRITICAL: 2}


class AlertError(ValueError):
    """业务规则错误（请求本身不合法）。"""


class AlertService:
    def __init__(self, store: Store):
        self.store = store
        self._lock = threading.RLock()
        self._rulesets: dict[str, Ruleset] = {}
        self._bootstrap_rules()

    # ------------------------------------------------------------------ #
    # 规则版本
    # ------------------------------------------------------------------ #
    def _bootstrap_rules(self) -> None:
        with self._lock:
            rs = default_ruleset()
            row = self.store.query_one(
                "SELECT version FROM rule_versions WHERE version=?", (rs.version,)
            )
            if row is None:
                self.store.execute(
                    "INSERT INTO rule_versions(version, valid_from, body, created_at) "
                    "VALUES(?,?,?,?)",
                    (rs.version, iso(rs.valid_from),
                     json.dumps(rs.to_json(), ensure_ascii=False), iso(utcnow())),
                )
            self._rulesets[rs.version] = rs
            for r in self.store.query("SELECT body FROM rule_versions"):
                payload = json.loads(r["body"])
                self._rulesets[payload["version"]] = Ruleset.from_json(payload)
            if self.store.get_kv("active_rule_version") is None:
                self.store.set_kv("active_rule_version", rs.version)
            self._resume_jobs()

    def register_ruleset(self, ruleset: Ruleset) -> None:
        """注册新版本。版本号不可变、不可覆盖；注册本身不会改变在用版本。"""
        with self._lock:
            existing = self.store.query_one(
                "SELECT body FROM rule_versions WHERE version=?", (ruleset.version,)
            )
            if existing is not None:
                if json.loads(existing["body"]) != ruleset.to_json():
                    raise AlertError(f"规则版本 {ruleset.version} 已存在且内容不同，不可修改")
                self._rulesets[ruleset.version] = ruleset
                return
            self.store.execute(
                "INSERT INTO rule_versions(version, valid_from, body, created_at) "
                "VALUES(?,?,?,?)",
                (ruleset.version, iso(ruleset.valid_from),
                 json.dumps(ruleset.to_json(), ensure_ascii=False), iso(utcnow())),
            )
            self._rulesets[ruleset.version] = ruleset

    def set_active_version(self, version: str) -> str:
        """切换在用规则版本，只影响切换之后的新计算。"""
        with self._lock:
            if version not in self._rulesets:
                raise AlertError(f"未知规则版本 {version}")
            old = self.store.get_kv("active_rule_version")
            self.store.set_kv("active_rule_version", version)
            return old or version

    @property
    def active_version(self) -> str:
        return self.store.get_kv("active_rule_version") or "rules-v1"

    def _ruleset(self, version: str) -> Ruleset:
        ruleset = self._rulesets.get(version)
        if ruleset is not None:
            return ruleset
        # 支持其他进程注册的新版本：缓存未命中时回查数据库
        row = self.store.query_one(
            "SELECT body FROM rule_versions WHERE version=?", (version,))
        if row is None:
            raise AlertError(f"未知规则版本 {version}")
        ruleset = Ruleset.from_json(json.loads(row["body"]))
        self._rulesets[version] = ruleset
        return ruleset

    # ------------------------------------------------------------------ #
    # 设备生命周期：安装 / 校准 / 维护窗口
    # ------------------------------------------------------------------ #
    def register_device(
        self, device_id: str, metric: MetricKind | str, name: Optional[str] = None
    ) -> dict:
        metric = MetricKind(metric)
        with self._lock:
            row = self.store.query_one(
                "SELECT device_id FROM devices WHERE device_id=?", (device_id,)
            )
            if row:
                raise AlertError(f"设备 {device_id} 已登记")
            self.store.execute(
                "INSERT INTO devices(device_id, name, metric, created_at) VALUES(?,?,?,?)",
                (device_id, name, metric.value, iso(utcnow())),
            )
            self.store.execute(
                "INSERT INTO device_state(device_id, active_rule_version) VALUES(?,?)",
                (device_id, self.active_version),
            )
            return self.get_device(device_id)

    def _require_device(self, device_id: str) -> dict:
        row = self.store.query_one("SELECT * FROM devices WHERE device_id=?", (device_id,))
        if row is None:
            raise AlertError(f"设备 {device_id} 未登记")
        return dict(row)

    def install(
        self,
        device_id: str,
        serial_no: str,
        installed_at: datetime | str,
        note: Optional[str] = None,
    ) -> dict:
        """登记一次换表安装；新旧安装实例的序列永不串联。"""
        with self._lock:
            self._require_device(device_id)
            installed_dt = to_dt(installed_at)
            current = self.store.query_one(
                "SELECT installation_id FROM installations "
                "WHERE device_id=? AND removed_at IS NULL",
                (device_id,),
            )
            if current is not None:
                raise AlertError("设备仍有未拆除的安装实例，请先 remove_installation")
            overlap = self.store.query_one(
                "SELECT installation_id FROM installations WHERE device_id=? "
                "AND serial_no=? AND installed_at=? ",
                (device_id, serial_no, iso(installed_dt)),
            )
            if overlap:
                raise AlertError("同一设备同一序列号的安装记录已存在")
            inst_id = _new_id("INST")
            self.store.begin()
            try:
                self.store.execute(
                    "INSERT INTO installations(installation_id, device_id, serial_no, "
                    "installed_at, removed_at, note) VALUES(?,?,?,?,NULL,?)",
                    (inst_id, device_id, serial_no, iso(installed_dt), note),
                )
                self.store.execute(
                    "INSERT INTO ingestion_cursors(device_id, installation_id) VALUES(?,?)",
                    (device_id, inst_id),
                )
                self.store.execute(
                    "UPDATE device_state SET current_installation_id=? WHERE device_id=?",
                    (inst_id, device_id),
                )
                self.store.commit()
            except Exception:
                self.store.rollback()
                raise
            return self.get_installation(inst_id)

    def remove_installation(
        self,
        installation_id: str,
        removed_at: datetime | str,
        reason: str,
    ) -> dict:
        if not reason:
            raise AlertError("拆除安装必须填写原因")
        with self._lock:
            row = self.store.query_one(
                "SELECT * FROM installations WHERE installation_id=?", (installation_id,)
            )
            if row is None:
                raise AlertError("安装实例不存在")
            if row["removed_at"]:
                raise AlertError("安装实例已拆除")
            removed_dt = to_dt(removed_at)
            if removed_dt < to_dt(row["installed_at"]):
                raise AlertError("拆除时间不能早于安装时间")
            self.store.execute(
                "UPDATE installations SET removed_at=? WHERE installation_id=?",
                (iso(removed_dt), installation_id),
            )
            self.store.execute(
                "UPDATE device_state SET current_installation_id=NULL "
                "WHERE device_id=? AND current_installation_id=?",
                (row["device_id"], installation_id),
            )
            return self.get_installation(installation_id)

    def add_calibration(
        self,
        calibration_id: str,
        device_id: str,
        factor: float,
        calibrated_at: datetime | str,
    ) -> dict:
        with self._lock:
            self._require_device(device_id)
            cal_dt = to_dt(calibrated_at)
            inst = self._installation_at(device_id, cal_dt)
            if inst is None:
                raise AlertError("校准时间点设备没有在役安装实例")
            if self.store.query_one(
                "SELECT 1 FROM calibrations WHERE calibration_id=?", (calibration_id,)
            ):
                raise AlertError(f"校准记录 {calibration_id} 已存在")
            self.store.execute(
                "INSERT INTO calibrations(calibration_id, installation_id, device_id, "
                "factor, calibrated_at) VALUES(?,?,?,?,?)",
                (calibration_id, inst["installation_id"], device_id, float(factor), iso(cal_dt)),
            )
            return dict(self.store.query_one(
                "SELECT * FROM calibrations WHERE calibration_id=?", (calibration_id,)
            ))

    def open_maintenance(
        self,
        device_id: str,
        start_at: datetime | str,
        reason: str,
        end_at: Optional[datetime | str] = None,
    ) -> dict:
        if not reason:
            raise AlertError("维护窗口必须填写原因")
        with self._lock:
            self._require_device(device_id)
            wid = _new_id("MW")
            self.store.execute(
                "INSERT INTO maintenance_windows(window_id, device_id, start_at, end_at, "
                "reason, closed_at) VALUES(?,?,?,?,?,?)",
                (wid, device_id, iso(to_dt(start_at)),
                 iso(to_dt(end_at)) if end_at else None, reason,
                 iso(utcnow()) if end_at else None),
            )
            return self.get_maintenance(wid)

    def close_maintenance(self, window_id: str, end_at: datetime | str) -> dict:
        with self._lock:
            row = self.store.query_one(
                "SELECT * FROM maintenance_windows WHERE window_id=?", (window_id,)
            )
            if row is None:
                raise AlertError("维护窗口不存在")
            if row["end_at"]:
                raise AlertError("维护窗口已关闭")
            end_dt = to_dt(end_at)
            if end_dt < to_dt(row["start_at"]):
                raise AlertError("维护结束时间不能早于开始时间")
            self.store.execute(
                "UPDATE maintenance_windows SET end_at=?, closed_at=? WHERE window_id=?",
                (iso(end_dt), iso(utcnow()), window_id),
            )
            return self.get_maintenance(window_id)

    def _installation_at(self, device_id: str, when: datetime) -> Optional[dict]:
        rows = self.store.query(
            "SELECT * FROM installations WHERE device_id=? AND installed_at<=? "
            "AND (removed_at IS NULL OR removed_at>?) ORDER BY installed_at DESC",
            (device_id, iso(when), iso(when)),
        )
        return dict(rows[0]) if rows else None

    def _calibration_at(self, installation_id: str, when: datetime) -> Optional[dict]:
        row = self.store.query_one(
            "SELECT * FROM calibrations WHERE installation_id=? AND calibrated_at<=? "
            "ORDER BY calibrated_at DESC LIMIT 1",
            (installation_id, iso(when)),
        )
        return dict(row) if row else None

    def _maintenance_at(self, device_id: str, when: datetime) -> Optional[dict]:
        row = self.store.query_one(
            "SELECT * FROM maintenance_windows WHERE device_id=? AND start_at<=? "
            "AND (end_at IS NULL OR end_at>?) ORDER BY start_at DESC LIMIT 1",
            (device_id, iso(when), iso(when)),
        )
        return dict(row) if row else None

    # ------------------------------------------------------------------ #
    # 观测摄入：去重 / 质量判定 / 评估
    # ------------------------------------------------------------------ #
    def ingest(
        self,
        device_id: str,
        sequence: int,
        observed_at: datetime | str,
        value: float,
        serial_no: Optional[str] = None,
        *,
        ingest_time: Optional[datetime] = None,
    ) -> dict:
        """摄入一条观测。重复 (安装实例, 采集序号) 直接去重，不改变任何统计。"""
        with self._lock:
            device = self._require_device(device_id)
            metric = MetricKind(device["metric"])
            when = to_dt(observed_at)
            now = to_dt(ingest_time) if ingest_time else utcnow()
            version = self.active_version
            ruleset = self._ruleset(version)

            self.store.begin()
            try:
                inst = self._installation_at(device_id, when)
                if inst is None:
                    reason = "观测时刻设备无在役安装实例（换表前后的旧/新序列不得串联）"
                    self._reject(device_id, serial_no, sequence, when, value, now,
                                 Quality.NOT_INSTALLED, reason)
                    self.store.commit()
                    return {"device_id": device_id, "sequence": sequence,
                            "duplicate": False, "accepted": False,
                            "quality": Quality.NOT_INSTALLED.value, "reason": reason}

                if serial_no and serial_no != inst["serial_no"]:
                    reason = (f"上报序列号 {serial_no} 与在役安装序列号 "
                              f"{inst['serial_no']} 不符，判定为换表残留数据")
                    self._reject(device_id, serial_no, sequence, when, value, now,
                                 Quality.NOT_INSTALLED, reason)
                    self.store.commit()
                    return {"device_id": device_id, "sequence": sequence,
                            "duplicate": False, "accepted": False,
                            "quality": Quality.NOT_INSTALLED.value, "reason": reason,
                            "installation_id": inst["installation_id"]}

                dup = self.store.query_one(
                    "SELECT * FROM observations WHERE installation_id=? AND sequence=?",
                    (inst["installation_id"], sequence),
                )
                if dup is not None:
                    # 乱序重投 / 重复消息：原样去重，统计与告警均不变
                    self.store.execute(
                        "UPDATE ingestion_cursors SET dup_count=dup_count+1 "
                        "WHERE device_id=? AND installation_id=?",
                        (device_id, inst["installation_id"]),
                    )
                    self.store.commit()
                    return {"device_id": device_id, "sequence": sequence,
                            "duplicate": True, "accepted": False,
                            "quality": dup["quality"],
                            "reason": "重复采集序号，已去重",
                            "installation_id": inst["installation_id"]}

                cal = self._calibration_at(inst["installation_id"], when)
                factor = cal["factor"] if cal else 1.0
                adjusted = float(value) * factor

                quality, qreason = self._quality(
                    ruleset, metric, float(value), device_id, when
                )

                self.store.execute(
                    "INSERT INTO observations(device_id, installation_id, sequence, "
                    "observed_at, ingested_at, raw_value, adjusted_value, quality, "
                    "quality_reason, calibration_id, rule_version) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    (device_id, inst["installation_id"], int(sequence), iso(when),
                     iso(now), float(value), adjusted, quality.value, qreason,
                     cal["calibration_id"] if cal else None, version),
                )
                cursor_field = "good_count" if quality is Quality.GOOD else "reject_count"
                self.store.execute(
                    f"UPDATE ingestion_cursors SET last_sequence="
                    f"CASE WHEN last_sequence IS NULL OR ? > last_sequence THEN ? "
                    f"ELSE last_sequence END, "
                    f"last_observed_at="
                    f"CASE WHEN last_observed_at IS NULL OR ? > last_observed_at THEN ? "
                    f"ELSE last_observed_at END, {cursor_field}={cursor_field}+1 "
                    "WHERE device_id=? AND installation_id=?",
                    (int(sequence), int(sequence), iso(when), iso(when),
                     device_id, inst["installation_id"]),
                )

                actions: list[dict] = []
                if quality is Quality.GOOD:
                    self.store.execute(
                        "UPDATE device_state SET last_good_observed_at=?, "
                        "last_good_sequence=?, active_rule_version=? WHERE device_id=?",
                        (iso(when), int(sequence), version, device_id),
                    )
                    window = self._good_window(inst["installation_id"], ruleset, when)
                    actions = self._evaluate(
                        metric, ruleset, inst, when, int(sequence),
                        float(value), adjusted, window, now,
                    )

                self.store.commit()
            except Exception:
                self.store.rollback()
                raise

            return {"device_id": device_id, "sequence": sequence,
                    "duplicate": False, "accepted": True,
                    "quality": quality.value, "reason": qreason,
                    "installation_id": inst["installation_id"],
                    "adjusted_value": adjusted,
                    "calibration_id": cal["calibration_id"] if cal else None,
                    "rule_version": version, "actions": actions}

    def ingest_observation(self, obs: Observation,
                           serial_no: Optional[str] = None) -> dict:
        return self.ingest(obs.device_id, obs.sequence, obs.observed_at,
                           obs.value, serial_no=serial_no)

    def _quality(self, ruleset: Ruleset, metric: MetricKind, raw_value: float,
                 device_id: str, when: datetime) -> tuple[Quality, Optional[str]]:
        mw = self._maintenance_at(device_id, when)
        if mw is not None:
            return Quality.MAINTENANCE, f"处于维护窗口 {mw['window_id']}（{mw['reason']}）内"
        rng = ruleset.physical_range(metric)
        if rng is not None and (raw_value < rng[0] or raw_value > rng[1]):
            return Quality.OUT_OF_RANGE, (
                f"原始读数 {raw_value} 超出 {metric.value} 物理量程 "
                f"[{rng[0]}, {rng[1]}]，疑似短时离线/故障毛刺")
        return Quality.GOOD, None

    def _reject(self, device_id, serial_hint, sequence, when, value, now,
                quality: Quality, reason: str) -> None:
        self.store.execute(
            "INSERT INTO rejected_observations(device_id, serial_hint, sequence, "
            "observed_at, raw_value, ingested_at, quality, reason) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (device_id, serial_hint, sequence, iso(when), value, iso(now),
             quality.value, reason),
        )
        inst = self._installation_at(device_id, when)
        if inst is not None:
            self.store.execute(
                "UPDATE ingestion_cursors SET reject_count=reject_count+1 "
                "WHERE device_id=? AND installation_id=?",
                (device_id, inst["installation_id"]),
            )

    def _good_window(self, installation_id: str, ruleset: Ruleset,
                     now_at: datetime) -> list[dict]:
        need = max(
            [r.window for r in ruleset.trend_rules.values()],
            default=1,
        )
        rows = self.store.query(
            "SELECT * FROM observations WHERE installation_id=? AND quality='GOOD' "
            "AND observed_at<=? ORDER BY observed_at DESC, sequence DESC LIMIT ?",
            (installation_id, iso(now_at), int(need)),
        )
        return [dict(r) for r in reversed(rows)]

    # ------------------------------------------------------------------ #
    # 规则评估与告警状态机
    # ------------------------------------------------------------------ #
    def _evaluate(self, metric: MetricKind, ruleset: Ruleset, inst: dict,
                  when: datetime, sequence: int, raw: float, adjusted: float,
                  window: list[dict], now: datetime,
                  job_id: Optional[str] = None) -> list[dict]:
        actions: list[dict] = []

        # 1) 绝对阈值
        hit = ruleset.level_for(metric, adjusted)
        actions += self._apply_group(
            group=f"level:{metric.value}",
            kind="threshold",
            hit=None if hit is None else {
                "rule_id": hit[0], "level": hit[1],
                "rule_desc": self._describe_level(ruleset, metric, hit[0]),
            },
            ruleset=ruleset, inst=inst, metric=metric, when=when,
            sequence=sequence, raw=raw, adjusted=adjusted, job_id=job_id,
        )

        # 2) 趋势聚合
        trend = ruleset.trend_rules.get(metric)
        if trend is not None:
            values = [r["adjusted_value"] for r in window]
            delta = trend.delta_of(values)
            hit_t = None
            if delta is not None:
                lvl = trend.level_for(delta)
                if lvl is not None:
                    hit_t = {
                        "rule_id": trend.rule_id, "level": lvl,
                        "rule_desc": (f"最近 {trend.window} 次合格读数上行 "
                                      f"{delta:.2f}（窗口 {[round(v, 2) for v in values]}）"),
                        "delta": round(delta, 4),
                    }
            actions += self._apply_group(
                group=f"trend:{trend.rule_id}",
                kind="trend",
                hit=hit_t,
                ruleset=ruleset, inst=inst, metric=metric, when=when,
                sequence=sequence, raw=raw, adjusted=adjusted, job_id=job_id,
                window=values, delta=None if delta is None else round(delta, 4),
            )
        return actions

    def _describe_level(self, ruleset: Ruleset, metric: MetricKind,
                        rule_id: str) -> str:
        for r in ruleset.level_rules.get(metric, []):
            if r.rule_id == rule_id:
                return f"校准后读数 {r.describe()}"
        return rule_id

    def _active_alert(self, installation_id: str, group: str,
                      job_id: Optional[str] = None) -> Optional[dict]:
        if job_id is None:
            # 线上视角：实时告警 + 已完成重算任务产出的告警
            sql = (
                "SELECT * FROM alerts WHERE installation_id=? AND rule_group=? "
                "AND state IN ('OPEN','ACKNOWLEDGED','ASSIGNED') "
                "AND (created_by_job_id IS NULL OR created_by_job_id IN "
                "(SELECT job_id FROM recompute_jobs WHERE status='DONE')) "
                "ORDER BY opened_at DESC LIMIT 1"
            )
            params = (installation_id, group)
        else:
            sql = ("SELECT * FROM alerts WHERE installation_id=? AND rule_group=? "
                   "AND state IN ('OPEN','ACKNOWLEDGED','ASSIGNED') "
                   "AND created_by_job_id=? ORDER BY opened_at DESC LIMIT 1")
            params = (installation_id, group, job_id)
        row = self.store.query_one(sql, params)
        return dict(row) if row else None

    def _apply_group(self, *, group: str, kind: str, hit: Optional[dict],
                     ruleset: Ruleset, inst: dict, metric: MetricKind,
                     when: datetime, sequence: int, raw: float, adjusted: float,
                     job_id: Optional[str], window: Optional[list[float]] = None,
                     delta: Optional[float] = None) -> list[dict]:
        """同一 group 的持续异常合并为一条告警，只在级别跃迁时升级。"""
        alert = self._active_alert(inst["installation_id"], group, job_id)

        if hit is None:
            if alert is None:
                return []
            self.store.execute(
                "UPDATE alerts SET recover_count=recover_count+1, "
                "update_count=update_count+1 WHERE alert_id=?",
                (alert["alert_id"],),
            )
            alert["recover_count"] += 1
            self._event(
                alert["alert_id"], "RECOVERY_OBS", when,
                reason=f"读数 {round(adjusted, 4)} 恢复正常区间，"
                       f"连续恢复 {alert['recover_count']}/{ruleset.recover_streak}",
                sequence=sequence, observed_at=when, value=adjusted,
                ruleset=ruleset, job_id=job_id,
            )
            if alert["recover_count"] >= ruleset.recover_streak:
                self._close_alert(
                    alert, AlertState.RESOLVED, when,
                    actor="system",
                    reason=f"连续 {ruleset.recover_streak} 次合格读数回到正常区间，自动解除",
                    job_id=job_id, ruleset=ruleset,
                )
                return [{"action": "resolved", "alert_id": alert["alert_id"], "group": group}]
            return [{"action": "recovery_obs", "alert_id": alert["alert_id"], "group": group}]

        if alert is None:
            alert_id = _new_id("ALT")
            self.store.execute(
                "INSERT INTO alerts(alert_id, device_id, installation_id, rule_group, "
                "rule_id, metric, level, state, opened_at, first_sequence, "
                "trigger_observed_at, trigger_raw, trigger_adjusted, rule_version, "
                "created_by_job_id, last_notified_at, update_count, recover_count) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, ?,0,0)",
                (alert_id, inst["device_id"], inst["installation_id"], group,
                 hit["rule_id"], metric.value, hit["level"].value, AlertState.OPEN.value,
                 iso(when), sequence, iso(when), raw, adjusted, ruleset.version, job_id,
                 iso(when)),
            )
            self._event(
                alert_id, "OPENED", when,
                reason=f"命中规则 {hit['rule_id']}（{hit['rule_desc']}）",
                to_state=AlertState.OPEN.value, level=hit["level"].value,
                sequence=sequence, observed_at=when, value=adjusted,
                ruleset=ruleset, job_id=job_id,
                detail={"kind": kind, "window": window, "delta": delta,
                        "raw_value": raw},
            )
            return [{"action": "opened", "alert_id": alert_id, "group": group,
                     "level": hit["level"].value, "rule_id": hit["rule_id"]}]

        # 已有未结告警：合并重复消息
        self.store.execute(
            "UPDATE alerts SET update_count=update_count+1, recover_count=0 "
            "WHERE alert_id=?",
            (alert["alert_id"],),
        )
        current = AlertLevel(alert["level"])
        if LEVEL_RANK[hit["level"]] > LEVEL_RANK[current]:
            self.store.execute(
                "UPDATE alerts SET level=?, rule_id=?, last_notified_at=? "
                "WHERE alert_id=?",
                (hit["level"].value, hit["rule_id"], iso(when),
                 alert["alert_id"]),
            )
            self._event(
                alert["alert_id"], "ESCALATED", when,
                reason=f"持续恶化：{hit['rule_desc']}，{current.value} 升级为 "
                       f"{hit['level'].value}",
                from_state=alert["state"], level=hit["level"].value,
                sequence=sequence, observed_at=when, value=adjusted,
                ruleset=ruleset, job_id=job_id,
                detail={"kind": kind, "window": window, "delta": delta},
            )
            return [{"action": "escalated", "alert_id": alert["alert_id"],
                     "group": group, "level": hit["level"].value}]

        self._event(
            alert["alert_id"], "SUPPRESSED", when,
            reason=f"异常持续（{hit['rule_desc']}），合并进未结告警，不重复通知",
            level=current.value, sequence=sequence, observed_at=when,
            value=adjusted, ruleset=ruleset, job_id=job_id,
            detail={"kind": kind, "window": window, "delta": delta},
        )
        return [{"action": "suppressed", "alert_id": alert["alert_id"], "group": group}]

    def _event(self, alert_id: str, event_type: str, when: datetime, *,
               reason: str, actor: Optional[str] = None,
               from_state: Optional[str] = None, to_state: Optional[str] = None,
               level: Optional[str] = None, sequence: Optional[int] = None,
               observed_at: Optional[datetime] = None, value: Optional[float] = None,
               ruleset: Optional[Ruleset] = None, job_id: Optional[str] = None,
               detail: Optional[dict] = None) -> None:
        if not reason:
            raise AlertError("告警事件必须记录原因")
        self.store.execute(
            "INSERT INTO alert_events(alert_id, event_type, at, actor, reason, "
            "from_state, to_state, level, observation_sequence, observed_at, "
            "value_adjusted, rule_version, recompute_job_id, detail) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (alert_id, event_type, iso(when), actor, reason, from_state, to_state,
             level, sequence, iso(observed_at) if observed_at else None, value,
             ruleset.version if ruleset else None, job_id,
             json.dumps(detail, ensure_ascii=False) if detail else None),
        )

    def _close_alert(self, alert: dict, state: AlertState, when: datetime, *,
                     actor: str, reason: str, job_id: Optional[str] = None,
                     ruleset: Optional[Ruleset] = None) -> None:
        self.store.execute(
            "UPDATE alerts SET state=?, resolved_at=? WHERE alert_id=?",
            (state.value, iso(when), alert["alert_id"]),
        )
        self._event(
            alert["alert_id"],
            "FALSE_ALARM_REVIEW" if state is AlertState.FALSE_ALARM else "RESOLVED",
            when, actor=actor, reason=reason,
            from_state=alert["state"], to_state=state.value,
            ruleset=ruleset, job_id=job_id,
        )

    # ------------------------------------------------------------------ #
    # 离线健康扫描
    # ------------------------------------------------------------------ #
    def sweep_offline(self, now: Optional[datetime] = None) -> list[dict]:
        """依据最后一条合格观测的时间判定短时/长时离线。"""
        now = to_dt(now) if now else utcnow()
        actions: list[dict] = []
        with self._lock:
            ruleset = self._ruleset(self.active_version)
            states = self.store.query(
                "SELECT ds.*, d.metric FROM device_state ds "
                "JOIN devices d ON d.device_id=ds.device_id "
                "WHERE ds.current_installation_id IS NOT NULL"
            )
            for st in states:
                last = st["last_good_observed_at"]
                if last is None:
                    continue
                elapsed = now - to_dt(last)
                level: Optional[AlertLevel] = None
                if elapsed >= ruleset.offline.critical_after:
                    level = AlertLevel.CRITICAL
                elif elapsed >= ruleset.offline.warning_after:
                    level = AlertLevel.WARNING
                inst = {"device_id": st["device_id"],
                        "installation_id": st["current_installation_id"]}
                hit = None
                if level is not None:
                    hit = {
                        "rule_id": "device-offline",
                        "level": level,
                        "rule_desc": f"已 {int(elapsed.total_seconds())}s 无合格读数",
                    }
                # 每设备独立事务：多设备扫描时单个失败不影响其他设备
                self.store.begin()
                try:
                    actions += self._apply_offline_group(
                        inst=inst, hit=hit, ruleset=ruleset, when=now,
                        elapsed=elapsed,
                    )
                    self.store.commit()
                except Exception:
                    self.store.rollback()
                    raise
        return actions

    def _apply_offline_group(self, *, inst: dict, hit: Optional[dict],
                             ruleset: Ruleset, when: datetime,
                             elapsed: timedelta) -> list[dict]:
        group = "offline"
        row = self.store.query_one(
            "SELECT * FROM alerts WHERE installation_id=? AND rule_group=? "
            "AND state IN ('OPEN','ACKNOWLEDGED','ASSIGNED') "
            "AND (created_by_job_id IS NULL OR created_by_job_id IN "
            "(SELECT job_id FROM recompute_jobs WHERE status='DONE')) "
            "ORDER BY opened_at DESC LIMIT 1",
            (inst["installation_id"], group),
        )
        alert = dict(row) if row else None
        if hit is None:
            if alert is None:
                return []
            self._close_alert(
                alert, AlertState.RESOLVED, when, actor="system",
                reason="设备恢复上报合格读数，离线告警解除", ruleset=ruleset,
            )
            return [{"action": "resolved", "alert_id": alert["alert_id"], "group": group}]
        if alert is None:
            alert_id = _new_id("ALT")
            self.store.execute(
                "INSERT INTO alerts(alert_id, device_id, installation_id, rule_group, "
                "rule_id, metric, level, state, opened_at, first_sequence, "
                "trigger_observed_at, trigger_raw, trigger_adjusted, rule_version, "
                "created_by_job_id, last_notified_at, update_count, recover_count) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,NULL,NULL,?,NULL,?,0,0)",
                (alert_id, inst["device_id"], inst["installation_id"], group,
                 "device-offline", MetricKind.HEALTH.value, hit["level"].value,
                 AlertState.OPEN.value, iso(when), 0, iso(when),
                 ruleset.version, iso(when)),
            )
            self._event(
                alert_id, "OPENED", when,
                reason=f"命中规则 device-offline（{hit['rule_desc']}）",
                to_state=AlertState.OPEN.value, level=hit["level"].value,
                ruleset=ruleset,
            )
            return [{"action": "opened", "alert_id": alert_id, "group": group,
                     "level": hit["level"].value}]
        current = AlertLevel(alert["level"])
        if LEVEL_RANK[hit["level"]] > LEVEL_RANK[current]:
            self.store.execute(
                "UPDATE alerts SET level=?, last_notified_at=? WHERE alert_id=?",
                (hit["level"].value, iso(when), alert["alert_id"]),
            )
            self._event(
                alert["alert_id"], "ESCALATED", when,
                reason=f"离线持续：{hit['rule_desc']}，{current.value} 升级为 "
                       f"{hit['level'].value}",
                from_state=alert["state"], level=hit["level"].value,
                ruleset=ruleset,
            )
            return [{"action": "escalated", "alert_id": alert["alert_id"],
                     "group": group, "level": hit["level"].value}]
        self.store.execute(
            "UPDATE alerts SET update_count=update_count+1 WHERE alert_id=?",
            (alert["alert_id"],),
        )
        self._event(
            alert["alert_id"], "SUPPRESSED", when,
            reason=f"设备仍离线（{int(elapsed.total_seconds())}s），合并重复扫描",
            level=current.value, ruleset=ruleset,
        )
        return [{"action": "suppressed", "alert_id": alert["alert_id"], "group": group}]

    # ------------------------------------------------------------------ #
    # 告警人工处置：确认 / 转派 / 解除 / 误报复核
    # ------------------------------------------------------------------ #
    def _get_active_alert(self, alert_id: str) -> dict:
        row = self.store.query_one("SELECT * FROM alerts WHERE alert_id=?", (alert_id,))
        if row is None:
            raise AlertError(f"告警 {alert_id} 不存在")
        return dict(row)

    def _transition(self, alert_id: str, event_type: str, actor: str, reason: str,
                    new_state: AlertState, *, allowed: set[AlertState],
                    detail: Optional[dict] = None) -> dict:
        if not actor:
            raise AlertError("处置操作必须记录操作人")
        if not reason:
            raise AlertError("处置操作必须填写原因")
        with self._lock:
            alert = self._get_active_alert(alert_id)
            state = AlertState(alert["state"])
            if state not in allowed:
                raise AlertError(
                    f"告警当前状态 {state.value} 不允许 {event_type}")
            self.store.begin()
            try:
                self.store.execute(
                    "UPDATE alerts SET state=? WHERE alert_id=?",
                    (new_state.value, alert_id),
                )
                self._event(
                    alert_id, event_type, utcnow(), actor=actor, reason=reason,
                    from_state=state.value, to_state=new_state.value,
                    level=alert["level"], detail=detail,
                )
                self.store.commit()
            except Exception:
                self.store.rollback()
                raise
            return self.get_alert(alert_id)

    def acknowledge(self, alert_id: str, actor: str, reason: str) -> dict:
        return self._transition(
            alert_id, "ACKNOWLEDGED", actor, reason, AlertState.ACKNOWLEDGED,
            allowed={AlertState.OPEN},
        )

    def assign(self, alert_id: str, actor: str, assignee: str, reason: str) -> dict:
        if not assignee:
            raise AlertError("转派必须指定处理人")
        return self._transition(
            alert_id, "ASSIGNED", actor, reason, AlertState.ASSIGNED,
            allowed={AlertState.OPEN, AlertState.ACKNOWLEDGED, AlertState.ASSIGNED},
            detail={"assignee": assignee},
        )

    def resolve(self, alert_id: str, actor: str, reason: str) -> dict:
        return self._transition(
            alert_id, "RESOLVED", actor, reason, AlertState.RESOLVED,
            allowed={AlertState.OPEN, AlertState.ACKNOWLEDGED, AlertState.ASSIGNED},
        )

    def mark_false_alarm(self, alert_id: str, actor: str, reason: str,
                         review_note: Optional[str] = None) -> dict:
        """误报复核：可从任何未结状态、或已解除状态复核为误报。"""
        alert = self._get_active_alert(alert_id)
        state = AlertState(alert["state"])
        allowed = {
            AlertState.OPEN, AlertState.ACKNOWLEDGED, AlertState.ASSIGNED,
            AlertState.RESOLVED,
        }
        return self._transition(
            alert_id, "FALSE_ALARM_REVIEW", actor, reason, AlertState.FALSE_ALARM,
            allowed=allowed,
            detail={"review_note": review_note, "previous_state": state.value},
        )

    # ------------------------------------------------------------------ #
    # 显式重算（新版本只作用于显式任务，可断点续跑）
    # ------------------------------------------------------------------ #
    def create_recompute_job(
        self,
        device_id: str,
        rule_version: str,
        reason: str,
        from_observed_at: Optional[datetime | str] = None,
        to_observed_at: Optional[datetime | str] = None,
    ) -> dict:
        if not reason:
            raise AlertError("重算任务必须说明原因")
        with self._lock:
            self._require_device(device_id)
            self._ruleset(rule_version)
            job_id = _new_id("JOB")
            self.store.execute(
                "INSERT INTO recompute_jobs(job_id, device_id, rule_version, "
                "from_observed_at, to_observed_at, status, reason, created_at) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (job_id, device_id, rule_version,
                 iso(to_dt(from_observed_at)) if from_observed_at else None,
                 iso(to_dt(to_observed_at)) if to_observed_at else None,
                 JobStatus.PENDING.value, reason, iso(utcnow())),
            )
            return self.get_job(job_id)

    def _resume_jobs(self) -> None:
        """重启后：上次崩溃时停在 RUNNING 的任务退回 PENDING，等待续跑。"""
        self.store.execute(
            "UPDATE recompute_jobs SET status='PENDING', error=NULL WHERE status='RUNNING'"
        )

    def run_pending_jobs(self, limit: int = 1) -> list[dict]:
        results = []
        with self._lock:
            jobs = self.store.query(
                "SELECT * FROM recompute_jobs WHERE status='PENDING' "
                "ORDER BY created_at LIMIT ?",
                (int(limit),),
            )
            for job in jobs:
                results.append(self._run_job(dict(job)))
        return results

    def _run_job(self, job: dict) -> dict:
        ruleset = self._ruleset(job["rule_version"])
        self.store.begin()
        try:
            self.store.execute(
                "UPDATE recompute_jobs SET status='RUNNING', started_at=COALESCE("
                "started_at, ?), error=NULL WHERE job_id=?",
                (iso(utcnow()), job["job_id"]),
            )
            self.store.commit()
        except Exception:
            self.store.rollback()
            raise

        self.store.begin()
        try:
            device = self._require_device(job["device_id"])
            metric = MetricKind(device["metric"])
            insts = self.store.query(
                "SELECT * FROM installations WHERE device_id=? ORDER BY installed_at",
                (job["device_id"],),
            )

            # 幂等续跑：先撤销本任务上次（可能因崩溃中断的）部分结果，再完整回放。
            old_alerts = self.store.query(
                "SELECT alert_id FROM alerts WHERE created_by_job_id=?",
                (job["job_id"],),
            )
            for a in old_alerts:
                self.store.execute(
                    "DELETE FROM alert_events WHERE alert_id=?", (a["alert_id"],))
                self.store.execute(
                    "DELETE FROM alerts WHERE alert_id=?", (a["alert_id"],))
            # 恢复上次被本任务判废的旧告警到判废前状态，使重跑结论可重复
            superseded = self.store.query(
                "SELECT alert_id, from_state FROM alert_events "
                "WHERE recompute_job_id=? AND event_type='SUPERSEDED'",
                (job["job_id"],),
            )
            for e in superseded:
                self.store.execute(
                    "UPDATE alerts SET state=? WHERE alert_id=? AND state='SUPERSEDED'",
                    (e["from_state"], e["alert_id"]),
                )
            self.store.execute(
                "DELETE FROM alert_events WHERE recompute_job_id=? "
                "AND event_type='SUPERSEDED'", (job["job_id"],),
            )

            range_clauses = ["quality='GOOD'"]
            range_params: list[Any] = []
            if job["from_observed_at"]:
                range_clauses.append("observed_at>=?")
                range_params.append(job["from_observed_at"])
            if job["to_observed_at"]:
                range_clauses.append("observed_at<=?")
                range_params.append(job["to_observed_at"])
            range_sql = " AND ".join(range_clauses)

            need = max([r.window for r in ruleset.trend_rules.values()], default=1)

            for inst_row in insts:
                inst = dict(inst_row)

                # 旧版本结论判废：只判废重算实际回放的规则组（阈值/趋势）；
                # 实时结论与其他任务结论都纳入；本任务产物已在前面清理。
                # 不触碰人工误报、已判废记录与离线告警（离线由扫描独立维护）。
                prior_sql = (
                    "SELECT * FROM alerts WHERE installation_id=? "
                    "AND COALESCE(created_by_job_id,'')<>? "
                    "AND rule_version<>? AND rule_group<>'offline' "
                    "AND state<>'FALSE_ALARM' AND state<>'SUPERSEDED'"
                )
                prior_params: list[Any] = [
                    inst["installation_id"], job["job_id"], job["rule_version"]]
                if job["from_observed_at"]:
                    prior_sql += " AND trigger_observed_at>=?"
                    prior_params.append(job["from_observed_at"])
                if job["to_observed_at"]:
                    prior_sql += " AND trigger_observed_at<=?"
                    prior_params.append(job["to_observed_at"])
                for a in self.store.query(prior_sql, prior_params):
                    self.store.execute(
                        "UPDATE alerts SET state='SUPERSEDED' WHERE alert_id=?",
                        (a["alert_id"],),
                    )
                    self._event(
                        a["alert_id"], "SUPERSEDED", to_dt(a["trigger_observed_at"]),
                        actor="system",
                        reason=f"重算任务 {job['job_id']} 以规则版本 "
                               f"{job['rule_version']} 重新判定（{job['reason']}），"
                               f"旧版本 {a['rule_version']} 结论作废",
                        from_state=a["state"], to_state=AlertState.SUPERSEDED.value,
                        level=a["level"], job_id=job["job_id"],
                    )

                rows = self.store.query(
                    f"SELECT * FROM observations WHERE installation_id=? AND {range_sql} "
                    "ORDER BY observed_at, sequence",
                    [inst["installation_id"], *range_params],
                )

                # 冷启动回放：每个安装实例独立窗口，新旧表序列互不串联；
                # 指定起点时用起点之前的合格观测预热趋势窗口。
                window: list[dict] = []
                if job["from_observed_at"] and need > 1:
                    seed = self.store.query(
                        "SELECT * FROM observations WHERE installation_id=? "
                        "AND quality='GOOD' AND observed_at<? "
                        "ORDER BY observed_at DESC, sequence DESC LIMIT ?",
                        (inst["installation_id"], job["from_observed_at"],
                         max(need * 3, 12)),
                    )
                    rng = ruleset.physical_range(metric)
                    kept = [
                        dict(r) for r in seed
                        if rng is None or rng[0] <= r["raw_value"] <= rng[1]
                    ][: need - 1]
                    window = list(reversed(kept))

                last_seq: Optional[int] = None
                for r in rows:
                    obs = dict(r)
                    # 按新版本重新做量程判定：新版本下落出物理量程的旧读数不参与回放
                    rng = ruleset.physical_range(metric)
                    if rng is not None and (
                            obs["raw_value"] < rng[0] or obs["raw_value"] > rng[1]):
                        continue
                    window.append(obs)
                    window = window[-need:]
                    self._evaluate(
                        metric, ruleset, inst,
                        to_dt(obs["observed_at"]), obs["sequence"],
                        obs["raw_value"], obs["adjusted_value"], window,
                        to_dt(obs["ingested_at"]), job_id=job["job_id"],
                    )
                    last_seq = obs["sequence"]

                # 检查点：按安装实例记录重算进度，崩溃重启后任务自动续跑
                self.store.execute(
                    "UPDATE recompute_jobs SET checkpoint_at=?, checkpoint_seq=? "
                    "WHERE job_id=?",
                    (iso(utcnow()), last_seq, job["job_id"]),
                )

            self.store.execute(
                "UPDATE recompute_jobs SET status='DONE', finished_at=? WHERE job_id=?",
                (iso(utcnow()), job["job_id"]),
            )
            self.store.commit()
        except Exception as exc:
            self.store.rollback()
            self.store.execute(
                "UPDATE recompute_jobs SET status='FAILED', error=? WHERE job_id=?",
                (str(exc), job["job_id"]),
            )
            raise
        return self.get_job(job["job_id"])

    # ------------------------------------------------------------------ #
    # 查询
    # ------------------------------------------------------------------ #
    def get_device(self, device_id: str) -> dict:
        d = self.store.row_to_dict(self.store.query_one(
            "SELECT * FROM devices WHERE device_id=?", (device_id,)))
        if d is None:
            raise AlertError(f"设备 {device_id} 不存在")
        d["state"] = self.store.row_to_dict(self.store.query_one(
            "SELECT * FROM device_state WHERE device_id=?", (device_id,)))
        return d

    def get_installation(self, installation_id: str) -> dict:
        row = self.store.row_to_dict(self.store.query_one(
            "SELECT * FROM installations WHERE installation_id=?", (installation_id,)))
        if row is None:
            raise AlertError("安装实例不存在")
        return row

    def get_maintenance(self, window_id: str) -> dict:
        row = self.store.row_to_dict(self.store.query_one(
            "SELECT * FROM maintenance_windows WHERE window_id=?", (window_id,)))
        if row is None:
            raise AlertError("维护窗口不存在")
        return row

    def get_cursor(self, device_id: str, installation_id: str) -> dict:
        row = self.store.row_to_dict(self.store.query_one(
            "SELECT * FROM ingestion_cursors WHERE device_id=? AND installation_id=?",
            (device_id, installation_id)))
        if row is None:
            raise AlertError("游标不存在")
        return row

    def list_alerts(self, device_id: Optional[str] = None,
                    active_only: bool = False) -> list[dict]:
        sql = "SELECT * FROM alerts WHERE 1=1"
        params: list[Any] = []
        if device_id:
            sql += " AND device_id=?"
            params.append(device_id)
        if active_only:
            sql += " AND state IN ('OPEN','ACKNOWLEDGED','ASSIGNED')"
        sql += " ORDER BY opened_at DESC"
        return [dict(r) for r in self.store.query(sql, params)]

    def get_alert(self, alert_id: str) -> dict:
        row = self.store.row_to_dict(
            self.store.query_one("SELECT * FROM alerts WHERE alert_id=?", (alert_id,)))
        if row is None:
            raise AlertError(f"告警 {alert_id} 不存在")
        return row

    def list_events(self, alert_id: str) -> list[dict]:
        rows = self.store.query(
            "SELECT * FROM alert_events WHERE alert_id=? ORDER BY event_id",
            (alert_id,))
        return [dict(r) for r in rows]

    def get_job(self, job_id: str) -> dict:
        row = self.store.row_to_dict(self.store.query_one(
            "SELECT * FROM recompute_jobs WHERE job_id=?", (job_id,)))
        if row is None:
            raise AlertError(f"重算任务 {job_id} 不存在")
        return row

    def list_jobs(self) -> list[dict]:
        return [dict(r) for r in self.store.query(
            "SELECT * FROM recompute_jobs ORDER BY created_at DESC")]

    def health(self, now: Optional[datetime] = None) -> dict:
        now = to_dt(now) if now else utcnow()
        ruleset = self._ruleset(self.active_version)
        result = {}
        for d in self.store.query("SELECT device_id FROM devices"):
            st = self.store.query_one(
                "SELECT * FROM device_state WHERE device_id=?", (d["device_id"],))
            last = st["last_good_observed_at"] if st else None
            status = HealthStatus.ONLINE.value
            if last is not None:
                elapsed = now - to_dt(last)
                if elapsed >= ruleset.offline.warning_after:
                    status = HealthStatus.OFFLINE.value
            result[d["device_id"]] = {
                "status": status,
                "last_good_observed_at": last,
                "has_active_installation": bool(
                    st and st["current_installation_id"]),
            }
        return result

    # ------------------------------------------------------------------ #
    # 告警全过程解释
    # ------------------------------------------------------------------ #
    def explain(self, alert_id: str) -> dict:
        """返回一条告警从原始读数到当前状态的完整证据链。"""
        alert = self.get_alert(alert_id)
        device = self.store.row_to_dict(self.store.query_one(
            "SELECT * FROM devices WHERE device_id=?", (alert["device_id"],)))
        inst = self.store.row_to_dict(self.store.query_one(
            "SELECT * FROM installations WHERE installation_id=?",
            (alert["installation_id"],)))
        events = self.list_events(alert_id)
        trigger = self.store.row_to_dict(self.store.query_one(
            "SELECT * FROM observations WHERE installation_id=? AND sequence=?",
            (alert["installation_id"], alert["first_sequence"])))

        is_offline = alert["rule_group"] == "offline"
        steps: list[dict] = []
        if is_offline:
            steps.append({
                "phase": "source",
                "title": "扫描判定（无观测读数）",
                "detail": ("离线扫描时刻 {at}；该告警不是由某条读数触发，而是因为超过"
                           "允许间隔仍无合格读数（游标序号记为 0）").format(
                    at=alert["trigger_observed_at"]),
            })
        else:
            steps.append({
                "phase": "source",
                "title": "原始读数",
                "detail": ("采集游标序号 {seq}，观测时刻 {at}，原始值 {raw}").format(
                    seq=alert["first_sequence"], at=alert["trigger_observed_at"],
                    raw=trigger["raw_value"] if trigger else alert["trigger_raw"]),
            })
        steps.append({
            "phase": "identity",
            "title": "设备身份与安装实例",
            "detail": ("设备 {dev}（{metric}）→ 安装实例 {inst}，序列号 {serial}，"
                       "安装于 {installed}{removed}").format(
                dev=alert["device_id"], metric=device["metric"],
                inst=inst["installation_id"], serial=inst["serial_no"],
                installed=inst["installed_at"],
                removed=f"，{inst['removed_at']} 拆除" if inst["removed_at"] else ""),
        })
        if trigger:
            cal = None
            if trigger["calibration_id"]:
                cal = self.store.row_to_dict(self.store.query_one(
                    "SELECT * FROM calibrations WHERE calibration_id=?",
                    (trigger["calibration_id"],)))
            steps.append({
                "phase": "quality",
                "title": "质量判定",
                "detail": f"判定结果 {trigger['quality']}：{trigger['quality_reason'] or '合格'}",
            })
            steps.append({
                "phase": "calibration",
                "title": "校准调整",
                "detail": ("采用校准 {cid}，系数 {factor} → 调整值 {adj}").format(
                    cid=trigger["calibration_id"] or "无（按原值 1.0）",
                    factor=cal["factor"] if cal else 1.0,
                    adj=round(trigger["adjusted_value"], 4)),
            })
            steps.append({
                "phase": "ruleset",
                "title": "规则版本",
                "detail": f"按固定版本 {trigger['rule_version']} 评估",
            })

        # 趋势窗口证据
        trend_events = [e for e in events if e["event_type"] in ("OPENED", "ESCALATED")]
        for e in trend_events:
            detail = json.loads(e["detail"]) if e["detail"] else {}
            if detail.get("window"):
                steps.append({
                    "phase": "trend_window",
                    "title": f"趋势聚合（事件 {e['event_type']} @ {e['at']}）",
                    "detail": f"合格观测窗口 {detail['window']}，变化量 {detail.get('delta')}",
                })

        steps.append({
            "phase": "rule_match",
            "title": "规则命中与告警产生",
            "detail": f"规则 {alert['rule_id']}，初始级别 {events[0]['level'] if events else alert['level']}",
        })

        lifecycle = []
        for e in events:
            lifecycle.append({
                "event": e["event_type"],
                "at": e["at"],
                "actor": e["actor"],
                "reason": e["reason"],
                "from_state": e["from_state"],
                "to_state": e["to_state"],
                "level": e["level"],
                "sequence": e["observation_sequence"],
                "value": e["value_adjusted"],
                "rule_version": e["rule_version"],
                "recompute_job_id": e["recompute_job_id"],
                "detail": json.loads(e["detail"]) if e["detail"] else None,
            })
        steps.append({
            "phase": "lifecycle",
            "title": "合并 / 升级 / 确认 / 转派 / 解除 / 误报复核留痕",
            "events": lifecycle,
        })
        steps.append({
            "phase": "current",
            "title": "最终状态",
            "detail": (f"{alert['state']} / {alert['level']}，"
                       f"合并更新 {alert['update_count']} 次，"
                       f"规则版本 {alert['rule_version']}"
                       + (f"，由重算任务 {alert['created_by_job_id']} 生成"
                          if alert["created_by_job_id"] else "")),
        })

        return {
            "alert": alert,
            "device": device,
            "installation": inst,
            "trigger_observation": trigger,
            "steps": steps,
            "timeline": lifecycle,
        }
