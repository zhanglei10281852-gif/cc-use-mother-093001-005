"""核心处理引擎：乱序去重、质量判定、趋势聚合、多级告警与显式重算。

关键语义：
- 观测按 (install_id, sequence) 去重；重复观测直接返回，不改变任何统计。
- 换表产生新 install_id，游标、去重、桶、告警全部按 install 隔离，新旧序列不串联。
- 桶在"下一桶首条观测到达"或 sweep 墙钟扫描时定稿(finalize)；定稿前允许乱序写入，
  定稿后迟到观测只入库留痕(late=1)，不改写结论，只能由显式重算修正。
- 处理时钉选当前规则版本；新版本只影响后续观测与桶，历史数据仅通过显式重算任务改变。
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from .contracts import (AlertLevel, AlertStatus, EventType, HealthStatus,
                        Observation, Quality)
from .registry import Registry
from .rules import MetricRule, RuleRegistry
from .storage import Storage, parse_iso, to_iso

MANUAL_EVENT_TYPES = (EventType.ACKED, EventType.ASSIGNED, EventType.ESCALATED,
                      EventType.REVIEWED)
AUTO_EVENT_TYPES = (EventType.CREATED, EventType.MERGED, EventType.LEVEL_CHANGED,
                    EventType.RESOLVED)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def bucket_floor(ts: datetime, width_s: int) -> datetime:
    epoch = int(ts.astimezone(timezone.utc).timestamp())
    return datetime.fromtimestamp(epoch - epoch % width_s, tz=timezone.utc)


class ProcessingError(Exception):
    pass


class AlertService:
    def __init__(self, storage: Storage):
        self.s = storage
        self.registry = Registry(storage)
        self.rules = RuleRegistry(storage)
        self.rules.initialize()

    # ============================================================
    # 观测接入：去重 -> 质量 -> 校准 -> 入桶 -> 定稿
    # ============================================================
    def ingest_batch(self, observations: list[dict]) -> dict:
        accepted, duplicate, late, failed = 0, 0, 0, []
        for raw in observations:
            try:
                r = self.ingest(raw, defer_flush=True)
                duplicate += r["duplicate"]
                late += r["late"]
                accepted += 0 if r["duplicate"] else 1
            except Exception as e:  # 单条失败不影响整批
                failed.append({"observation": raw, "error": str(e)})
        finalized = self._flush_due_buckets() if accepted else []
        return {"accepted": accepted, "duplicate": duplicate, "late": late,
                "failed": failed, "finalized_buckets": len(finalized)}

    def ingest(self, raw: dict, defer_flush: bool = False) -> dict:
        obs = self._parse_observation(raw)
        version = self.rules.current_version()
        rset = self.rules.load(version)
        dev = self.registry.get_device(obs.device_id)
        rule = rset.for_metric(dev["metric_type"])
        install = self.registry.install_at(obs.device_id, obs.observed_at)
        install_id = install["install_id"]

        with self.s.tx() as c:
            # 1) 乱序去重：同 install 下游标重复即丢弃，不触达任何统计
            dup = c.execute(
                "SELECT id FROM observations WHERE install_id=? AND sequence=?",
                (install_id, obs.sequence)).fetchone()
            if dup is not None:
                return {"duplicate": True, "late": False, "install_id": install_id,
                        "sequence": obs.sequence}

            # 2) 校准（取观测时刻生效的校准记录）
            cal = self.registry.calibration_at(install_id, obs.observed_at)
            calibrated = obs.value * cal["factor"]

            # 3) 质量判定
            quality, reasons = self._classify_quality(
                c, install, dev, cal, obs, rule)

            bstart = bucket_floor(obs.observed_at, rule.bucket_seconds)
            b_iso = to_iso(bstart)
            existing_bucket = c.execute(
                "SELECT finalized FROM buckets WHERE install_id=? AND bucket_start=?",
                (install_id, b_iso)).fetchone()
            is_late = bool(existing_bucket and existing_bucket["finalized"])

            c.execute(
                "INSERT INTO observations (install_id, device_id, sequence, observed_at,"
                " raw_value, calibrated_value, factor, calibration_id, quality,"
                " quality_reasons, late, bucket_start, rule_version, processed_at)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (install_id, obs.device_id, obs.sequence, to_iso(obs.observed_at),
                 obs.value, calibrated, cal["factor"], cal["calibration_id"],
                 quality, json.dumps(reasons, ensure_ascii=False), int(is_late),
                 b_iso, version, to_iso(now_utc())))

            # 4) 入桶统计：只有非迟到、非抑制的好值参与趋势聚合
            if not is_late:
                self._upsert_bucket_stat(c, install_id, b_iso, version,
                                         calibrated, quality)

            # 5) 游标推进（重复观测不推进；乱序只更新 max）
            self._advance_cursor(c, install_id, obs.sequence, obs.observed_at)

            result = {"duplicate": False, "late": is_late, "install_id": install_id,
                      "sequence": obs.sequence, "quality": quality,
                      "quality_reasons": reasons, "bucket_start": b_iso,
                      "calibrated_value": calibrated, "rule_version": version}

        # 6) 定稿后继桶已到达的旧桶（迟到于定稿后的观测已在上面标 late）
        if not defer_flush:
            self._flush_due_buckets()
        return result

    def _parse_observation(self, raw: dict) -> Observation:
        try:
            device_id = str(raw["device_id"])
            sequence = int(raw["sequence"])
            value = float(raw["value"])
            ts = raw["observed_at"]
            if isinstance(ts, str):
                ts = parse_iso(ts)
            if not isinstance(ts, datetime) or ts.tzinfo is None:
                raise ValueError
            metric = raw.get("metric_type")
            return Observation(device_id, sequence, ts, value, metric)
        except (KeyError, TypeError, ValueError) as e:
            raise ProcessingError(f"观测格式无效: {raw} ({e})") from None

    def _classify_quality(self, c, install: dict, dev: dict, cal: dict,
                          obs: Observation, rule: MetricRule) -> tuple[str, list[str]]:
        ts = obs.observed_at
        reasons: list[str] = []

        # 维护窗口：抑制一切告警
        mw = self.registry.in_maintenance(install["install_id"], ts)
        if mw:
            return Quality.SUPPRESSED, [f"维护窗口 {mw['window_id']}：{mw['reason']}"]

        # 物理硬量程：坏值
        v = obs.value * cal["factor"]
        if dev["hard_min"] is not None and v < dev["hard_min"]:
            reasons.append(f"校准值 {v:.3f} 低于硬量程下限 {dev['hard_min']}")
        if dev["hard_max"] is not None and v > dev["hard_max"]:
            reasons.append(f"校准值 {v:.3f} 高于硬量程上限 {dev['hard_max']}")
        if reasons:
            return Quality.BAD, reasons

        # 设备健康：故障 -> 坏值；降级 -> 可疑
        health = self.registry.health_at(install["install_id"], ts)
        if health["status"] == HealthStatus.FAULTY:
            return Quality.BAD, [f"设备处于故障状态：{health.get('note') or '未注明'}"]
        if health["status"] == HealthStatus.OFFLINE:
            reasons.append("设备登记为离线状态")
        elif health["status"] == HealthStatus.DEGRADED:
            reasons.append(f"设备健康降级：{health.get('note') or '未注明'}")

        # 短时离线：同 install 内与时间上最近的一条历史观测间隔过大 -> 恢复首点可疑
        prev = c.execute(
            "SELECT observed_at FROM observations WHERE install_id=? AND observed_at<?"
            " ORDER BY observed_at DESC LIMIT 1",
            (install["install_id"], to_iso(ts))).fetchone()
        if prev is not None:
            gap = (ts - parse_iso(prev["observed_at"])).total_seconds()
            if gap > rule.short_offline_gap_s:
                reasons.append(
                    f"距上一条观测 {gap:.0f}s，超过短时离线门限"
                    f" {rule.short_offline_gap_s}s，恢复首点可疑")

        # 校准稳定期
        cal_from = parse_iso(cal["effective_from"])
        age = (ts - cal_from).total_seconds()
        if 0 <= age < rule.calibration_settle_s:
            reasons.append(
                f"处于校准 {cal['calibration_id']} 生效后稳定期"
                f"（{age:.0f}s < {rule.calibration_settle_s}s）")

        return (Quality.SUSPECT, reasons) if reasons else (Quality.GOOD, ["正常"])

    def _upsert_bucket_stat(self, c, install_id: str, b_iso: str, version: int,
                            value: float, quality: str) -> None:
        c.execute(
            "INSERT INTO buckets (install_id, bucket_start, rule_version) VALUES (?,?,?)"
            " ON CONFLICT(install_id, bucket_start) DO NOTHING",
            (install_id, b_iso, version))
        col = {Quality.GOOD: "good_count", Quality.SUSPECT: "suspect_count",
               Quality.BAD: "bad_count", Quality.SUPPRESSED: "suppressed_count"}[quality]
        if quality == Quality.GOOD:
            c.execute(
                f"UPDATE buckets SET {col}={col}+1, sum_value=sum_value+?,"
                " min_value=COALESCE(MIN(min_value, ?), ?),"
                " max_value=COALESCE(MAX(max_value, ?), ?)"
                " WHERE install_id=? AND bucket_start=?",
                (value, value, value, value, value, install_id, b_iso))
        else:
            c.execute(
                f"UPDATE buckets SET {col}={col}+1"
                " WHERE install_id=? AND bucket_start=?",
                (install_id, b_iso))

    def _advance_cursor(self, c, install_id: str, sequence: int,
                        observed_at: datetime) -> None:
        c.execute(
            "INSERT INTO dedup_cursor (install_id, last_sequence, last_observed_at, updated_at)"
            " VALUES (?,?,?,?) ON CONFLICT(install_id) DO UPDATE SET"
            " last_sequence=MAX(last_sequence, excluded.last_sequence),"
            " last_observed_at=MAX(last_observed_at, excluded.last_observed_at),"
            " updated_at=excluded.updated_at",
            (install_id, sequence, to_iso(observed_at), to_iso(now_utc())))

    # ============================================================
    # 桶定稿与告警生命周期
    # ============================================================
    def _flush_due_buckets(self, now: Optional[datetime] = None) -> list[dict]:
        """定稿规则：除当前最新桶外，更老的未结桶全部定稿（缺失桶即离线缺口，

        由评估时的时间相邻判断重置连击）；乱序观测若落进尚不存在的更老桶，
        该桶在本次 flush 中按顺序定稿，仍被计入；只有落入已定稿桶才标记 late。
        sweep（注入/墙钟时间到达桶结束时刻）额外定稿尾部最新桶。
        """
        out = []
        install_ids = [r["install_id"] for r in self.s.query(
            "SELECT DISTINCT install_id FROM buckets WHERE finalized=0")]
        for install_id in install_ids:
            width = self._width_for_install(install_id)
            with self.s.tx() as c:
                pending = [dict(x) for x in c.execute(
                    "SELECT * FROM buckets WHERE install_id=? AND finalized=0"
                    " ORDER BY bucket_start", (install_id,)).fetchall()]
                if not pending:
                    continue
                due = pending[:-1]
                tail = pending[-1]
                if now is not None and now >= parse_iso(tail["bucket_start"]) \
                        + timedelta(seconds=width):
                    due.append(tail)
                for b in due:
                    out.append(self._finalize_one(c, b))
        return out

    def sweep(self, now: Optional[datetime] = None) -> dict:
        """墙钟驱动：定稿结束时刻已过的尾部桶（设备短时离线也能出告警结论）。"""
        finalized = self._flush_due_buckets(now=now or now_utc())
        return {"finalized_buckets": len(finalized),
                "buckets": [b["bucket_start"] for b in finalized]}

    def _width_for_install(self, install_id: str) -> int:
        dev = self.s.query_one(
            "SELECT d.metric_type FROM installs i JOIN devices d"
            " ON d.device_id=i.device_id WHERE i.install_id=?", (install_id,))
        return self.rules.load().for_metric(dev["metric_type"]).bucket_seconds

    def _finalize_one(self, c, b: dict) -> dict:
        install_id = b["install_id"]
        rule = self._rule_for_bucket(b)
        prev = c.execute(
            "SELECT * FROM buckets WHERE install_id=? AND bucket_start<? ORDER BY"
            " bucket_start DESC LIMIT 1", (install_id, b["bucket_start"])).fetchone()
        rec = self._evaluate_bucket(c, dict(b) if not isinstance(b, dict) else b,
                                    dict(prev) if prev else None, rule)
        self._persist_bucket_eval(c, rec)
        self._apply_to_alerts(c, install_id, rec, rule)
        c.execute("UPDATE buckets SET finalized=1 WHERE id=?", (b["id"],))
        return {"install_id": install_id, "bucket_start": b["bucket_start"],
                "level": rec["level"], "triggers": rec["triggers"]}

    def _rule_for_bucket(self, b: dict) -> MetricRule:
        return self.rules.load(b["rule_version"]).for_metric(self._metric_of(b["install_id"]))

    def _metric_of(self, install_id: str) -> str:
        r = self.s.query_one(
            "SELECT d.metric_type FROM installs i JOIN devices d"
            " ON d.device_id=i.device_id WHERE i.install_id=?", (install_id,))
        return r["metric_type"]

    def _evaluate_bucket(self, c, b: dict, prev: Optional[dict],
                         rule: MetricRule) -> dict:
        """根据本桶好值统计与前桶计算连续超限/危急/恢复连击与斜率。"""
        width = rule.bucket_seconds
        triggers: list[str] = []
        level = None
        direction = None
        slope = None

        mean = (b["sum_value"] / b["good_count"]) if b["good_count"] else None

        contiguous = prev is not None and (
            parse_iso(b["bucket_start"]) - parse_iso(prev["bucket_start"])
        ).total_seconds() == width
        # 中间存在空桶（离线缺口）=> 连击全部中断
        p = prev if contiguous else None

        hi_level = lo_level = None
        if mean is not None:
            if rule.crit_high is not None and mean >= rule.crit_high:
                hi_level = "crit"
            elif rule.warn_high is not None and mean >= rule.warn_high:
                hi_level = "warn"
            if rule.crit_low is not None and mean <= rule.crit_low:
                lo_level = "crit"
            elif rule.warn_low is not None and mean <= rule.warn_low:
                lo_level = "warn"

        breach_hi = (p["breach_hi_streak"] if p else 0)
        crit_hi = (p["crit_hi_streak"] if p else 0)
        breach_lo = (p["breach_lo_streak"] if p else 0)
        crit_lo = (p["crit_lo_streak"] if p else 0)
        rec_hi = (p["recover_hi"] if p else 0)
        rec_lo = (p["recover_lo"] if p else 0)

        if hi_level:
            breach_hi += 1
            crit_hi = crit_hi + 1 if hi_level == "crit" else 0
            rec_hi = 0
            triggers.append("high_crit" if hi_level == "crit" else "high_warn")
        else:
            breach_hi = crit_hi = 0
            rec_hi = rec_hi + 1 if (mean is not None and rule.warn_high is not None
                                    and mean < rule.warn_high) else 0

        if lo_level:
            breach_lo += 1
            crit_lo = crit_lo + 1 if lo_level == "crit" else 0
            rec_lo = 0
            triggers.append("low_crit" if lo_level == "crit" else "low_warn")
        else:
            breach_lo = crit_lo = 0
            rec_lo = rec_lo + 1 if (mean is not None and rule.warn_low is not None
                                    and mean < rule.warn_low) else 0

        # 趋势斜率（相邻完整桶，单位/分钟）
        if p and p["good_count"] and mean is not None:
            prev_mean = p["sum_value"] / p["good_count"]
            slope = (mean - prev_mean) / (width / 60.0)
            if rule.slope_warn is not None and slope >= rule.slope_warn and mean > prev_mean:
                triggers.append("slope_rise")
            if rule.slope_warn is not None and -slope >= rule.slope_warn and mean < prev_mean:
                triggers.append("slope_drop")

        if hi_level == "crit" or (lo_level == "crit"):
            level = "crit"
        elif hi_level or lo_level:
            level = "warn"
        if hi_level and lo_level:
            direction = "both"
        elif hi_level:
            direction = "high"
        elif lo_level:
            direction = "low"

        b.update({
            "triggers": triggers, "level": level, "direction": direction,
            "trend_slope": slope,
            "breach_hi_streak": breach_hi, "crit_hi_streak": crit_hi,
            "breach_lo_streak": breach_lo, "crit_lo_streak": crit_lo,
            "recover_hi": rec_hi, "recover_lo": rec_lo,
            "mean_value": mean,
        })
        return b

    def _persist_bucket_eval(self, c, b: dict) -> None:
        c.execute(
            "UPDATE buckets SET trend_slope=?, direction=?, level=?, triggers=?,"
            " breach_hi_streak=?, crit_hi_streak=?, breach_lo_streak=?,"
            " crit_lo_streak=?, recover_hi=?, recover_lo=? WHERE id=?",
            (b["trend_slope"], b["direction"], b["level"],
             json.dumps(b["triggers"], ensure_ascii=False),
             b["breach_hi_streak"], b["crit_hi_streak"],
             b["breach_lo_streak"], b["crit_lo_streak"],
             b["recover_hi"], b["recover_lo"], b["id"]))

    def _apply_to_alerts(self, c, install_id: str, b: dict,
                         rule: MetricRule, id_preference: Optional[dict] = None) -> dict:
        """定稿桶驱动 high / low 两个方向上的告警状态机。

        id_preference: {direction: alert_id}，重算时复用历史告警 id 以保留人工事件。
        返回 {direction: alert_id_or_None}。
        """
        result = {}
        for d, breach, crit, rec, wthr, cthr in (
            ("high", b["breach_hi_streak"], b["crit_hi_streak"], b["recover_hi"],
             rule.warn_high, rule.crit_high),
            ("low", b["breach_lo_streak"], b["crit_lo_streak"], b["recover_lo"],
             rule.warn_low, rule.crit_low),
        ):
            if wthr is None:
                continue
            active = c.execute(
                "SELECT * FROM alerts WHERE install_id=? AND direction=? AND status NOT IN"
                " (?, ?) ORDER BY created_at DESC LIMIT 1",
                (install_id, d, AlertStatus.RESOLVED, AlertStatus.FALSE_ALARM)).fetchone()
            mean = b["mean_value"]
            at = parse_iso(b["bucket_start"]) + timedelta(seconds=rule.bucket_seconds)
            breach_now = breach > 0 and (
                f"{'high' if d == 'high' else 'low'}_warn" in b["triggers"]
                or f"{'high' if d == 'high' else 'low'}_crit" in b["triggers"])

            if active is not None and breach_now:
                aid = active["alert_id"]
                c.execute(
                    "UPDATE alerts SET latest_bucket=?, latest_value=?, updated_at=? WHERE alert_id=?",
                    (b["bucket_start"], mean, to_iso(at), aid))
                self._event(c, aid, EventType.MERGED, at, "system",
                            f"桶 {b['bucket_start']} 聚合均值 {mean:.3f}"
                            f" 持续{('高于' if d=='high' else '低于')}阈值 {wthr}，"
                            f"连续第 {breach} 桶，并入既有告警",
                            detail={"bucket": b["bucket_start"], "mean": mean,
                                    "streak": breach, "triggers": b["triggers"]})
                if crit >= rule.crit_streak and active["level"] != AlertLevel.CRITICAL:
                    c.execute("UPDATE alerts SET level=?, threshold=? WHERE alert_id=?",
                              (AlertLevel.CRITICAL, cthr, aid))
                    self._event(c, aid, EventType.LEVEL_CHANGED, at, "system",
                                f"连续 {crit} 个桶达到危急阈值 {cthr}，自动升级为 CRITICAL",
                                from_level=active["level"], to_level=AlertLevel.CRITICAL)
                result[d] = aid

            elif active is not None and not breach_now:
                if rec >= rule.recover_streak:
                    c.execute(
                        "UPDATE alerts SET status=?, resolved_at=?, updated_at=? WHERE alert_id=?",
                        (AlertStatus.RESOLVED, to_iso(at), to_iso(at),
                         active["alert_id"]))
                    self._event(c, active["alert_id"], EventType.RESOLVED, at, "system",
                                f"连续 {rec} 个聚合桶回落至阈值 {wthr} 以内，自动解除",
                                from_status=active["status"],
                                to_status=AlertStatus.RESOLVED)
                result[d] = active["alert_id"]

            elif active is None and breach >= rule.breach_streak:
                is_crit = crit >= rule.crit_streak
                level = AlertLevel.CRITICAL if is_crit else AlertLevel.WARN
                first_start = (parse_iso(b["bucket_start"])
                               - timedelta(seconds=rule.bucket_seconds * (breach - 1)))
                aid = (id_preference or {}).get(d) or f"AL-{uuid.uuid4().hex[:12]}"
                dev = c.execute(
                    "SELECT i.device_id, d.metric_type FROM installs i JOIN devices d"
                    " ON d.device_id=i.device_id WHERE i.install_id=?",
                    (install_id,)).fetchone()
                c.execute(
                    "INSERT INTO alerts (alert_id, install_id, device_id, metric_type,"
                    " rule_version, level, status, direction, first_bucket, latest_bucket,"
                    " first_value, latest_value, threshold, created_at, updated_at)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (aid, install_id, dev["device_id"], dev["metric_type"],
                     b["rule_version"], level, AlertStatus.OPEN, d,
                     to_iso(first_start), b["bucket_start"], mean, mean,
                     cthr if is_crit else wthr, to_iso(at), to_iso(at)))
                trig = f"连续 {breach} 个桶均值{'达到' if is_crit else '超过'}" \
                       f"{'危急' if is_crit else '高报'}阈值 {cthr if is_crit else wthr}"
                self._event(c, aid, EventType.CREATED, at, "system",
                            f"规则 v{b['rule_version']} 触发：{trig}",
                            to_status=AlertStatus.OPEN, to_level=level,
                            detail={"bucket": b["bucket_start"], "mean": mean,
                                    "streak": breach, "triggers": b["triggers"]})
                result[d] = aid

            elif active is None:
                # 斜率趋势预警：尚未越限但上升/下降过快
                tkey = "slope_rise" if d == "high" else "slope_drop"
                if tkey in b["triggers"]:
                    aid = (id_preference or {}).get(d) or f"AL-{uuid.uuid4().hex[:12]}"
                    dev = c.execute(
                        "SELECT i.device_id, d.metric_type FROM installs i JOIN devices d"
                        " ON d.device_id=i.device_id WHERE i.install_id=?",
                        (install_id,)).fetchone()
                    c.execute(
                        "INSERT INTO alerts (alert_id, install_id, device_id, metric_type,"
                        " rule_version, level, status, direction, first_bucket, latest_bucket,"
                        " first_value, latest_value, threshold, created_at, updated_at)"
                        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (aid, install_id, dev["device_id"], dev["metric_type"],
                         b["rule_version"], AlertLevel.WARN, AlertStatus.OPEN, d,
                         b["bucket_start"], b["bucket_start"], mean, mean, wthr,
                         to_iso(at), to_iso(at)))
                    self._event(c, aid, EventType.CREATED, at, "system",
                                f"规则 v{b['rule_version']} 趋势预警：桶 {b['bucket_start']}"
                                f" 斜率 {b['trend_slope']:.4f}/分钟 超过门限"
                                f" {rule.slope_warn}/分钟",
                                to_status=AlertStatus.OPEN, to_level=AlertLevel.WARN,
                                detail={"bucket": b["bucket_start"], "mean": mean,
                                        "slope": b["trend_slope"],
                                        "triggers": b["triggers"]})
                    result[d] = aid
        return result

    def _event(self, c, alert_id: str, event_type: str, at: datetime, actor: str,
               reason: str = "", from_status: Optional[str] = None,
               to_status: Optional[str] = None, from_level: Optional[str] = None,
               to_level: Optional[str] = None, detail: Optional[dict] = None) -> None:
        if event_type in MANUAL_EVENT_TYPES and not reason.strip():
            raise ProcessingError(f"{event_type} 操作必须填写原因")
        c.execute(
            "INSERT INTO alert_events (alert_id, event_type, at, actor, reason,"
            " from_status, to_status, from_level, to_level, detail) VALUES"
            " (?,?,?,?,?,?,?,?,?,?)",
            (alert_id, event_type, to_iso(at), actor, reason,
             from_status, to_status, from_level, to_level,
             json.dumps(detail or {}, ensure_ascii=False)))

    # ============================================================
    # 人工处置：确认 / 转派 / 升级 / 解除 / 误报复核 / 合并
    # ============================================================
    def list_alerts(self, device_id: Optional[str] = None,
                    status: Optional[str] = None) -> list[dict]:
        sql, params = "SELECT * FROM alerts WHERE 1=1", []
        if device_id:
            sql += " AND device_id=?"; params.append(device_id)
        if status:
            sql += " AND status=?"; params.append(status)
        return [dict(r) for r in self.s.query(sql + " ORDER BY created_at", params)]

    def get_alert(self, alert_id: str) -> dict:
        r = self.s.query_one("SELECT * FROM alerts WHERE alert_id=?", (alert_id,))
        if r is None:
            raise ProcessingError(f"告警 {alert_id} 不存在")
        return dict(r)

    def _load_open_alert(self, c, alert_id: str) -> dict:
        r = c.execute("SELECT * FROM alerts WHERE alert_id=?", (alert_id,)).fetchone()
        if r is None:
            raise ProcessingError(f"告警 {alert_id} 不存在")
        if r["status"] in AlertStatus.CLOSED:
            raise ProcessingError(f"告警 {alert_id} 已关闭（{r['status']}），不能继续处置")
        return dict(r)

    def acknowledge(self, alert_id: str, actor: str, reason: str) -> dict:
        self._require(reason, "确认")
        with self.s.tx() as c:
            a = self._load_open_alert(c, alert_id)
            c.execute("UPDATE alerts SET status=?, acked_by=?, acked_at=?, updated_at=?"
                      " WHERE alert_id=?",
                      (AlertStatus.ACKED, actor, to_iso(now_utc()), to_iso(now_utc()),
                       alert_id))
            self._event(c, alert_id, EventType.ACKED, now_utc(), actor, reason,
                        from_status=a["status"], to_status=AlertStatus.ACKED)
        return self.get_alert(alert_id)

    def assign(self, alert_id: str, actor: str, assignee: str, reason: str) -> dict:
        self._require(reason, "转派")
        if not assignee or not assignee.strip():
            raise ProcessingError("转派必须指定受理人")
        with self.s.tx() as c:
            a = self._load_open_alert(c, alert_id)
            c.execute("UPDATE alerts SET status=?, assignee=?, assigned_at=?, updated_at=?"
                      " WHERE alert_id=?",
                      (AlertStatus.ASSIGNED, assignee, to_iso(now_utc()),
                       to_iso(now_utc()), alert_id))
            self._event(c, alert_id, EventType.ASSIGNED, now_utc(), actor, reason,
                        from_status=a["status"], to_status=AlertStatus.ASSIGNED,
                        detail={"assignee": assignee})
        return self.get_alert(alert_id)

    def escalate(self, alert_id: str, actor: str, reason: str) -> dict:
        self._require(reason, "升级")
        with self.s.tx() as c:
            a = self._load_open_alert(c, alert_id)
            if a["level"] == AlertLevel.CRITICAL:
                raise ProcessingError("告警已是 CRITICAL")
            c.execute("UPDATE alerts SET level=?, updated_at=? WHERE alert_id=?",
                      (AlertLevel.CRITICAL, to_iso(now_utc()), alert_id))
            self._event(c, alert_id, EventType.ESCALATED, now_utc(), actor, reason,
                        from_level=a["level"], to_level=AlertLevel.CRITICAL)
        return self.get_alert(alert_id)

    def resolve(self, alert_id: str, actor: str, reason: str) -> dict:
        self._require(reason, "解除")
        with self.s.tx() as c:
            a = self._load_open_alert(c, alert_id)
            at = now_utc()
            c.execute("UPDATE alerts SET status=?, resolved_at=?, updated_at=? WHERE alert_id=?",
                      (AlertStatus.RESOLVED, to_iso(at), to_iso(at), alert_id))
            self._event(c, alert_id, EventType.RESOLVED, at, actor, reason,
                        from_status=a["status"], to_status=AlertStatus.RESOLVED)
        return self.get_alert(alert_id)

    def review_false_alarm(self, alert_id: str, actor: str, reason: str,
                           verdict: str = "false_alarm") -> dict:
        """误报复核：false_alarm 关闭；confirmed_real 保留告警并记录复核结论。"""
        self._require(reason, "误报复核")
        if verdict not in ("false_alarm", "confirmed_real"):
            raise ProcessingError("verdict 必须为 false_alarm 或 confirmed_real")
        with self.s.tx() as c:
            a_row = c.execute(
                "SELECT * FROM alerts WHERE alert_id=?", (alert_id,)).fetchone()
            if a_row is None:
                raise ProcessingError(f"告警 {alert_id} 不存在")
            a = dict(a_row)
            at = now_utc()
            if verdict == "false_alarm":
                if a["status"] in AlertStatus.CLOSED and a["status"] != AlertStatus.FALSE_ALARM:
                    raise ProcessingError("已解除的告警不能改判误报，请新开复核")
                c.execute("UPDATE alerts SET status=?, resolved_at=?, updated_at=? WHERE alert_id=?",
                          (AlertStatus.FALSE_ALARM, to_iso(at), to_iso(at), alert_id))
                self._event(c, alert_id, EventType.REVIEWED, at, actor, reason,
                            from_status=a["status"], to_status=AlertStatus.FALSE_ALARM,
                            detail={"verdict": verdict})
            else:
                self._event(c, alert_id, EventType.REVIEWED, at, actor, reason,
                            detail={"verdict": verdict})
        return self.get_alert(alert_id)

    def merge_alerts(self, source_id: str, target_id: str, actor: str,
                     reason: str) -> dict:
        """手工合并：源告警并入目标，源关闭并挂 root_alert_id，全程留因。"""
        self._require(reason, "合并")
        if source_id == target_id:
            raise ProcessingError("不能合并告警自身")
        with self.s.tx() as c:
            src = self._load_open_alert(c, source_id)
            tgt = c.execute("SELECT * FROM alerts WHERE alert_id=?",
                            (target_id,)).fetchone()
            if tgt is None:
                raise ProcessingError(f"目标告警 {target_id} 不存在")
            if tgt["status"] in AlertStatus.CLOSED:
                raise ProcessingError("不能并入已关闭告警")
            if src["install_id"] != tgt["install_id"]:
                raise ProcessingError("只能合并同一设备安装序列下的告警")
            at = now_utc()
            c.execute(
                "UPDATE alerts SET status=?, resolved_at=?, root_alert_id=?, updated_at=?"
                " WHERE alert_id=?",
                (AlertStatus.RESOLVED, to_iso(at), target_id, to_iso(at), source_id))
            self._event(c, source_id, EventType.MERGED, at, actor, reason,
                        from_status=src["status"], to_status=AlertStatus.RESOLVED,
                        detail={"merged_into": target_id})
            self._event(c, target_id, EventType.MERGED, at, actor,
                        f"并入告警 {source_id}：{reason}",
                        detail={"merged_from": source_id})
        return self.get_alert(target_id)

    @staticmethod
    def _require(reason: str, action: str) -> None:
        if not reason or not reason.strip():
            raise ProcessingError(f"{action}必须填写原因")

    # ============================================================
    # 可解释：原始读数 -> 校准 -> 质量 -> 桶 -> 规则 -> 告警时间线
    # ============================================================
    def explain_observation(self, device_id: str, sequence: int) -> dict:
        obs_rows = self.s.query(
            "SELECT o.* FROM observations o WHERE o.device_id=? AND o.sequence=?",
            (device_id, sequence))
        if not obs_rows:
            raise ProcessingError(f"设备 {device_id} 序号 {sequence} 的观测不存在")
        results = []
        for r in obs_rows:  # 换表后同序号可能分属不同 install
            o = dict(r)
            b = self.s.query_one(
                "SELECT * FROM buckets WHERE install_id=? AND bucket_start=?",
                (o["install_id"], o["bucket_start"]))
            cal = None
            if o["calibration_id"]:
                cal = self.s.query_one(
                    "SELECT * FROM calibrations WHERE calibration_id=?",
                    (o["calibration_id"],))
            related = [dict(a) for a in self.s.query(
                "SELECT * FROM alerts WHERE install_id=? AND ? BETWEEN first_bucket AND latest_bucket",
                (o["install_id"], o["bucket_start"]))]
            results.append({
                "observation": {k: o[k] for k in (
                    "device_id", "install_id", "sequence", "observed_at",
                    "raw_value", "calibrated_value", "factor", "calibration_id",
                    "quality", "quality_reasons", "late", "bucket_start",
                    "rule_version")},
                "calibration": dict(cal) if cal else None,
                "quality_reasons": json.loads(o["quality_reasons"]),
                "formula": f"{o['raw_value']} × {o['factor']} = {o['calibrated_value']}",
                "bucket": self._bucket_view(dict(b)) if b else None,
                "related_alerts": [a["alert_id"] for a in related],
            })
        return {"device_id": device_id, "sequence": sequence, "matches": results}

    def explain_alert(self, alert_id: str) -> dict:
        a = self.get_alert(alert_id)
        events = [dict(e) for e in self.s.query(
            "SELECT * FROM alert_events WHERE alert_id=? ORDER BY id", (alert_id,))]
        for e in events:
            e["detail"] = json.loads(e["detail"]) if e["detail"] else {}
        buckets = [self._bucket_view(dict(b)) for b in self.s.query(
            "SELECT * FROM buckets WHERE install_id=? AND bucket_start>=? AND bucket_start<=?"
            " ORDER BY bucket_start",
            (a["install_id"], a["first_bucket"], a["latest_bucket"]))]
        contributing = [self._bucket_view(dict(b)) for b in self.s.query(
            "SELECT * FROM buckets WHERE install_id=? AND bucket_start>=? AND bucket_start<=?"
            " AND (breach_hi_streak>0 OR breach_lo_streak>0 OR triggers != '[]')"
            " ORDER BY bucket_start",
            (a["install_id"], a["first_bucket"], a["latest_bucket"]))]
        rset = self.rules.load(a["rule_version"])
        rule = asdict_safe(rset.for_metric(a["metric_type"]))
        install = self.registry.get_install(a["install_id"])
        sample_obs = [dict(o) for o in self.s.query(
            "SELECT sequence, observed_at, raw_value, calibrated_value, quality,"
            " quality_reasons, late FROM observations WHERE install_id=?"
            " AND observed_at>=? ORDER BY observed_at LIMIT 200",
            (a["install_id"], a["first_bucket"]))]
        for o in sample_obs:
            o["quality_reasons"] = json.loads(o["quality_reasons"])
        return {
            "alert": a, "install": install,
            "rule_version": a["rule_version"], "rule": rule,
            "timeline": events,
            "contributing_buckets": contributing,
            "all_buckets_in_span": buckets,
            "sample_observations": sample_obs,
            "explanation": self._narrative(a, rule, events, contributing),
        }

    def _bucket_view(self, b: dict) -> dict:
        return {
            "bucket_start": b["bucket_start"], "rule_version": b["rule_version"],
            "good_count": b["good_count"], "suspect_count": b["suspect_count"],
            "bad_count": b["bad_count"], "suppressed_count": b["suppressed_count"],
            "mean_value": (b["sum_value"] / b["good_count"]) if b["good_count"] else None,
            "min_value": b["min_value"], "max_value": b["max_value"],
            "trend_slope": b["trend_slope"], "direction": b["direction"],
            "level": b["level"], "triggers": json.loads(b["triggers"]),
            "breach_hi_streak": b["breach_hi_streak"], "crit_hi_streak": b["crit_hi_streak"],
            "breach_lo_streak": b["breach_lo_streak"], "crit_lo_streak": b["crit_lo_streak"],
            "recover_hi": b["recover_hi"], "recover_lo": b["recover_lo"],
            "finalized": bool(b["finalized"]),
        }

    def _narrative(self, a: dict, rule: dict, events: list[dict],
                   buckets: list[dict]) -> list[str]:
        lines = [
            f"设备 {a['device_id']}（安装序列 {a['install_id']}）指标 {a['metric_type']}",
            f"在规则版本 v{a['rule_version']} 下，自 {a['first_bucket']} 起连续超限，"
            f"于 {events[0]['at'] if events else a['created_at']} 生成 {a['direction']}"
            f" 向 {a['level']} 告警；阈值 {a['threshold']}。",
            f"判定链：原始观测按校准系数换算 → 质量门（维护/硬量程/健康/短时离线/稳定期）"
            f" → 仅 GOOD 值进入 {rule['bucket_seconds']}s 聚合桶"
            f" → 连续 {rule['breach_streak']} 桶越限触发、{rule['crit_streak']} 桶危急升级、"
            f"{rule['recover_streak']} 桶回落自动解除。",
        ]
        lines.append("时间线：")
        for e in events:
            line = f"  - {e['at']} [{e['event_type']}] {e['actor']}: {e['reason']}"
            if e["from_status"] or e["to_status"]:
                line += f"（{e['from_status'] or '∅'} → {e['to_status'] or '∅'}）"
            lines.append(line)
        return lines

    # ============================================================
    # 显式重算任务（分桶小事务、检查点续跑）
    # ============================================================
    def create_recalc_job(self, device_id: str,
                          rule_version: Optional[int] = None,
                          from_bucket: Optional[str] = None) -> dict:
        self.registry.get_device(device_id)
        installs = self.registry.list_installs(device_id)
        if not installs:
            raise ProcessingError("设备无安装记录")
        install_id = installs[-1]["install_id"]
        version = rule_version or self.rules.current_version()
        self.rules.load(version)  # 钉选版本必须存在
        if from_bucket is not None:
            parse_iso(from_bucket)
        job_id = f"JOB-{uuid.uuid4().hex[:12]}"
        with self.s.tx() as c:
            c.execute(
                "INSERT INTO recalc_jobs (job_id, install_id, rule_version, from_bucket,"
                " status, phase, created_at) VALUES (?,?,?,?,?,?,?)",
                (job_id, install_id, version, from_bucket, "pending", "queued",
                 to_iso(now_utc())))
        return self.get_recalc_job(job_id)

    def get_recalc_job(self, job_id: str) -> dict:
        r = self.s.query_one("SELECT * FROM recalc_jobs WHERE job_id=?", (job_id,))
        if r is None:
            raise ProcessingError(f"重算任务 {job_id} 不存在")
        return dict(r)

    def list_recalc_jobs(self) -> list[dict]:
        return [dict(r) for r in self.s.query(
            "SELECT * FROM recalc_jobs ORDER BY created_at")]

    def run_recalc_jobs(self, limit: int = 1) -> list[dict]:
        """领取 pending/running 任务执行；崩溃重启后 running 任务从检查点继续。"""
        jobs = self.s.query(
            "SELECT job_id FROM recalc_jobs WHERE status IN ('pending','running')"
            " ORDER BY created_at LIMIT ?", (limit,))
        return [self._run_job(r["job_id"]) for r in jobs]

    def _run_job(self, job_id: str) -> dict:
        job = self.get_recalc_job(job_id)
        install_id, version = job["install_id"], job["rule_version"]
        rule = self.rules.load(version).for_metric(self._metric_of(install_id))
        width = rule.bucket_seconds
        try:
            if job["phase"] == "queued":
                # 阶段 A（单事务、可整体重试）：保存人工痕迹 -> 重算质量 -> 重建桶
                self._recalc_prepare(job, rule, width)
            # 阶段 B（可续跑）：逐桶小事务评估并打检查点
            self._recalc_replay(job_id, rule)
            # 阶段 C（单事务）：对账告警，恢复人工痕迹
            self._recalc_finalize(job_id, rule)
        except Exception as e:
            with self.s.tx() as c:
                c.execute(
                    "UPDATE recalc_jobs SET status='pending', error=? WHERE job_id=?",
                    (f"{type(e).__name__}: {e}", job_id))
            raise
        return self.get_recalc_job(job_id)

    def _recalc_prepare(self, job: dict, rule: MetricRule, width: int) -> None:
        job_id, install_id, version = job["job_id"], job["install_id"], job["rule_version"]
        with self.s.tx() as c:
            c.execute("UPDATE recalc_jobs SET status='running', phase='rebuilding',"
                      " started_at=COALESCE(started_at, ?), error=NULL WHERE job_id=?",
                      (to_iso(now_utc()), job_id))
            # 1) 留存人工处置痕迹（确认/转派/升级/误报复核），按方向归档
            for a in c.execute("SELECT * FROM alerts WHERE install_id=?",
                               (install_id,)).fetchall():
                evs = [dict(e) for e in c.execute(
                    "SELECT * FROM alert_events WHERE alert_id=? AND event_type IN (?,?,?,?)"
                    " ORDER BY id",
                    (a["alert_id"],) + MANUAL_EVENT_TYPES).fetchall()]
                if not evs:
                    continue
                c.execute(
                    "INSERT OR REPLACE INTO recalc_saved_manual"
                    " (job_id, direction, alert_id, state_json, events_json)"
                    " VALUES (?,?,?,?,?)",
                    (job_id, a["direction"], a["alert_id"],
                     json.dumps(dict(a), ensure_ascii=False),
                     json.dumps(evs, ensure_ascii=False)))

            # 2) 重算范围内每条观测的质量（from_bucket 之前的观测/桶保持原样）
            scope = job["from_bucket"]
            install = self.registry.get_install(install_id)
            dev = self.registry.get_device(install["device_id"])
            if scope:
                obs_rows = c.execute(
                    "SELECT * FROM observations WHERE install_id=? AND bucket_start>=?"
                    " ORDER BY observed_at", (install_id, scope)).fetchall()
            else:
                obs_rows = c.execute(
                    "SELECT * FROM observations WHERE install_id=? ORDER BY observed_at",
                    (install_id,)).fetchall()
            for orow in obs_rows:
                o = dict(orow)
                ts = parse_iso(o["observed_at"])
                cal = self.registry.calibration_at(install_id, ts)
                pseudo = Observation(o["device_id"], o["sequence"], ts, o["raw_value"])
                q, qr = self._classify_quality(c, install, dev, cal, pseudo, rule)
                c.execute(
                    "UPDATE observations SET quality=?, quality_reasons=?,"
                    " calibrated_value=?, factor=?, calibration_id=?, rule_version=? WHERE id=?",
                    (q, json.dumps(qr, ensure_ascii=False),
                     o["raw_value"] * cal["factor"], cal["factor"],
                     cal["calibration_id"], version, o["id"]))

            # 3) 幂等重建范围内桶统计；范围外旧桶保留，充当连击前驱
            if scope:
                c.execute("DELETE FROM buckets WHERE install_id=? AND bucket_start>=?",
                          (install_id, scope))
            else:
                c.execute("DELETE FROM buckets WHERE install_id=?", (install_id,))
            for orow in obs_rows:
                o = dict(orow)
                ts = parse_iso(o["observed_at"])
                bstart = to_iso(bucket_floor(ts, width))
                self._upsert_bucket_stat(
                    c, install_id, bstart, version,
                    o["raw_value"] * o["factor"], o["quality"])

            total = c.execute(
                "SELECT COUNT(*) n FROM buckets WHERE install_id=? AND finalized=0",
                (install_id,)).fetchone()["n"]
            c.execute("UPDATE recalc_jobs SET total_buckets=?, cursor_bucket=NULL,"
                      " processed_buckets=0 WHERE job_id=?", (total, job_id))

    def _recalc_replay(self, job_id: str, rule: MetricRule) -> None:
        """逐桶评估：每桶独立事务提交并推进 cursor_bucket，重启后从下一桶继续。"""
        while True:
            job = self.get_recalc_job(job_id)
            sql = ("SELECT * FROM buckets WHERE install_id=? AND finalized=0"
                   + (" AND bucket_start>?" if job["cursor_bucket"] else "")
                   + " ORDER BY bucket_start LIMIT 1")
            params = ((job["install_id"], job["cursor_bucket"])
                      if job["cursor_bucket"] else (job["install_id"],))
            rows = self.s.query(sql, params)
            if not rows:
                break
            b = dict(rows[0])
            with self.s.tx() as c:
                prev = c.execute(
                    "SELECT * FROM buckets WHERE install_id=? AND bucket_start<?"
                    " ORDER BY bucket_start DESC LIMIT 1",
                    (b["install_id"], b["bucket_start"])).fetchone()
                rec = self._evaluate_bucket(c, b, dict(prev) if prev else None, rule)
                self._persist_bucket_eval(c, rec)
                c.execute("UPDATE buckets SET finalized=1 WHERE id=?", (b["id"],))
                c.execute("UPDATE recalc_jobs SET cursor_bucket=?, processed_buckets=?,"
                          " status='running' WHERE job_id=?",
                          (b["bucket_start"], job["processed_buckets"] + 1, job_id))

    def _recalc_finalize(self, job_id: str, rule: MetricRule) -> None:
        job = self.get_recalc_job(job_id)
        install_id, version = job["install_id"], job["rule_version"]
        with self.s.tx() as c:
            saved = {r["direction"]: (json.loads(r["state_json"]),
                                      json.loads(r["events_json"]))
                     for r in c.execute(
                         "SELECT * FROM recalc_saved_manual WHERE job_id=?",
                         (job_id,)).fetchall()}
            c.execute("DELETE FROM alert_events WHERE alert_id IN"
                      " (SELECT alert_id FROM alerts WHERE install_id=?)", (install_id,))
            c.execute("DELETE FROM alerts WHERE install_id=?", (install_id,))

            brows = c.execute(
                "SELECT * FROM buckets WHERE install_id=? ORDER BY bucket_start",
                (install_id,)).fetchall()
            prev_b, created_ids = None, {}
            pref = {d: st[0]["alert_id"] for d, st in saved.items()}
            for b in brows:
                rec = self._evaluate_bucket(c, dict(b), prev_b, rule)
                created_ids.update(
                    self._apply_to_alerts(c, install_id, rec, rule, id_preference=pref))
                prev_b = rec

            # 恢复人工痕迹
            for direction, (state, events) in saved.items():
                if direction in created_ids:
                    aid = created_ids[direction]
                    for e in events:
                        c.execute(
                            "INSERT INTO alert_events (alert_id, event_type, at, actor,"
                            " reason, from_status, to_status, from_level, to_level, detail)"
                            " VALUES (?,?,?,?,?,?,?,?,?,?)",
                            (aid, e["event_type"], e["at"], e["actor"], e["reason"],
                             e["from_status"], e["to_status"], e["from_level"],
                             e["to_level"], e["detail"]))
                    cur = c.execute("SELECT * FROM alerts WHERE alert_id=?",
                                    (aid,)).fetchone()
                    status, level = cur["status"], cur["level"]
                    # 误报判定优先于自动重算结果；确认/转派保留人员归属
                    if state["status"] == AlertStatus.FALSE_ALARM:
                        status = AlertStatus.FALSE_ALARM
                    elif state["acked_by"]:
                        status = AlertStatus.ACKED
                    elif state["assignee"]:
                        status = AlertStatus.ASSIGNED
                    if state["level"] == AlertLevel.CRITICAL:
                        level = AlertLevel.CRITICAL
                    c.execute(
                        "UPDATE alerts SET status=?, level=?, acked_by=?, acked_at=?,"
                        " assignee=?, assigned_at=?, resolved_at=?, rule_version=?,"
                        " updated_at=? WHERE alert_id=?",
                        (status, level, state["acked_by"], state["acked_at"],
                         state["assignee"], state["assigned_at"],
                         state["resolved_at"] if status in AlertStatus.CLOSED else None,
                         version, to_iso(now_utc()), aid))
                else:
                    # 重算后该方向不再触发：保留原卡片快照并记自动解除，人工历史不丢
                    self._recreate_resolved_with_manual(
                        c, state, events, install_id, version,
                        f"按规则 v{version} 重算后该方向不再满足触发条件，自动解除")

            c.execute("DELETE FROM recalc_saved_manual WHERE job_id=?", (job_id,))
            c.execute("UPDATE recalc_jobs SET status='completed', phase='done',"
                      " completed_at=?, error=NULL WHERE job_id=?",
                      (to_iso(now_utc()), job_id))

    def _recreate_resolved_with_manual(self, c, state: dict, events: list[dict],
                                       install_id: str, version: int,
                                       sys_reason: str) -> None:
        at = now_utc()
        c.execute(
            "INSERT INTO alerts (alert_id, install_id, device_id, metric_type,"
            " rule_version, level, status, direction, first_bucket, latest_bucket,"
            " first_value, latest_value, threshold, created_at, updated_at, resolved_at,"
            " acked_by, acked_at, assignee, assigned_at) VALUES"
            " (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (state["alert_id"], install_id, state["device_id"], state["metric_type"],
             version, state["level"], AlertStatus.RESOLVED, state["direction"],
             state["first_bucket"], state["latest_bucket"], state["first_value"],
             state["latest_value"], state["threshold"], state["created_at"],
             to_iso(at), to_iso(at), state["acked_by"], state["acked_at"],
             state["assignee"], state["assigned_at"]))
        for e in events:
            c.execute(
                "INSERT INTO alert_events (alert_id, event_type, at, actor, reason,"
                " from_status, to_status, from_level, to_level, detail)"
                " VALUES (?,?,?,?,?,?,?,?,?,?)",
                (state["alert_id"], e["event_type"], e["at"], e["actor"], e["reason"],
                 e["from_status"], e["to_status"], e["from_level"], e["to_level"],
                 e["detail"]))
        self._event(c, state["alert_id"], EventType.RESOLVED, at, "system", sys_reason,
                    to_status=AlertStatus.RESOLVED, detail={"recalc": True})

    def get_cursor(self, install_id: str) -> Optional[dict]:
        r = self.s.query_one("SELECT * FROM dedup_cursor WHERE install_id=?", (install_id,))
        return dict(r) if r else None


def asdict_safe(rule: MetricRule) -> dict:
    from dataclasses import asdict
    return asdict(rule)
