"""核心服务端到端测试：覆盖需求中的每条行为约束。"""
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from sensor_alerts.contracts import (  # noqa: E402
    AlertLevel, AlertState, JobStatus, MetricKind, Observation, Quality,
)
from sensor_alerts.rules import (  # noqa: E402
    LevelRule, Ruleset, TrendRule, default_ruleset,
)
from sensor_alerts.service import AlertError, AlertService  # noqa: E402
from sensor_alerts.store import Store  # noqa: E402

T0 = datetime(2026, 10, 4, 10, 0, tzinfo=timezone.utc)


def make_service(path=":memory:"):
    return AlertService(Store(path))


def seed_pressure_device(svc, device="P-1", serial="SN-1", factor=1.0, at=T0):
    svc.register_device(device, MetricKind.PRESSURE, "压力传感器")
    svc.install(device, serial, at)
    if factor != 1.0:
        svc.add_calibration(f"CAL-{device}", device, factor, at)


class IngestionDedupTests(unittest.TestCase):
    def setUp(self):
        self.svc = make_service()
        seed_pressure_device(self.svc)

    def test_duplicate_sequence_ignored(self):
        r1 = self.svc.ingest("P-1", 1, T0, 1500)
        r2 = self.svc.ingest("P-1", 1, T0, 9999)  # 重投，值不同也忽略
        self.assertFalse(r1["duplicate"])
        self.assertTrue(r2["duplicate"])
        cur = self.svc.get_cursor("P-1", r1["installation_id"])
        self.assertEqual(cur["good_count"], 1)
        rows = self.svc.store.query(
            "SELECT COUNT(*) c FROM observations WHERE installation_id=?",
            (r1["installation_id"],))
        self.assertEqual(rows[0]["c"], 1)
        self.assertEqual(cur["dup_count"], 1)

    def test_out_of_order_tracking(self):
        r_late = self.svc.ingest("P-1", 5, T0 + timedelta(minutes=5), 1500)
        r_early = self.svc.ingest("P-1", 1, T0, 1400)
        cur = self.svc.get_cursor("P-1", r_late["installation_id"])
        self.assertEqual(cur["last_sequence"], 5)
        self.assertEqual(cur["good_count"], 2)

    def test_observation_contract_used(self):
        r = self.svc.ingest_observation(Observation("P-1", 7, T0, 1500))
        self.assertEqual(r["sequence"], 7)


class QualityTests(unittest.TestCase):
    def setUp(self):
        self.svc = make_service()
        seed_pressure_device(self.svc)

    def test_maintenance_window_suppresses_spike(self):
        # 换表校准期间的极端读数不应产生告警
        self.svc.open_maintenance("P-1", T0 + timedelta(minutes=10),
                                  "换表校准", end_at=T0 + timedelta(minutes=20))
        r = self.svc.ingest("P-1", 1, T0 + timedelta(minutes=15), 3000)
        self.assertEqual(r["quality"], Quality.MAINTENANCE.value)
        self.assertEqual(self.svc.list_alerts(active_only=True), [])

    def test_open_ended_maintenance_then_close(self):
        w = self.svc.open_maintenance("P-1", T0, "检修")
        self.svc.ingest("P-1", 1, T0, 2200)
        self.assertEqual(self.svc.list_alerts(active_only=True), [])
        self.svc.close_maintenance(w["window_id"], T0 + timedelta(minutes=30))
        r = self.svc.ingest("P-1", 2, T0 + timedelta(minutes=31), 2200)
        self.assertEqual(r["quality"], Quality.GOOD.value)
        self.assertEqual(len(self.svc.list_alerts(active_only=True)), 1)

    def test_out_of_range_glitch_rejected(self):
        r = self.svc.ingest("P-1", 1, T0, 9999)
        self.assertEqual(r["quality"], Quality.OUT_OF_RANGE.value)
        self.assertEqual(self.svc.list_alerts(active_only=True), [])

    def test_not_installed_before_or_after_lifecycle(self):
        with self.assertRaises(AlertError):
            self.svc.ingest("GHOST", 1, T0, 1)
        r = self.svc.ingest("P-1", 1, T0 - timedelta(days=1), 1500)
        self.assertEqual(r["quality"], Quality.NOT_INSTALLED.value)

    def test_calibration_applied_to_adjusted_value(self):
        svc = make_service()
        seed_pressure_device(svc, "P-2", factor=1.05)
        r = svc.ingest("P-2", 1, T0, 1000)
        self.assertAlmostEqual(r["adjusted_value"], 1050.0)
        # 校准后的值参与阈值：1000*1.05 不报警，但 1950*1.05≈2047 报临界
        r = svc.ingest("P-2", 2, T0 + timedelta(minutes=1), 1950)
        alerts = svc.list_alerts(active_only=True)
        self.assertEqual(alerts[0]["level"], AlertLevel.CRITICAL.value)


class DeviceReplacementTests(unittest.TestCase):
    def test_replacement_keeps_sequences_isolated(self):
        svc = make_service()
        seed_pressure_device(svc, serial="SN-OLD")
        svc.ingest("P-1", 1, T0, 2200)  # 旧表产生告警
        self.assertEqual(len(svc.list_alerts(active_only=True)), 1)

        # 拆旧表、装新表，游标从 0 重新开始
        insts = svc.store.query(
            "SELECT installation_id FROM installations WHERE device_id='P-1'")
        svc.remove_installation(insts[0]["installation_id"],
                                T0 + timedelta(days=1), "到期换表")
        svc.install("P-1", "SN-NEW", T0 + timedelta(days=1, minutes=1))

        # 新表同样的序号 1 不是重复，且压力正常 → 不新开告警
        r = svc.ingest("P-1", 1, T0 + timedelta(days=1, minutes=2), 1400,
                       serial_no="SN-NEW")
        self.assertFalse(r["duplicate"])
        self.assertEqual(r["quality"], Quality.GOOD.value)
        self.assertEqual(len(svc.list_alerts(active_only=True)), 1)  # 仍是旧告警

        # 上报旧序列号 → 判定为换表残留
        r2 = svc.ingest("P-1", 2, T0 + timedelta(days=1, minutes=3), 1500,
                        serial_no="SN-OLD")
        self.assertEqual(r2["quality"], Quality.NOT_INSTALLED.value)

        cursors = svc.store.query(
            "SELECT * FROM ingestion_cursors WHERE device_id='P-1'")
        self.assertEqual(len(cursors), 2)

    def test_cannot_install_twice_without_remove(self):
        svc = make_service()
        seed_pressure_device(svc)
        with self.assertRaises(AlertError):
            svc.install("P-1", "SN-X", T0 + timedelta(days=1))


class AlertLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.svc = make_service()
        seed_pressure_device(self.svc)

    def _open_alert(self, value=2200):
        r = self.svc.ingest("P-1", 1, T0, value)
        return r["actions"][0]["alert_id"]

    def test_warning_then_critical_escalation_single_alert(self):
        r1 = self.svc.ingest("P-1", 1, T0, 1700)
        self.assertEqual(r1["actions"][0]["action"], "opened")
        self.assertEqual(r1["actions"][0]["level"], AlertLevel.WARNING.value)
        alert_id = r1["actions"][0]["alert_id"]

        r2 = self.svc.ingest("P-1", 2, T0 + timedelta(minutes=1), 1700)
        self.assertEqual(r2["actions"][0]["action"], "suppressed")  # 合并

        r3 = self.svc.ingest("P-1", 3, T0 + timedelta(minutes=2), 2200)
        self.assertEqual(r3["actions"][0]["action"], "escalated")
        self.assertEqual(r3["actions"][0]["level"], AlertLevel.CRITICAL.value)

        alerts = self.svc.list_alerts()
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["level"], AlertLevel.CRITICAL.value)
        events = self.svc.list_events(alert_id)
        kinds = [e["event_type"] for e in events]
        self.assertEqual(kinds, ["OPENED", "SUPPRESSED", "ESCALATED"])

    def test_auto_resolve_after_recovery_streak(self):
        alert_id = self._open_alert()
        # recover_streak=3：连续 3 次正常读数后自动解除
        for seq in (2, 3):
            r = self.svc.ingest("P-1", seq, T0 + timedelta(minutes=seq), 1400)
            self.assertEqual(r["actions"][0]["action"], "recovery_obs")
        r = self.svc.ingest("P-1", 4, T0 + timedelta(minutes=4), 1400)
        self.assertEqual(r["actions"][0]["action"], "resolved")
        self.assertEqual(self.svc.get_alert(alert_id)["state"],
                         AlertState.RESOLVED.value)

    def test_one_abnormal_resets_recovery(self):
        self._open_alert()
        self.svc.ingest("P-1", 2, T0 + timedelta(minutes=2), 1400)
        self.svc.ingest("P-1", 3, T0 + timedelta(minutes=3), 2200)  # 复位计数
        r = self.svc.ingest("P-1", 4, T0 + timedelta(minutes=4), 1400)
        self.assertEqual(r["actions"][0]["action"], "recovery_obs")
        self.assertEqual(len(self.svc.list_alerts(active_only=True)), 1)

    def test_ack_assign_false_alarm_require_reasons_and_leave_trail(self):
        alert_id = self._open_alert()
        with self.assertRaises(AlertError):
            self.svc.acknowledge(alert_id, "", "x")
        with self.assertRaises(AlertError):
            self.svc.acknowledge(alert_id, "op", "")
        self.svc.acknowledge(alert_id, "值班员-李", "已接单排查")
        self.svc.assign(alert_id, "值班员-李", "维修班-王", "需现场处理")
        self.assertEqual(self.svc.get_alert(alert_id)["state"],
                         AlertState.ASSIGNED.value)
        self.svc.mark_false_alarm(alert_id, "班长-赵", "确认为换表校准残差",
                                  review_note="已更新维护窗口登记")
        alert = self.svc.get_alert(alert_id)
        self.assertEqual(alert["state"], AlertState.FALSE_ALARM.value)
        reasons = [e["reason"] for e in self.svc.list_events(alert_id)]
        self.assertTrue(any("换表校准残差" in r for r in reasons))

    def test_cannot_acknowledge_resolved_alert(self):
        alert_id = self._open_alert()
        self.svc.resolve(alert_id, "op", "现场处置完成")
        with self.assertRaises(AlertError):
            self.svc.acknowledge(alert_id, "op", "再次确认")

    def test_resolved_can_be_reviewed_as_false_alarm(self):
        alert_id = self._open_alert()
        self.svc.resolve(alert_id, "op", "压力回落")
        self.svc.mark_false_alarm(alert_id, "reviewer", "事后复核为短时离线毛刺")
        self.assertEqual(self.svc.get_alert(alert_id)["state"],
                         AlertState.FALSE_ALARM.value)


class TrendRuleTests(unittest.TestCase):
    def test_persistent_rise_caught_even_without_threshold(self):
        svc = make_service()
        svc.register_device("G-1", MetricKind.GAS, "可燃气体")
        svc.install("G-1", "SN-G", T0)
        # 全部低于 25 的告警线，但 4 点上行 16 ≥ warning_delta 5...实际 20-4=16
        for i, v in enumerate([4, 10, 15, 20]):
            r = svc.ingest("G-1", i + 1, T0 + timedelta(minutes=i), v)
        actions = r["actions"]
        self.assertTrue(any(a["group"] == "trend:gas-rising" for a in actions))
        # 20-4=16 ≥ critical_delta 10 → CRITICAL
        trend = [a for a in actions if a["group"] == "trend:gas-rising"][0]
        self.assertEqual(trend["level"], AlertLevel.CRITICAL.value)

    def test_trend_window_excludes_bad_quality(self):
        svc = make_service()
        svc.register_device("G-2", MetricKind.GAS)
        svc.install("G-2", "SN-G2", T0)
        svc.open_maintenance("G-2", T0 + timedelta(minutes=1), "校准",
                             end_at=T0 + timedelta(minutes=2))
        svc.ingest("G-2", 1, T0, 4)
        svc.ingest("G-2", 2, T0 + timedelta(minutes=1, seconds=30), 99)  # 维护期
        r = svc.ingest("G-2", 3, T0 + timedelta(minutes=3), 8)
        # 窗口只有 [4, 8]，凑不齐 4 点 → 无趋势告警
        self.assertFalse(any(a["group"].startswith("trend:")
                             for a in r.get("actions", [])))


class OfflineSweepTests(unittest.TestCase):
    def test_offline_warning_critical_and_recovery(self):
        svc = make_service()
        seed_pressure_device(svc)
        svc.ingest("P-1", 1, T0, 1500)

        acts = svc.sweep_offline(T0 + timedelta(minutes=6))
        self.assertEqual(acts[0]["action"], "opened")
        self.assertEqual(acts[0]["level"], AlertLevel.WARNING.value)

        acts = svc.sweep_offline(T0 + timedelta(minutes=16))
        self.assertEqual(acts[0]["action"], "escalated")
        self.assertEqual(acts[0]["level"], AlertLevel.CRITICAL.value)

        # 重复扫描不新增告警
        acts = svc.sweep_offline(T0 + timedelta(minutes=17))
        self.assertEqual(acts[0]["action"], "suppressed")

        # 恢复上报 → 解除
        svc.ingest("P-1", 2, T0 + timedelta(minutes=18), 1500)
        acts = svc.sweep_offline(T0 + timedelta(minutes=18, seconds=1))
        self.assertEqual(acts[0]["action"], "resolved")
        offline = [a for a in svc.list_alerts() if a["rule_group"] == "offline"][0]
        self.assertEqual(offline["state"], AlertState.RESOLVED.value)


class RuleVersionTests(unittest.TestCase):
    def test_new_version_does_not_change_past(self):
        svc = make_service()
        seed_pressure_device(svc)
        svc.ingest("P-1", 1, T0, 1700)  # v1 下是 WARNING
        self.assertEqual(len(svc.list_alerts()), 1)

        v2 = Ruleset(
            version="rules-v2",
            valid_from=T0 + timedelta(days=1),
            physical_ranges=default_ruleset().physical_ranges,
            level_rules={
                MetricKind.PRESSURE: [
                    LevelRule("p-warn", AlertLevel.WARNING, min_value=1800.0),
                ],
            },
            trend_rules={},
            offline=default_ruleset().offline,
        )
        svc.register_ruleset(v2)
        svc.set_active_version("rules-v2")
        # 历史告警不受影响
        self.assertEqual(svc.list_alerts()[0]["level"], AlertLevel.WARNING.value)
        self.assertEqual(svc.list_alerts()[0]["rule_version"], "rules-v1")
        # 新计算用 v2：1700 不再越限，对旧 v1 告警开始累计恢复计数
        r = svc.ingest("P-1", 2, T0 + timedelta(days=1, minutes=1), 1700)
        self.assertEqual(r["rule_version"], "rules-v2")
        self.assertEqual(r["actions"][0]["action"], "recovery_obs")

    def test_duplicate_version_registration_is_idempotent_but_immutable(self):
        svc = make_service()
        svc.register_ruleset(default_ruleset())  # 同名同内容，幂等
        v2_bad = Ruleset(
            version="rules-v1", valid_from=T0,
            physical_ranges={}, level_rules={}, trend_rules={},
            offline=default_ruleset().offline,
        )
        with self.assertRaises(AlertError):
            svc.register_ruleset(v2_bad)


class RecomputeTests(unittest.TestCase):
    def _device_with_history(self, svc, device="P-9"):
        seed_pressure_device(svc, device=device, factor=1.0)
        # 1700 在 v1 下为 WARNING
        svc.ingest(device, 1, T0, 1700)
        return svc.list_alerts(device_id=device)[0]

    def test_explicit_recompute_supersedes_old_and_is_traceable(self):
        svc = make_service()
        old = self._device_with_history(svc)
        v2 = Ruleset(
            version="rules-v2-loose", valid_from=T0,
            physical_ranges=default_ruleset().physical_ranges,
            level_rules={
                MetricKind.PRESSURE: [
                    LevelRule("p-warn", AlertLevel.WARNING, min_value=1800.0),
                ],
            },
            trend_rules={},
            offline=default_ruleset().offline,
        )
        svc.register_ruleset(v2)
        job = svc.create_recompute_job(
            "P-9", "rules-v2-loose", "阈值校准复核，放宽压力预警线")
        results = svc.run_pending_jobs()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["status"], JobStatus.DONE.value)

        # 旧告警判废（仍可追溯原因）
        self.assertEqual(svc.get_alert(old["alert_id"])["state"],
                         AlertState.SUPERSEDED.value)
        events = svc.list_events(old["alert_id"])
        self.assertTrue(any(e["event_type"] == "SUPERSEDED" for e in events))
        # 放宽后没有新告警
        active = svc.list_alerts("P-9", active_only=True)
        self.assertEqual(active, [])

    def test_recompute_idempotent_rerun(self):
        svc = make_service()
        self._device_with_history(svc)
        v2 = Ruleset(
            version="rules-v2-strict", valid_from=T0,
            physical_ranges=default_ruleset().physical_ranges,
            level_rules={
                MetricKind.PRESSURE: [
                    LevelRule("p-warn", AlertLevel.WARNING, min_value=1000.0),
                    LevelRule("p-crit", AlertLevel.CRITICAL, min_value=1600.0),
                ],
            },
            trend_rules={},
            offline=default_ruleset().offline,
        )
        svc.register_ruleset(v2)
        job = svc.create_recompute_job("P-9", "rules-v2-strict", "收紧阈值")
        svc.run_pending_jobs()
        produced_1 = {a["alert_id"] for a in svc.list_alerts("P-9")
                      if a["created_by_job_id"] == job["job_id"]}
        self.assertTrue(produced_1)
        # 再次运行同一任务：结论可重复，不产生重复告警
        svc.run_pending_jobs()  # DONE 不会被拾取
        # 手工重置为 PENDING 模拟运维要求重跑
        svc.store.execute(
            "UPDATE recompute_jobs SET status='PENDING' WHERE job_id=?",
            (job["job_id"],))
        svc.run_pending_jobs()
        active = svc.list_alerts("P-9", active_only=True)
        # 同一 group 仅一条未结告警
        self.assertEqual(len(active), 1)
        self.assertEqual(active[0]["level"], AlertLevel.CRITICAL.value)

    def test_interrupted_job_resumes_after_restart(self):
        svc = make_service()
        self._device_with_history(svc)
        v2 = Ruleset(
            version="rules-v2-x", valid_from=T0,
            physical_ranges=default_ruleset().physical_ranges,
            level_rules={
                MetricKind.PRESSURE: [
                    LevelRule("p-warn", AlertLevel.WARNING, min_value=1000.0),
                ],
            },
            trend_rules={},
            offline=default_ruleset().offline,
        )
        svc.register_ruleset(v2)
        job = svc.create_recompute_job("P-9", "rules-v2-x", "中断恢复测试")
        # 模拟进程崩溃：任务卡在 RUNNING
        svc.store.execute(
            "UPDATE recompute_jobs SET status='RUNNING' WHERE job_id=?",
            (job["job_id"],))

        # 重启：重新构造服务
        svc2 = AlertService(svc.store)
        pending = svc2.store.query(
            "SELECT status FROM recompute_jobs WHERE job_id=?", (job["job_id"],))
        self.assertEqual(pending[0]["status"], JobStatus.PENDING.value)
        results = svc2.run_pending_jobs()
        self.assertEqual(results[0]["status"], JobStatus.DONE.value)

    def test_persistence_on_disk_across_restart(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "alerts.db")
            svc = AlertService(Store(db))
            seed_pressure_device(svc)
            svc.ingest("P-1", 1, T0, 2200)
            svc.close = svc.store.close
            svc.store.close()

            svc2 = AlertService(Store(db))
            alerts = svc2.list_alerts(active_only=True)
            self.assertEqual(len(alerts), 1)
            cur = svc2.store.query(
                "SELECT last_sequence, good_count FROM ingestion_cursors")
            self.assertEqual(cur[0]["last_sequence"], 1)
            self.assertEqual(cur[0]["good_count"], 1)
            svc2.store.close()


    def test_recompute_with_stricter_physical_range(self):
        svc = make_service()
        svc.register_device("P-R", MetricKind.PRESSURE)
        svc.install("P-R", "SN-R", T0)
        # 2400 在 v1 量程内、越临界线 → 告警
        svc.ingest("P-R", 1, T0, 2400)
        self.assertTrue(svc.list_alerts("P-R", active_only=True))

        v2 = Ruleset(
            version="rules-v2-range", valid_from=T0,
            physical_ranges={MetricKind.PRESSURE: (0.0, 2000.0)},
            level_rules=default_ruleset().level_rules,
            trend_rules={},
            offline=default_ruleset().offline,
        )
        svc.register_ruleset(v2)
        job = svc.create_recompute_job("P-R", "rules-v2-range", "量程收紧复核")
        svc.run_pending_jobs()
        # 旧告警判废；2400 在新量程外，回放不产生新告警
        self.assertEqual(svc.list_alerts("P-R", active_only=True), [])
        self.assertEqual(svc.get_job(job["job_id"])["status"],
                         JobStatus.DONE.value)


    def test_recompute_does_not_touch_offline_alerts(self):
        svc = make_service()
        seed_pressure_device(svc, device="P-O")
        svc.ingest("P-O", 1, T0, 1500)
        svc.sweep_offline(T0 + timedelta(minutes=10))
        offline = [a for a in svc.list_alerts("P-O")
                   if a["rule_group"] == "offline"][0]

        v2 = Ruleset(
            version="rules-v2-off", valid_from=T0,
            physical_ranges=default_ruleset().physical_ranges,
            level_rules={
                MetricKind.PRESSURE: [
                    LevelRule("p-warn", AlertLevel.WARNING, min_value=9999.0)],
            },
            trend_rules={},
            offline=default_ruleset().offline,
        )
        svc.register_ruleset(v2)
        job = svc.create_recompute_job("P-O", "rules-v2-off", "离线不应被重算判废")
        svc.run_pending_jobs()
        # 离线告警保持 OPEN，不被 SUPERSEDED
        self.assertEqual(svc.get_alert(offline["alert_id"])["state"],
                         AlertState.OPEN.value)


class ExplainTests(unittest.TestCase):
    def test_explain_covers_full_chain(self):
        svc = make_service()
        seed_pressure_device(svc, factor=1.02)
        svc.ingest("P-1", 1, T0, 1700)
        r = svc.ingest("P-1", 2, T0 + timedelta(minutes=1), 2200)
        alert_id = r["actions"][0]["alert_id"]
        svc.acknowledge(alert_id, "值班员-李", "接单")
        svc.mark_false_alarm(alert_id, "班长-赵", "校准残差导致的误报")

        report = svc.explain(alert_id)
        phases = [s["phase"] for s in report["steps"]]
        for expected in ("source", "identity", "quality", "calibration",
                         "ruleset", "rule_match", "lifecycle", "current"):
            self.assertIn(expected, phases)
        trigger = report["trigger_observation"]
        self.assertAlmostEqual(trigger["raw_value"], 1700.0)
        self.assertEqual(trigger["calibration_id"], "CAL-P-1")
        timeline_types = [e["event"] for e in report["timeline"]]
        self.assertIn("FALSE_ALARM_REVIEW", timeline_types)
        self.assertIn("ACKNOWLEDGED", timeline_types)


if __name__ == "__main__":
    unittest.main()
