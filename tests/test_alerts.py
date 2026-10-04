"""多级告警：触发、合并、升级、确认、转派、解除、误报复核，全部强制留因。"""
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from sensor_alerts.contracts import AlertLevel, AlertStatus
from sensor_alerts.processor import AlertService, ProcessingError
from sensor_alerts.storage import Storage

T0 = datetime(2026, 10, 4, 8, 0, tzinfo=timezone.utc)


def obs(seq, value, minute):
    return {"device_id": "D1", "sequence": seq,
            "observed_at": (T0 + timedelta(minutes=minute)).isoformat(),
            "value": value}


class AlertLifecycleTests(unittest.TestCase):
    def setUp(self):
        self.svc = AlertService(Storage(":memory:"))
        self.svc.registry.register_device(
            "D1", "pressure", hard_min=-1, hard_max=100,
            installed_at=T0 - timedelta(minutes=10))

    def _high_buckets(self, start_min, values):
        """每个值占一个 5 分钟桶，桶内 2 条好值（相隔 30s）。"""
        seq = start_min * 10
        for k, v in enumerate(values):
            m = start_min + k * 5
            self.svc.ingest(obs(seq + k * 2, v, m))
            self.svc.ingest(obs(seq + k * 2 + 1, v, m + 0.5))

    def test_warn_then_critical_then_auto_resolve(self):
        self._high_buckets(0, [4.5, 4.5])          # 连续 2 桶越 warn
        self.svc.sweep(now=T0 + timedelta(minutes=15))
        alerts = self.svc.list_alerts()
        self.assertEqual(len(alerts), 1)
        aid = alerts[0]["alert_id"]
        self.assertEqual(alerts[0]["level"], AlertLevel.WARN)

        self._high_buckets(20, [5.5, 5.5])         # 连续 2 桶达 crit
        self.svc.sweep(now=T0 + timedelta(minutes=30))
        self.assertEqual(self.svc.get_alert(aid)["level"], AlertLevel.CRITICAL)

        # 连续 2 桶回落 -> 自动解除
        self._high_buckets(40, [3.0, 3.0])
        self.svc.sweep(now=T0 + timedelta(minutes=50))
        self.assertEqual(self.svc.get_alert(aid)["status"], AlertStatus.RESOLVED)

        kinds = [r["event_type"] for r in self.svc.s.query(
            "SELECT event_type FROM alert_events WHERE alert_id=? ORDER BY id", (aid,))]
        self.assertEqual(kinds.count("MERGED"), 2)  # 危急两桶并入
        self.assertIn("CREATED", kinds)
        self.assertIn("LEVEL_CHANGED", kinds)
        self.assertIn("RESOLVED", kinds)

    def test_gap_bucket_resets_streak(self):
        self._high_buckets(0, [4.5])               # 只 1 桶
        self._high_buckets(10, [4.5])              # 空桶后只再 1 桶
        self.svc.sweep(now=T0 + timedelta(minutes=15))
        self.assertEqual(self.svc.list_alerts(), [])

    def test_manual_actions_require_reason(self):
        self._high_buckets(0, [4.5, 4.5])
        self.svc.sweep(now=T0 + timedelta(minutes=15))
        aid = self.svc.list_alerts()[0]["alert_id"]
        for fn, kwargs in (
            (self.svc.acknowledge, {"actor": "a", "reason": " "}),
            (self.svc.escalate, {"actor": "a", "reason": ""}),
            (self.svc.resolve, {"actor": "a", "reason": "x"}),
        ):
            if fn == self.svc.resolve:
                continue
            with self.assertRaises(ProcessingError):
                fn(aid, **kwargs)
        with self.assertRaises(ProcessingError):
            self.svc.assign(aid, "a", "班组", "   ")

    def test_ack_assign_escalate_review_flow(self):
        self._high_buckets(0, [4.5, 4.5])
        self.svc.sweep(now=T0 + timedelta(minutes=15))
        aid = self.svc.list_alerts()[0]["alert_id"]
        self.svc.acknowledge(aid, "op1", "已电话通知")
        self.assertEqual(self.svc.get_alert(aid)["status"], AlertStatus.ACKED)
        self.svc.assign(aid, "op1", "班组A", "专业对口")
        self.assertEqual(self.svc.get_alert(aid)["assignee"], "班组A")
        self.svc.escalate(aid, "op2", "现场有泄漏迹象")
        self.assertEqual(self.svc.get_alert(aid)["level"], AlertLevel.CRITICAL)
        self.svc.review_false_alarm(aid, "专家", "标定误报，确认非真实异常")
        a = self.svc.get_alert(aid)
        self.assertEqual(a["status"], AlertStatus.FALSE_ALARM)
        # 已关闭告警不能再处置
        with self.assertRaises(ProcessingError):
            self.svc.acknowledge(aid, "op", "再确认")
        events = [r["event_type"] for r in self.svc.s.query(
            "SELECT event_type FROM alert_events WHERE alert_id=? ORDER BY id", (aid,))]
        for k in ("ACKED", "ASSIGNED", "ESCALATED", "REVIEWED"):
            self.assertIn(k, events)

    def test_manual_merge_closes_source_and_records_reason(self):
        # 用恢复连击更大的规则版本，让 high 与 low 告警在相邻时段并存
        from dataclasses import asdict, replace
        from sensor_alerts.rules import DEFAULT_RULES
        self.svc.rules.create_version(
            {"pressure": asdict(replace(DEFAULT_RULES["pressure"], recover_streak=10)),
             "gas": asdict(DEFAULT_RULES["gas"])})
        # high 两桶 -> 告警；紧接 low 两桶 -> 另一告警（high 未到 10 桶恢复，仍未结）
        self.svc.ingest(obs(1, 4.5, 0)); self.svc.ingest(obs(2, 4.5, 0.5))
        self.svc.ingest(obs(3, 4.5, 5)); self.svc.ingest(obs(4, 4.5, 5.5))
        self.svc.ingest(obs(5, 0.15, 10)); self.svc.ingest(obs(6, 0.15, 10.5))
        self.svc.ingest(obs(7, 0.15, 15)); self.svc.ingest(obs(8, 0.15, 15.5))
        self.svc.sweep(now=T0 + timedelta(minutes=25))
        alerts = self.svc.list_alerts()
        self.assertEqual(len(alerts), 2)
        hi = next(a for a in alerts if a["direction"] == "high")
        lo = next(a for a in alerts if a["direction"] == "low")
        self.svc.merge_alerts(lo["alert_id"], hi["alert_id"], "值班长",
                              "低压为高压同一引压管波动，合并处理")
        self.assertEqual(self.svc.get_alert(lo["alert_id"])["status"],
                         AlertStatus.RESOLVED)
        self.assertEqual(self.svc.get_alert(lo["alert_id"])["root_alert_id"],
                         hi["alert_id"])
        with self.assertRaises(ProcessingError):
            self.svc.merge_alerts(hi["alert_id"], lo["alert_id"], "x", "目标已关闭")

    def test_cross_install_merge_rejected(self):
        self._high_buckets(0, [4.5, 4.5])
        self.svc.sweep(now=T0 + timedelta(minutes=15))
        a1 = self.svc.list_alerts()[0]["alert_id"]
        self.svc.registry.replace_meter("D1", T0 + timedelta(minutes=20), "换表")
        self.svc.ingest(obs(100, 4.5, 21)); self.svc.ingest(obs(101, 4.5, 21.5))
        self.svc.ingest(obs(110, 4.5, 26)); self.svc.ingest(obs(111, 4.5, 26.5))
        self.svc.sweep(now=T0 + timedelta(minutes=35))
        a2 = next(a for a in self.svc.list_alerts(status="open")
                  if a["install_id"] == "D1#I2")
        with self.assertRaises(ProcessingError):
            self.svc.merge_alerts(a1, a2["alert_id"], "x", "跨安装不能合并")


if __name__ == "__main__":
    unittest.main()
