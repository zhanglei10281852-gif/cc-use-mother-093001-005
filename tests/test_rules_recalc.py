"""规则版本钉选与显式重算任务（含断点续跑、人工痕迹保留）。"""
import sys
import tempfile
import unittest
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from sensor_alerts.contracts import AlertStatus
from sensor_alerts.processor import AlertService
from sensor_alerts.rules import DEFAULT_RULES
from sensor_alerts.storage import Storage, to_iso

T0 = datetime(2026, 10, 4, 8, 0, tzinfo=timezone.utc)


def obs(seq, value, minute):
    return {"device_id": "G1", "sequence": seq,
            "observed_at": (T0 + timedelta(minutes=minute)).isoformat(),
            "value": value}


class RuleVersionTests(unittest.TestCase):
    def setUp(self):
        self.svc = AlertService(Storage(":memory:"))
        self.svc.registry.register_device(
            "G1", "gas", hard_min=0, hard_max=100, installed_at=T0 - timedelta(minutes=10))

    def _two_breaching_buckets(self, m0=0, seq0=1):
        # 两个 5 分钟桶各 2 个好值
        self.svc.ingest(obs(seq0, 22.0, m0))
        self.svc.ingest(obs(seq0 + 1, 23.0, m0 + 0.5))
        self.svc.ingest(obs(seq0 + 2, 22.5, m0 + 5))
        self.svc.ingest(obs(seq0 + 3, 22.0, m0 + 5.5))

    def test_new_version_only_affects_future(self):
        # v1 门限 25：22/23 不触发
        self._two_breaching_buckets(0)
        self.svc.sweep(now=T0 + timedelta(minutes=10))
        self.assertEqual(self.svc.list_alerts(), [])
        v2 = self.svc.rules.create_version(
            {"gas": asdict(replace(DEFAULT_RULES["gas"], warn_high=20.0)),
             "pressure": asdict(DEFAULT_RULES["pressure"])}, note="降低气体门限")
        self.assertEqual(v2, 2)
        self._two_breaching_buckets(20, seq0=100)
        self.svc.sweep(now=T0 + timedelta(minutes=30))
        alerts = self.svc.list_alerts()
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["rule_version"], 2)

    def test_explicit_recalc_with_old_version_removes_alert(self):
        self.svc.rules.create_version(
            {"gas": asdict(replace(DEFAULT_RULES["gas"], warn_high=20.0)),
             "pressure": asdict(DEFAULT_RULES["pressure"])})
        self._two_breaching_buckets(0)
        self.svc.sweep(now=T0 + timedelta(minutes=10))
        self.assertEqual(len(self.svc.list_alerts()), 1)
        # 用 v1（门限 25）显式重算 -> 告警消失
        job = self.svc.create_recalc_job("G1", rule_version=1)
        done = self.svc.run_recalc_jobs()[0]
        self.assertEqual(done["status"], "completed")
        self.assertEqual(self.svc.list_alerts(), [])
        # 观测与桶的规则版本标记已改为重算版本
        rv = {r["rule_version"] for r in self.svc.s.query(
            "SELECT DISTINCT rule_version FROM buckets")}
        self.assertEqual(rv, {1})

    def test_recalc_preserves_manual_false_alarm_verdict(self):
        self.svc.rules.create_version(
            {"gas": asdict(replace(DEFAULT_RULES["gas"], warn_high=20.0)),
             "pressure": asdict(DEFAULT_RULES["pressure"])})
        self._two_breaching_buckets(0)
        self.svc.sweep(now=T0 + timedelta(minutes=10))
        aid = self.svc.list_alerts()[0]["alert_id"]
        self.svc.review_false_alarm(aid, "专家", "标定误报")
        # 同版本重算，告警应重建且误报判定与事件保留
        self.svc.run_recalc_jobs() if False else None
        job = self.svc.create_recalc_job("G1", rule_version=2)
        self.svc.run_recalc_jobs()
        self.assertEqual(self.svc.get_alert(aid)["status"], AlertStatus.FALSE_ALARM)
        kinds = [r["event_type"] for r in self.svc.s.query(
            "SELECT event_type FROM alert_events WHERE alert_id=? ORDER BY id", (aid,))]
        self.assertIn("REVIEWED", kinds)

    def test_recalc_resumes_from_cursor_after_restart(self):
        path = tempfile.mktemp(suffix=".db")
        svc = AlertService(Storage(path))
        svc.registry.register_device(
            "G1", "gas", 0, 100, installed_at=T0 - timedelta(minutes=10))
        svc.rules.create_version(
            {"gas": asdict(replace(DEFAULT_RULES["gas"], warn_high=20.0)),
             "pressure": asdict(DEFAULT_RULES["pressure"])})
        for m in range(0, 12):
            svc.ingest(obs(m * 2 + 1, 22.0, m * 5))
            svc.ingest(obs(m * 2 + 2, 23.0, m * 5 + 0.5))
        svc.sweep(now=T0 + timedelta(minutes=70))
        self.assertTrue(svc.list_alerts())
        job = svc.create_recalc_job("G1", rule_version=1)
        # 模拟在重放阶段崩溃：prepare 完成、cursor 停在第 3 个桶
        svc._run_job(job["job_id"]) if False else None
        prep_job = svc.get_recalc_job(job["job_id"])
        svc._recalc_prepare(prep_job,
                            svc.rules.load(1).for_metric("gas"),
                            DEFAULT_RULES["gas"].bucket_seconds)
        all_buckets = svc.s.query(
            "SELECT bucket_start FROM buckets ORDER BY bucket_start")
        third = all_buckets[2]["bucket_start"]
        with svc.s.tx() as c:
            c.execute("UPDATE buckets SET finalized=1 WHERE bucket_start<=?", (third,))
            c.execute("UPDATE recalc_jobs SET status='running', cursor_bucket=?,"
                      " processed_buckets=3 WHERE job_id=?", (third, job["job_id"]))
        svc.s.close()
        # 重启：running 任务被重新领取并从检查点收尾
        svc2 = AlertService(Storage(path))
        done = svc2.run_recalc_jobs()[0]
        self.assertEqual(done["status"], "completed")
        self.assertGreaterEqual(done["processed_buckets"], 3)
        # v1 下不触发
        self.assertEqual(svc2.list_alerts(), [])
        svc2.s.close()
        Path(path).unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
