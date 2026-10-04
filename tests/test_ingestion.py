"""乱序去重、校准、质量判定。"""
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from sensor_alerts.processor import AlertService
from sensor_alerts.storage import Storage

T0 = datetime(2026, 10, 4, 8, 0, tzinfo=timezone.utc)


def obs(device, seq, value, ts):
    return {"device_id": device, "sequence": seq,
            "observed_at": ts.isoformat(), "value": value}


class IngestionTests(unittest.TestCase):
    def setUp(self):
        self.svc = AlertService(Storage(":memory:"))
        self.svc.registry.register_device(
            "D1", "pressure", hard_min=-1, hard_max=100,
            installed_at=T0 - timedelta(minutes=2))

    def test_duplicate_does_not_change_stats(self):
        self.svc.ingest(obs("D1", 1, 3.0, T0))
        dup = self.svc.ingest(obs("D1", 1, 99.0, T0))
        self.assertTrue(dup["duplicate"])
        row = self.svc.s.query_one(
            "SELECT COUNT(*) c, SUM(raw_value) s FROM observations WHERE device_id='D1'")
        self.assertEqual((row["c"], row["s"]), (1, 3.0))

    def test_dedup_scoped_per_install_after_replacement(self):
        self.svc.ingest(obs("D1", 1, 3.0, T0))
        self.svc.registry.replace_meter("D1", T0 + timedelta(minutes=30), "换表")
        r = self.svc.ingest(obs("D1", 1, 4.0, T0 + timedelta(minutes=31)))
        self.assertFalse(r["duplicate"])
        self.assertEqual(r["install_id"], "D1#I2")
        self.assertEqual(self.svc.get_cursor("D1#I1")["last_sequence"], 1)
        self.assertEqual(self.svc.get_cursor("D1#I2")["last_sequence"], 1)

    def test_calibration_factor_applied(self):
        self.svc.registry.add_calibration(
            "CAL1", "D1", 1.10, T0 - timedelta(seconds=20))
        r = self.svc.ingest(obs("D1", 2, 4.0, T0 + timedelta(seconds=30)))
        self.assertAlmostEqual(r["calibrated_value"], 4.4)
        # 校准生效后稳定期内首点可疑
        self.assertEqual(r["quality"], "suspect")
        self.assertTrue(any("稳定期" in x for x in r["quality_reasons"]))

    def test_hard_range_is_bad(self):
        r = self.svc.ingest(obs("D1", 3, 200.0, T0 + timedelta(minutes=10)))
        self.assertEqual(r["quality"], "bad")
        self.assertTrue(any("硬量程" in x for x in r["quality_reasons"]))

    def test_short_offline_first_recovery_point_suspect(self):
        self.svc.ingest(obs("D1", 1, 3.0, T0))
        r = self.svc.ingest(obs("D1", 2, 3.1, T0 + timedelta(seconds=300)))
        self.assertEqual(r["quality"], "suspect")
        self.assertTrue(any("短时离线" in x for x in r["quality_reasons"]))

    def test_maintenance_window_suppresses(self):
        self.svc.registry.add_maintenance_window(
            "W1", "D1", T0, T0 + timedelta(minutes=30), "换表复检")
        r = self.svc.ingest(obs("D1", 1, 99.0, T0 + timedelta(minutes=5)))
        self.assertEqual(r["quality"], "suppressed")

    def test_health_faulty_bad_degraded_suspect(self):
        self.svc.registry.record_health("D1", "faulty", T0, "故障")
        self.assertEqual(self.svc.ingest(obs("D1", 1, 3.0, T0 + timedelta(seconds=1)))["quality"],
                         "bad")
        self.svc.registry.record_health("D1", "degraded", T0 + timedelta(minutes=1), "漂移")
        self.assertEqual(
            self.svc.ingest(obs("D1", 2, 3.0, T0 + timedelta(minutes=2)))["quality"], "suspect")

    def test_out_of_order_within_open_buckets_counts(self):
        # 先到 08:05，后补 08:00（桶未定稿），两桶均值都应包含补点
        self.svc.ingest(obs("D1", 10, 4.5, T0 + timedelta(minutes=5)))
        self.svc.ingest(obs("D1", 1, 4.5, T0))
        self.svc.sweep(now=T0 + timedelta(minutes=20))
        b0 = self.svc.s.query_one("SELECT good_count FROM buckets WHERE bucket_start LIKE '%08:00%'")
        self.assertEqual(b0["good_count"], 1)

    def test_late_after_finalize_marked_and_stats_unchanged(self):
        for i, v in [(1, 3.0), (2, 3.1)]:
            self.svc.ingest(obs("D1", i, v, T0 + timedelta(minutes=i)))
        for i, v in [(10, 3.0), (11, 3.1)]:
            self.svc.ingest(obs("D1", i, v, T0 + timedelta(minutes=10 + i)))
        self.svc.sweep(now=T0 + timedelta(minutes=30))
        before = self.svc.s.query_one(
            "SELECT good_count,sum_value FROM buckets WHERE bucket_start LIKE '%08:00%'")
        late = self.svc.ingest(obs("D1", 3, 50.0, T0 + timedelta(seconds=90)))
        self.assertTrue(late["late"])
        after = self.svc.s.query_one(
            "SELECT good_count,sum_value FROM buckets WHERE bucket_start LIKE '%08:00%'")
        self.assertEqual(dict(before), dict(after))


if __name__ == "__main__":
    unittest.main()
