"""趋势斜率预警与可燃气体场景。"""
import sys
import unittest
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from sensor_alerts.processor import AlertService
from sensor_alerts.rules import DEFAULT_RULES
from sensor_alerts.storage import Storage

T0 = datetime(2026, 10, 4, 8, 0, tzinfo=timezone.utc)


class TrendTests(unittest.TestCase):
    def setUp(self):
        self.svc = AlertService(Storage(":memory:"))
        self.svc.registry.register_device(
            "G1", "gas", hard_min=0, hard_max=100,
            installed_at=T0 - timedelta(hours=1))

    def _bucket_means(self, m0, means):
        seq = 1
        for k, mean in enumerate(means):
            m = m0 + k * 5
            self.svc.ingest({"device_id": "G1", "sequence": seq,
                             "observed_at": (T0 + timedelta(minutes=m)).isoformat(),
                             "value": mean - 0.2}); seq += 1
            self.svc.ingest({"device_id": "G1", "sequence": seq,
                             "observed_at": (T0 + timedelta(minutes=m + 0.5)).isoformat(),
                             "value": mean + 0.2}); seq += 1

    def test_steep_rise_trend_warn_before_threshold(self):
        # 门限 25：均值 10 -> 18 未越限，但 8/5min 超过 slope_warn=5/分钟? 8/5=1.6/分钟
        # 需要配置更灵敏的斜率门限来演示趋势预警
        self.svc.rules.create_version(
            {"gas": asdict(replace(DEFAULT_RULES["gas"], slope_warn=1.0)),
             "pressure": asdict(DEFAULT_RULES["pressure"])})
        # 新版本生效后的数据
        self._bucket_means(60, [10.0, 10.0, 18.0])
        self.svc.sweep(now=T0 + timedelta(minutes=80))
        alerts = self.svc.list_alerts()
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["level"], "WARN")
        b = self.svc.s.query_one(
            "SELECT trend_slope, triggers FROM buckets WHERE triggers LIKE '%slope%'"
            " ORDER BY bucket_start LIMIT 1")
        self.assertIn("slope_rise", b["triggers"])

    def test_repeated_noise_without_streak_does_not_alert(self):
        # 单桶越限后立刻恢复，不满足连续 2 桶，不告警
        self._bucket_means(0, [10.0, 26.0, 10.0, 10.0])
        self.svc.sweep(now=T0 + timedelta(minutes=25))
        self.assertEqual(self.svc.list_alerts(), [])


if __name__ == "__main__":
    unittest.main()
