"""HTTP 接口端到端测试。"""
import json
import sys
import unittest
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from sensor_alerts.api import ApiServer  # noqa: E402
from sensor_alerts.service import AlertService  # noqa: E402
from sensor_alerts.store import Store  # noqa: E402

T0 = datetime(2026, 10, 4, 10, 0, tzinfo=timezone.utc)


class ApiClient:
    def __init__(self, base):
        self.base = base

    def request(self, method, path, body=None):
        data = None
        headers = {}
        if body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(
            self.base + path, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode())


class ApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.service = AlertService(Store(":memory:"))
        cls.server = ApiServer(cls.service)
        httpd = cls.server.start("127.0.0.1", 0)
        cls.port = httpd.server_address[1]
        cls.api = ApiClient(f"http://127.0.0.1:{cls.port}")

    @classmethod
    def tearDownClass(cls):
        cls.server.stop()
        cls.service.store.close()

    def test_01_full_flow(self):
        code, _ = self.api.request("POST", "/devices", {
            "device_id": "P-HTTP", "metric": "pressure", "name": "接口压力设备"})
        self.assertEqual(code, 200)

        code, _ = self.api.request("POST", "/devices/P-HTTP/installations", {
            "serial_no": "SN-HTTP", "installed_at": T0.isoformat()})
        self.assertEqual(code, 200)

        code, _ = self.api.request("POST", "/devices/P-HTTP/calibrations", {
            "calibration_id": "CAL-HTTP", "factor": 1.0,
            "calibrated_at": T0.isoformat()})
        self.assertEqual(code, 200)

        # 维护窗口内的高读数 → 被抑制
        code, w = self.api.request("POST", "/devices/P-HTTP/maintenance", {
            "start_at": (T0 + timedelta(minutes=10)).isoformat(),
            "end_at": (T0 + timedelta(minutes=20)).isoformat(),
            "reason": "换表校准"})
        self.assertEqual(code, 200)
        code, r = self.api.request("POST", "/observations", {
            "device_id": "P-HTTP", "sequence": 1,
            "observed_at": (T0 + timedelta(minutes=15)).isoformat(),
            "value": 3000})
        self.assertEqual(code, 200)
        self.assertEqual(r["quality"], "MAINTENANCE")

        # 重复观测
        code, r2 = self.api.request("POST", "/observations", {
            "device_id": "P-HTTP", "sequence": 1,
            "observed_at": (T0 + timedelta(minutes=15)).isoformat(),
            "value": 3000})
        self.assertTrue(r2["duplicate"])

        # 持续恶化 → 开警、升级（阈值组与趋势组各自合并为一条）
        opened_ids = set()
        for seq, (minute, value) in enumerate(
                [(21, 1700), (22, 1800), (23, 1900), (24, 2200)], start=2):
            code, r = self.api.request("POST", "/observations", {
                "device_id": "P-HTTP", "sequence": seq,
                "observed_at": (T0 + timedelta(minutes=minute)).isoformat(),
                "value": value})
            for act in r.get("actions", []):
                if act["action"] == "opened":
                    opened_ids.add(act["alert_id"])
        self.assertTrue(opened_ids)
        alert_id = sorted(a for a in opened_ids
                          if self.api.request("GET", f"/alerts/{a}")[1]["rule_group"]
                          == "level:pressure")[0]

        # 确认 / 转派
        code, a = self.api.request("POST", f"/alerts/{alert_id}/acknowledge",
                                   {"actor": "李", "reason": "接单"})
        self.assertEqual(a["state"], "ACKNOWLEDGED")
        code, a = self.api.request("POST", f"/alerts/{alert_id}/assign",
                                   {"actor": "李", "assignee": "王",
                                    "reason": "转派现场"})
        self.assertEqual(a["state"], "ASSIGNED")

        # 缺少原因 → 400
        code, err = self.api.request("POST", f"/alerts/{alert_id}/resolve",
                                     {"actor": "李"})
        self.assertEqual(code, 400)
        self.assertIn("reason", err["error"])

        # 解除全部未结告警
        code, active_before = self.api.request(
            "GET", "/alerts?active_only=true&device_id=P-HTTP")
        for a in active_before:
            code, r = self.api.request("POST",
                                       f"/alerts/{a['alert_id']}/resolve",
                                       {"actor": "王", "reason": "现场处置，压力恢复"})
            self.assertEqual(code, 200)
        code, a = self.api.request("GET", f"/alerts/{alert_id}")
        self.assertEqual(a["state"], "RESOLVED")

        # 解释链路
        code, report = self.api.request("GET", f"/alerts/{alert_id}/explain")
        self.assertEqual(code, 200)
        phases = [s["phase"] for s in report["steps"]]
        self.assertIn("calibration", phases)
        self.assertIn("lifecycle", phases)

        # 活跃告警列表为空，全量列表有记录
        code, active = self.api.request("GET", "/alerts?active_only=true")
        self.assertEqual(active, [])
        code, all_alerts = self.api.request("GET", "/alerts")
        self.assertTrue(any(a["alert_id"] == alert_id for a in all_alerts))

    def test_02_rules_and_404(self):
        code, payload = self.api.request("GET", "/rules")
        self.assertEqual(payload["active_version"], "rules-v1")
        code, _ = self.api.request("GET", "/nope")
        self.assertEqual(code, 404)
        code, _ = self.api.request("POST", "/devices", {
            "device_id": "X", "metric": "unknown"})
        self.assertEqual(code, 400)


if __name__ == "__main__":
    unittest.main()
