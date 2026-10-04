"""HTTP 接口与全链路解释。"""
import json
import sys
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib import request as urlrequest
from urllib.error import HTTPError

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from sensor_alerts.api import build_server
from sensor_alerts.storage import Storage

T0 = datetime(2026, 10, 4, 8, 0, tzinfo=timezone.utc)


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.srv = build_server(":memory:", "127.0.0.1", 0)
        self.port = self.srv.server_address[1]
        self.thread = threading.Thread(target=self.srv.serve_forever, daemon=True)
        self.thread.start()
        self.svc = self.srv.svc

    def tearDown(self):
        self.srv.shutdown()
        self.srv.server_close()
        self.thread.join(timeout=2)

    def _call(self, method, path, payload=None):
        data = json.dumps(payload).encode() if payload is not None else None
        req = urlrequest.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
            headers={"Content-Type": "application/json"})
        try:
            with urlrequest.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read())
        except HTTPError as e:
            return e.code, json.loads(e.read())

    def test_health_and_device_flow(self):
        st, body = self._call("GET", "/health")
        self.assertEqual(st, 200)
        self.assertEqual(body["rule_version"], 1)
        st, body = self._call("POST", "/devices", {
            "device_id": "D1", "metric_type": "pressure",
            "hard_min": -1, "hard_max": 100,
            "installed_at": (T0 - timedelta(minutes=10)).isoformat()})
        self.assertEqual(st, 201)

    def test_ingest_alerts_action_explain(self):
        self._call("POST", "/devices", {
            "device_id": "D1", "metric_type": "pressure",
            "hard_min": -1, "hard_max": 100,
            "installed_at": (T0 - timedelta(minutes=10)).isoformat()})
        obs = [{"device_id": "D1", "sequence": i,
                "observed_at": (T0 + timedelta(minutes=m)).isoformat(),
                "value": v}
               for i, (m, v) in enumerate(
                   [(0, 4.5), (0.5, 4.5), (5, 4.5), (5.5, 4.5)], start=1)]
        st, body = self._call("POST", "/observations", {"observations": obs})
        self.assertEqual(st, 202)
        self.assertEqual(body["duplicate"], 0)
        self._call("POST", "/sweep", {"now": (T0 + timedelta(minutes=15)).isoformat()})
        st, body = self._call("GET", "/alerts")
        self.assertEqual(len(body["alerts"]), 1)
        aid = body["alerts"][0]["alert_id"]

        # 缺原因 -> 400
        st, body = self._call("POST", f"/alerts/{aid}/ack", {"actor": "op"})
        self.assertEqual(st, 400)
        st, body = self._call("POST", f"/alerts/{aid}/ack",
                              {"actor": "op", "reason": "已通知现场"})
        self.assertEqual(st, 200)
        self.assertEqual(body["alert"]["status"], "acked")

        st, ex = self._call("GET", f"/alerts/{aid}/explain")
        self.assertEqual(st, 200)
        self.assertTrue(ex["explanation"])
        self.assertTrue(ex["timeline"])
        self.assertEqual(ex["rule"]["breach_streak"], 2)
        self.assertTrue(any(b["breach_hi_streak"] >= 1
                            for b in ex["contributing_buckets"]))

        st, exo = self._call("GET", "/devices/D1/explain?sequence=1")
        self.assertEqual(st, 200)
        m = exo["matches"][0]
        self.assertIn("formula", m)
        self.assertEqual(m["observation"]["sequence"], 1)

    def test_rules_endpoint_creates_new_version(self):
        st, body = self._call("GET", "/rules")
        self.assertEqual(st, 200)
        self.assertEqual(body["current"], 1)
        gas = body["versions"][0]["rules"]["gas"]
        gas["warn_high"] = 20.0
        st, body = self._call("POST", "/rules",
                              {"rules": body["versions"][0]["rules"], "note": "调门限"})
        self.assertEqual(st, 201)
        self.assertEqual(body["rule_version"], 2)


if __name__ == "__main__":
    unittest.main()
