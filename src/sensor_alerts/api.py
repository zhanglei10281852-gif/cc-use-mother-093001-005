"""基于标准库 http.server 的 JSON 接口。

启动：python -m sensor_alerts.api --db data/alerts.db --port 8080
"""
from __future__ import annotations

import json
import threading
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

from .contracts import MetricKind
from .rules import Ruleset
from .service import AlertError, AlertService
from .store import Store


def _json_dt(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"无法序列化 {type(value)}")


class ApiServer:
    def __init__(self, service: AlertService):
        self.service = service
        self.httpd: Optional[ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None

    def start(self, host: str = "127.0.0.1", port: int = 8080) -> ThreadingHTTPServer:
        svc = self.service

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt: str, *args: Any) -> None:
                return

            def _send(self, code: int, payload: Any) -> None:
                body = json.dumps(payload, ensure_ascii=False, default=_json_dt).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _body(self) -> dict:
                length = int(self.headers.get("Content-Length") or 0)
                if not length:
                    return {}
                raw = self.rfile.read(length)
                try:
                    data = json.loads(raw.decode())
                except json.JSONDecodeError as exc:
                    raise AlertError(f"请求体不是合法 JSON: {exc}")
                if not isinstance(data, dict):
                    raise AlertError("请求体必须是 JSON 对象")
                return data

            def do_GET(self) -> None:  # noqa: N802
                try:
                    parsed = urlparse(self.path)
                    q = {k: v[0] for k, v in parse_qs(parsed.query).items()}
                    route = parsed.path.rstrip("/")
                    if route == "/health":
                        return self._send(200, {
                            "ok": True, "devices": svc.health(q.get("now"))})
                    if route == "/rules":
                        active = svc.active_version
                        return self._send(200, {
                            "active_version": active,
                            "ruleset": svc._ruleset(active).to_json(),
                        })
                    if route == "/alerts":
                        return self._send(200, svc.list_alerts(
                            device_id=q.get("device_id"),
                            active_only=q.get("active_only", "").lower() in ("1", "true"),
                        ))
                    if route.startswith("/alerts/"):
                        alert_id = route.split("/")[2]
                        sub = route.split("/")[3:]
                        if sub == ["explain"]:
                            return self._send(200, svc.explain(alert_id))
                        if sub == ["events"]:
                            return self._send(200, svc.list_events(alert_id))
                        return self._send(200, svc.get_alert(alert_id))
                    if route.startswith("/devices/"):
                        parts = route.split("/")
                        return self._send(200, svc.get_device(parts[2]))
                    if route == "/jobs":
                        return self._send(200, svc.list_jobs())
                    if route.startswith("/jobs/"):
                        return self._send(200, svc.get_job(route.split("/")[2]))
                    self._send(404, {"error": "not found"})
                except (AlertError, ValueError) as exc:
                    self._send(400, {"error": str(exc)})
                except Exception as exc:  # noqa: BLE001
                    self._send(500, {"error": f"服务器内部错误: {exc}"})

            def do_POST(self) -> None:  # noqa: N802
                try:
                    route = urlparse(self.path).path.rstrip("/")
                    b = self._body()
                    result = self._dispatch(route, b)
                    if result is not None:
                        self._send(200, result)
                except (AlertError, ValueError) as exc:
                    self._send(400, {"error": str(exc)})
                except Exception as exc:  # noqa: BLE001
                    self._send(500, {"error": f"服务器内部错误: {exc}"})

            def _require(self, b: dict, key: str) -> Any:
                if key not in b:
                    raise AlertError(f"缺少参数 {key}")
                return b[key]

            def _dispatch(self, route: str, b: dict) -> Any:
                if route == "/devices":
                    return svc.register_device(
                        self._require(b, "device_id"),
                        MetricKind(self._require(b, "metric")),
                        b.get("name"),
                    )
                if route.endswith("/installations"):
                    device_id = route.split("/")[2]
                    return svc.install(
                        device_id, self._require(b, "serial_no"),
                        self._require(b, "installed_at"), b.get("note"),
                    )
                if route.endswith("/calibrations"):
                    device_id = route.split("/")[2]
                    return svc.add_calibration(
                        self._require(b, "calibration_id"), device_id,
                        float(self._require(b, "factor")),
                        self._require(b, "calibrated_at"),
                    )
                if route.endswith("/maintenance"):
                    device_id = route.split("/")[2]
                    return svc.open_maintenance(
                        device_id, self._require(b, "start_at"),
                        self._require(b, "reason"), b.get("end_at"),
                    )
                if route == "/observations":
                    return svc.ingest(
                        self._require(b, "device_id"),
                        int(self._require(b, "sequence")),
                        self._require(b, "observed_at"),
                        float(self._require(b, "value")),
                        b.get("serial_no"),
                    )
                if route == "/sweep/offline":
                    return {"actions": svc.sweep_offline(b.get("now"))}
                if route == "/rules/activate":
                    old = svc.set_active_version(self._require(b, "version"))
                    return {"previous_version": old,
                            "active_version": svc.active_version}
                if route == "/rules":
                    ruleset = Ruleset.from_json(b)
                    svc.register_ruleset(ruleset)
                    return {"version": ruleset.version,
                            "valid_from": ruleset.valid_from.isoformat(),
                            "registered": True}
                if route == "/recompute":
                    job = svc.create_recompute_job(
                        self._require(b, "device_id"),
                        self._require(b, "rule_version"),
                        self._require(b, "reason"),
                        b.get("from_observed_at"), b.get("to_observed_at"),
                    )
                    if b.get("run", True):
                        done = svc.run_pending_jobs(limit=10)
                        for j in done:
                            if j["job_id"] == job["job_id"]:
                                job = j
                                break
                    return job
                if route == "/recompute/run-pending":
                    return {"jobs": svc.run_pending_jobs(int(b.get("limit", 10)))}
                if route.startswith("/maintenance/") and route.endswith("/close"):
                    window_id = route.split("/")[2]
                    return svc.close_maintenance(
                        window_id, self._require(b, "end_at"))
                if route.startswith("/installations/") and route.endswith("/remove"):
                    installation_id = route.split("/")[2]
                    return svc.remove_installation(
                        installation_id, self._require(b, "removed_at"),
                        self._require(b, "reason"))
                if route.startswith("/alerts/"):
                    parts = route.split("/")
                    alert_id, action = parts[2], parts[3]
                    if action == "acknowledge":
                        return svc.acknowledge(
                            alert_id, self._require(b, "actor"),
                            self._require(b, "reason"))
                    if action == "assign":
                        return svc.assign(
                            alert_id, self._require(b, "actor"),
                            self._require(b, "assignee"),
                            self._require(b, "reason"))
                    if action == "resolve":
                        return svc.resolve(
                            alert_id, self._require(b, "actor"),
                            self._require(b, "reason"))
                    if action == "false-alarm":
                        return svc.mark_false_alarm(
                            alert_id, self._require(b, "actor"),
                            self._require(b, "reason"), b.get("review_note"))
                raise AlertError(f"未知接口 {route}")

        self.httpd = ThreadingHTTPServer((host, port), Handler)
        self._thread = threading.Thread(
            target=self.httpd.serve_forever, daemon=True)
        self._thread.start()
        return self.httpd

    def stop(self) -> None:
        if self.httpd:
            self.httpd.shutdown()
            self.httpd.server_close()
            self.httpd = None


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="地下感知主动预警 HTTP 服务")
    parser.add_argument("--db", default="data/alerts.db")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()

    store = Store(args.db)
    service = AlertService(store)
    server = ApiServer(service)
    server.start(args.host, args.port)
    print(f"预警服务已启动: http://{args.host}:{args.port}  数据库 {args.db}")
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        server.stop()
        store.close()


if __name__ == "__main__":
    main()
