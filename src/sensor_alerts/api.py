"""HTTP 接口（标准库 http.server）：台账、观测接入、告警处置、解释、规则、重算任务。"""
from __future__ import annotations

import json
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

from .processor import AlertService, ProcessingError
from .registry import RegistryError
from .storage import Storage, parse_iso


def _err(code: str, message: str, http_status: int = 400) -> tuple[int, dict]:
    return http_status, {"error": code, "message": message}


class ApiHandler(BaseHTTPRequestHandler):
    server_version = "SensorAlert/1.0"

    # ---- 工具 ----
    def _send(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self) -> dict:
        n = int(self.headers.get("Content-Length", 0))
        if not n:
            return {}
        try:
            return json.loads(self.rfile.read(n).decode("utf-8"))
        except json.JSONDecodeError as e:
            raise ProcessingError(f"请求体不是合法 JSON: {e}")

    @property
    def svc(self) -> AlertService:
        return self.server.svc  # type: ignore[attr-defined]

    def log_message(self, fmt: str, *args: Any) -> None:
        pass  # 静默；由调用方自行记录

    # ---- 路由 ----
    def do_GET(self) -> None:
        self._dispatch("GET")

    def do_POST(self) -> None:
        self._dispatch("POST")

    def _dispatch(self, method: str) -> None:
        try:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            qs = {k: v[0] for k, v in parse_qs(parsed.query).items()}
            for pattern, verbs, fn in ROUTES:
                params = self._match(pattern, path)
                if params is not None and method in verbs:
                    status, payload = fn(self, params, qs)
                    self._send(status, payload)
                    return
            self._send(404, {"error": "not_found", "message": f"无此接口: {method} {path}"})
        except (ProcessingError, RegistryError) as e:
            self._send(400, {"error": "domain_error", "message": str(e)})
        except KeyError as e:
            self._send(404, {"error": "not_found", "message": str(e)})
        except Exception as e:  # noqa: BLE001
            self._send(500, {"error": "internal", "message": f"{type(e).__name__}: {e}"})

    @staticmethod
    def _match(pattern: str, path: str):
        pp, xp = pattern.strip("/").split("/"), path.strip("/").split("/")
        if len(pp) != len(xp):
            return None
        out = {}
        for a, b in zip(pp, xp):
            if a.startswith("{") and a.endswith("}"):
                out[a[1:-1]] = b
            elif a != b:
                return None
        return out


# ============================================================
# 具体处理器
# ============================================================
def h_health(h: ApiHandler, p, q):
    return 200, {"status": "ok", "rule_version": h.svc.rules.current_version()}


def h_register_device(h, p, q):
    b = h._json()
    inst = h.svc.registry.register_device(
        b["device_id"], b["metric_type"],
        b.get("hard_min"), b.get("hard_max"),
        parse_iso(b["installed_at"]) if b.get("installed_at") else None)
    return 201, {"install": inst}


def h_replace_meter(h, p, q):
    b = h._json()
    inst = h.svc.registry.replace_meter(
        p["device_id"], parse_iso(b["installed_at"]), b.get("reason", "换表"))
    return 201, {"install": inst}


def h_list_installs(h, p, q):
    return 200, {"installs": h.svc.registry.list_installs(p["device_id"])}


def h_add_calibration(h, p, q):
    b = h._json()
    res = h.svc.registry.add_calibration(
        b["calibration_id"], p["device_id"], float(b["factor"]),
        parse_iso(b["effective_from"]), b.get("note", ""))
    return 201, res


def h_health_event(h, p, q):
    b = h._json()
    res = h.svc.registry.record_health(
        p["device_id"], b["status"], parse_iso(b["effective_from"]),
        b.get("note", ""))
    return 201, res


def h_maintenance(h, p, q):
    b = h._json()
    res = h.svc.registry.add_maintenance_window(
        b["window_id"], p["device_id"], parse_iso(b["start_at"]),
        parse_iso(b["end_at"]), b["reason"])
    return 201, res


def h_ingest(h, p, q):
    b = h._json()
    items = b["observations"] if isinstance(b, dict) and "observations" in b else [b]
    return 202, h.svc.ingest_batch(items)


def h_sweep(h, p, q):
    b = {}
    try:
        b = h._json()
    except ProcessingError:
        b = {}
    now = parse_iso(b["now"]) if b.get("now") else (
        parse_iso(q["now"]) if q.get("now") else None)
    return 200, h.svc.sweep(now=now)


def h_list_alerts(h, p, q):
    return 200, {"alerts": h.svc.list_alerts(q.get("device_id"), q.get("status"))}


def h_get_alert(h, p, q):
    return 200, {"alert": h.svc.get_alert(p["alert_id"])}


def h_alert_action(h, p, q):
    b = h._json()
    aid, actor = p["alert_id"], b.get("actor", "anonymous")
    action = p["action"]
    reason = b.get("reason", "")
    if action == "ack":
        out = h.svc.acknowledge(aid, actor, reason)
    elif action == "assign":
        out = h.svc.assign(aid, actor, b["assignee"], reason)
    elif action == "escalate":
        out = h.svc.escalate(aid, actor, reason)
    elif action == "resolve":
        out = h.svc.resolve(aid, actor, reason)
    elif action == "review":
        out = h.svc.review_false_alarm(aid, actor, reason,
                                       b.get("verdict", "false_alarm"))
    else:
        return _err("unknown_action", f"未知处置动作 {action}", 404)
    return 200, {"alert": out}


def h_merge(h, p, q):
    b = h._json()
    out = h.svc.merge_alerts(p["alert_id"], b["target_id"],
                             b.get("actor", "anonymous"), b.get("reason", ""))
    return 200, {"alert": out}


def h_alert_events(h, p, q):
    h.svc.get_alert(p["alert_id"])
    rows = h.svc.s.query(
        "SELECT * FROM alert_events WHERE alert_id=? ORDER BY id",
        (p["alert_id"],))
    import json as _json
    events = []
    for r in rows:
        d = dict(r)
        d["detail"] = _json.loads(d["detail"]) if d["detail"] else {}
        events.append(d)
    return 200, {"alert_id": p["alert_id"], "events": events}


def h_explain_alert(h, p, q):
    return 200, h.svc.explain_alert(p["alert_id"])


def h_explain_observation(h, p, q):
    try:
        sequence = int(q["sequence"])
    except (KeyError, ValueError):
        return _err("bad_request", "需要 ?sequence=N 查询参数")
    return 200, h.svc.explain_observation(p["device_id"], sequence)


def h_rule_versions(h, p, q):
    return 200, {"versions": h.svc.rules.list_versions(),
                 "current": h.svc.rules.current_version()}


def h_create_rule(h, p, q):
    b = h._json()
    vid = h.svc.rules.create_version(b["rules"], b.get("note", ""))
    return 201, {"rule_version": vid, "current": h.svc.rules.current_version()}


def h_recalc_create(h, p, q):
    b = h._json()
    job = h.svc.create_recalc_job(
        b["device_id"], b.get("rule_version"), b.get("from_bucket"))
    return 201, {"job": job}


def h_recalc_run(h, p, q):
    jobs = h.svc.run_recalc_jobs(limit=int(q.get("limit", 10)))
    return 200, {"jobs": jobs}


def h_recalc_list(h, p, q):
    return 200, {"jobs": h.svc.list_recalc_jobs()}


def h_cursor(h, p, q):
    installs = h.svc.registry.list_installs(p["device_id"])
    return 200, {"cursors": [c for c in (h.svc.get_cursor(i["install_id"])
                                         for i in installs) if c]}


ROUTES: list[tuple[str, tuple[str, ...], Callable]] = [
    ("/health", ("GET",), h_health),
    ("/devices", ("POST",), h_register_device),
    ("/devices/{device_id}/replace-meter", ("POST",), h_replace_meter),
    ("/devices/{device_id}/installs", ("GET",), h_list_installs),
    ("/devices/{device_id}/calibrations", ("POST",), h_add_calibration),
    ("/devices/{device_id}/health", ("POST",), h_health_event),
    ("/devices/{device_id}/maintenance", ("POST",), h_maintenance),
    ("/devices/{device_id}/cursor", ("GET",), h_cursor),
    ("/devices/{device_id}/explain", ("GET",), h_explain_observation),
    ("/observations", ("POST",), h_ingest),
    ("/sweep", ("POST",), h_sweep),
    ("/alerts", ("GET",), h_list_alerts),
    ("/alerts/{alert_id}", ("GET",), h_get_alert),
    ("/alerts/{alert_id}/events", ("GET",), h_alert_events),
    ("/alerts/{alert_id}/explain", ("GET",), h_explain_alert),
    ("/alerts/{alert_id}/merge", ("POST",), h_merge),
    ("/alerts/{alert_id}/{action}", ("POST",), h_alert_action),
    ("/rules", ("GET",), h_rule_versions),
    ("/rules", ("POST",), h_create_rule),
    ("/recalc/jobs", ("POST",), h_recalc_create),
    ("/recalc/jobs", ("GET",), h_recalc_list),
    ("/recalc/run", ("POST",), h_recalc_run),
]


def build_server(db_path: str, host: str = "127.0.0.1", port: int = 8080) -> ThreadingHTTPServer:
    storage = Storage(db_path)
    server = ThreadingHTTPServer((host, port), ApiHandler)
    server.svc = AlertService(storage)  # type: ignore[attr-defined]
    return server


def serve(db_path: str = "data/sensor_alerts.db", host: str = "127.0.0.1",
          port: int = 8080) -> None:
    srv = build_server(db_path, host, port)
    print(f"预警服务监听 http://{host}:{port}（数据库 {db_path}）")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.svc.s.close()
        srv.server_close()
