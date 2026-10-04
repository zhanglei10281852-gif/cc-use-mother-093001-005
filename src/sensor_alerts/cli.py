"""命令行：台账登记、观测接入、告警处置、全链路解释、规则版本、重算任务。

示例：
  python -m sensor_alerts.cli --db data/a.db alerts
  python -m sensor_alerts.cli explain-alert AL-xxxx
  python -m sensor_alerts.cli --db data/a.db explain-observation --device D1 --sequence 7
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

if __package__ in (None, ""):  # 支持直接 python src/sensor_alerts/cli.py 运行
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from sensor_alerts.processor import AlertService, ProcessingError
    from sensor_alerts.registry import RegistryError
    from sensor_alerts.storage import Storage, parse_iso
else:
    from .processor import AlertService, ProcessingError
    from .registry import RegistryError
    from .storage import Storage, parse_iso

DEFAULT_DB = "data/sensor_alerts.db"


def _iso(s: str):
    return parse_iso(s)


def _print(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, default=str))


def build(argv=None) -> tuple[AlertService, argparse.Namespace]:
    p = argparse.ArgumentParser(prog="sensor-alerts", description="地下感知主动预警命令行")
    p.add_argument("--db", default=DEFAULT_DB, help=f"SQLite 路径（默认 {DEFAULT_DB}）")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("serve", help="启动 HTTP 服务")
    sp.add_argument("--host", default="127.0.0.1"); sp.add_argument("--port", type=int, default=8080)

    sp = sub.add_parser("register-device"); sp.add_argument("device_id")
    sp.add_argument("--metric", choices=("pressure", "gas"), required=True)
    sp.add_argument("--hard-min", type=float); sp.add_argument("--hard-max", type=float)
    sp.add_argument("--installed-at", help="首次安装时间 ISO8601，缺省当前时间")

    sp = sub.add_parser("replace-meter"); sp.add_argument("device_id")
    sp.add_argument("--at", required=True, help="安装时间 ISO8601")
    sp.add_argument("--reason", required=True)

    sp = sub.add_parser("installs"); sp.add_argument("device_id")

    sp = sub.add_parser("calibration"); sp.add_argument("device_id")
    sp.add_argument("--id", required=True); sp.add_argument("--factor", type=float, required=True)
    sp.add_argument("--at", required=True); sp.add_argument("--note", default="")

    sp = sub.add_parser("health"); sp.add_argument("device_id")
    sp.add_argument("--status", required=True,
                    choices=("ok", "degraded", "faulty", "offline"))
    sp.add_argument("--at", required=True); sp.add_argument("--note", default="")

    sp = sub.add_parser("maintenance"); sp.add_argument("device_id")
    sp.add_argument("--id", required=True); sp.add_argument("--start", required=True)
    sp.add_argument("--end", required=True); sp.add_argument("--reason", required=True)

    sp = sub.add_parser("ingest", help="从 JSON 文件或标准输入接入观测（单条或 {observations:[...]}）")
    sp.add_argument("file", nargs="?", help="JSON 文件；缺省读标准输入")

    sp = sub.add_parser("sweep"); sp.add_argument("--now", help="注入墙钟 ISO8601，缺省当前时间")

    sp = sub.add_parser("alerts", help="列出告警")
    sp.add_argument("--device"); sp.add_argument("--status")

    sp = sub.add_parser("events"); sp.add_argument("alert_id")

    for name, helptext in (
        ("ack", "确认告警"), ("assign", "转派告警"),
        ("escalate", "手动升级为 CRITICAL"), ("resolve", "手动解除"),
        ("review", "误报复核")):
        sp = sub.add_parser(name, help=helptext); sp.add_argument("alert_id")
        sp.add_argument("--actor", required=True); sp.add_argument("--reason", required=True)
        if name == "assign":
            sp.add_argument("--assignee", required=True)
        if name == "review":
            sp.add_argument("--verdict", default="false_alarm",
                            choices=("false_alarm", "confirmed_real"))

    sp = sub.add_parser("merge", help="合并告警")
    sp.add_argument("alert_id"); sp.add_argument("--target", required=True)
    sp.add_argument("--actor", required=True); sp.add_argument("--reason", required=True)

    sp = sub.add_parser("explain-alert", help="解释告警：原始读数→质量→桶→规则→时间线")
    sp.add_argument("alert_id")

    sp = sub.add_parser("explain-observation")
    sp.add_argument("--device", required=True); sp.add_argument("--sequence", type=int, required=True)

    sp = sub.add_parser("rules", help="列出规则版本")

    sp = sub.add_parser("create-rule", help="新建规则版本（只影响后续计算）")
    sp.add_argument("file", help="JSON 文件：{\"pressure\": {...}, \"gas\": {...}}")
    sp.add_argument("--note", default="")

    sp = sub.add_parser("recalc-create"); sp.add_argument("device_id")
    sp.add_argument("--rule-version", type=int); sp.add_argument("--from-bucket")

    sp = sub.add_parser("recalc-run"); sp.add_argument("--limit", type=int, default=10)
    sub.add_parser("recalc-list")

    args = p.parse_args(argv)
    svc = AlertService(Storage(args.db))
    return svc, args


def main(argv=None) -> int:
    svc, args = build(argv)
    try:
        if args.cmd == "serve":
            from .api import serve
            svc.s.close()
            serve(args.db, args.host, args.port)
            return 0

        if args.cmd == "register-device":
            _print(svc.registry.register_device(
                args.device_id, args.metric, args.hard_min, args.hard_max,
                _iso(args.installed_at) if args.installed_at else None))
        elif args.cmd == "replace-meter":
            _print(svc.registry.replace_meter(args.device_id, _iso(args.at), args.reason))
        elif args.cmd == "installs":
            _print(svc.registry.list_installs(args.device_id))
        elif args.cmd == "calibration":
            _print(svc.registry.add_calibration(
                args.id, args.device_id, args.factor, _iso(args.at), args.note))
        elif args.cmd == "health":
            _print(svc.registry.record_health(
                args.device_id, args.status, _iso(args.at), args.note))
        elif args.cmd == "maintenance":
            _print(svc.registry.add_maintenance_window(
                args.id, args.device_id, _iso(args.start), _iso(args.end), args.reason))
        elif args.cmd == "ingest":
            raw = json.loads(Path(args.file).read_text() if args.file
                             else sys.stdin.read())
            items = raw["observations"] if isinstance(raw, dict) and "observations" in raw else [raw]
            _print(svc.ingest_batch(items))
        elif args.cmd == "sweep":
            _print(svc.sweep(_iso(args.now) if args.now else None))
        elif args.cmd == "alerts":
            _print(svc.list_alerts(args.device, args.status))
        elif args.cmd == "events":
            rows = svc.s.query(
                "SELECT * FROM alert_events WHERE alert_id=? ORDER BY id",
                (args.alert_id,))
            _print([dict(r) for r in rows])
        elif args.cmd == "ack":
            _print(svc.acknowledge(args.alert_id, args.actor, args.reason))
        elif args.cmd == "assign":
            _print(svc.assign(args.alert_id, args.actor, args.assignee, args.reason))
        elif args.cmd == "escalate":
            _print(svc.escalate(args.alert_id, args.actor, args.reason))
        elif args.cmd == "resolve":
            _print(svc.resolve(args.alert_id, args.actor, args.reason))
        elif args.cmd == "review":
            _print(svc.review_false_alarm(
                args.alert_id, args.actor, args.reason, args.verdict))
        elif args.cmd == "merge":
            _print(svc.merge_alerts(args.alert_id, args.target, args.actor, args.reason))
        elif args.cmd == "explain-alert":
            ex = svc.explain_alert(args.alert_id)
            print("\n".join(ex["explanation"]))
            print("\n— 贡献桶 —"); _print(ex["contributing_buckets"])
            print("— 规则 v%d —" % ex["rule_version"]); _print(ex["rule"])
            print("— 完整时间线 —"); _print(ex["timeline"])
            print("— 相关原始观测（前 200 条）—"); _print(ex["sample_observations"])
        elif args.cmd == "explain-observation":
            _print(svc.explain_observation(args.device, args.sequence))
        elif args.cmd == "rules":
            _print({"versions": svc.rules.list_versions(),
                    "current": svc.rules.current_version()})
        elif args.cmd == "create-rule":
            rules = json.loads(Path(args.file).read_text())
            vid = svc.rules.create_version(rules, args.note)
            _print({"created_rule_version": vid, "current": svc.rules.current_version()})
        elif args.cmd == "recalc-create":
            _print(svc.create_recalc_job(
                args.device_id, args.rule_version, args.from_bucket))
        elif args.cmd == "recalc-run":
            _print(svc.run_recalc_jobs(args.limit))
        elif args.cmd == "recalc-list":
            _print(svc.list_recalc_jobs())
        return 0
    except (ProcessingError, RegistryError, KeyError, ValueError, FileNotFoundError) as e:
        print(f"错误: {type(e).__name__}: {e}", file=sys.stderr)
        return 2
    finally:
        svc.s.close()


if __name__ == "__main__":
    raise SystemExit(main())
