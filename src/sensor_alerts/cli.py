"""命令行工具：解释一条告警从原始读数到最终状态的全过程，并支持常用运维操作。

示例：
  python -m sensor_alerts.cli --db data/alerts.db explain ALT-xxxx
  python -m sensor_alerts.cli --db data/alerts.db alerts --active
  python -m sensor_alerts.cli --db data/alerts.db observe P-1 101 2026-10-04T10:00:00Z 1700
  python -m sensor_alerts.cli --db data/alerts.db sweep
"""
from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Optional

from .contracts import MetricKind
from .rules import default_ruleset
from .service import AlertError, AlertService
from .store import Store


def _print_json(obj: Any) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, default=str))


def _build_service(args: argparse.Namespace) -> tuple[Store, AlertService]:
    store = Store(args.db)
    return store, AlertService(store)


def cmd_explain(args: argparse.Namespace) -> int:
    store, svc = _build_service(args)
    try:
        report = svc.explain(args.alert_id)
    finally:
        store.close()

    if args.json:
        _print_json(report)
        return 0

    a = report["alert"]
    print("=" * 72)
    print(f"告警 {a['alert_id']}  全过程解释")
    print("=" * 72)
    for step in report["steps"]:
        print(f"\n[{step['phase']}] {step['title']}")
        if "detail" in step:
            print(f"    {step['detail']}")
        for ev in step.get("events", []):
            arrow = f"{ev['from_state'] or '—'} → {ev['to_state'] or '—'}"
            lvl = f" [{ev['level']}]" if ev["level"] else ""
            actor = f" 操作人={ev['actor']}" if ev["actor"] else ""
            job = f" 重算任务={ev['recompute_job_id']}" if ev["recompute_job_id"] else ""
            print(f"    - {ev['at']} {ev['event']}{lvl} {arrow}{actor}{job}")
            print(f"        原因: {ev['reason']}")
            if ev["sequence"] is not None:
                print(f"        观测: 序号 {ev['sequence']}，调整值 {ev['value']}，"
                      f"规则版本 {ev['rule_version']}")
            if ev["detail"]:
                print(f"        详情: {json.dumps(ev['detail'], ensure_ascii=False)}")
    print("\n" + "=" * 72)
    print(f"最终状态: {a['state']} / {a['level']}（合并 {a['update_count']} 次，"
          f"版本 {a['rule_version']}）")
    print("=" * 72)
    return 0


def cmd_alerts(args: argparse.Namespace) -> int:
    store, svc = _build_service(args)
    try:
        rows = svc.list_alerts(device_id=args.device_id, active_only=args.active)
    finally:
        store.close()
    if args.json:
        _print_json(rows)
        return 0
    if not rows:
        print("（无告警）")
        return 0
    for a in rows:
        print(f"{a['opened_at']}  {a['alert_id']}  {a['device_id']}  "
              f"{a['rule_id']:<26} {a['level']:<9} {a['state']}")
    return 0


def cmd_observe(args: argparse.Namespace) -> int:
    store, svc = _build_service(args)
    try:
        result = svc.ingest(
            args.device_id, args.sequence, args.observed_at, args.value,
            serial_no=args.serial_no,
        )
    finally:
        store.close()
    _print_json(result)
    return 0


def cmd_register_device(args: argparse.Namespace) -> int:
    store, svc = _build_service(args)
    try:
        _print_json(svc.register_device(args.device_id, MetricKind(args.metric),
                                        args.name))
    finally:
        store.close()
    return 0


def cmd_install(args: argparse.Namespace) -> int:
    store, svc = _build_service(args)
    try:
        _print_json(svc.install(args.device_id, args.serial_no, args.installed_at,
                                args.note))
    finally:
        store.close()
    return 0


def cmd_calibrate(args: argparse.Namespace) -> int:
    store, svc = _build_service(args)
    try:
        _print_json(svc.add_calibration(
            args.calibration_id, args.device_id, args.factor, args.calibrated_at))
    finally:
        store.close()
    return 0


def cmd_maintenance(args: argparse.Namespace) -> int:
    store, svc = _build_service(args)
    try:
        _print_json(svc.open_maintenance(
            args.device_id, args.start_at, args.reason, args.end_at))
    finally:
        store.close()
    return 0


def cmd_sweep(args: argparse.Namespace) -> int:
    store, svc = _build_service(args)
    try:
        _print_json({"actions": svc.sweep_offline(args.now)})
    finally:
        store.close()
    return 0


def _add_action(p: argparse.ArgumentParser, event: str) -> None:
    p.add_argument("alert_id")
    p.add_argument("--actor", required=True)
    p.add_argument("--reason", required=True)
    p.set_defaults(
        func=lambda a: _action(a, event))


def _action(args: argparse.Namespace, event: str) -> int:
    store, svc = _build_service(args)
    try:
        if event == "ack":
            out = svc.acknowledge(args.alert_id, args.actor, args.reason)
        elif event == "assign":
            if not args.assignee:
                raise AlertError("转派需要 --assignee")
            out = svc.assign(args.alert_id, args.actor, args.assignee, args.reason)
        elif event == "resolve":
            out = svc.resolve(args.alert_id, args.actor, args.reason)
        else:
            out = svc.mark_false_alarm(args.alert_id, args.actor, args.reason,
                                       args.review_note)
    finally:
        store.close()
    _print_json(out)
    return 0


def cmd_recompute(args: argparse.Namespace) -> int:
    store, svc = _build_service(args)
    try:
        job = svc.create_recompute_job(
            args.device_id, args.rule_version, args.reason,
            args.from_at, args.to_at)
        if not args.create_only:
            results = svc.run_pending_jobs(limit=10)
            for j in results:
                if j["job_id"] == job["job_id"]:
                    job = j
                    break
        _print_json(job)
    finally:
        store.close()
    return 0


def cmd_rules(args: argparse.Namespace) -> int:
    store, svc = _build_service(args)
    try:
        if args.export:
            with open(args.export, "w", encoding="utf-8") as fh:
                json.dump(svc._ruleset(svc.active_version).to_json(),
                          fh, ensure_ascii=False, indent=2)
            print(f"已导出现行规则到 {args.export}，可修改 version 后用 "
                  f"register-rules 注册为新版本")
            return 0
        if args.activate:
            old = svc.set_active_version(args.activate)
            print(f"规则版本切换: {old} → {svc.active_version}（只影响后续计算）")
        else:
            _print_json({
                "active_version": svc.active_version,
                "ruleset": svc._ruleset(svc.active_version).to_json(),
            })
    finally:
        store.close()
    return 0


def cmd_rules_register(args: argparse.Namespace) -> int:
    from .rules import Ruleset

    store, svc = _build_service(args)
    try:
        with open(args.file, encoding="utf-8") as fh:
            ruleset = Ruleset.from_json(json.load(fh))
        svc.register_ruleset(ruleset)
        print(f"已注册规则版本 {ruleset.version}（生效时刻 {ruleset.valid_from}），"
              f"不影响任何历史结论；用 rules --activate {ruleset.version} 切换")
    finally:
        store.close()
    return 0


def cmd_jobs(args: argparse.Namespace) -> int:
    store, svc = _build_service(args)
    try:
        _print_json(svc.list_jobs())
    finally:
        store.close()
    return 0


def cmd_seed_demo(args: argparse.Namespace) -> int:
    """写入一套演示数据，便于直接体验 explain。"""
    from datetime import datetime, timedelta, timezone

    store, svc = _build_service(args)
    try:
        rs = default_ruleset()
        svc.register_ruleset(rs)
        t0 = datetime(2026, 10, 4, 9, 0, tzinfo=timezone.utc)
        dev = "P-DEMO"
        try:
            svc.register_device(dev, MetricKind.PRESSURE, "演示压力传感器")
        except AlertError:
            pass
        svc.install(dev, "SN-2026-A", t0, note="新装")
        svc.add_calibration("CAL-DEMO-1", dev, 1.02, t0)
        values = [1400, 1500, 1650, 1800, 1950, 2100]
        alert_id = None
        for i, v in enumerate(values):
            r = svc.ingest(dev, i + 1, t0 + timedelta(minutes=10 * i), v)
            for act in r.get("actions", []):
                if act["action"] == "opened":
                    alert_id = act["alert_id"]
        # 维护窗口内的假告警样本
        svc.open_maintenance(dev, t0 + timedelta(minutes=70), "换表校准",
                             end_at=t0 + timedelta(minutes=80))
        r = svc.ingest(dev, 8, t0 + timedelta(minutes=75), 3000)
        print("维护窗口内读数判定:", r["quality"], "-", r["reason"])
        if alert_id:
            svc.acknowledge(alert_id, "值班员-李", "已收到压力告警，开始排查")
            svc.assign(alert_id, "值班员-李", "维修班-王", "持续恶化，转派现场")
            print(f"\n演示告警: {alert_id}")
            print(f"查看全过程: python -m sensor_alerts.cli --db {args.db} "
                  f"explain {alert_id}")
    finally:
        store.close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sensor-alerts", description="地下感知主动预警命令行")
    parser.add_argument("--db", default="data/alerts.db", help="SQLite 数据库路径")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("explain", help="解释告警全过程")
    p.add_argument("alert_id")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_explain)

    p = sub.add_parser("alerts", help="列出告警")
    p.add_argument("--device-id", dest="device_id")
    p.add_argument("--active", action="store_true")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_alerts)

    p = sub.add_parser("observe", help="摄入一条观测")
    p.add_argument("device_id")
    p.add_argument("sequence", type=int)
    p.add_argument("observed_at")
    p.add_argument("value", type=float)
    p.add_argument("--serial-no", dest="serial_no")
    p.set_defaults(func=cmd_observe)

    p = sub.add_parser("register-device", help="登记设备")
    p.add_argument("device_id")
    p.add_argument("metric", choices=[m.value for m in MetricKind])
    p.add_argument("--name")
    p.set_defaults(func=cmd_register_device)

    p = sub.add_parser("install", help="登记换表安装")
    p.add_argument("device_id")
    p.add_argument("serial_no")
    p.add_argument("installed_at")
    p.add_argument("--note")
    p.set_defaults(func=cmd_install)

    p = sub.add_parser("calibrate", help="登记校准")
    p.add_argument("calibration_id")
    p.add_argument("device_id")
    p.add_argument("factor", type=float)
    p.add_argument("calibrated_at")
    p.set_defaults(func=cmd_calibrate)

    p = sub.add_parser("maintenance", help="打开维护窗口")
    p.add_argument("device_id")
    p.add_argument("start_at")
    p.add_argument("reason")
    p.add_argument("--end-at", dest="end_at")
    p.set_defaults(func=cmd_maintenance)

    p = sub.add_parser("sweep", help="扫描离线设备")
    p.add_argument("--now")
    p.set_defaults(func=cmd_sweep)

    for name, help_text in (("ack", "确认告警"), ("assign", "转派告警"),
                            ("resolve", "解除告警"), ("false-alarm", "误报复核")):
        p = sub.add_parser(name, help=help_text)
        _add_action(p, name)
        if name == "assign":
            p.add_argument("--assignee")
        if name == "false-alarm":
            p.add_argument("--review-note", dest="review_note")

    p = sub.add_parser("recompute", help="创建并运行显式重算任务")
    p.add_argument("device_id")
    p.add_argument("rule_version")
    p.add_argument("reason")
    p.add_argument("--from-at", dest="from_at")
    p.add_argument("--to-at", dest="to_at")
    p.add_argument("--create-only", action="store_true")
    p.set_defaults(func=cmd_recompute)

    p = sub.add_parser("jobs", help="列出重算任务")
    p.set_defaults(func=cmd_jobs)

    p = sub.add_parser("rules", help="查看/切换规则版本")
    p.add_argument("--activate")
    p.add_argument("--export", help="导出现行规则到 JSON 文件")
    p.set_defaults(func=cmd_rules)

    p = sub.add_parser("register-rules", help="从 JSON 文件注册新规则版本")
    p.add_argument("file")
    p.set_defaults(func=cmd_rules_register)

    p = sub.add_parser("seed-demo", help="写入演示数据")
    p.set_defaults(func=cmd_seed_demo)

    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except AlertError as exc:
        print(f"错误: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
