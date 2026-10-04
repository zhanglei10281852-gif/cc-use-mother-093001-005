"""设备台账：注册、安装/换表（install 序列隔离）、校准、健康事件、维护窗口。"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from .contracts import HealthStatus, MetricType
from .storage import Storage, parse_iso, to_iso


class RegistryError(Exception):
    pass


class Registry:
    def __init__(self, storage: Storage):
        self.s = storage

    # ---------- 设备 ----------
    def register_device(self, device_id: str, metric_type: str,
                        hard_min: Optional[float] = None,
                        hard_max: Optional[float] = None,
                        installed_at: Optional[datetime] = None) -> dict:
        if metric_type not in MetricType.ALL:
            raise RegistryError(f"未知指标类型 {metric_type}")
        if hard_min is not None and hard_max is not None and hard_min >= hard_max:
            raise RegistryError("硬量程下限必须小于上限")
        try:
            with self.s.tx() as c:
                c.execute(
                    "INSERT INTO devices (device_id, metric_type, hard_min, hard_max, created_at)"
                    " VALUES (?,?,?,?,?)",
                    (device_id, metric_type, hard_min, hard_max,
                     to_iso(datetime.now(timezone.utc))))
        except Exception as e:
            if "UNIQUE" in str(e):
                raise RegistryError(f"设备 {device_id} 已存在") from None
            raise
        # 首次安装
        return self.replace_meter(
            device_id, installed_at=installed_at or datetime.now(timezone.utc),
            reason="设备首次安装")

    def get_device(self, device_id: str) -> dict:
        r = self.s.query_one("SELECT * FROM devices WHERE device_id=?", (device_id,))
        if r is None:
            raise RegistryError(f"设备 {device_id} 不存在")
        return dict(r)

    # ---------- 安装 / 换表 ----------
    def replace_meter(self, device_id: str, installed_at: datetime,
                      reason: str) -> dict:
        """换表：关闭旧 install 并开启新 install；新旧序列永不串联。"""
        if not reason:
            raise RegistryError("换表必须登记原因")
        dev = self.get_device(device_id)
        with self.s.tx() as c:
            prev = c.execute(
                "SELECT * FROM installs WHERE device_id=? ORDER BY seq DESC LIMIT 1",
                (device_id,)).fetchone()
            seq = (prev["seq"] + 1) if prev else 1
            if prev and prev["removed_at"] is None:
                if parse_iso(prev["installed_at"]) >= installed_at:
                    raise RegistryError("新安装时间必须晚于当前安装时间")
                c.execute("UPDATE installs SET removed_at=?, remove_reason=? WHERE install_id=?",
                          (to_iso(installed_at), reason, prev["install_id"]))
            install_id = f"{device_id}#I{seq}"
            c.execute(
                "INSERT INTO installs (install_id, device_id, seq, installed_at,"
                " removed_at, remove_reason, replacement_of) VALUES (?,?,?,?,?,?,?)",
                (install_id, device_id, seq, to_iso(installed_at),
                 None, None, prev["install_id"] if prev else None))
            # 换表自带基线校准系数 1.0，保证新表可立即出值
            c.execute(
                "INSERT INTO calibrations (calibration_id, install_id, factor,"
                " effective_from, note) VALUES (?,?,?,?,?)",
                (f"{install_id}#BASE", install_id, 1.0, to_iso(installed_at), "安装基线"))
        return self.get_install(install_id)

    def get_install(self, install_id: str) -> dict:
        r = self.s.query_one("SELECT * FROM installs WHERE install_id=?", (install_id,))
        if r is None:
            raise RegistryError(f"安装 {install_id} 不存在")
        return dict(r)

    def install_at(self, device_id: str, ts: datetime) -> dict:
        """返回 ts 时刻设备所在的 install；观测据此归属，换表前后序列隔离。"""
        r = self.s.query_one(
            "SELECT * FROM installs WHERE device_id=? AND installed_at<=?"
            " ORDER BY installed_at DESC LIMIT 1",
            (device_id, to_iso(ts)))
        if r is None:
            raise RegistryError(f"设备 {device_id} 在 {ts.isoformat()} 无有效安装记录")
        return dict(r)

    def list_installs(self, device_id: str) -> list[dict]:
        return [dict(r) for r in self.s.query(
            "SELECT * FROM installs WHERE device_id=? ORDER BY seq", (device_id,))]

    # ---------- 校准 ----------
    def add_calibration(self, calibration_id: str, device_id: str, factor: float,
                        effective_from: datetime, note: str = "",
                        install_id: Optional[str] = None) -> dict:
        if factor <= 0:
            raise RegistryError("校准系数必须大于零")
        if install_id is None:
            install_id = self.install_at(device_id, effective_from)["install_id"]
        try:
            with self.s.tx() as c:
                c.execute(
                    "INSERT INTO calibrations (calibration_id, install_id, factor,"
                    " effective_from, note) VALUES (?,?,?,?,?)",
                    (calibration_id, install_id, factor, to_iso(effective_from), note))
        except Exception as e:
            if "UNIQUE" in str(e):
                raise RegistryError(f"校准 {calibration_id} 已存在") from None
            raise
        return {"calibration_id": calibration_id, "install_id": install_id,
                "factor": factor, "effective_from": to_iso(effective_from)}

    def calibration_at(self, install_id: str, ts: datetime) -> dict:
        r = self.s.query_one(
            "SELECT * FROM calibrations WHERE install_id=? AND effective_from<=?"
            " ORDER BY effective_from DESC LIMIT 1",
            (install_id, to_iso(ts)))
        if r is None:  # 安装基线始终存在
            raise RegistryError(f"安装 {install_id} 缺少校准记录")
        return dict(r)

    # ---------- 健康 ----------
    def record_health(self, device_id: str, status: str,
                      effective_from: datetime, note: str = "") -> dict:
        if status not in HealthStatus.ALL:
            raise RegistryError(f"未知健康状态 {status}")
        install_id = self.install_at(device_id, effective_from)["install_id"]
        with self.s.tx() as c:
            cur = c.execute(
                "INSERT INTO health_events (install_id, status, effective_from, note)"
                " VALUES (?,?,?,?)",
                (install_id, status, to_iso(effective_from), note))
            return {"id": cur.lastrowid, "install_id": install_id, "status": status,
                    "effective_from": to_iso(effective_from)}

    def health_at(self, install_id: str, ts: datetime) -> dict:
        r = self.s.query_one(
            "SELECT * FROM health_events WHERE install_id=? AND effective_from<=?"
            " ORDER BY effective_from DESC LIMIT 1",
            (install_id, to_iso(ts)))
        return dict(r) if r else {"status": HealthStatus.OK, "note": ""}

    # ---------- 维护窗口 ----------
    def add_maintenance_window(self, window_id: str, device_id: str,
                               start_at: datetime, end_at: datetime, reason: str) -> dict:
        if end_at <= start_at:
            raise RegistryError("维护窗口结束时间必须晚于开始时间")
        if not reason:
            raise RegistryError("维护窗口必须登记原因")
        install_s = self.install_at(device_id, start_at)
        install_e = self.install_at(device_id, end_at)
        if install_s["install_id"] != install_e["install_id"]:
            raise RegistryError("维护窗口不能跨越换表安装")
        try:
            with self.s.tx() as c:
                c.execute(
                    "INSERT INTO maintenance_windows (window_id, install_id, start_at, end_at, reason)"
                    " VALUES (?,?,?,?,?)",
                    (window_id, install_s["install_id"], to_iso(start_at),
                     to_iso(end_at), reason))
        except Exception as e:
            if "UNIQUE" in str(e):
                raise RegistryError(f"维护窗口 {window_id} 已存在") from None
            raise
        return {"window_id": window_id, "install_id": install_s["install_id"],
                "start_at": to_iso(start_at), "end_at": to_iso(end_at), "reason": reason}

    def in_maintenance(self, install_id: str, ts: datetime) -> Optional[dict]:
        iso = to_iso(ts)
        r = self.s.query_one(
            "SELECT * FROM maintenance_windows WHERE install_id=? AND start_at<=? AND end_at>?",
            (install_id, iso, iso))
        return dict(r) if r else None
