"""固定版本的规则定义。规则一旦被观测/告警引用即不可变；新版本只影响后续计算。"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Optional

from .storage import Storage, to_iso


@dataclass(frozen=True)
class MetricRule:
    warn_high: Optional[float] = None      # 校准值连续超限高报
    crit_high: Optional[float] = None
    warn_low: Optional[float] = None
    crit_low: Optional[float] = None
    bucket_seconds: int = 300              # 聚合桶宽
    breach_streak: int = 2                 # 连续几个桶超限才告警
    crit_streak: int = 2                   # 连续几个桶达危急阈值才升级
    recover_streak: int = 2                # 连续几个桶回落才自动解除
    slope_warn: Optional[float] = None     # 桶均/分钟 上升斜率预警（单位/分钟）
    short_offline_gap_s: int = 120         # 同安装内相邻观测超此间隔判短时离线
    calibration_settle_s: int = 60         # 校准生效后稳定期，期内质量降为 suspect

    def validate(self) -> None:
        for name in ("warn_high", "crit_high"):
            hi = getattr(self, name)
            lo = getattr(self, "warn_low", None)
            if hi is not None and lo is not None and hi <= lo:
                raise ValueError("高阈值必须高于低阈值")
        if self.crit_high is not None and self.warn_high is not None \
                and self.crit_high < self.warn_high:
            raise ValueError("危急高阈值不能低于高报阈值")
        if self.crit_low is not None and self.warn_low is not None \
                and self.crit_low > self.warn_low:
            raise ValueError("危急低阈值不能高于低报阈值")
        for n in ("bucket_seconds", "breach_streak", "crit_streak",
                  "recover_streak", "short_offline_gap_s", "calibration_settle_s"):
            if getattr(self, n) <= 0:
                raise ValueError(f"{n} 必须为正整数")


DEFAULT_RULES = {
    "pressure": MetricRule(
        warn_high=4.0, crit_high=5.0,
        warn_low=0.2, crit_low=0.1,
        bucket_seconds=300, breach_streak=2, crit_streak=2, recover_streak=2,
        slope_warn=0.25, short_offline_gap_s=120, calibration_settle_s=60,
    ),
    "gas": MetricRule(
        warn_high=25.0, crit_high=50.0,
        warn_low=None, crit_low=None,
        bucket_seconds=300, breach_streak=2, crit_streak=2, recover_streak=2,
        slope_warn=5.0, short_offline_gap_s=120, calibration_settle_s=60,
    ),
}


@dataclass(frozen=True)
class RuleSet:
    version: int
    rules: dict  # metric_type -> MetricRule
    created_at: datetime

    def for_metric(self, metric_type: str) -> MetricRule:
        if metric_type not in self.rules:
            raise KeyError(f"规则版本 {self.version} 缺少指标 {metric_type} 的定义")
        return self.rules[metric_type]


class RuleRegistry:
    """管理规则版本：当前版本用于后续处理；历史版本冻结供重算与解释使用。"""

    def __init__(self, storage: Storage):
        self.storage = storage

    def initialize(self) -> None:
        if self.storage.query_one("SELECT COUNT(*) c FROM rule_versions")["c"] == 0:
            self.create_version(
                {k: asdict(v) for k, v in DEFAULT_RULES.items()},
                note="系统初始规则",
            )

    def create_version(self, rules: dict, note: str = "") -> int:
        parsed = {m: MetricRule(**cfg) for m, cfg in rules.items()}
        for r in parsed.values():
            r.validate()
        with self.storage.tx() as c:
            cur = c.execute(
                "INSERT INTO rule_versions (payload, created_at, note) VALUES (?,?,?)",
                (json.dumps(rules, ensure_ascii=False, sort_keys=True),
                 to_iso(datetime.now(timezone.utc)), note),
            )
            return cur.lastrowid

    def current_version(self) -> int:
        row = self.storage.query_one(
            "SELECT MAX(rule_version) v FROM rule_versions")
        if row["v"] is None:
            raise RuntimeError("规则尚未初始化")
        return row["v"]

    def load(self, version: Optional[int] = None) -> RuleSet:
        if version is None:
            version = self.current_version()
        row = self.storage.query_one(
            "SELECT * FROM rule_versions WHERE rule_version=?", (version,))
        if row is None:
            raise KeyError(f"规则版本 {version} 不存在")
        payload = json.loads(row["payload"])
        rules = {m: MetricRule(**cfg) for m, cfg in payload.items()}
        return RuleSet(version=version, rules=rules,
                       created_at=datetime.fromisoformat(row["created_at"].replace("Z", "+00:00")))

    def list_versions(self) -> list[dict]:
        rows = self.storage.query(
            "SELECT rule_version, created_at, note, payload FROM rule_versions ORDER BY rule_version")
        return [{"version": r["rule_version"], "created_at": r["created_at"],
                 "note": r["note"], "rules": json.loads(r["payload"])} for r in rows]
