"""固定版本的告警规则定义与评估。

规则集一旦生效即不可变：新版本只能影响其生效之后的计算，或被显式重算任务引用。
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from typing import Optional

from .contracts import AlertLevel, MetricKind


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class LevelRule:
    """绝对阈值规则：调整后读数越过 min/max 即触发对应级别。"""

    rule_id: str
    level: AlertLevel
    min_value: Optional[float] = None
    max_value: Optional[float] = None

    def matches(self, value: float) -> bool:
        if self.min_value is not None and value < self.min_value:
            return False
        if self.max_value is not None and value > self.max_value:
            return False
        return True

    def describe(self) -> str:
        parts = []
        if self.min_value is not None:
            parts.append(f"≥ {self.min_value}")
        if self.max_value is not None:
            parts.append(f"≤ {self.max_value}")
        return " 且 ".join(parts)


@dataclass(frozen=True)
class TrendRule:
    """趋势规则：最近 window 个合格观测的首末差值达到 delta 即触发。"""

    rule_id: str
    direction: str  # "up" 恶化上行 / "down" 恶化下行
    window: int
    warning_delta: float
    critical_delta: float

    def delta_of(self, window_values: list[float]) -> Optional[float]:
        if len(window_values) < self.window:
            return None
        span = window_values[-self.window:]
        change = span[-1] - span[0]
        return change if self.direction == "up" else -change

    def level_for(self, delta: float) -> Optional[AlertLevel]:
        if delta >= self.critical_delta:
            return AlertLevel.CRITICAL
        if delta >= self.warning_delta:
            return AlertLevel.WARNING
        return None


@dataclass(frozen=True)
class OfflineRule:
    warning_after: timedelta
    critical_after: timedelta


@dataclass(frozen=True)
class Ruleset:
    version: str
    valid_from: datetime
    physical_ranges: dict[MetricKind, tuple[float, float]]
    level_rules: dict[MetricKind, list[LevelRule]]
    trend_rules: dict[MetricKind, TrendRule]
    offline: OfflineRule
    cooldown: timedelta = field(default_factory=lambda: timedelta(seconds=60))
    recover_streak: int = 3

    def to_json(self) -> dict:
        def td(t: timedelta) -> float:
            return t.total_seconds()

        return {
            "version": self.version,
            "valid_from": self.valid_from.isoformat(),
            "physical_ranges": {
                m.value: list(rng) for m, rng in self.physical_ranges.items()
            },
            "level_rules": {
                m.value: [
                    {
                        "rule_id": r.rule_id,
                        "level": r.level.value,
                        "min_value": r.min_value,
                        "max_value": r.max_value,
                    }
                    for r in rules
                ]
                for m, rules in self.level_rules.items()
            },
            "trend_rules": {
                m.value: {
                    "rule_id": r.rule_id,
                    "direction": r.direction,
                    "window": r.window,
                    "warning_delta": r.warning_delta,
                    "critical_delta": r.critical_delta,
                }
                for m, r in self.trend_rules.items()
            },
            "offline": {
                "warning_after": td(self.offline.warning_after),
                "critical_after": td(self.offline.critical_after),
            },
            "cooldown": td(self.cooldown),
            "recover_streak": self.recover_streak,
        }

    @classmethod
    def from_json(cls, payload: dict) -> "Ruleset":
        metric = lambda s: MetricKind(s)
        return cls(
            version=payload["version"],
            valid_from=parse_dt(payload["valid_from"]),
            physical_ranges={
                metric(m): (float(rng[0]), float(rng[1]))
                for m, rng in payload["physical_ranges"].items()
            },
            level_rules={
                metric(m): [
                    LevelRule(
                        rule_id=r["rule_id"],
                        level=AlertLevel(r["level"]),
                        min_value=r["min_value"],
                        max_value=r["max_value"],
                    )
                    for r in rules
                ]
                for m, rules in payload["level_rules"].items()
            },
            trend_rules={
                metric(m): TrendRule(
                    rule_id=r["rule_id"],
                    direction=r["direction"],
                    window=int(r["window"]),
                    warning_delta=float(r["warning_delta"]),
                    critical_delta=float(r["critical_delta"]),
                )
                for m, r in payload["trend_rules"].items()
            },
            offline=OfflineRule(
                warning_after=timedelta(seconds=payload["offline"]["warning_after"]),
                critical_after=timedelta(seconds=payload["offline"]["critical_after"]),
            ),
            cooldown=timedelta(seconds=payload.get("cooldown", 60)),
            recover_streak=int(payload.get("recover_streak", 3)),
        )

    def level_for(self, metric: MetricKind, value: float) -> Optional[tuple[str, AlertLevel]]:
        """返回 (rule_id, 最高匹配级别)。"""
        best: Optional[tuple[str, AlertLevel]] = None
        for rule in self.level_rules.get(metric, []):
            if rule.matches(value):
                if best is None or rule.level == AlertLevel.CRITICAL:
                    best = (rule.rule_id, rule.level)
        return best

    def physical_range(self, metric: MetricKind) -> Optional[tuple[float, float]]:
        return self.physical_ranges.get(metric)


def parse_dt(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def default_ruleset(valid_from: Optional[datetime] = None) -> Ruleset:
    """内置 v1 规则：压力高限、可燃气体浓度、上行趋势、离线。"""
    return Ruleset(
        version="rules-v1",
        valid_from=valid_from or datetime(2000, 1, 1, tzinfo=timezone.utc),
        physical_ranges={
            MetricKind.PRESSURE: (0.0, 2500.0),
            MetricKind.GAS: (0.0, 100.0),
            MetricKind.HEALTH: (0.0, 1.0),
        },
        level_rules={
            MetricKind.PRESSURE: [
                LevelRule("pressure-high-warning", AlertLevel.WARNING, min_value=1600.0),
                LevelRule("pressure-high-critical", AlertLevel.CRITICAL, min_value=2000.0),
            ],
            MetricKind.GAS: [
                LevelRule("gas-leak-warning", AlertLevel.WARNING, min_value=25.0),
                LevelRule("gas-leak-critical", AlertLevel.CRITICAL, min_value=50.0),
            ],
        },
        trend_rules={
            MetricKind.PRESSURE: TrendRule(
                "pressure-rising", "up", window=4,
                warning_delta=100.0, critical_delta=200.0,
            ),
            MetricKind.GAS: TrendRule(
                "gas-rising", "up", window=4,
                warning_delta=5.0, critical_delta=10.0,
            ),
        },
        offline=OfflineRule(
            warning_after=timedelta(minutes=5),
            critical_after=timedelta(minutes=15),
        ),
        cooldown=timedelta(seconds=60),
        recover_streak=3,
    )
