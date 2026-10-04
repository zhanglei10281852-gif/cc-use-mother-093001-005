"""设备校准和观测数据的基础契约与领域常量。"""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


class MetricType:
    PRESSURE = "pressure"   # 压力（MPa）
    GAS = "gas"             # 可燃气体（%LEL）
    ALL = (PRESSURE, GAS)


class Quality:
    """观测质量：只有 GOOD 参与趋势聚合。"""
    GOOD = "good"
    SUSPECT = "suspect"        # 可疑：短时离线、校准稳定期、健康降级
    BAD = "bad"                # 坏值：超硬量程、设备故障
    SUPPRESSED = "suppressed"  # 维护窗口内，不触发告警


class HealthStatus:
    OK = "ok"
    DEGRADED = "degraded"
    FAULTY = "faulty"
    OFFLINE = "offline"
    ALL = (OK, DEGRADED, FAULTY, OFFLINE)


class AlertLevel:
    WARN = "WARN"
    CRITICAL = "CRITICAL"


class AlertStatus:
    OPEN = "open"
    ACKED = "acked"
    ASSIGNED = "assigned"
    RESOLVED = "resolved"
    FALSE_ALARM = "false_alarm"
    CLOSED = (RESOLVED, FALSE_ALARM)


class EventType:
    CREATED = "CREATED"                 # 规则触发
    MERGED = "MERGED"                   # 合并后续窗口
    LEVEL_CHANGED = "LEVEL_CHANGED"     # 自动升级
    ESCALATED = "ESCALATED"             # 手动升级
    ACKED = "ACKED"                     # 确认
    ASSIGNED = "ASSIGNED"               # 转派
    RESOLVED = "RESOLVED"               # 解除（手动/自动）
    REVIEWED = "REVIEWED"               # 误报复核
    NOTE = "NOTE"


@dataclass(frozen=True)
class Calibration:
    calibration_id: str
    device_id: str
    factor: float
    effective_from: Optional[datetime] = None

    def __post_init__(self) -> None:
        if self.factor <= 0:
            raise ValueError("校准系数必须大于零")


@dataclass(frozen=True)
class Observation:
    device_id: str
    sequence: int
    observed_at: datetime
    value: float
    metric_type: Optional[str] = None

    def __post_init__(self) -> None:
        if self.sequence < 0 or self.observed_at.tzinfo is None:
            raise ValueError("观测序号和时间无效")


@dataclass(frozen=True)
class HealthEvent:
    device_id: str
    status: str
    effective_from: datetime
    note: str = ""


@dataclass(frozen=True)
class MaintenanceWindow:
    device_id: str
    start_at: datetime
    end_at: datetime
    reason: str

    def __post_init__(self) -> None:
        if self.end_at <= self.start_at:
            raise ValueError("维护窗口结束时间必须晚于开始时间")
        if not self.reason:
            raise ValueError("维护窗口必须登记原因")
