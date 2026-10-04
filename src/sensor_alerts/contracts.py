"""设备、校准、观测与告警生命周期的基础契约。"""
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import math


class Quality(str, Enum):
    """观测质量判定结果。"""

    GOOD = "GOOD"                       # 可参与统计
    MAINTENANCE = "MAINTENANCE"         # 落在维护窗口内
    NOT_INSTALLED = "NOT_INSTALLED"     # 安装区间之外（换表前后的旧/新序列）
    OUT_OF_RANGE = "OUT_OF_RANGE"       # 超出传感器物理量程


class MetricKind(str, Enum):
    PRESSURE = "pressure"
    GAS = "gas"
    HEALTH = "health"


class AlertLevel(str, Enum):
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class AlertState(str, Enum):
    OPEN = "OPEN"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    ASSIGNED = "ASSIGNED"
    RESOLVED = "RESOLVED"
    FALSE_ALARM = "FALSE_ALARM"
    SUPERSEDED = "SUPERSEDED"           # 被显式重算结论替代

    @property
    def terminal(self) -> bool:
        return self in (
            AlertState.RESOLVED,
            AlertState.FALSE_ALARM,
            AlertState.SUPERSEDED,
        )

    @property
    def active(self) -> bool:
        return self in (AlertState.OPEN, AlertState.ACKNOWLEDGED, AlertState.ASSIGNED)


class HealthStatus(str, Enum):
    ONLINE = "online"
    OFFLINE = "offline"


class JobStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"


@dataclass(frozen=True)
class Calibration:
    calibration_id: str
    device_id: str
    factor: float

    def __post_init__(self) -> None:
        if not self.calibration_id:
            raise ValueError("校准编号不能为空")
        if not math.isfinite(self.factor) or self.factor <= 0:
            raise ValueError("校准系数必须为大于零的有限数")


@dataclass(frozen=True)
class Observation:
    device_id: str
    sequence: int
    observed_at: datetime
    value: float

    def __post_init__(self) -> None:
        if not self.device_id:
            raise ValueError("设备序号不能为空")
        if self.sequence < 0 or self.observed_at.tzinfo is None:
            raise ValueError("观测序号和时间无效")
        if not math.isfinite(self.value):
            raise ValueError("观测读数必须是有限数")
