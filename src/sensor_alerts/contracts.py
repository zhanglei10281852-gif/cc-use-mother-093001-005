"""设备校准和观测数据的基础契约。"""
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Calibration:
    calibration_id: str
    device_id: str
    factor: float

    def __post_init__(self) -> None:
        if self.factor <= 0:
            raise ValueError("校准系数必须大于零")


@dataclass(frozen=True)
class Observation:
    device_id: str
    sequence: int
    observed_at: datetime
    value: float

    def __post_init__(self) -> None:
        if self.sequence < 0 or self.observed_at.tzinfo is None:
            raise ValueError("观测序号和时间无效")
