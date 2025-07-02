from __future__ import annotations

from dataclasses import dataclass

from datetime import datetime


@dataclass(frozen=True, slots=True)
class ElectricCurrentMeasure:
  timestamp: datetime
  ampere: float
  state: int = -1