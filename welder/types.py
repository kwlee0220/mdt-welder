from __future__ import annotations

from dataclasses import dataclass

from datetime import datetime, timedelta
from mdtpy.client.utils import datetime_to_iso8601


@dataclass(frozen=True, slots=True)
class ElectricCurrentMeasure:
    timestamp: datetime
    ampere: float
    state: int = -1
  
            
@dataclass(slots=True)
class NozzleProductionAudit:
    Timestamp: datetime
    QuantityProduced: int
    AvgProcessingTime: timedelta
    AvgWaitingTime: timedelta
    DefectVolume: int
    AvgDefectRate: float

    def __repr__(self):
        return f"NozzleProductionAudit: Timestamp={datetime_to_iso8601(self.Timestamp)}, " \
               f"AvgProcessingTime={self.AvgProcessingTime.total_seconds():.3f}s, " \
               f"AvgWaitingTime={self.AvgWaitingTime.total_seconds():.3f}s, " \
               f"AvgDefectRate={self.AvgDefectRate:.3f} ({self.DefectVolume}/{self.QuantityProduced})"
