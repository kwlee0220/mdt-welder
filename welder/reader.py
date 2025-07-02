from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generator

import csv
from dateutil.parser import parse
from pyutils.utils import datetime2utc

from .types import ElectricCurrentMeasure


def read_measures_from_csv(file:str) -> Generator[ElectricCurrentMeasure,None,None]:
  with open(file, 'r') as f:
    for line in csv.reader(f):
      ts = parse(line[0])
      ampere = float(line[1])
      yield ElectricCurrentMeasure(timestamp=ts, ampere=ampere)