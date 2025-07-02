from __future__ import annotations

from collections.abc import Iterator, Generator, Iterable
from dataclasses import dataclass, replace, asdict

import csv
import json
import heapq
from dateutil.parser import parse
import argparse
from datetime import datetime

from pyutils.utils import synchronize_time, datetime2utc, utc_now_millis, utc2datetime

from welder.work_recognizer import recognize_work


@dataclass(frozen=True, slots=True)
class ElectricCurrentMeasure:
    ts: int
    points: dict[str,float]


def define_args(parser):
    parser.add_argument("files", nargs='+', help="CSV files to be merged")
    parser.add_argument("--sync", action='store_true', default=False)
       
     
def run(args):
    readers = [parse_csv_file(csv_file) for csv_file in args.files]
    measures = heapq.merge(*readers, key=lambda m: m.ts)
    measures = compact(measures)
    
    # for i in range(args.skip):
    #     next(measures) 
    
    if args.sync:
        measures = synchronize_time(measures)
        
    offset:int = -1
    for measure in measures:
        if offset < 0:
            offset = utc_now_millis() - measure.ts
        # ts_str = utc2datetime((measure.ts + offset)).strftime('%Y-%m-%dT%H:%M:%S')
        ts = utc2datetime(measure.ts)
        current = measure.points['Mean']
        work = recognize_work(ts, current)
        print(f"{ts} {current}: {work}")

def parse_csv_file(file:str) -> Generator[ElectricCurrentMeasure,None,None]:
    f = open(file, 'r')
    for line in csv.reader(f):
        ts = datetime2utc(parse(line[0]))
        phase = line[1]
        ampere = float(line[2])
        yield ElectricCurrentMeasure(ts=ts, points={phase:ampere})
    
def compact(measures:Iterable[ElectricCurrentMeasure]) -> Generator[ElectricCurrentMeasure,None,None]:
    points = dict()
    last_ts = -1
    for measure in measures:
        if last_ts < 0:
            last_ts = measure.ts
            points.update(measure.points)
        elif last_ts == measure.ts:
            points.update(measure.points)
        else:
            yield ElectricCurrentMeasure(ts=last_ts, points=points)
            last_ts = measure.ts
            points = measure.points
    yield ElectricCurrentMeasure(ts=last_ts, points=points)
        

def main():
    parser = argparse.ArgumentParser(description="Merge multiple CSV files")
    define_args(parser)
    
    args = parser.parse_args()
    run(args)

if __name__ == '__main__':
    main()