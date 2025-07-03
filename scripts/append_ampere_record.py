from __future__ import annotations

from collections.abc import Generator, Iterable
from dataclasses import replace

import heapq
import argparse
import time
from datetime import datetime, timedelta
import logging

from pyutils.utils import synchronize_time
from mdtpy import connect

from welder import ElectricCurrentMeasure, read_measures_from_csv
from welder.database_utils import open_connection, create_ampere_log_table_if_absent, log_measure

DATABASE_PARAMS = {
    'dbname': 'mdt_app',
    'user': 'mdt',
    'password': 'mdt2025',
    'host': 'localhost',
    'port': '5432'
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('append_ampere_record')


def define_args(parser):
    parser.add_argument("files", nargs='+', help="CSV files to be merged")
    parser.add_argument("--interval", type=float, default=1, help="Interval in seconds")
    parser.add_argument("--sync", action='store_true', default=False)
  
def get_utc_millis(measure:ElectricCurrentMeasure):
    return round(measure.timestamp.timestamp() * 1000)

def run(args):
    readers = [read_measures_from_csv(csv_file) for csv_file in args.files]
    measures = heapq.merge(*readers, key=lambda m: m.timestamp)
    measures = compact(measures)
    if args.sync:
        measures = synchronize_time(measures, utc_millis=get_utc_millis)
    else:
        measures = emulate_measure(measures, args.interval)

    # 데이터베이스에 연결하고, 전류 로그 테이블이 없으면 생성한다.
    with open_connection(DATABASE_PARAMS) as conn:
        create_ampere_log_table_if_absent(conn)
        
        for measure in measures:
            # 전류 값을 데이터베이스에 저장
            log_measure(conn, measure)
            logger.info(f"ts={measure.timestamp} ampere={measure.ampere:.3}")
            

def emulate_measure(measures:Iterable[ElectricCurrentMeasure], interval:float) -> Generator[ElectricCurrentMeasure,None,None]:
    for measure in measures:
        started = time.time()
        yield measure
        wait_time = interval - (time.time() - started)
        if wait_time > 0.003:
            time.sleep(wait_time-0.002)
            
def compact(measures:Iterable[ElectricCurrentMeasure]) -> Generator[ElectricCurrentMeasure,None,None]:
    last_ts = None
    for measure in measures:
        if last_ts is None or last_ts < measure.timestamp:
            yield measure
    last_ts = measure.timestamp

def main():
    parser = argparse.ArgumentParser(description="Update welder parameters")
    define_args(parser)
    args = parser.parse_args()
    run(args)

if __name__ == '__main__':
    main()