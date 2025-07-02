from __future__ import annotations

from collections.abc import Generator, Iterable
from dataclasses import replace

import heapq
import argparse
from datetime import datetime
import logging

from pyutils.utils import synchronize_time
from mdtpy import connect

from welder import ElectricCurrentMeasure, recognize_work, read_measures_from_csv
from welder.database_utils import open_connection, create_ampere_log_table_if_absent, log_measure


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('detect_work_state')

DATABASE_PARAMS = {
    'dbname': 'mdt',
    'user': 'mdt',
    'password': 'urc2004',
    'host': 'localhost',
    'port': '5432'
}

def define_args(parser):
    parser.add_argument("files", nargs='+', help="CSV files to be merged")
    parser.add_argument("--sync", action='store_true', default=False)
  
def get_utc_millis(measure:ElectricCurrentMeasure):
    return round(measure.timestamp.timestamp() * 1000)

last_state:int = 0
started_ts: datetime = None
stopped_ts: datetime = None
nproduceds:int = 0
avg_waiting_time:float = 0.0
avg_processing_time:float = 0.0

def run(args):
    with open_connection(DATABASE_PARAMS) as conn:
        create_ampere_log_table_if_absent(conn)

    readers = [read_measures_from_csv(csv_file) for csv_file in args.files]
    measures = heapq.merge(*readers, key=lambda m: m.timestamp)
    measures = compact(measures)

    if args.sync:
        measures = synchronize_time(measures, utc_millis=get_utc_millis)

    # MDT 프레임워크 서버에 연결
    mdt = connect()

    # 목표 트윈 인스턴스 찾기
    twin = mdt.instances['welder']
    parameters = twin.parameters

    last_state = -1
    product_count = int(parameters['QuantityProduced'].value)
    avg_waiting_time = float(parameters['AvgWaitingTime'].value)
    avg_processing_time = float(parameters['AvgProcessingTime'].value)
    last_stopped_ts = None
    last_started_ts = None

    # 데이터베이스에 연결하고, 전류 로그 테이블이 없으면 생성한다.
    with open_connection(DATABASE_PARAMS) as conn:
        for measure in measures:
            # 입력 전류 값을 통해 현재 상태 판단
            state = recognize_work(measure.timestamp, measure.ampere)
            logger.info(f"ts={measure.timestamp} ampere={measure.ampere:.3} state={state}")

            # 현재 작업 상태를 데이터베이스에 저장
            measure = replace(measure, state=state)
            log_measure(conn, measure)
            
            if state == -1:
                continue 

            if last_state == 0 and state == 1:
                # Water nozzle 생성이 시작되었을 때
                parameters['Status'] = '"WORKING"'
                if last_stopped_ts is not None:
                    # waiting time 계산하고 평균 대기 시간(avg_waiting_time)을 갱신함
                    elapsed = (measure.timestamp - last_stopped_ts).total_seconds()
                    avg_waiting_time = avg_waiting_time + (elapsed - avg_waiting_time) / (product_count+1)
                    parameters['AvgWaitingTime'] = avg_waiting_time
                last_started_ts = measure.timestamp
            elif last_state == 2 and state == 3:
                # Water nozzle 생성이 완료되었을 때 처리
                # 만일 'last_started_ts'가 없으면 nozzle 생성이 시작되지 않은 것으로 간주하여 온전한 작업이 아니라고 판단함
                if last_started_ts is not None:
                    product_count += 1
                    parameters['QuantityProduced'] = product_count

                    elapsed = (measure.timestamp - last_started_ts).total_seconds()
                    avg_processing_time = avg_processing_time + (elapsed - avg_processing_time) / product_count
                    parameters['AvgProcessingTime'] = avg_processing_time
                    
                    # 의도적으로 'Status' 파라미터를 마지막으로 갱신함
                    parameters['Status'] = '"IDLE"'
                    pass
                last_stopped_ts = measure.timestamp
            last_state = state
    

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

    logging.basicConfig(
        level=logging.INFO
    )

    run(args)

if __name__ == '__main__':
    main()