from __future__ import annotations

from typing import Any
from dataclasses import asdict

import time
import argparse
from datetime import datetime, timedelta

from mdtpy import connect
from welder import ElectricCurrentMeasure, NozzleProductionAudit, recognize_work, inspect_waveform, \
                    open_connection, create_nozzle_production_audit_table, create_ampere_log_table_if_absent


DATABASE_PARAMS = {
    'dbname': 'mdt_app',
    'user': 'mdt',
    'password': 'mdt2025',
    'host': 'localhost',
    'port': '5432'
}

STATE_UNKNOWN = -1
STATE_IDLE = 0
STATE_RUNNING = 1


def define_args(parser):
    parser.add_argument("--host", default="localhost", help="MDT 프레임워크 서버 호스트")
    parser.add_argument("--port", default=12985, help="MDT 프레임워크 서버 포트")
    parser.add_argument("--instance", help="MDT 인스턴스 식별자")
    parser.add_argument("--interval", type=int, default=700, help="조회 주기(milli-second)")

def calc_moving_average(old_avg:float, new_value:float, count:int) -> float:
    return (old_avg * (count-1) + new_value) / count

idle_count = 0
def on_nozzle_production_started(production:NozzleProductionAudit, waveform:list[ElectricCurrentMeasure]):
    global idle_count
    
    if ( len(waveform) == 0 ):
        return

    idle_count += 1
    waiting_time = waveform[-1].timestamp - waveform[0].timestamp
    production.AvgWaitingTime = calc_moving_average(production.AvgWaitingTime, waiting_time, idle_count)

  
def on_nozzle_production_finished(production:NozzleProductionAudit, waveform:list[ElectricCurrentMeasure]):
    # processing_time = timedelta(seconds=(waveform[-1].timestamp - waveform[0].timestamp).total_seconds())
    processing_time = waveform[-1].timestamp - waveform[0].timestamp
    production.Timestamp = waveform[-1].timestamp
    production.QuantityProduced += 1
    production.AvgProcessingTime = calc_moving_average(production.AvgProcessingTime, processing_time, production.QuantityProduced)

    # Waveform을 검사하여 불량 파형인지 확인한다.
    is_defect = inspect_waveform(waveform)
    if is_defect:
        production.DefectVolume += 1
        production.AvgDefectRate = production.DefectVolume / production.QuantityProduced  


def run(args):
    # 노즐 생산 로그 테이블이 존재하지 않으면 생성한다.
    with open_connection(DATABASE_PARAMS) as conn:
        create_ampere_log_table_if_absent(conn)
        create_nozzle_production_audit_table(conn)

    # MDT 프레임워크 서버에 연결하고 대상 인스턴스를 찾음
    mdt = connect(host=args.host, port=args.port)
    instance = mdt.instances[args.instance]
    parameters = instance.parameters

    prod_smc = parameters['NozzleProduction'].read_value()
    value = prod_smc['ParameterValue'] | { 'Timestamp': prod_smc['EventDateTime'] }
    production = NozzleProductionAudit(**value)
    
    state = STATE_UNKNOWN
    waveform = []
    last_ts = None
    
    while True:
        started = datetime.now()
        
        ampere_smc:dict[str, Any] = parameters['Ampere'].read_value()
        ts, ampere = ampere_smc['EventDateTime'], ampere_smc['ParameterValue']
        
        if ts != last_ts:
            code = recognize_work(ts, ampere)
            if state == STATE_RUNNING:
                waveform.append(ElectricCurrentMeasure(ts, ampere, code))
                if code == 3:
                    on_nozzle_production_finished(production, waveform)

                    prod_dict = asdict(production)
                    ts = prod_dict.pop('Timestamp')
                    prod_dict = { 'EventDateTime': ts, 'ParameterValue': prod_dict }

                    parameters['NozzleProduction'] = prod_dict
                    parameters['Status'] = { 'EventDateTime': ts, 'ParameterValue': 'IDLE' }
                    print(production)
                    waveform = []
                    state = STATE_IDLE
            elif state == STATE_IDLE:
                if code == 0:
                    waveform.append(ElectricCurrentMeasure(ts, ampere, code))
                elif code == 1:
                    on_nozzle_production_started(production, waveform)
                    parameters['Status'] = { 'EventDateTime': ts, 'ParameterValue': 'WORKING' }
                    waveform = [ElectricCurrentMeasure(ts, ampere, code)]
                    state = STATE_RUNNING
            else:
                if code == 1:
                    waveform = [ElectricCurrentMeasure(ts, ampere, code)]
                    state = STATE_RUNNING
                elif code == 3:
                    waveform = []
                    state = STATE_IDLE
            last_ts = ts
        
        # 주기에서 수행시간 만큼 뺀 시간만큼 대기함.
        elapsed = (datetime.now() - started).total_seconds() * 1000
        sleep_millis = args.interval - elapsed
        if sleep_millis > 10:
            time.sleep(sleep_millis / 1000)
        
def main():
    parser = argparse.ArgumentParser(description="Merge multiple CSV files")
    define_args(parser)
    
    args = parser.parse_args()
    run(args)

if __name__ == '__main__':
    main()