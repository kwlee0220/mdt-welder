from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Generator, Iterable, Optional

import argparse
import json
import logging

from mdtpy import connect
from mdtpy.client import MDTInstance
from welder import ElectricCurrentMeasure, NozzleProductionAudit, extract_last_waveform, log_nozzle_waveform, process_nozzle_waveform
from welder.mqtt_client import MQTTClient
from welder.database_utils import open_connection, create_nozzle_production_audit_table
from paho.mqtt.client import MQTTMessage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('inspect_waveform')
  
instance: Optional[MDTInstance] = None
DATABASE_PARAMS = {
    'dbname': 'mdt',
    'user': 'mdt',
    'password': 'urc2004',
    'host': 'localhost',
    'port': '5432'
}


def define_args(parser):
    parser.add_argument("instance_id", help="Target MDT instance id")
    parser.add_argument("--mqtt_broker", "-b", help="URL to MQTT broker")
    parser.add_argument("--mqtt_topic", "-t", help="Topic for 'Status' parameter")


def run(args):
    # MDT 프레임워크 서버에 연결
    mdt = connect()

    # 목표 트윈 인스턴스 찾기
    global instance_id, instance
    instance_id = args.instance_id
    instance = mdt.instances[args.instance_id]
    
    # 노즐 생산 로그 테이블이 존재하지 않으면 생성한다.
    with open_connection(DATABASE_PARAMS) as conn:
        create_nozzle_production_audit_table(conn)

    # 클라이언트 연결
    mqtt = MQTTClient()
    mqtt.connect()

    # 감시 토픽 설정하고 subscribe한다.
    topic = args.mqtt_topic if args.mqtt_topic else f'/mdt/instances/{args.instance_id}/parameters/Status'
    mqtt.subscribe(topic, on_status_changed)

    # 프로그램이 계속 실행되도록 유지
    try:
        # 여기에 메인 로직 작성
        import time
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        mqtt.disconnect()


last_status = None
def on_status_changed(client, userdata, msg:MQTTMessage):
    global last_status, instance

    topic = msg.topic
    try:
        payload = json.loads(msg.payload.decode('utf-8'))
        value = payload['value']
        job_finished = (value == 'IDLE' and last_status == 'WORKING')
        last_status = value
        if not job_finished:
            return
        
        # 'Tail' 시계열 데이터에서 최근 노즐 파형을 추출한다.
        waveform = extract_last_waveform(instance)
        logger.info(f'waveform: start={waveform[0].timestamp}, end={waveform[-1].timestamp}, length={len(waveform)}')
        
        # Waveform을 검사하여 불량 노즐인지 확인하고 관련 처리한다.
        logEntry: NozzleProductionAudit = process_nozzle_waveform(instance, waveform)
        with open_connection(DATABASE_PARAMS) as conn:
            log_nozzle_waveform(conn, logEntry)

    except json.JSONDecodeError:
        logger.error(f"Failed to decode JSON payload from topic {topic}")
    except Exception as e:
        logger.error(f"Error processing message from topic {topic}: {e}")
  

def main():
    parser = argparse.ArgumentParser(description="Inspect the last water-nozzle from waveform and log the result to the database")
    define_args(parser)
    
    args = parser.parse_args()
    run(args)

if __name__ == '__main__':
    main()