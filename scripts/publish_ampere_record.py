from __future__ import annotations

from collections.abc import Generator, Iterable
from dataclasses import replace

import heapq
import argparse
import time
from datetime import datetime, timedelta
import logging
import json
import paho.mqtt.client as mqtt

from pyutils.utils import synchronize_time
from mdtpy import connect

from welder import ElectricCurrentMeasure, read_measures_from_csv
from welder.database_utils import open_connection, create_ampere_log_table_if_absent, log_measure

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "mdt/test/parameters/Data"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('append_ampere_record')

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("Connected to MQTT broker")
    else:
        logger.error(f"Failed to connect to MQTT broker with code: {rc}")

def define_args(parser):
    parser.add_argument("files", nargs='+', help="CSV files to be merged")
    parser.add_argument("--interval", type=int, default=1000, help="Interval in milliseconds")
    parser.add_argument("--sync", action='store_true', default=False)
    parser.add_argument("--mqtt-broker", type=str, default=MQTT_BROKER, help="MQTT broker address")
    parser.add_argument("--mqtt-port", type=int, default=MQTT_PORT, help="MQTT broker port")
    parser.add_argument("--mqtt-topic", type=str, default=MQTT_TOPIC, help="MQTT topic to publish")
  
def get_utc_millis(measure:ElectricCurrentMeasure):
    return round(measure.timestamp.timestamp() * 1000)

def run(args):
    # MQTT 클라이언트 설정
    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.connect(args.mqtt_broker, args.mqtt_port)
    mqtt_client.loop_start()

    readers = [read_measures_from_csv(csv_file) for csv_file in args.files]
    measures = heapq.merge(*readers, key=lambda m: m.timestamp)
    measures = compact(measures)
    if args.sync:
        measures = synchronize_time(measures, utc_millis=get_utc_millis)
    else:
        measures = emulate_measure(measures, args.interval)

    count = 0
    started = time.time()
    # 데이터베이스에 연결하고, 전류 로그 테이블이 없으면 생성한다.
    for measure in measures:
        # logger.info(f"ts={measure.timestamp} ampere={measure.ampere:.3}")
        
        # MQTT로 전류 값 publish
        payload = str(measure.ampere)
        mqtt_client.publish(args.mqtt_topic, payload)
        count += 1
        if count % 1000 == 0:
            millis_per_msg = int(((time.time() - started) * 1000) / count)
            logger.info(f"published {count} messages {millis_per_msg} millis/msg")
    
    mqtt_client.loop_stop()
    mqtt_client.disconnect()

def emulate_measure(measures:Iterable[ElectricCurrentMeasure], interval:int) -> Generator[ElectricCurrentMeasure,None,None]:
    for measure in measures:
        started = time.time()
        yield measure
        wait_time = interval - (time.time() - started)*1000
        if wait_time > 1:
            time.sleep((wait_time) / 1000)
            
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