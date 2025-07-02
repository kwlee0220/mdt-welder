from __future__ import annotations

import sys
import csv
import argparse
from datetime import datetime
import logging
from typing import Generator

from welder import NozzleProductionAudit

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('read_nozzle_audit')

def define_args(parser):
    parser.add_argument("input", nargs='?', default="output.csv", help="Input CSV file path (default: output.csv)")
    parser.add_argument("--delimiter", default=",", help="CSV delimiter character")

def read_audit_csv(file_path: str, delimiter: str = ",") -> Generator[NozzleProductionAudit, None, None]:
    """
    CSV 파일을 읽어서 NozzleProductionAudit 객체를 생성하는 제너레이터
    
    Args:
        file_path: CSV 파일 경로
        delimiter: CSV 구분자 (기본값: 쉼표)
        
    Yields:
        NozzleProductionAudit: CSV의 각 행을 변환한 객체
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        for row in reader:
            try:
                # CSV의 각 필드를 NozzleProductionAudit 객체의 필드에 맞게 변환
                audit_data = {
                    'Timestamp': datetime.fromisoformat(row['timestamp']),
                    'QuantityProduced': int(row['quantity_produced']),
                    'AvgProcessingTime': float(row['avg_processing_time'])/1000,
                    'AvgWaitingTime': float(row['avg_waiting_time'])/1000,
                    'DefectVolume': int(row['defect_volume']),
                    'AvgDefectRate': float(row['avg_defect_rate'])
                }
                audit = NozzleProductionAudit(**audit_data)
                yield audit
            except (KeyError, ValueError) as e:
                logger.error(f"Error processing row: {row}, Error: {e}")
                continue

def run(args):
    count = 0
    for audit in read_audit_csv(args.input, args.delimiter):
        print(audit)
        count += 1
    logger.info(f"Processed {count} records from {args.input}")

def main():
    parser = argparse.ArgumentParser(description="Read nozzle production audit records from CSV")
    define_args(parser)
    args = parser.parse_args()
    run(args)

if __name__ == '__main__':
    main() 