from __future__ import annotations

import csv
import argparse
from datetime import datetime
from typing import Generator

import psycopg2
from psycopg2.extras import RealDictCursor


DATABASE_PARAMS = {
    'dbname': 'mdt',
    'user': 'mdt',
    'password': 'urc2004',
    'host': 'localhost',
    'port': '5432'
}


def define_args(parser):
    parser.add_argument("output", help="Output CSV file path")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")


def read_nozzle_productions(conn, start_date: str = None, end_date: str = None) -> Generator[dict, None, None]:
    query = """
        SELECT 
            timestamp,
            quantity_produced,
            avg_processing_time,
            avg_waiting_time,
            defect_volume,
            avg_defect_rate
        FROM nozzle_productions
    """
    params = []
    if start_date or end_date:
        conditions = []
        if start_date:
            conditions.append("timestamp >= %s")
            params.append(start_date)
        if end_date:
            conditions.append("timestamp <= %s")
            params.append(end_date)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY timestamp"

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params)
        for row in cur:
            yield row


def run(args):
    with psycopg2.connect(**DATABASE_PARAMS) as conn:
        records = read_nozzle_productions(conn, args.start, args.end)
        
        with open(args.output, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'timestamp',
                'quantity_produced',
                'avg_processing_time',
                'avg_waiting_time',
                'defect_volume',
                'avg_defect_rate'
            ])
            writer.writeheader()
            for record in records:
                writer.writerow(record)


def main():
    parser = argparse.ArgumentParser(description="Export nozzle production records to CSV")
    define_args(parser)
    args = parser.parse_args()
    run(args)


if __name__ == '__main__':
    main() 