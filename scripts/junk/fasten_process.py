from __future__ import annotations

from dataclasses import replace, astuple

import csv
import heapq
import argparse
from datetime import datetime, timedelta

from pyutils import iterables
from welder import ElectricCurrentMeasure, recognize_work, read_measures_from_csv


def define_args(parser):
  parser.add_argument("file", help="CSV files to be merged")

STEP_DELTA = timedelta(milliseconds=200)

def run(args):
    measures = read_measures_from_csv(args.file)
    
    measures = iterables.to_peekable(measures)
    start_ts = measures.peek().timestamp

    step = 0
    with open("fasten.csv", mode="w", encoding="utf-8") as file:
        writer = csv.writer(file)
        for measure in measures:
            new_ts = start_ts + (step * STEP_DELTA)
            measure = replace(measure, timestamp=new_ts)
            
            arr = list(astuple(measure))
            writer.writerow(arr)
            step += 1
    
  
def main():
  parser = argparse.ArgumentParser(description="Merge multiple CSV files")
  define_args(parser)
  
  args = parser.parse_args()
  run(args)

if __name__ == '__main__':
    main()