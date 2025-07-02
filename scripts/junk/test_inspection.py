from __future__ import annotations

from dataclasses import replace

import heapq
import argparse

from pyutils.utils import synchronize_time2
from welder import ElectricCurrentMeasure, recognize_work, read_measures_from_csv


def define_args(parser):
  parser.add_argument("files", nargs='+', help="CSV files to be merged")
  parser.add_argument("--sync", action='store_true', default=False)
  
def get_time(measure:ElectricCurrentMeasure) -> int:
    return measure.timestamp.timestamp()

def process_waveform(waveform:list[ElectricCurrentMeasure]) -> bool:
    print(len(waveform))


def run(args):
    readers = [read_measures_from_csv(csv_file) for csv_file in args.files]
    measures = heapq.merge(*readers, key=lambda m: m.timestamp)

    if args.sync:
        measures = synchronize_time2(measures, get_time)
  
    last_state = 0
    last_stopped_ts = None
    last_started_ts = None
    waveform:list[ElectricCurrentMeasure] = []

    for measure in measures:
        state = recognize_work(measure.timestamp, measure.ampere)
        if state == -1:
            continue 
        
        measure = replace(measure, state=state)

        if last_state == 0 and state == 1:
            # Water nozzle의 생성이 시작되었을 때
            waveform = [measure]
            last_stopped_ts = measure.timestamp
        elif last_state == 2 and state == 3:
            # Water nozzle의 생성이 완료되었을 때
            waveform.append(measure)
            
            if last_started_ts is not None and last_stopped_ts is not None:
                process_waveform(waveform)
                waveform = []
            last_started_ts = measure.timestamp
        elif state != 0:
            waveform.append(measure)
            
        last_state = state
    
  
def main():
  parser = argparse.ArgumentParser(description="Merge multiple CSV files")
  define_args(parser)
  
  args = parser.parse_args()
  run(args)

if __name__ == '__main__':
    main()