from __future__ import annotations

from typing import Iterable, Iterator, Generator

import datetime
import random

from scipy.signal import find_peaks


# 상태 정의 (직접 상수 사용)
STATUS_UNKNOWN = -1
STATUS_INITIAL = 0  # 초기 상태
STATUS_START = 1    # 시작 상태
STATUS_MIDDLE = 2   # 중간 상태
STATUS_END = 3      # 종료 상태
VALUE_THRESHOLD = 9 # 값 임계치

# 전역 변수 선언
data_buffer = []  # 데이터 버퍼
current_status = STATUS_INITIAL  # 현재 상태
status_1_time = None  # 상태 1 시간
status_2_recorded = set()  # 상태 2가 기록된 타임스탬프
status_3_recorded = set()  # 상태 3이 기록된 타임스탬프
processed_timestamps = set()  # 처리된 타임스탬프
status_job_id = None  # 작업 ID
status_3_condition_met = False  # 상태 3 조건 충족 여부
status_initial_timestamp = []  # 초기 상태 타임스탬프

def recognize_work(timestamp:datetime, value:float) -> int:
  global data_buffer, current_status, status_1_time
  global status_2_recorded, status_3_recorded
  global status_job_id, status_3_condition_met, status_initial_timestamp

  # print(f"{timestamp} {ampere}")
  
  # 데이터 버퍼에 추가
  data_buffer.append((timestamp, value))

  # 버퍼 크기가 15보다 작으면 STATUS_UNKNOWN를 반환
  if len(data_buffer) < 15:
    return STATUS_UNKNOWN
  elif len(data_buffer) > 15:
    data_buffer.pop(0)
    
  # 이미 처리된 타임스탬프인 경우 현재 상태 리턴
  if timestamp in processed_timestamps:
    return current_status

    
  # 현재 상태에 따른 로직 처리
  if current_status == STATUS_INITIAL:
    if value < 6:
      # 상태 초기: 데이터 값이 6 미만인 경우
      status_initial_timestamp.append(timestamp)
      # print(f"상태: {STATUS_INITIAL} (초기 상태)")
      processed_timestamps.add(timestamp)
      return STATUS_INITIAL
    else:
      # 데이터 값이 6 이상인 경우
      status_job_id = datetime.datetime.now().strftime("10%Y%m%d%H%M%S") + str(random.randint(10000, 99999))
      # print(f"상태: {STATUS_START} (시작 상태)")
      # 초기 상태에서 대기한 시간 계산
      if status_initial_timestamp:
        waiting_time = max(status_initial_timestamp) - min(status_initial_timestamp)
        # print(f"대기 시간: {waiting_time}")
        status_initial_timestamp.clear()
      current_status = STATUS_START
      status_1_time = timestamp
      status_2_recorded.clear()
      processed_timestamps.add(timestamp)
      return STATUS_START

  elif current_status == STATUS_START:
    # 상태 시작: 현재 타임스탬프가 상태 1 시간과 다른 경우
    if timestamp != status_1_time:
      # 버퍼에서 x값(타임스탬프)과 y값(부동소수점 값) 추출
      x_values = [item[0] for item in data_buffer]
      y_values = [item[1] for item in data_buffer]
      
      # scipy.signal.find_peaks를 사용하여 피크 찾기
      peaks, _ = find_peaks(y_values, distance=2)
      
      # 피크가 2개 이상 있고, 마지막에서 두 번째 피크의 값이 마지막 피크보다 크며 임계값보다 큰 경우
      if len(peaks) >= 2 and y_values[peaks[-2]] > y_values[peaks[-1]] and y_values[peaks[-2]] > VALUE_THRESHOLD:
          # 마지막 피크 이후의 값들 중 5 이하인 값이 있는지 확인
          if any(y <= 5 for y in y_values[peaks[-1] + 1:]):
            status_3_condition_met = True  # 상태 3의 조건 충족
      
      # 상태 3의 조건이 충족되지 않은 경우
      if not status_3_condition_met:
          if timestamp not in status_2_recorded:
            # print(f"상태: {STATUS_MIDDLE} (중간 상태)")
            status_2_recorded.add(timestamp)
            processed_timestamps.add(timestamp)
            return STATUS_MIDDLE
      else:
        # 상태 3의 조건이 충족된 경우
        for i, y in enumerate(y_values[peaks[-1] + 1:], start=peaks[-1] + 1):
          ts = x_values[i]
          if y <= 5 and ts not in status_3_recorded:
            # print(f"상태: {STATUS_END} (종료 상태)")
            processing_time = ts - status_1_time
            # print(f"처리 시간: {processing_time}")
            status_3_recorded.add(ts)
            current_status = STATUS_INITIAL
            status_job_id = None
            status_3_condition_met = False
            processed_timestamps.add(timestamp)
            return STATUS_END
      
      processed_timestamps.add(timestamp)
      return current_status
  
  #현재 상태 리턴
  processed_timestamps.add(ts)
  return current_status