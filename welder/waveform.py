from __future__ import annotations

from typing import Iterable, Iterator, Generator

import numpy as np
from scipy.signal import find_peaks, peak_widths
from dateutil.parser import parse

from mdtpy.model import Record
from welder import ElectricCurrentMeasure


def recognize_waveform(measures:Iterable[Record]) -> list[ElectricCurrentMeasure]:
    waveform = []
    phase = 'WAIT_1'
    for record in measures:
        state = int(record['State'])
        if phase == 'WAIT_1':
            if state == 1:
                waveform = [record]
                phase = 'WAIT_3'
        elif phase == 'WAIT_3':
            waveform.append(record)
            if state == 3:
                phase = 'WAIT_1'
    if phase == 'WAIT_1' and int(waveform[-1]['State']) == 3:
        return [ElectricCurrentMeasure(timestamp=parse(rec['Time']),
                                       ampere=float(rec['Ampere']),
                                       state=int(rec['State'])) for rec in waveform]
    else:
        raise ValueError('failed to recognize a waveform: cannot find a end marker record')
  
# 기준 패턴 데이터
BASE_PATTERNS = [
    (5.10753,9.12962,10.2315,8.1372),
    (5.08334,9.17975,10.0207,8.05877),
    (6.91321,9.68777,9.89942,5.3125),
    (5.86139,6.90757,9.77626,7.80729),
    (5.8189,7.18364,9.63604,7.71469),
    (5.79495,6.96342,9.75177,7.83492),
    (5.08086,9.24413,10.302,5.13871),
    (5.2174,9.31485,10.3445,5.11919),
    (5.06745,9.22025,10.1932,5.10037),
    (5.83547,6.94665,9.77594,8.64057)
]
  
def inspect_waveform(waveform:list[ElectricCurrentMeasure]) -> bool:
    # # 상태와 데이터 출력
    # for measure in waveform:
    #     print(measure)
    
    # state 2 구간의 데이터 추출
    state2_data = []
    for measure in waveform:
        if measure.state == 2:
            state2_data.append(measure.ampere)
    
    if not state2_data:
        return False
    
    # state 2 구간에서 피크 찾기
    state2_amperes = np.array(state2_data)
    peaks, _ = find_peaks(state2_amperes, height=8.0, distance=1)
    
    if len(peaks) == 0:
        return False
    
    # 가장 큰 피크 찾기 (9.0A 이상)
    max_peak_idx = np.argmax(state2_amperes)
    max_peak_value = state2_amperes[max_peak_idx]
    
    if max_peak_value < 9.0:
        return False
    
    # 피크의 너비 계산
    widths = peak_widths(state2_amperes, [max_peak_idx])[0]
    
    # 기본 품질 판단 (너비 기준)
    WIDTH_THRESHOLD = 2.0
    quality = 10 if widths[0] >= WIDTH_THRESHOLD else 11
    
    # 현재 패턴 추출 (4개 포인트)
    pattern_start = max(0, max_peak_idx - 1)
    pattern_end = min(len(state2_amperes), max_peak_idx + 3)
    if pattern_end - pattern_start >= 4:
        current_pattern = state2_amperes[pattern_start:pattern_start + 4]
        
        # 모든 기준 패턴과 비교하여 가장 작은 DTW 거리 찾기
        min_dtw_dist = float('inf')
        for base_pattern in BASE_PATTERNS:
            base_array = np.array(base_pattern)
            
            # DTW 거리 계산
            n, m = len(current_pattern), len(base_array)
            dtw_matrix = np.full((n+1, m+1), np.inf)
            dtw_matrix[0, 0] = 0
            
            for i in range(1, n+1):
                for j in range(1, m+1):
                    cost = abs(current_pattern[i-1] - base_array[j-1])
                    dtw_matrix[i, j] = cost + min(dtw_matrix[i-1, j],
                                                dtw_matrix[i, j-1],
                                                dtw_matrix[i-1, j-1])
            
            min_dtw_dist = min(min_dtw_dist, dtw_matrix[n, m])
        
        # DTW 기반 품질 판단
        DTW_THRESHOLD = 2.0
        dtw_quality = 10 if min_dtw_dist <= DTW_THRESHOLD else 11
        
        # print(f"\n분석 결과:")
        # print(f"현재 패턴: {', '.join(f'{x:.2f}' for x in current_pattern)}")
        # print(f"최대 피크: {max_peak_value:.2f}")
        # print(f"피크 너비: {widths[0]:.2f}")
        # print(f"최소 DTW 거리: {min_dtw_dist:.2f}")
        # print(f"너비 기준 품질: {quality}")
        # print(f"DTW 기준 품질: {dtw_quality}")
        
        # 최종 품질 판단 (둘 다 10이어야 True)
        result = quality == 10 and dtw_quality == 10
        return result
    return False