from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import logging

import psycopg2
from psycopg2.extensions import connection

from mdtpy.client import MDTInstance
from mdtpy.model import TimeseriesSubmodelServiceCollection, Segment
from welder import recognize_waveform, inspect_waveform, ElectricCurrentMeasure, NozzleProductionAudit


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('inspect_nozzle')


def extract_last_waveform(instance:MDTInstance) -> list[ElectricCurrentMeasure]:
    timeseries:TimeseriesSubmodelServiceCollection = instance.timeseries['WelderAmpereLog']
    
    # WelderAmpereLog 타임시리즈에서 Tail 세그먼트의 레코드를 가져온다.
    tail:Segment = timeseries.segment('Tail')
    logger.debug(f'TailSegment: start={tail.StartTime}, end={tail.EndTime}, count={tail.RecordCount}')
    
    # 가장 최근의 waveform을 추출한다.
    return recognize_waveform(tail.records)


def process_nozzle_waveform(welder:MDTInstance, waveform:list[ElectricCurrentMeasure]) -> NozzleProductionAudit:
    # Waveform을 검사하여 불량 파형인지 확인한다.
    is_defect = inspect_waveform(waveform)
    
    # MDT 인스턴스에서 파라미터를 읽어서 노즐 생산 로그를 생성한다.
    parameters = welder.parameters
    logEntry = NozzleProductionAudit(
        timestamp=waveform[-1].timestamp,
        quantity_produced=int(parameters['QuantityProduced'].value),
        avg_processing_time=float(parameters['AvgProcessingTime'].value),
        avg_waiting_time=float(parameters['AvgWaitingTime'].value),
        defect_volume=int(parameters['DefectVolume'].value),
        avg_defect_rate=float(parameters['AvgDefectRate'].value),
        defect_estimation=is_defect
    )
    
    if is_defect:
        # 불량 파형일 경우 파라미터를 업데이트 한다.
        logEntry.defect_volume += 1
        logEntry.avg_defect_rate = logEntry.defect_volume / logEntry.quantity_produced
        parameters['DefectVolume'] = f'{logEntry.defect_volume}'
        parameters['AvgDefectRate'] = f'{logEntry.avg_defect_rate:.3f}'
    
    defect_result = 'Defect' if is_defect else 'Good'
    logger.info(f'nozzle: status={defect_result}, {logEntry.defect_volume}/{logEntry.quantity_produced} = {logEntry.avg_defect_rate:.3f}')
    return logEntry
            

def log_nozzle_waveform(conn:connection, logEntry:NozzleProductionAudit) -> None:
    try:
        cur = conn.cursor()
        
        # Insert the log entry into nozzle_productions table
        cur.execute("""
            INSERT INTO nozzle_productions (
                timestamp,
                quantity_produced,
                avg_processing_time,
                avg_waiting_time, 
                defect_volume,
                avg_defect_rate
            ) VALUES (
                %s, %s, %s, %s, %s, %s
            );
        """, (
            logEntry.timestamp,
            logEntry.quantity_produced,
            logEntry.avg_processing_time,
            logEntry.avg_waiting_time,
            logEntry.defect_volume,
            logEntry.avg_defect_rate
        ))
        conn.commit()
        
    except Exception as e:
        logger.error(f"Error logging nozzle data: {e}")
        raise
    finally:
        if 'cur' in locals():
            cur.close()
