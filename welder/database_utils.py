from __future__ import annotations

import logging

import psycopg2
from psycopg2.extensions import connection

from .types import ElectricCurrentMeasure
from .types import NozzleProductionAudit

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def open_connection(connection_params:dict) -> connection:
    return psycopg2.connect(**connection_params)

def create_ampere_log_table_if_absent(conn:connection) -> None:
    """Initialize database table if it doesn't exist"""
    with conn.cursor() as cur:
        # Check if table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'welder_ampere_log'
            )
        """)
        table_exists: bool = cur.fetchone()[0]
        
        if not table_exists:
            cur.execute("""
                CREATE TABLE welder_ampere_log (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP NOT NULL,
                    ampere FLOAT NOT NULL
                )
            """)
            conn.commit()

def log_measure(conn:connection, measure: ElectricCurrentMeasure) -> None:
    """Log single ElectricCurrentMeasure data to PostgreSQL database"""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO welder_ampere_log (timestamp, ampere)
            VALUES (%s, %s)
        """, (measure.timestamp, measure.ampere))
    conn.commit()
        

def create_nozzle_production_audit_table(conn:connection) -> None:
    """
    Create a nozzle_productions table in PostgreSQL if it doesn't exist.
    
    Args:
        conn: psycopg2.extensions.connection
    """
    try:
        cur = conn.cursor()
        
        # Check if table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'nozzle_productions'
            );
        """)
        table_exists = cur.fetchone()[0]
        
        if not table_exists:
            # Create table
            cur.execute("""
                CREATE TABLE nozzle_productions (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP NOT NULL,
                    quantity_produced INTEGER NOT NULL,
                    avg_processing_time BIGINT NOT NULL,
                    avg_waiting_time BIGINT NOT NULL,
                    defect_volume INTEGER NOT NULL,
                    avg_defect_rate REAL NOT NULL
                );
            """)
            conn.commit()
            logger.info("Table 'nozzle_productions' created successfully")
            
    except Exception as e:
        logger.error(f"Error creating table: {e}")
        raise
    finally:
        if 'cur' in locals():
            cur.close()

def audit_nozzle_production(conn:connection, audit:NozzleProductionAudit) -> int:
    """
    Insert a NozzleProductionAudit record into the nozzle_productions table.
    
    Args:
        conn: psycopg2.extensions.connection
        audit: NozzleProductionAudit object containing production data
    """
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO nozzle_productions (
                    timestamp, quantity_produced, avg_processing_time, 
                    avg_waiting_time, defect_volume, avg_defect_rate, 
                    defect_estimated
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                audit.timestamp, 
                audit.quantity_produced, 
                audit.avg_processing_time,
                audit.avg_waiting_time, 
                audit.defect_volume, 
                audit.avg_defect_rate, 
                audit.defect_estimation
            ))
            record_id = cur.fetchone()[0]
            conn.commit()
            return record_id
    except Exception as e:
        logger.error(f"Error inserting nozzle production record: {e}")
        conn.rollback()
        raise