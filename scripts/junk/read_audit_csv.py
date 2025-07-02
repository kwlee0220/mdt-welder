import csv
from datetime import datetime
from mdtpy.client.utils import iso8601_to_datetime
from welder import NozzleProductionAudit

def read_audit_csv(file_path: str) -> list[NozzleProductionAudit]:
    audits = []
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # CSV의 각 필드를 NozzleProductionAudit 객체의 필드에 맞게 변환
            audit_data = {
                'Timestamp': iso8601_to_datetime(row['timestamp']),
                'QuantityProduced': int(row['quantity_produced']),
                'AvgProcessingTime': row['avg_processing_time'],
                'AvgWaitingTime': row['avg_waiting_time'],
                'DefectVolume': int(row['defect_volume']),
                'AvgDefectRate': row['avg_defect_rate']
            }
            audit = NozzleProductionAudit(**audit_data)
            audits.append(audit)
    return audits

def main():
    file_path = 'output.csv'
    try:
        audits = read_audit_csv(file_path)
        print(len(audits))
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main() 