from datetime import datetime
from airflow.decorators import dag
from src.process import read_files, process_data, write_results

@dag(
    dag_id="0_drugs_processing",
    start_date=datetime(2025, 8, 22),
    schedule_interval=None,
    catchup=False,
)
def drugs_processing_dag():
    """
    A DAG that processes drug data from various sources.
    """
    file_data = read_files()
    processed_data_result = process_data(file_data)
    write_results(processed_data_result)

dag = drugs_processing_dag()