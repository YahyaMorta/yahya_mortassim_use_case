from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from dags import constants
from dags.process import read_csv, is_drug_in_pub, group_by_journal, get_journals, create_json_file, write_to_json


drugs_df = read_csv(f"{constants.PATH}input_files/drugs.csv")

pubs_df = read_csv(f"{constants.PATH}input_files/pubmed.csv")

trials_df = read_csv(f"{constants.PATH}input_files/clinical_trials.csv")


with DAG(dag_id='servier_use_case_dag', start_date=datetime(2022, 10, 25), schedule_interval=None) as dag:
    create_json_file = PythonOperator(
        task_id="creating_json_file",
        python_callable=create_json_file,
        provide_context=True,
    )
    for drug_name in drugs_df['drug'].to_list():
        start_process_drug = EmptyOperator(task_id=f"starting_pipeline_for_{drug_name}")
        drug_pub = PythonOperator(
            task_id=f"looking_for_{drug_name}_in_pubs",
            python_callable=is_drug_in_pub,
            op_kwargs=dict(drug=drug_name, df=pubs_df, col='title'),
            provide_context=True,
        )
        drug_trial = PythonOperator(
            task_id=f"looking_for_{drug_name}_in_trials",
            python_callable=is_drug_in_pub,
            op_kwargs=dict(drug=drug_name, df=trials_df, col='scientific_title'),
            provide_context=True,
        )
        journals_list = PythonOperator(
            task_id=f"looking_for_journals_of_{drug_name}",
            op_kwargs=dict(drug=drug_name),
            python_callable=get_journals,
            provide_context=True,
        )

        write_to_json_file = PythonOperator(
            task_id=f"writing_to_json_for_drug_{drug_name}",
            op_kwargs=dict(drug=drug_name),
            python_callable=write_to_json,
            provide_context=True,
        )

        start_process_drug >> (drug_pub, drug_trial) >> journals_list
        journals_list >> write_to_json_file
        create_json_file >> write_to_json_file
