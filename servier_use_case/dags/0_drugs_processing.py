from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from collections import defaultdict
from datetime import datetime
import json

PATH = "/opt/airflow/dags/"
drugs_df = pd.read_csv(f"{PATH}input_files/drugs.csv")

pubs_df = pd.read_csv(f"{PATH}input_files/pubmed.csv")

trials_df = pd.read_csv(f"{PATH}input_files/clinical_trials.csv")


def is_drug_in_pub(drug, df, col):
    res = df[df[col].str.contains(drug, case=False)]
    return res


def group_by_journal(df, title_col, mention_type):

    pub_dict = (
        df.groupby('journal')
        .apply(lambda x: [{f'{mention_type}_title': i, 'date': j} for i, j in zip(x[title_col], x['date'])])
        .to_dict()
    )
    pubs_journals = [{"name": k, "mentions": v} for k, v in pub_dict.items() if v]
    return pubs_journals


def get_journals(drug, **kwargs):
    ti = kwargs['ti']
    df_pubs, df_trials = ti.xcom_pull(
        key=None, task_ids=[f"looking_for_{drug}_in_pubs", f"looking_for_{drug}_in_trials"]
    )

    pubs_journals = group_by_journal(df_pubs, 'title', 'pubmed')
    trials_journals = group_by_journal(df_trials, 'scientific_title', 'trial')

    merged_lists = pubs_journals + trials_journals

    tmp = defaultdict(list)
    for item in merged_lists:
        tmp[item['name']].append(item['mentions'])
    parsed_list = [{'name': k, 'mentions': v} for k, v in tmp.items()]

    return parsed_list


def create_json_file():
    output_file_name = f"{PATH}output_files/drugs_file_{datetime.utcnow().timestamp()}.ndjson"
    open(output_file_name, "w")
    return output_file_name


def write_to_json(drug, **kwargs):
    ti = kwargs['ti']
    list_of_journals = ti.xcom_pull(key=None, task_ids=f"looking_for_journals_of_{drug}")
    output_file_name = ti.xcom_pull(key=None, task_ids="creating_json_file")
    with open(output_file_name, "a") as f:
        json.dump({"drug": drug, "journals": list_of_journals}, f)
        f.write('\n')


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
