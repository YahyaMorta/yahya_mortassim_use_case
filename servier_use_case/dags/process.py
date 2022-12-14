import json
from datetime import datetime
from collections import defaultdict
from dags import constants
import pandas as pd


def read_csv(filepath):
    """Reads csv file as pandas dataframe"""
    return pd.read_csv(filepath, sep=",")


def is_drug_in_pub(drug, df, col):
    """Filters the drugs dataset for one drug"""

    res = df[df[col].str.contains(drug, case=False)]
    return res


def group_by_journal(df, title_col, mention_type):
    """Groups by journal name and formats the result into a list of dicts"""
    pub_dict = (
        df.groupby("journal")
        .apply(
            lambda x: [
                {f"{mention_type}_title": i, "date": j}
                for i, j in zip(x[title_col], x["date"])
            ]
        )
        .to_dict()
    )
    pubs_journals = [{"name": k, "mentions": v} for k, v in pub_dict.items() if v]
    return pubs_journals


def get_journals(drug, **kwargs):
    """merges the results of processing pubmeds and clinical trials"""
    ti = kwargs["ti"]
    df_pubs, df_trials = ti.xcom_pull(
        key=None,
        task_ids=[f"looking_for_{drug}_in_pubs", f"looking_for_{drug}_in_trials"],
    )

    pubs_journals = group_by_journal(df_pubs, "title", "pubmed")
    trials_journals = group_by_journal(df_trials, "scientific_title", "trial")

    merged_lists = pubs_journals + trials_journals

    tmp = defaultdict(list)
    for item in merged_lists:
        tmp[item["name"]].append(item["mentions"])
    parsed_list = [{"name": k, "mentions": v} for k, v in tmp.items()]

    return parsed_list


def create_json_file():
    """Creates the output json file with a timestamp to avoid overriding the same file"""
    output_file_name = (
        f"{constants.PATH}output_files/drugs_file_{datetime.utcnow().timestamp()}.json"
    )
    open(output_file_name, "w")
    return output_file_name


def write_to_json(drug, **kwargs):
    """Writes the result in the new line delimited JSON file"""
    ti = kwargs["ti"]
    list_of_journals = ti.xcom_pull(
        key=None, task_ids=f"looking_for_journals_of_{drug}"
    )
    output_file_name = ti.xcom_pull(key=None, task_ids="creating_json_file")
    with open(output_file_name, "a") as f:
        json.dump({"drug": drug, "journals": list_of_journals}, f)
        f.write("\n")
