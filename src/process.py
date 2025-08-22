from io import StringIO
import json
from typing import List, Dict

import pandas as pd
from airflow.decorators import task
from airflow.models import Variable

from .helpers import (
    word_pattern,
    read_csv_safe,
    format_dates,
    normalize_text_cols,
)


@task
def read_files() -> Dict[str, str]:
    """
    Read source CSVs and return compact JSON strings (good for XCom).
    """
    drugs_fp = Variable.get("drugs_file_path", "/opt/airflow/dags/input_files/drugs.csv")
    pub_fp = Variable.get("pubmed_file_path", "/opt/airflow/dags/input_files/pubmed.csv")
    trials_fp = Variable.get(
        "clinical_trials_file_path", "/opt/airflow/dags/input_files/clinical_trials.csv"
    )

    drugs = read_csv_safe(drugs_fp)
    pub = read_csv_safe(pub_fp)
    trials = read_csv_safe(trials_fp)

    return {
        "drugs": drugs.to_json(orient="records"),
        "pub": pub.to_json(orient="records"),
        "trials": trials.to_json(orient="records"),
    }


@task
def process_data(file_data: Dict[str, str]) -> List[Dict]:
    """
    Per drug:
      journals: [
        { name, mentions: [pubmed_list?, clinical_trial_list?] }
      ]
    'mentions' contains only non-empty lists. Dates are DD/MM/YYYY.
    """
    drugs = pd.read_json(StringIO(file_data["drugs"]))
    pub = pd.read_json(StringIO(file_data["pub"]))
    trials = pd.read_json(StringIO(file_data["trials"]))

    # normalize dates & text
    pub = normalize_text_cols(format_dates(pub), ["title", "journal"])
    trials = normalize_text_cols(format_dates(trials), ["scientific_title", "journal"])

    # ensure expected columns exist to avoid KeyErrors
    for df, cols in ((pub, ["title", "journal", "date"]), (trials, ["scientific_title", "journal", "date"])):
        for c in cols:
            if c not in df.columns:
                df[c] = None

    results: List[Dict] = []
    for drug in drugs.get("drug", pd.Series(dtype=str)).astype(str):
        pat = word_pattern(drug)

        # rows that mention the drug
        pubs_hit = pub.loc[pub["title"].str.contains(pat, na=False), ["title", "date", "journal"]]
        trials_hit = trials.loc[
            trials["scientific_title"].str.contains(pat, na=False), ["scientific_title", "date", "journal"]
        ]

        # all journals appearing in either source
        journals_seen = set(pubs_hit["journal"].dropna().astype(str)) | set(
            trials_hit["journal"].dropna().astype(str)
        )

        journals = []
        for j in sorted(journals_seen):
            pub_list = (
                pubs_hit.loc[pubs_hit["journal"] == j, ["title", "date"]]
                .rename(columns={"title": "pubmed_title"})
                .to_dict("records")
            )
            trial_list = (
                trials_hit.loc[trials_hit["journal"] == j, ["scientific_title", "date"]]
                .rename(columns={"scientific_title": "clinical_trial_title"})
                .to_dict("records")
            )

            mentions = []
            if pub_list:
                mentions.append(pub_list)
            if trial_list:
                mentions.append(trial_list)

            if mentions:  # keep only journals with at least one list
                journals.append({"name": j, "mentions": mentions})

        results.append({"drug": drug, "journals": journals})

    return results


@task
def write_results(processed_data: List[Dict]) -> None:
    """
    Write newline-delimited JSON (JSONL), UTF-8 with real characters.
    """
    out_path = Variable.get("output_file_path", "/opt/airflow/dags/output_files/drugs_file.json")
    with open(out_path, "w", encoding="utf-8") as f:
        for row in processed_data:
            json.dump(row, f, ensure_ascii=False)
            f.write("\n")
