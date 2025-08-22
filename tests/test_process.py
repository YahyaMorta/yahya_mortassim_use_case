
import json
from io import StringIO
from unittest.mock import patch

import pandas as pd

from src.process import process_data, write_results


def test_process_data():
    # Mock input data
    drugs_data = pd.DataFrame({"drug": ["DrugA", "DrugB"]})
    pub_data = pd.DataFrame({
        "title": ["Title mentioning DrugA", "Another title"],
        "journal": ["Journal1", "Journal2"],
        "date": ["2023-01-01", "2023-01-02"],
    })
    trials_data = pd.DataFrame({
        "scientific_title": ["A trial for DrugA", "A trial for DrugC"],
        "journal": ["Journal1", "Journal3"],
        "date": ["2023-02-01", "2023-02-02"],
    })

    file_data = {
        "drugs": drugs_data.to_json(orient="records"),
        "pub": pub_data.to_json(orient="records"),
        "trials": trials_data.to_json(orient="records"),
    }

    result = process_data.function(file_data)

    assert len(result) == 2
    assert result[0]["drug"] == "DrugA"
    assert len(result[0]["journals"]) == 1
    assert result[0]["journals"][0]["name"] == "Journal1"
    assert len(result[0]["journals"][0]["mentions"]) == 2  # Both pub and trial

    assert result[1]["drug"] == "DrugB"
    assert len(result[1]["journals"]) == 0


@patch("src.process.Variable")
def test_write_results(mock_variable, tmp_path):
    # Mock Variable.get to return a temporary path
    output_path = tmp_path / "output.json"
    mock_variable.get.return_value = str(output_path)

    processed_data = [
        {"drug": "DrugA", "journals": [{"name": "Journal1", "mentions": []}]}
    ]

    write_results.function(processed_data)

    with open(output_path, "r", encoding="utf-8") as f:
        lines = f.readlines()
        assert len(lines) == 1
        data = json.loads(lines[0])
        assert data["drug"] == "DrugA"
