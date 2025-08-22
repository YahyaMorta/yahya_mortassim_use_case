
import pandas as pd
from src.helpers import (
    word_pattern,
    read_csv_safe,
    format_dates,
    fix_escaped_bytes,
    normalize_text_cols,
)


def test_word_pattern():
    pattern = word_pattern("test")
    assert pattern.match("test")
    assert pattern.search("a test string")
    assert not pattern.search("teststring")
    assert pattern.search("Test")


def test_read_csv_safe(tmp_path):
    # Test with utf-8
    utf8_content = "col1,col2\nval1,val2"
    utf8_path = tmp_path / "utf8.csv"
    utf8_path.write_text(utf8_content, encoding="utf-8")
    df = read_csv_safe(str(utf8_path))
    assert df.shape == (1, 2)
    assert df.columns.tolist() == ["col1", "col2"]

    # Test with latin-1
    latin1_content = "col1,col2\né,à"
    latin1_path = tmp_path / "latin1.csv"
    latin1_path.write_text(latin1_content, encoding="latin-1")
    df = read_csv_safe(str(latin1_path))
    assert df.shape == (1, 2)
    assert df.iloc[0, 0] == "é"


def test_format_dates():
    df = pd.DataFrame({"date": ["2023-01-01", "2023-01-02", None]})
    formatted_df = format_dates(df)
    assert formatted_df["date"].tolist() == ["01/01/2023", "02/01/2023", "NaT"]


def test_fix_escaped_bytes():
    assert fix_escaped_bytes("caf\xc3\xa9") == "café"
    assert fix_escaped_bytes("not escaped") == "not escaped"
    assert fix_escaped_bytes(123) == 123


def test_normalize_text_cols():
    df = pd.DataFrame({
        "a": ["caf\xc3\xa9", "test"],
        "b": [1, 2],
        "c": ["another \xc3\xa9", "string"]
    })
    normalized_df = normalize_text_cols(df, ["a", "c"])
    assert normalized_df["a"].tolist() == ["café", "test"]
    assert normalized_df["c"].tolist() == ["another é", "string"]
    assert normalized_df["b"].tolist() == [1, 2]
