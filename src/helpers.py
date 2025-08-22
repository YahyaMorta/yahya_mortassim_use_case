import re
import pandas as pd

def word_pattern(s: str):
    return re.compile(rf"\b{re.escape(str(s))}\b", re.I)

def read_csv_safe(path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(path, encoding="utf-8")
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="latin1")

def format_dates(df: pd.DataFrame) -> pd.DataFrame:
    """If 'date' exists, convert to DD/MM/YYYY strings; invalid -> 'NaT' string."""
    if "date" not in df.columns:
        return df
    out = df.copy()
    ser = pd.to_datetime(out["date"], errors="coerce")
    ser = ser.dt.strftime("%d/%m/%Y")
    out["date"] = ser.fillna("NaT")
    return out

def fix_escaped_bytes(s):
    """
    Convert strings containing Python-style byte escapes (e.g. '\\xc3\\xa9')
    into proper UTF-8 text (e.g. 'é'). Leave non-strings unchanged.
    """
    if not isinstance(s, str):
        return s
    try:
        step1 = s.encode("latin1", "ignore").decode("unicode_escape")
        b = step1.encode("latin1", "ignore")
        return b.decode("utf-8", "ignore")
    except Exception:
        return s

def normalize_text_cols(df: pd.DataFrame, cols) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = out[c].map(fix_escaped_bytes)
    return out
