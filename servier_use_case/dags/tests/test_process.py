import pandas as pd
from io import StringIO

from dags.process import read_csv, is_drug_in_pub




mock_pubmed_csv ="""id,title,date,journal\n
11,epinephrine treatment,01/03/2020,The journal of allergy and clinical immunology. In practice\n
12,paracetamol treatment,01/03/2021,The journal of medicine\n
13,paracetamol treatment 2,01/03/2021,The journal of medicine 2"""

mock_pubmed_df = read_csv(StringIO(mock_pubmed_csv))


def test_is_drug_in_pub_ok():

    expected_df = read_csv(StringIO("""id,title,date,journal
    11,epinephrine treatment,01/03/2020,The journal of allergy and clinical immunology. In practice"""))
    res_df = is_drug_in_pub("EPINEPHRINE", mock_pubmed_df, "title")

    assert res_df.equals(expected_df)