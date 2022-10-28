def is_drug_in_pub(drug, df, col):
    """Filters the drugs dataset for one drug"""

    res = df[df[col].str.contains(drug, case=False)]
    return res