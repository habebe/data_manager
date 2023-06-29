import pandas as pd

def null_indices(data: pd.core.frame.DataFrame):
    return data.isnull().any(axis=1)

def nulls(data: pd.core.frame.DataFrame):
    return data.loc[null_indices(data)]