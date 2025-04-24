import pandas as pd
from rapidfuzz import fuzz

def add_similarity_score(df, col1, col2, new_col_name='similarity_score'):
    """
    Adds a column to the DataFrame containing the similarity score between two name columns,
    using rapidfuzz's fuzz.ratio().

    Args:
        df (pd.DataFrame): The DataFrame to modify.
        col1 (str): The name of the first column to compare.
        col2 (str): The name of the second column to compare.
        new_col_name (str): The name of the new column to add.

    Returns:
        pd.DataFrame: The modified DataFrame with the new similarity score column.
    """

    df[new_col_name] = df.apply(lambda row: fuzz.ratio(row[col1], row[col2]), axis=1)
    return df