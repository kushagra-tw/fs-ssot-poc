import pandas as pd
import re  # Regular expression library


def normalize_dataframe_columns(df, cols_to_normalize):
    """
    Normalizes specified columns in a Pandas DataFrame and adds new columns
    with the suffix '_normalized'.

    Normalization steps include:
    1. Convert to uppercase (or lowercase if preferred).
    2. Remove specific punctuation ('.', ',', '\'').
    3. Replace hyphens '-' and underscores '_' with spaces.
    4. Trim leading/trailing whitespace.
    5. Collapse multiple internal spaces into a single space.

    Args:
        df (pd.DataFrame): The input DataFrame.
        cols_to_normalize (list): A list of column names (strings)
                                  to be normalized.

    Returns:
        pd.DataFrame: DataFrame with new columns added for each normalized
                      column (suffixed with '_normalized').
                      Original columns are preserved.
    """

    df_normalized = df.copy()  # Work on a copy to avoid modifying the original df

    for col_name in cols_to_normalize:
        # Check if the column exists in the DataFrame
        if col_name not in df_normalized.columns:
            print(f"Warning: Column '{col_name}' not found in DataFrame. Skipping.")
            continue  # Skip to the next column name in the list

        new_col_name = f"{col_name}_standardized"

        # Make sure the column is treated as string, handle NaNs
        # Convert NaN to empty string '' BEFORE string operations
        df_normalized[new_col_name] = df_normalized[col_name].fillna('').astype(str)

        # --- Apply Normalization Steps ---
        # 1. Convert to uppercase (change to .str.lower() if you prefer lowercase)
        df_normalized[new_col_name] = df_normalized[new_col_name].str.lower()

        # 2. Remove specific punctuation: '.', ',', '\''
        df_normalized[new_col_name] = df_normalized[new_col_name].str.replace('.', '', regex=False)
        df_normalized[new_col_name] = df_normalized[new_col_name].str.replace(',', '', regex=False)
        df_normalized[new_col_name] = df_normalized[new_col_name].str.replace('\'', '', regex=False)

        # 3. Replace hyphens and underscores with spaces
        df_normalized[new_col_name] = df_normalized[new_col_name].str.replace('-', ' ', regex=False)
        df_normalized[new_col_name] = df_normalized[new_col_name].str.replace('_', ' ', regex=False)

        # 4. Trim leading/trailing whitespace
        df_normalized[new_col_name] = df_normalized[new_col_name].str.strip()

        # 5. Collapse multiple internal spaces into a single space
        df_normalized[new_col_name] = df_normalized[new_col_name].str.replace(r'\s+', ' ', regex=True)
        # --- End of Normalization Steps ---

    return df_normalized
