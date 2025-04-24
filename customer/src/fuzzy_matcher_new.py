import pandas as pd
from rapidfuzz import process, fuzz

def find_best_fuzzy_match(left_name, right_names, threshold=75):
    best_match, score, _ = process.extractOne(left_name, right_names, scorer=fuzz.ratio)
    if score >= threshold:
        return best_match, score
    return None

def fuzzy_join_with_apply(df_left, df_right, left_on, right_on, threshold=75):
    right_names = df_right[right_on].astype(str).tolist()
    right_df_indexed = df_right.set_index(right_on) # Index right df for faster lookup

    def apply_fuzzy_match(row_left):
        match_info = find_best_fuzzy_match(str(row_left[left_on]), right_names, threshold)
        if match_info:
            best_match, score = match_info
            try:
                matched_row_right = right_df_indexed.loc[best_match]
                # Concatenate as Series to maintain consistency
                return pd.concat([row_left, matched_row_right])
            except KeyError:
                # Handle cases where the best match might not be a unique index
                return pd.Series([pd.NA] * len(df_right.columns), index=df_right.columns)
        else:
            return pd.Series([pd.NA] * len(df_right.columns), index=df_right.columns)

    merged_series = df_left.apply(apply_fuzzy_match, axis=1)

    # Convert the Series of Series to a DataFrame
    df_merged = pd.DataFrame(merged_series.tolist(), index=df_left.index)

    # Rename the columns of the merged part
    right_cols = {col: f"{right_on}_{col}" for col in df_right.columns}
    df_merged = df_merged.rename(columns=right_cols)

    # Concatenate with the original left DataFrame
    df_final = pd.concat([df_left, df_merged], axis=1)

    # Filter out rows where no match was found (all right columns are NaN)
    right_cols_merged = [col for col in df_final.columns if right_on in col]
    df_final_filtered = df_final[~df_final[right_cols_merged].isnull().all(axis=1)].reset_index(drop=True)

    return df_final_filtered