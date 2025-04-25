import pandas as pd
from rapidfuzz import fuzz
from tqdm import tqdm

from domains.customer.Ingester import filter_sf_data
from domains.customer.Reader import read_data


def compare_distinct_sd_series_focus_sf(series1, series2, threshold=80, method=fuzz.ratio):
    """
    Compares distinct values between two pandas Series using fuzzy matching.

    Args:
        series1 (pd.Series): The first pandas Series.
        series2 (pd.Series): The second pandas Series.
        threshold (int): The minimum similarity score (0-100) to consider a match.
        method (callable): The fuzzy matching function from rapidfuzz (e.g., fuzz.ratio).

    Returns:
        pd.DataFrame: A DataFrame containing the distinct values from both series
                      that have a similarity score above the threshold, along with
                      their similarity score.
    """
    distinct_series1 = series1.astype(str).str.lower().str.strip().unique()
    print("focus series1 "+ str(len(distinct_series1)))
    distinct_series2 = series2.astype(str).str.lower().str.strip().unique()
    print("sf series2 "+ str(len(distinct_series2)))

    comparison_data = []

    for val1 in tqdm(distinct_series1, desc=f"Comparing distinct values from '{series1.name}'"):
        for val2 in distinct_series2:

            phrase_to_check1 = 'public schools'
            phrase_to_check2 = 'school district'
            phrase_to_check3 = 'consolidated school district'
            phrase_to_check4 = 'unified school district'
            phrase_to_check5 = 'township school district'
            modified_str1 = None
            modified_str2 = None
            if phrase_to_check5 in val1.lower() and phrase_to_check5 in val2.lower():
                modified_str1 = val1.lower().replace(phrase_to_check5.lower(), '').strip()
                modified_str2 = val2.lower().replace(phrase_to_check5.lower(), '').strip()
            elif phrase_to_check3 in val1.lower() and phrase_to_check3 in val2.lower():
                modified_str1 = val1.lower().replace(phrase_to_check3.lower(), '').strip()
                modified_str2 = val2.lower().replace(phrase_to_check3.lower(), '').strip()
            elif phrase_to_check4 in val1.lower() and phrase_to_check4 in val2.lower():
                modified_str1 = val1.lower().replace(phrase_to_check4.lower(), '').strip()
                modified_str2 = val2.lower().replace(phrase_to_check4.lower(), '').strip()
            elif phrase_to_check1 in val1.lower() and phrase_to_check1 in val2.lower():
                modified_str1 = val1.lower().replace(phrase_to_check1.lower(), '').strip()
                modified_str2 = val2.lower().replace(phrase_to_check1.lower(), '').strip()
            elif phrase_to_check2 in val1.lower() and phrase_to_check2 in val2.lower():
                modified_str1 = val1.lower().replace(phrase_to_check2.lower(), '').strip()
                modified_str2 = val2.lower().replace(phrase_to_check2.lower(), '').strip()

            similarity_score = method(modified_str1, modified_str2)
            if similarity_score >= threshold:
                comparison_data.append({
                    f"{series1.name}": val1,
                    f"{series2.name}": val2,
                    "SF_FOCUS_sd_similarity_score": similarity_score
                })

    return pd.DataFrame(comparison_data)
