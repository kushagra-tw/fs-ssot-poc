import pandas as pd
from rapidfuzz import fuzz
from tqdm import tqdm

from domains.customer.Ingester import filter_sf_data
from domains.customer.Reader import read_data


def compare_distinct_series(series1, series2, threshold=80, method=fuzz.ratio):
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
    distinct_series1 = series1.astype(str).unique()
    print("series1 "+ str(len(distinct_series1)))
    distinct_series2 = series2.astype(str).unique()
    print("series2 "+ str(len(distinct_series2)))

    comparison_data = []

    for val1 in tqdm(distinct_series1, desc=f"Comparing distinct values from '{series1.name}'"):
        for val2 in distinct_series2:
            similarity_score = method(val1, val2)
            if similarity_score >= threshold:
                comparison_data.append({
                    f"{series1.name}": val1,
                    f"{series2.name}": val2,
                    "similarity_score": similarity_score
                })

    return pd.DataFrame(comparison_data)

# def link_dataframes_with_details_no_prefix(df1, df2, column1, column2, threshold=80, method=fuzz.ratio):
#     linked_records = []
#     for index1, row1 in tqdm(df1.iterrows(), total=len(df1), desc="Processing df1"):
#         string1 = str(row1[column1])
#         for index2, row2 in df2.iterrows():
#             string2 = str(row2[column2])
#             similarity_score = method(string1, string2)
#             #print(f"Comparing '{string1}' from df1 with '{string2}' from df2: Similarity = {similarity_score}")
#             if similarity_score >= threshold:
#                 print(f"Comparing '{string1}' from df1 with '{string2}' from df2: Similarity = {similarity_score}")
#                 dict1 = row1.to_dict()
#                 dict2 = row2.to_dict()
#                 # Manually add prefix to keys of dict2
#                 prefixed_dict2 = {f"match_{key}": value for key, value in dict2.items()}
#                 linked_record = {**dict1, **prefixed_dict2, "similarity_score": similarity_score}
#                 linked_records.append(linked_record)
#     return pd.DataFrame(linked_records)

# Example Usage (same as before)
focus_data = read_data(
    '/Users/kirtanshah/PycharmProjects/fs-ssot-poc/domains/customer/DataFiles/FOCUS_SCHOOLS_DISTRICTS.csv')
# validated_focus_data = validate_focus_data(focus_data)

sf_data = read_data('/Users/kirtanshah/PycharmProjects/fs-ssot-poc/domains/customer/DataFiles/SF_ACCOUNTS.csv')
filtered_sf_data = filter_sf_data(sf_data)
# data1 = {'id': [1, 2, 3],
#          'name': ["Apple Inc.", "Google LLC", "Microsoft Corporation"]}
# df1 = pd.DataFrame(data1)
#
# data2 = {'ref_id': ["A1", "G2", "M3", "F4"],
#          'company': ["Apple Computer", "Alphabet Inc (Google)", "Microsoft Corp", "Facebook"]}
# df2 = pd.DataFrame(data2)

# linked_df = link_dataframes_with_details_no_prefix(focus_data, filtered_sf_data, 'SCHOOL_DISTRICT_NAME', 'NAME', threshold=80, method=fuzz.ratio)

# print("\nLinked Records:")
# print(linked_df)

# Assuming 'focus_data' has a column named 'SCHOOL_DISTRICT_NAME'
focus_series = focus_data['SCHOOL_DISTRICT_NAME']
focus_series.name = 'FOCUS_DISTRICT'  # Give the series a name for better output

# Assuming 'filtered_sf_data' has a column named 'NAME'
sf_series = filtered_sf_data['NAME']
sf_series.name = 'SF_ACCOUNT_NAME'  # Give the series a name for better output

compared_df = compare_distinct_series(focus_series, sf_series, threshold=85, method=fuzz.ratio)

print("\nComparison of Distinct Series:")
print(compared_df.columns)
print(len(compared_df))
print(len(compared_df['FOCUS_DISTRICT'].unique()))
compared_df.to_csv(
    '/Users/kirtanshah/PycharmProjects/fs-ssot-poc/domains/customer/DataFiles/output_fuzzy_sd.csv', index=False)

# with pd.option_context('display.max_rows', None, 'display.max_columns', None):
#     print("--- Full DataFrame Output ---")
#     print(compared_df)
#     print("--- End of Full Output ---")