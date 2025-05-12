import pandas as pd
from unidecode import unidecode
from domains.customer.Reader import read_data
from domains.customer.geo_matching import create_geodataframe_from_lat_lon, join_geodataframes_by_lat_lon_columns
from domains.customer.name_similarity_scoring import add_similarity_score
from domains.customer.normalize_names import normalize_dataframe_columns

import hashlib
import numpy as np


def create_authority_hash_12char(row):
    # Check for NaN values (since '..' was converted to NaN on load)
    auth_id = str(row['ODEF_authority_id']) if pd.notna(row['ODEF_authority_id']) else ''
    auth_name = str(row['ODEF_authority_name']) if pd.notna(row['ODEF_authority_name']) else ''
    prov_code = str(row['ODEF_province_code']) if pd.notna(row['ODEF_province_code']) else ''  # Added province code

    # Concatenate the strings using a separator
    combined_string = f"{auth_id}|{auth_name}|{prov_code}"

    # Create SHA-256 hash and take the first 12 characters
    full_hash = hashlib.sha256(combined_string.encode('utf-8')).hexdigest()
    return full_hash[:12]


BASE_PATH = '/Users/kirtanshah/Documents/'

focus_data = read_data(BASE_PATH +
                       'DataFiles/FOCUS_SCHOOLS_DISTRICTS.csv')
focus_data = focus_data.add_prefix('FOCUS_')
canada_state_abbvs = ['AB', 'BC', 'MB', 'NB', 'NL', 'NT', 'NS', 'NU', 'ON', 'PE', 'QC', 'SK', 'YT', 'QB', 'PQ']
canada_records = focus_data.loc[(focus_data['FOCUS_STATE'].isin(canada_state_abbvs))]
print("Count of canada records in Focus classic: ")
print(len(canada_records))
odef_file_data = pd.read_csv(BASE_PATH + "DataFiles/odef_v3.csv", na_values='..', encoding='utf-8')
odef_file_data = odef_file_data.add_prefix('ODEF_')

# print(odef_file_data.head())
odef_file_data[['ODEF_Longitude', 'ODEF_Latitude']] = odef_file_data['ODEF_geometry'].str.extract(
    r'POINT \(([-0-9.]+) ([-0-9.]+)\)').astype(float)

focus_geodf = create_geodataframe_from_lat_lon(canada_records, lat_col='FOCUS_ADDRESS_LATITUDE',
                                               lon_col='FOCUS_ADDRESS_LONGITUDE')
odef_geodf = create_geodataframe_from_lat_lon(odef_file_data, lat_col='ODEF_Latitude', lon_col='ODEF_Longitude')

DISTANCE = 100

left_joined_gdf = join_geodataframes_by_lat_lon_columns(focus_geodf, odef_geodf,
                                                        left_lat='FOCUS_ADDRESS_LATITUDE',
                                                        left_lon='FOCUS_ADDRESS_LONGITUDE',
                                                        right_lat='ODEF_Latitude',
                                                        right_lon='ODEF_Longitude', how='left', distance=DISTANCE)
print("count after geo join")
print(len(left_joined_gdf))
# Identify matched records: rows where a column from the right dataframe (e.g., 'ODEF_unique_id') is NOT null
# 'ODEF_unique_id' is assumed to be a key column from the odef_geodf dataframe.
joined_gdf = left_joined_gdf.dropna(subset=['ODEF_unique_id']).copy()

# Identify non-matched records: rows where the key column from the right dataframe is null
non_matched_records = left_joined_gdf[left_joined_gdf['ODEF_unique_id'].isnull()].copy()

# Print the length of both dataframes
print(f"Length of matched records: {len(joined_gdf)}")
print(f"Length of non-matched records: {len(non_matched_records)}")

focus_geodf = focus_geodf.drop('geometry', axis=1)
joined_gdf = joined_gdf.drop('geometry', axis=1)

# quarantine geo match filtered records
quarantined_records = pd.DataFrame()

non_matched_records['quarantine_reason'] = 'No Proximity Match'
quarantined_records = pd.concat([quarantined_records, non_matched_records], ignore_index=True)
# print("no of records in quarantine after proximity match")
# print(len(quarantined_records))

print("Count of records matched based on proximity check")
print(len(joined_gdf))
columns_to_unidecode = ['FOCUS_SCHOOL_DISTRICT_NAME', 'FOCUS_SCHOOL_NAME', 'ODEF_facility_name', 'ODEF_authority_name']

for col in columns_to_unidecode:
    if col in joined_gdf.columns:
        new_col_name = f"{col}_without_accent"
        joined_gdf[new_col_name] = joined_gdf[col].apply(
            lambda x: unidecode(x) if pd.notna(x) and isinstance(x, str) else x)
    else:
        print(f"Warning: Column '{col}' not found in DataFrame.")

columns_to_process = ['FOCUS_SCHOOL_DISTRICT_NAME_without_accent', 'FOCUS_SCHOOL_NAME_without_accent',
                      'ODEF_facility_name_without_accent',
                      'ODEF_authority_name_without_accent']

normalized_df = normalize_dataframe_columns(joined_gdf, columns_to_process)

# FOCUS_SCHOOL_DISTRICT_NAME_without_accent_standardized	FOCUS_SCHOOL_NAME_without_accent_standardized	ODEF_Facility_Name_without_accent_standardized	ODEF_Authority_Name_without_accent_standardized
sim_sn_focus_df = add_similarity_score(normalized_df, 'FOCUS_SCHOOL_NAME_without_accent_standardized',
                                       'ODEF_facility_name_without_accent_standardized',
                                       'focus_odef_school_name_similarity')
final_focus_df = add_similarity_score(sim_sn_focus_df, 'FOCUS_SCHOOL_DISTRICT_NAME_without_accent_standardized',
                                      'ODEF_authority_name_without_accent_standardized',
                                      'focus_odef_district_name_similarity')

final_focus_df['authority_hash_12'] = final_focus_df.apply(create_authority_hash_12char, axis=1)

records_filtered_out_by_similarity = final_focus_df[final_focus_df['focus_odef_school_name_similarity'] < 50]
print(f"Count of records to be filtered out by similarity (< 50): {len(records_filtered_out_by_similarity)}")

filtered_df = final_focus_df[final_focus_df['focus_odef_school_name_similarity'] >= 50]

# records quarantined where school name similarity is less than 50
quarantined_step3 = final_focus_df[final_focus_df['focus_odef_school_name_similarity'] < 50].copy()
quarantined_step3['quarantine_reason'] = 'School Name Similarity Below 50'
quarantined_records = pd.concat([quarantined_records, quarantined_step3], ignore_index=True)
# print("no of records in quarantine after similarity filter")
# print(len(quarantined_records))

focus_schools_dupe = filtered_df.groupby('FOCUS_SCHOOL_ID').filter(lambda x: len(x) > 1).sort_values('FOCUS_SCHOOL_ID')
odef_schools_dupe = filtered_df.groupby('ODEF_unique_id').filter(lambda x: len(x) > 1).sort_values('ODEF_unique_id')
schools_dupe_df = pd.concat([focus_schools_dupe, odef_schools_dupe]).drop_duplicates().reset_index(drop=True)
print(len(focus_schools_dupe))
print(len(odef_schools_dupe))
schools_dupe_df['quarantine_reason'] = 'Duplicate Record'
quarantined_records = pd.concat([quarantined_records, schools_dupe_df], ignore_index=True)
print("schools dupe")
print(len(schools_dupe_df))
# print("no of records in quarantine after school dupes")
# print(len(quarantined_records))

filtered_df = filtered_df[~filtered_df['ODEF_unique_id'].isin(schools_dupe_df['ODEF_unique_id'])]


authority_counts = filtered_df[["FOCUS_SCHOOL_DISTRICT_ID", "authority_hash_12",
                                "ODEF_authority_id", "ODEF_authority_name",
                                "ODEF_province_code"]].drop_duplicates().groupby('authority_hash_12')[
    'FOCUS_SCHOOL_DISTRICT_ID'].count()
single_match_authorities = authority_counts[authority_counts == 1]
print("single")
records_filtered_out_by_authority_match = filtered_df[
    ~filtered_df['authority_hash_12'].isin(single_match_authorities.index)].copy()

print(
    f"Count of records to be filtered out by authority match (not in single_match_authorities): {len(records_filtered_out_by_authority_match)}")

records_filtered_out_by_authority_match['quarantine_reason'] = 'No Single Authority Match'
quarantined_records = pd.concat([quarantined_records, records_filtered_out_by_authority_match], ignore_index=True)
# print("no of records in quarantine after single authority match")
# print(len(quarantined_records))
filtered_df = filtered_df[filtered_df['authority_hash_12'].isin(single_match_authorities.index)]
# filtered_df = filtered_df[~filtered_df['authority_hash_12'].isin(records_filtered_out_by_authority_match['authority_hash_12'])]
print("count of records in result Canada schools dataset ")
print(len(filtered_df))
filtered_df.sort_values(by='focus_odef_school_name_similarity').to_csv(
    "/Users/kirtanshah/PycharmProjects/fs-ssot-poc/customer/data/interim/canada_schools.csv", index=False,
    encoding='utf-8-sig')

print("count of records in result quarantine Canada schools dataset ")
quarantined_records.sort_values(by='quarantine_reason').to_csv(
    "/Users/kirtanshah/PycharmProjects/fs-ssot-poc/customer/data/interim/quarantine_canada_schools.csv", index=False,
    encoding='utf-8-sig')

print(len(quarantined_records))
