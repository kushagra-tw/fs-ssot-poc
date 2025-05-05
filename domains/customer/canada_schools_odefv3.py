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
  prov_code = str(row['ODEF_province_code']) if pd.notna(row['ODEF_province_code']) else '' # Added province code

  # Concatenate the strings using a separator
  combined_string = f"{auth_id}|{auth_name}|{prov_code}"

  # Create SHA-256 hash and take the first 12 characters
  full_hash = hashlib.sha256(combined_string.encode('utf-8')).hexdigest()
  return full_hash[:12]

BASE_PATH = '/Users/kirtanshah/Documents/'

focus_data = read_data(BASE_PATH +
    'DataFiles/FOCUS_SCHOOLS_DISTRICTS.csv')
focus_data = focus_data.add_prefix('FOCUS_')
canada_state_abbvs = ['AB', 'BC', 'MB', 'NB', 'NL', 'NT', 'NS', 'NU', 'ON', 'PE', 'QC', 'SK', 'YT', 'QB','PQ']
canada_records = focus_data.loc[(focus_data['FOCUS_STATE'].isin(canada_state_abbvs))]
print("canada records: ")
print(len(canada_records))
odef_file_data = pd.read_csv(BASE_PATH + "DataFiles/odef_v3.csv", na_values='..', encoding='utf-8')
odef_file_data = odef_file_data.add_prefix('ODEF_')

# print(odef_file_data.head())
odef_file_data[['ODEF_Longitude', 'ODEF_Latitude']] = odef_file_data['ODEF_geometry'].str.extract(r'POINT \(([-0-9.]+) ([-0-9.]+)\)').astype(float)


focus_geodf = create_geodataframe_from_lat_lon(canada_records, lat_col='FOCUS_ADDRESS_LATITUDE',
                                               lon_col='FOCUS_ADDRESS_LONGITUDE')
odef_geodf = create_geodataframe_from_lat_lon(odef_file_data, lat_col='ODEF_Latitude', lon_col='ODEF_Longitude')

DISTANCE = 100

joined_gdf = join_geodataframes_by_lat_lon_columns(focus_geodf, odef_geodf,
                                                   left_lat='FOCUS_ADDRESS_LATITUDE',
                                                   left_lon='FOCUS_ADDRESS_LONGITUDE',
                                                   right_lat='ODEF_Latitude',
                                                   right_lon='ODEF_Longitude', how='inner', distance=DISTANCE)

print("join res")
print(len(joined_gdf))

columns_to_unidecode = ['FOCUS_SCHOOL_DISTRICT_NAME', 'FOCUS_SCHOOL_NAME', 'ODEF_facility_name', 'ODEF_authority_name']

for col in columns_to_unidecode:
    if col in joined_gdf.columns:
        new_col_name = f"{col}_without_accent"
        joined_gdf[new_col_name] = joined_gdf[col].apply(lambda x: unidecode(x) if pd.notna(x) and isinstance(x, str) else x)
    else:
        print(f"Warning: Column '{col}' not found in DataFrame.")

columns_to_process = ['FOCUS_SCHOOL_DISTRICT_NAME_without_accent', 'FOCUS_SCHOOL_NAME_without_accent', 'ODEF_facility_name_without_accent',
                      'ODEF_authority_name_without_accent']


normalized_df = normalize_dataframe_columns(joined_gdf, columns_to_process)

# FOCUS_SCHOOL_DISTRICT_NAME_without_accent_standardized	FOCUS_SCHOOL_NAME_without_accent_standardized	ODEF_Facility_Name_without_accent_standardized	ODEF_Authority_Name_without_accent_standardized
sim_sn_focus_df = add_similarity_score(normalized_df, 'FOCUS_SCHOOL_NAME_without_accent_standardized', 'ODEF_facility_name_without_accent_standardized',
                                       'focus_odef_school_name_similarity')
final_focus_df = add_similarity_score(sim_sn_focus_df, 'FOCUS_SCHOOL_DISTRICT_NAME_without_accent_standardized', 'ODEF_authority_name_without_accent_standardized',
                                      'focus_odef_district_name_similarity')

final_focus_df['authority_hash_12'] = final_focus_df.apply(create_authority_hash_12char, axis=1)

filtered_df = final_focus_df[final_focus_df['focus_odef_school_name_similarity'] >= 50]
print(len(filtered_df))
filtered_df.sort_values(by='focus_odef_school_name_similarity').to_csv("/Users/kirtanshah/PycharmProjects/fs-ssot-poc/customer/data/raw/canada_schools.csv", index=False, encoding='utf-8-sig')
