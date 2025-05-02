import pandas as pd
from unidecode import unidecode
from domains.customer.Reader import read_data
from domains.customer.geo_matching import create_geodataframe_from_lat_lon, join_geodataframes_by_lat_lon_columns
from domains.customer.name_similarity_scoring import add_similarity_score
from domains.customer.normalize_names import normalize_dataframe_columns

BASE_PATH = '/Users/kirtanshah/Documents/'

focus_data = read_data(BASE_PATH +
    'DataFiles/FOCUS_SCHOOLS_DISTRICTS.csv')
focus_data = focus_data.add_prefix('FOCUS_')
canada_state_abbvs = ['AB', 'BC', 'MB', 'NB', 'NL', 'NT', 'NS', 'NU', 'ON', 'PE', 'QC', 'SK', 'YT', 'QB','PQ']
canada_records = focus_data.loc[(focus_data['FOCUS_STATE'].isin(canada_state_abbvs))]
print("canada records: ")
print(len(canada_records))
odef_file_data = pd.read_csv(BASE_PATH + "DataFiles/ODEF_v2_1.csv", na_values='..', encoding='cp1252')
odef_file_data = odef_file_data.add_prefix('ODEF_')

# print(odef_file_data.head())


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

columns_to_unidecode = ['FOCUS_SCHOOL_DISTRICT_NAME', 'FOCUS_SCHOOL_NAME', 'ODEF_Facility_Name', 'ODEF_Authority_Name']

for col in columns_to_unidecode:
    if col in joined_gdf.columns:
        new_col_name = f"{col}_without_accent"
        joined_gdf[new_col_name] = joined_gdf[col].apply(lambda x: unidecode(x) if pd.notna(x) and isinstance(x, str) else x)
    else:
        print(f"Warning: Column '{col}' not found in DataFrame.")

columns_to_process = ['FOCUS_SCHOOL_DISTRICT_NAME_without_accent', 'FOCUS_SCHOOL_NAME_without_accent', 'ODEF_Facility_Name_without_accent',
                      'ODEF_Authority_Name_without_accent']


normalized_df = normalize_dataframe_columns(joined_gdf, columns_to_process)

# FOCUS_SCHOOL_DISTRICT_NAME_without_accent_standardized	FOCUS_SCHOOL_NAME_without_accent_standardized	ODEF_Facility_Name_without_accent_standardized	ODEF_Authority_Name_without_accent_standardized
sim_sn_focus_df = add_similarity_score(normalized_df, 'FOCUS_SCHOOL_NAME_without_accent_standardized', 'ODEF_Facility_Name_without_accent_standardized',
                                       'focus_odef_school_name_similarity')
final_focus_df = add_similarity_score(sim_sn_focus_df, 'FOCUS_SCHOOL_DISTRICT_NAME_without_accent_standardized', 'ODEF_Authority_Name_without_accent_standardized',
                                      'focus_odef_district_name_similarity')

filtered_df = final_focus_df[final_focus_df['focus_odef_school_name_similarity'] >= 50]
print(len(filtered_df))
filtered_df.sort_values(by='focus_odef_school_name_similarity').to_csv("/Users/kirtanshah/PycharmProjects/fs-ssot-poc/customer/data/raw/canada_schools.csv", index=False, encoding='utf-8-sig')
