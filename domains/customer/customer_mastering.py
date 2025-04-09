import pandas as pd
from rapidfuzz import fuzz

from domains.customer.Reader import read_data
from domains.customer.geo_matching import create_geodataframe_from_lat_lon, join_geodataframes_by_lat_lon_columns
from domains.customer.name_similarity_scoring import add_similarity_score
from domains.customer.fuzzy_name_merge import compare_distinct_sd_series_focus_sf


def filter_sf_data(sf_data):
    sf_customer_type = ['Former Customer', 'Customer']
    filtered_sf_data = sf_data[sf_data['TYPE'].isin(sf_customer_type)]
    return filtered_sf_data


focus_data = read_data(
    '/Users/kirtanshah/PycharmProjects/fs-ssot-poc/domains/customer/DataFiles/FOCUS_SCHOOLS_DISTRICTS.csv')
focus_data = focus_data.add_prefix('FOCUS_')

sf_file_data = read_data('/Users/kirtanshah/PycharmProjects/fs-ssot-poc/domains/customer/DataFiles/SF_ACCOUNTS.csv')
sf_data = filter_sf_data(sf_file_data)
sf_data = sf_data.add_prefix('SF_')

nces_data = read_data(
    '/Users/kirtanshah/PycharmProjects/fs-ssot-poc/domains/customer/DataFiles/NCES_PUBL_PRIV_POSTSEC_SCHOOL_LOCATIONS.csv')
nces_data = nces_data.add_prefix('NCES_')

# 1 ============= focus sf merge ===========

focus_series = focus_data['FOCUS_SCHOOL_DISTRICT_NAME']
focus_series.name = 'FOCUS_DISTRICT'
sf_series = sf_data['SF_NAME']
sf_series.name = 'SF_DISTRICT'
focus_sf_mapped_df = compare_distinct_sd_series_focus_sf(focus_series, sf_series, threshold=75,
                                                         method=fuzz.ratio)

focus_data['focus_temp_district_name'] = focus_data['FOCUS_SCHOOL_DISTRICT_NAME'].astype(str).str.lower().str.strip()
focus_with_mapping = pd.merge(focus_data, focus_sf_mapped_df,
                              left_on='focus_temp_district_name',
                              right_on='FOCUS_DISTRICT',
                              how='left')
print("focus matches found with sf" + str(len(focus_with_mapping['FOCUS_DISTRICT'].unique())))

sf_data['sf_temp_district_name'] = sf_data['SF_NAME'].astype(str).str.lower().str.strip()
focus_sf_merge = pd.merge(focus_with_mapping, sf_data,
                          left_on='FOCUS_DISTRICT',
                          right_on='sf_temp_district_name',
                          how='left')
focus_sf_merge['is_focus_sf_merge'] = focus_sf_merge['sf_temp_district_name'].notna()

# 2 ====== focus + nces on geo match

# focus_data_no_nces_id = focus_sf_merge[focus_sf_merge['SF_NCES_ID__C'].isna()]
focus_geodf = create_geodataframe_from_lat_lon(focus_sf_merge, lat_col='FOCUS_ADDRESS_LATITUDE',
                                               lon_col='FOCUS_ADDRESS_LONGITUDE')
nces_geodf = create_geodataframe_from_lat_lon(nces_data, lat_col='NCES_LAT', lon_col='NCES_LON')

joined_gdf = join_geodataframes_by_lat_lon_columns(focus_geodf, nces_geodf,
                                                   left_lat='FOCUS_ADDRESS_LATITUDE',
                                                   left_lon='FOCUS_ADDRESS_LONGITUDE',
                                                   right_lat='NCES_LAT',
                                                   right_lon='NCES_LON', how='left', distance=50)

# focus_with_nces_id = focus_sf_merge[focus_sf_merge['SF_NCES_ID__C'].notna()]
# complete_focus_df  = pd.concat([focus_with_nces_id,joined_gdf_no_nces_id],ignore_index=True)

sim_sn_focus_df = add_similarity_score(joined_gdf, 'FOCUS_SCHOOL_NAME', 'NCES_SCH_NAME',
                                       'focus_nces_school_name_similarity')
final_focus_df = add_similarity_score(sim_sn_focus_df, 'FOCUS_SCHOOL_DISTRICT_NAME', 'NCES_NAME',
                                      'focus_nces_district_name_similarity')
final_focus_df = add_similarity_score(final_focus_df, 'FOCUS_CITY', 'NCES_CITY',
                                      'focus_nces_city_name_similarity')
# FOCUS_STATE NCES_STATE
final_focus_df = add_similarity_score(final_focus_df, 'FOCUS_STATE', 'NCES_STATE',
                                      'focus_nces_state_name_similarity')
final_focus_df['zip_code_match'] = final_focus_df['FOCUS_POSTAL_CODE'].eq(final_focus_df['NCES_ZIP'])

# reorder to push all sf columns at the end
all_columns = final_focus_df.columns.tolist()

sf_cols = [col for col in all_columns if col.startswith('SF_')]
other_cols = [col for col in all_columns if not col.startswith('SF_')]

new_column_order = other_cols + sf_cols

final_focus_df = final_focus_df[new_column_order]

final_focus_df.to_csv('op.csv')
