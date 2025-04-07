import pandas as pd
from rapidfuzz import fuzz

from domains.customer.Ingester import filter_sf_data
from domains.customer.Reader import read_data
from domains.customer.geo_matching import create_geodataframe_from_lat_lon, join_geodataframes_by_lat_lon_columns
from domains.customer.name_similarity import add_similarity_score
from domains.customer.sample_test_fuzzy import compare_distinct_sd_series

focus_data = read_data(
    '/Users/kirtanshah/PycharmProjects/fs-ssot-poc/domains/customer/DataFiles/FOCUS_SCHOOLS_DISTRICTS.csv')
focus_data = focus_data.add_prefix('FOCUS_')

sf_file_data = read_data('/Users/kirtanshah/PycharmProjects/fs-ssot-poc/domains/customer/DataFiles/SF_ACCOUNTS.csv')
sf_data = filter_sf_data(sf_file_data)
sf_data = sf_data.add_prefix('SF_')

nces_data = read_data(
    '/Users/kirtanshah/PycharmProjects/fs-ssot-poc/domains/customer/DataFiles/NCES_PUBL_PRIV_POSTSEC_SCHOOL_LOCATIONS.csv')
nces_data = nces_data.add_prefix('NCES_')




#1 ============= focus sf merge ===========

focus_series = focus_data['FOCUS_SCHOOL_DISTRICT_NAME']
focus_series.name = 'FOCUS_DISTRICT'
sf_series = sf_data['SF_NAME']
sf_series.name = 'SF_DISTRICT'
focus_sf_mapped_df = compare_distinct_sd_series(focus_series, sf_series, threshold=75, method=fuzz.ratio).sort_values(by='similarity_score')
print(len(focus_sf_mapped_df['FOCUS_DISTRICT']))

focus_data['focus_temp_district_name'] = focus_data['FOCUS_SCHOOL_DISTRICT_NAME'].astype(str).str.lower().str.strip()
focus_with_mapping = pd.merge(focus_data, focus_sf_mapped_df,
                     left_on='focus_temp_district_name',
                     right_on='FOCUS_DISTRICT',
                     how='left')
print(len(focus_with_mapping['FOCUS_DISTRICT'].unique()))

sf_data['sf_temp_district_name'] = sf_data['SF_NAME'].astype(str).str.lower().str.strip()
focus_sf_merge = pd.merge(focus_with_mapping, sf_data,
                     left_on='FOCUS_DISTRICT',
                     right_on='sf_temp_district_name',
                     how='left')
focus_sf_merge['is_focus_sf_merge'] = focus_sf_merge['sf_temp_district_name'].notna()




#2 ====== focus + nces on geo match
print(len(focus_sf_merge[focus_sf_merge['SF_NCES_ID__C'].notna()]))
print(len(focus_sf_merge[focus_sf_merge['SF_NCES_ID__C'].isna()]))

focus_data_no_nces_id = focus_sf_merge[focus_sf_merge['SF_NCES_ID__C'].isna()]
focus_geodf_no_nces_id = create_geodataframe_from_lat_lon(focus_data_no_nces_id, lat_col='FOCUS_ADDRESS_LATITUDE', lon_col='FOCUS_ADDRESS_LONGITUDE')
nces_geodf = create_geodataframe_from_lat_lon(nces_data, lat_col='NCES_LAT', lon_col='NCES_LON')

# #
joined_gdf_no_nces_id = join_geodataframes_by_lat_lon_columns(focus_geodf_no_nces_id, nces_geodf,
                                                   left_lat='FOCUS_ADDRESS_LATITUDE', left_lon='FOCUS_ADDRESS_LONGITUDE',
                                                   right_lat='NCES_LAT',
                                                   right_lon='NCES_LON', how='left', distance=50)
# df_with_name_similarity = add_similarity_score(joined_gdf_no_nces_id, 'FOCUS_SCHOOL_NAME', 'NCES_SCH_NAME', 'school_name_similarity_ratio_focus_nces')
# # print(df_with_name_similarity.columns.to_list())
# # # exit()
# print(df_with_name_similarity.columns.to_list())
# df_with_name_district_similarity = add_similarity_score(df_with_name_similarity, 'FOCUS_SCHOOL_DISTRICT_NAME', 'NCES_NAME',
#                                                         'school_district_name_similarity_ratio_focus_nces')
# # focus_data['matched_district_name'] = focus_data['SCHOOL_DISTRICT_NAME'].apply(lambda x: name_matcher(x, NCES_data['NAME']))
# # focus_sf_merge.to_csv('op.csv')

focus_with_nces_id = focus_sf_merge[focus_sf_merge['SF_NCES_ID__C'].notna()]
complete_focus_df  = pd.concat([focus_with_nces_id,joined_gdf_no_nces_id],ignore_index=True)




#3 - name match focus and nces where there is not lat long based match
complete_focus_df.to_csv('op.csv')



