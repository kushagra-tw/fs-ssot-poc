import sys

from domains.customer.normalize_names import normalize_dataframe_columns
from domains.customer.standardize_district_terms import standardize_terms_in_school_district

sys.path.insert(0, "/Users/michaelbarnett/Desktop/clients/FirstStudent/fs-ssot-poc/")

import pandas as pd
from rapidfuzz import fuzz

from domains.customer.Reader import read_data
from domains.customer.geo_matching import create_geodataframe_from_lat_lon, join_geodataframes_by_lat_lon_columns
from domains.customer.name_similarity_scoring import add_similarity_score
from domains.customer.fuzzy_name_merge import compare_distinct_sd_series_focus_sf
from domains.customer.standardize_school_terms import standardize_school_names


def filter_sf_data(sf_data):
    sf_customer_type = ['Former Customer', 'Customer']
    filtered_sf_data = sf_data[sf_data['TYPE'].isin(sf_customer_type)]
    return filtered_sf_data

def setsubtract(df1, df2):
    outer_joined_df = df1.merge(df2, how="outer", indicator=True, on="FOCUS_SCHOOL_ID", suffixes=[None, '_to_drop'])
    return outer_joined_df[(outer_joined_df._merge == 'left_only')] \
        .drop('_merge', axis = 1) \
        .drop([column + "_to_drop" for column in df2.columns if "FOCUS_SCHOOL_ID" not in column + ""], axis=1)

def quarantine(df, df_to_quarantine, quarantine_df, quarantine_reason):

    subtracted_df = setsubtract(df, df_to_quarantine)

    df_to_quarantine["quarantine_reason"] = quarantine_reason

    quarantine_df = pd.concat([quarantine_df, df_to_quarantine])

    return subtracted_df, quarantine_df

BASE_PATH = '/Users/kirtanshah/Documents/'

focus_data = read_data(BASE_PATH +
    'DataFiles/FOCUS_SCHOOLS_DISTRICTS.csv')
focus_data = focus_data.add_prefix('FOCUS_')

# sf_file_data = read_data('/Users/michaelbarnett/Desktop/clients/FirstStudent/fs-ssot-poc/domains/customer/DataFiles/SF_ACCOUNTS.csv')
# sf_data = filter_sf_data(sf_file_data)
# sf_data = sf_data.add_prefix('SF_')

nces_data = read_data(
    BASE_PATH +'DataFiles/NCES_PUBL_PRIV_POSTSEC_SCHOOL_LOCATIONS.csv')
nces_data = nces_data.add_prefix('NCES_')


# 1 ============= focus sf merge ===========

# focus_series = focus_data['FOCUS_SCHOOL_DISTRICT_NAME']
# focus_series.name = 'FOCUS_DISTRICT'
# sf_series = sf_data['SF_NAME']
# sf_series.name = 'SF_DISTRICT'
# focus_sf_mapped_df = compare_distinct_sd_series_focus_sf(focus_series, sf_series, threshold=75,
#                                                          method=fuzz.ratio)

# focus_data['focus_temp_district_name'] = focus_data['FOCUS_SCHOOL_DISTRICT_NAME'].astype(str).str.lower().str.strip()
# focus_with_mapping = pd.merge(focus_data, focus_sf_mapped_df,
#                               left_on='focus_temp_district_name',
#                               right_on='FOCUS_DISTRICT',
#                               how='left')
# print("focus matches found with sf" + str(len(focus_with_mapping['FOCUS_DISTRICT'].unique())))

# sf_data['sf_temp_district_name'] = sf_data['SF_NAME'].astype(str).str.lower().str.strip()
# focus_sf_merge = pd.merge(focus_with_mapping, sf_data,
#                           left_on='FOCUS_DISTRICT',
#                           right_on='sf_temp_district_name',
#                           how='left')
# focus_sf_merge['is_focus_sf_merge'] = focus_sf_merge['sf_temp_district_name'].notna()

# 2 ====== focus + nces on geo match

canada_state_abbvs = ['AB', 'BC', 'MB', 'NB', 'NL', 'NT', 'NS', 'NU', 'ON', 'PE', 'QC', 'SK', 'YT']
canada_records = focus_data.loc[(focus_data['FOCUS_STATE'].isin(canada_state_abbvs))]
focus_data, quarantined_df = quarantine(focus_data, canada_records, pd.DataFrame(), "Canada")

# focus_data_no_nces_id = focus_sf_merge[focus_sf_merge['SF_NCES_ID__C'].isna()]
focus_geodf = create_geodataframe_from_lat_lon(focus_data, lat_col='FOCUS_ADDRESS_LATITUDE',
                                               lon_col='FOCUS_ADDRESS_LONGITUDE')
nces_geodf = create_geodataframe_from_lat_lon(nces_data, lat_col='NCES_LAT', lon_col='NCES_LON')

DISTANCE = 100

joined_gdf = join_geodataframes_by_lat_lon_columns(focus_geodf, nces_geodf,
                                                   left_lat='FOCUS_ADDRESS_LATITUDE',
                                                   left_lon='FOCUS_ADDRESS_LONGITUDE',
                                                   right_lat='NCES_LAT',
                                                   right_lon='NCES_LON', how='left', distance=DISTANCE)
lonely_schools = setsubtract(joined_gdf, focus_geodf)
joined_gdf, quarantined_df = quarantine(joined_gdf, lonely_schools, quarantined_df, "No nearby candidate schools")

# focus_with_nces_id = focus_sf_merge[focus_sf_merge['SF_NCES_ID__C'].notna()]
# complete_focus_df  = pd.concat([focus_with_nces_id,joined_gdf_no_nces_id],ignore_index=True)

joined_gdf = joined_gdf.loc[(joined_gdf['actual_distance_m'] <= DISTANCE)]
columns_to_process = [
    'FOCUS_SCHOOL_DISTRICT_NAME',
    'NCES_NAME',
    'FOCUS_SCHOOL_NAME', # Add based on your actual columns
    'NCES_SCH_NAME'      # Add based on your actual columns
]
normalized_df = normalize_dataframe_columns(joined_gdf, columns_to_process)
columns_to_standardize = ['FOCUS_SCHOOL_DISTRICT_NAME_standardized', 'NCES_NAME_standardized']
standardized_names_df = standardize_terms_in_school_district(normalized_df, columns_to_standardize)
school_columns_to_standardize = ['FOCUS_SCHOOL_NAME_standardized','NCES_SCH_NAME_standardized']
standardized_names_df = standardize_school_names(standardized_names_df, school_columns_to_standardize)
# standardized_names_df = standardize_school_names(joined_gdf, ["FOCUS_SCHOOL_NAME", "NCES_SCH_NAME"])

d = standardized_names_df.loc[(standardized_names_df["FOCUS_SCHOOL_NAME"] != standardized_names_df["FOCUS_SCHOOL_NAME_standardized"])]
print(d["FOCUS_SCHOOL_NAME"])
print(d["FOCUS_SCHOOL_NAME_standardized"])


sim_sn_focus_df = add_similarity_score(standardized_names_df, 'FOCUS_SCHOOL_NAME_standardized', 'NCES_SCH_NAME_standardized',
                                       'focus_nces_school_name_similarity')
final_focus_df = add_similarity_score(sim_sn_focus_df, 'FOCUS_SCHOOL_DISTRICT_NAME', 'NCES_NAME',
                                      'focus_nces_district_name_similarity')
final_focus_df = add_similarity_score(final_focus_df, 'FOCUS_CITY', 'NCES_CITY',
                                      'focus_nces_city_name_similarity')
# FOCUS_STATE NCES_STATE
final_focus_df = add_similarity_score(final_focus_df, 'FOCUS_STATE', 'NCES_STATE',
                                      'focus_nces_state_name_similarity')
final_focus_df['zip_code_match'] = final_focus_df['FOCUS_POSTAL_CODE'].eq(final_focus_df['NCES_ZIP'])

names_disagree_df = final_focus_df.loc[(final_focus_df['focus_nces_school_name_similarity'] < 60)]
final_focus_df, quarantined_df = quarantine(final_focus_df, names_disagree_df, quarantined_df, "School names disagree")

#filter out where school district names don't match (provided there is a school district in nces; second conditional is a null-check)
districts_disagree_df = final_focus_df.loc[(final_focus_df['focus_nces_district_name_similarity'] < 60) & (final_focus_df['NCES_LEAID'] == final_focus_df['NCES_LEAID'])]
final_focus_df, quarantined_df = quarantine(final_focus_df, districts_disagree_df, quarantined_df, "District names disagree")


# focus_to_nces_multiple_matches_df = final_focus_df.groupby('nces_id').filter(lambda x: x['FOCUS_SCHOOL_ID'].nunique() > 1)
# final_focus_df, quarantined_df = quarantine(final_focus_df, focus_to_nces_multiple_matches_df, quarantined_df, "Multiple Focus Schools matched with single NCES school")

# Pick "best guess" school
final_focus_df = \
    final_focus_df.sort_values(
        by=['FOCUS_SCHOOL_ID','focus_nces_school_name_similarity'],
        ascending=False
    ).groupby('FOCUS_SCHOOL_ID').first()

# reorder to push all sf columns at the end
all_columns = final_focus_df.columns.tolist()

sf_cols = [col for col in all_columns if col.startswith('SF_')]
other_cols = [col for col in all_columns if not col.startswith('SF_')]

new_column_order = other_cols + sf_cols

final_focus_df = final_focus_df[new_column_order]

print(f"Matched {final_focus_df.shape[0]} records!")

final_focus_df.to_csv('outputs/schools/schools_0417_3.csv')
quarantined_df.to_csv('outputs/schools/quarantined_schools_0417_3.csv')
