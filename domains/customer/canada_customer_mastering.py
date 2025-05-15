import sys
import pandas as pd
import os

from domains.customer.Reader import read_data
#pd.set_option('display.max_columns', None)
BASE_PATH = '/Users/kirtanshah/Documents/'
schools_df = read_data("/Users/kirtanshah/PycharmProjects/fs-ssot-poc/customer/data/interim/canada_schools.csv")
schools_df = schools_df.dropna(subset=["ODEF_authority_id", "ODEF_authority_name"], how='all')



# authority_counts = schools_df[[ "FOCUS_SCHOOL_DISTRICT_ID","authority_hash_12",
#                                 "ODEF_authority_id", "ODEF_authority_name",
#                                 "ODEF_province_code"]].drop_duplicates().groupby('authority_hash_12')['FOCUS_SCHOOL_DISTRICT_ID'].count()
# single_match_authorities = authority_counts[authority_counts == 1]


distinct_customers_df = schools_df[[ "FOCUS_SCHOOL_DISTRICT_ID","authority_hash_12", "ODEF_authority_id", "ODEF_authority_name","ODEF_province_code"]].drop_duplicates()
# # ,"ODEF_province_code"
print(distinct_customers_df)

distinct_customers_df.to_csv(f'/Users/kirtanshah/PycharmProjects/fs-ssot-poc/customer/data/interim/canada_customers_{os.environ.get("FILE_DATE_SUFFIX")}.csv')
