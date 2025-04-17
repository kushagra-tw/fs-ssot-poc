import sys
sys.path.insert(0, "/Users/michaelbarnett/Desktop/clients/FirstStudent/fs-ssot-poc/")


import pandas as pd

from domains.customer.Reader import read_data

schools_df = read_data(
    '/Users/michaelbarnett/Desktop/clients/FirstStudent/fs-ssot-poc/outputs/schools/schools_0417_2.csv')

customers_df = schools_df.filter(items=["FOCUS_SCHOOL_DISTRICT_ID", "NCES_LEAID"], axis=1) \
    .groupby("FOCUS_SCHOOL_DISTRICT_ID", as_index=False) \
    .nunique()

# print(customers_df)

# print(customers_df.head(10))
agreeing_customers = customers_df.loc[(customers_df["NCES_LEAID"] == 1)]
agreeing_customers.columns.values[0] = "FOCUS_SCHOOL_DISTRICT_ID"


full_customer_df = schools_df.filter(items=[
        "FOCUS_SCHOOL_DISTRICT_ID",
        "NCES_LEAID",
        'FOCUS_SCHOOL_DISTRICT_NAME', 
        'NCES_NAME',
    ], axis=1).loc[(schools_df["FOCUS_SCHOOL_DISTRICT_ID"].isin(agreeing_customers["FOCUS_SCHOOL_DISTRICT_ID"]))] \
    .loc[(schools_df["NCES_LEAID"] == schools_df["NCES_LEAID"])] \
    .drop_duplicates() \
    
full_customer_df.to_csv('outputs/customers/customers_0417_2.csv')