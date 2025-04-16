import sys
sys.path.insert(0, "/Users/michaelbarnett/Desktop/clients/FirstStudent/fs-ssot-poc/")


import pandas as pd

from domains.customer.Reader import read_data

schools_df = read_data(
    '/Users/michaelbarnett/Desktop/clients/FirstStudent/fs-ssot-poc/schools_11.csv')

customers_df = schools_df.filter(items=["FOCUS_SCHOOL_DISTRICT_ID", "NCES_LEAID"], axis=1) \
    .groupby("FOCUS_SCHOOL_DISTRICT_ID", as_index=False) \
    .nunique()

# print(customers_df.head(10))
agreeing_customers = customers_df.loc[(customers_df["NCES_LEAID"] == 1)]
agreeing_customers.columns.values[0] = "FOCUS_SCHOOL_DISTRICT_ID"


full_customer_df = schools_df.filter(items=[
        "FOCUS_SCHOOL_DISTRICT_ID",
        "NCES_LEAID",
        'FOCUS_SCHOOL_DISTRICT_NAME', 
        'NCES_NAME',
    ], axis=1).loc[(schools_df["FOCUS_SCHOOL_DISTRICT_ID"].isin(agreeing_customers["FOCUS_SCHOOL_DISTRICT_ID"]))] \
    .drop_duplicates() \
    
print(full_customer_df)

full_customer_df.to_csv('customers_11.csv')