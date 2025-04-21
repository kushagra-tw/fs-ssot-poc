import sys
sys.path.insert(0, "/Users/michaelbarnett/Desktop/clients/FirstStudent/fs-ssot-poc/")


import pandas as pd
import os

from domains.customer.Reader import read_data

BASE_PATH = '/Users/michaelbarnett/Desktop/clients/FirstStudent/fs-ssot-poc/'
schools_df = read_data(
    BASE_PATH+f'outputs/schools/schools_{os.environ.get("FILE_DATE_SUFFIX")}.csv')

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
        "NCES_SCHID"
        'FOCUS_SCHOOL_DISTRICT_NAME', 
        'NCES_NAME',
        "NCES_LEAID",
        "NCES_NAME",
        "NCES_STATE.1",
        "NCES_CITY.1",
        "NCES_STREET.1",
        "NCES_ZIP.1"
    ], axis=1).loc[(schools_df["FOCUS_SCHOOL_DISTRICT_ID"].isin(agreeing_customers["FOCUS_SCHOOL_DISTRICT_ID"]))] \
    .loc[(schools_df["NCES_LEAID"] == schools_df["NCES_LEAID"])] \
    .drop_duplicates() \
    
existing_customers = read_data('/Users/michaelbarnett/Desktop/clients/FirstStudent/fs-ssot-poc/domains/customer/DataFiles/customer-export-2025-04-19-07-35.csv')

merged_df = full_customer_df.merge(
    right=existing_customers,
    left_on="NCES_LEAID",
    right_on="MASTERPROPERTIES_NCESSCHOOLDISTRICTID",
    how="left",
).filter(items=[
        "FOCUS_SCHOOL_DISTRICT_ID",
        "NCES_LEAID",
        "NCES_SCHID"
        'FOCUS_SCHOOL_DISTRICT_NAME', 
        'NCES_NAME',
        "NCES_LEAID",
        "NCES_NAME",
        "NCES_STATE.1",
        "NCES_CITY.1",
        "NCES_STREET.1",
        "NCES_ZIP.1",
        "MASTERPROPERTIES_ID"
    ], axis=1)

creates = len(merged_df.loc[(merged_df["MASTERPROPERTIES_ID"] != merged_df["MASTERPROPERTIES_ID"])])
updates = len(merged_df.loc[(merged_df["MASTERPROPERTIES_ID"] == merged_df["MASTERPROPERTIES_ID"])])

print(f"For customers, we have {creates} creates and {updates} updates")
    
merged_df.to_csv(f'outputs/customers/customers_{os.environ.get("FILE_DATE_SUFFIX")}.csv')