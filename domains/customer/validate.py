import sys
sys.path.insert(0, "/Users/michaelbarnett/Desktop/clients/FirstStudent/fs-ssot-poc/")

from domains.customer.Reader import read_data
import pandas as pd


tw_data = read_data(
    '/Users/michaelbarnett/Desktop/clients/FirstStudent/fs-ssot-poc/op_6.csv')

nix_data = read_data(
    '/Users/michaelbarnett/Desktop/clients/FirstStudent/fs-ssot-poc/domains/customer/DataFiles/n-ix.csv')

merged = tw_data.merge(nix_data, on="FOCUS_SCHOOL_ID", how="outer")

merged['agree'] = merged.apply(
        lambda row: row['NCES_SCHID'] == row['NCES_SCHOOL_ID'], axis=1
    )

merged = merged.filter(items=["FOCUS_SCHOOL_ID", "NCES_SCHID", "NCES_SCHOOL_ID", "agree"])

merged.to_csv('val_6.csv')