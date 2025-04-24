import sys
sys.path.insert(0, "/Users/michaelbarnett/Desktop/clients/FirstStudent/fs-ssot-poc/")

from domains.customer.Reader import read_data
import pandas as pd
import numpy as np
from math import isnan


def is_null(field):
    return field != field


tw_data = read_data(
    '/Users/michaelbarnett/Desktop/clients/FirstStudent/fs-ssot-poc/op_9.csv')

nix_data = read_data(
    '/Users/michaelbarnett/Desktop/clients/FirstStudent/fs-ssot-poc/domains/customer/DataFiles/n-ix2.csv')

merged = tw_data.merge(nix_data, on="FOCUS_SCHOOL_ID", how="outer")



merged['agree'] = merged.apply(
        lambda row: row['NCES_NCESSCH'] == row['nix_NCES_ID'], axis=1
    )
merged['nix_miss'] = merged.apply(
    lambda row: (is_null(row['nix_NCES_ID'])) & (not is_null(row['NCES_NCESSCH'])), axis=1
)

merged['tw_miss'] = merged.apply(
    lambda row: (is_null(row['NCES_NCESSCH'])) & (not is_null(row['nix_NCES_ID'])), axis=1
)

merged['conflict'] = merged.apply(
    lambda row: (not is_null(row['nix_NCES_ID'])) & (not is_null(row['NCES_NCESSCH'])) & (row['NCES_NCESSCH'] != row['nix_NCES_ID']), axis=1
)


print(f"Agreed: {merged.loc[(merged['agree'])].shape[0]}")
print(f"Nix Miss: {merged.loc[(merged['nix_miss'])].shape[0]}")
print(f"Tw Miss: {merged.loc[(merged['tw_miss'])].shape[0]}")
print(f"Conflicts: {merged.loc[(merged['conflict'])].shape[0]}")


merged = merged.filter(items=["FOCUS_SCHOOL_ID", "NCES_NCESSCH", "nix_NCES_ID", "agree", "nix_miss", "tw_miss", "conflict"])

# print(merged.head(50))
# raise Exception()

merged.to_csv('val_9.csv')