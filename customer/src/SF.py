from Reader import read_data
from FuzzyMatcher import name_matcher

def fetch_NCES_Id_from_SF(district):
    sf_data = read_data('domains/customer/DataFiles/SF_ACCOUNTS.csv')
    for index, row in sf_data.iterrows():
        if (name_matcher(district, row['NAME'])):
            return row['NCES_ID__C']
        