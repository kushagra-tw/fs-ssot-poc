
import pandas as pd
from Reader import read_data
from NCES import add_NCES_info, fetch_NCES_Id_from_NCES
from SF import fetch_NCES_Id_from_SF
from FuzzyMatcher import name_matcher

def extract_NCES_Id_from_NCES(focus_data, NCES_data):
    focus_data = focus_data[focus_data['NCES_ID__C'].isna()]
    focus_data['matched_district_name'] = focus_data['SCHOOL_DISTRICT_NAME'].apply(lambda x: name_matcher(x, NCES_data['NAME']))
    focus_data = focus_data.merge(NCES_data[['NAME', 'LEAID']], left_on='matched_district_name', right_on='NAME', how='left')
    focus_data.drop(columns='matched_district_name', inplace=True)
    return focus_data

def extract_NCES_Id_from_SF(focus_data, sf_data):
    focus_data['matched_district_name'] = focus_data['SCHOOL_DISTRICT_NAME'].apply(lambda x: name_matcher(x, sf_data['NAME']))
    focus_data = focus_data.merge(sf_data[['NAME', 'NCES_ID__C']], left_on='matched_district_name', right_on='NAME', how='left')
    focus_data.drop(columns='matched_district_name', inplace=True)
    return focus_data

def populate_NCES_data(focus_data, sf_data, NCES_data):
    print("Total count of focus: ", len(focus_data))
    focus_data_with_SF_match = extract_NCES_Id_from_SF(focus_data, sf_data)
    focus_data_with_NCES_match = extract_NCES_Id_from_NCES(focus_data_with_SF_match, NCES_data)
    focus_data = pd.concat([focus_data_with_SF_match, focus_data_with_NCES_match], ignore_index=True)
    print("Total count with NCES: ", len(focus_data[focus_data['NCES_ID__C'].notna()]))
#     for nonMatches in FocusList[]:
#         keepit as such without NCES info

#     for nonMatches in SF with FocusList[]:
#         goto DataStewards
    pass

def validate_focus_data(focus_data):
    # completeness of data, 
    # identify incorrectness,
    # review data quality, 
    # validate data fields,
    # categorize issues. 
    return focus_data

def filter_sf_data(sf_data):
    sf_customer_type = ['Prospect','Customer']
    filtered_sf_data = sf_data[sf_data['TYPE'].isin(sf_customer_type)]
    return filtered_sf_data


def process_data():
    focus_data = read_data('domains/customer/DataFiles/FOCUS_SCHOOLS_DISTRICTS.csv')
    validated_focus_data = validate_focus_data(focus_data)
    
    sf_data = read_data('domains/customer/DataFiles/SF_ACCOUNTS.csv')
    filtered_sf_data = filter_sf_data(sf_data)
    NCES_data = read_data('domains/customer/DataFiles/NCES_PUBL_PRIV_POSTSEC_SCHOOL_LOCATIONS.csv')
    populate_NCES_data(validated_focus_data, filtered_sf_data, NCES_data)

def takeNCESIDFromSF(merged_df,focus_only_df):
    """
    This function takes dataframe as input and returns all the focus colum and only nces id column from the salesforce
    :param merged_df: combined dataframe for focus and salesforce customer
    :param focus_only_df: focus only df
    :return: dataframe
    """
    focus_columns = focus_only_df.columns.to_list()
    columns_to_be_included = focus_columns + ['NCES_ID__C'] 
    merged_df = merged_df[columns_to_be_included]
    return merged_df

def takeNCESData(merged_df,nces_df,left_key_col,right_key_col):
    """
    Use to join dataframe which has both focus and salesforce data with nces data using nces id.
    :param merged_df: dataframe containing focus data and nces id column from the salesforce data
    :param nces_df: dataframe containing only nces record
    :param left_key_col: col for left dataframe which will be used in join
    :param right_key_col: col for right dataframe which will be used in join
    :return: merged dataframe which has focus and nces data.
    """
    return pd.merge(left=merged_df, right=nces_df, how='left', left_on=f'{left_key_col}', right_on=f'{right_key_col}')

def main():
    process_data()

if __name__ == "__main__":
    main()


