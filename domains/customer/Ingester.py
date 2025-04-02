
import pandas as pd
from Reader import read_data
from NCES import add_NCES_info, fetch_NCES_Id_from_NCES
from SF import fetch_NCES_Id_from_SF

def populate_nces_data(focus_data):
    for index, focus_row in focus_data.iterrows():
        focus_district = focus_row['SCHOOL_DISTRICT_NAME']
        nces_id = fetch_NCES_Id_from_SF(focus_district)
        if nces_id is None:
            nces_id = fetch_NCES_Id_from_NCES(focus_district)            
        if nces_id is not None:
            add_NCES_info(nces_id, focus_row)
        
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

def process_data():
    focus_file = 'domains/customer/DataFiles/FOCUS_SCHOOLS_DISTRICTS.csv'
    focus_data = read_data( focus_file)
    validated_focus_data = validate_focus_data(focus_data)
    
    
    populate_nces_data(validated_focus_data)

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


