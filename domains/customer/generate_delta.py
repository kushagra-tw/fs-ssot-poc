
import pandas as pd

from domains.customer.prettify_schools import school_prettifier

pd.set_option('display.max_columns', None)

# merge intermediate with db export
existing_db_df = pd.read_csv('/Users/kirtanshah/Documents/school-export-2025-04-18-09-22.csv')
intermediate_file_df = pd.read_csv('/Users/kirtanshah/Documents/schools_20250331.csv')
# print(existing_db_df.head())
# print(intermediate_file_df.head())
print("records in existing " + str(len(existing_db_df)))
print("records in intermediate " + str(len(intermediate_file_df)))

print("distinct school id in existing " + str(len(existing_db_df['MASTERPROPERTIES_NCESSCHOOLID'].unique())))
grouped_counts = existing_db_df.groupby('MASTERPROPERTIES_NCESSCHOOLID').size()
filtered_counts = grouped_counts[grouped_counts > 1]
print(filtered_counts)
print("distinct school id in intermediate " + str(len(intermediate_file_df['MASTERPROPERTIES_NCESSCHOOLID'].unique())))



#merged_df = pd.merge(existing_db_df, intermediate_file_df, on='MASTERPROPERTIES_NCESSCHOOLID')
#2156
merged_df = pd.merge(existing_db_df, intermediate_file_df, on=['MASTERPROPERTIES_NCESSCHOOLID'])
# , 'XREF_VALUE1'
print("records in merged " + str(len(merged_df)))
merged_df.head(10)


# copy the values of focus id from intermediate file to db export df

merged_df.drop(columns=['XREF_SOURCESYSTEM2_x', 'XREF_KEYNAME2_x', 'XREF_VALUE2_x'], inplace=True)
merged_df.rename(columns={'XREF_SOURCESYSTEM2_y': 'XREF_SOURCESYSTEM2_x',
                         'XREF_KEYNAME2_y': 'XREF_KEYNAME2_x',
                          'XREF_VALUE2_y': 'XREF_VALUE2_x',
                         }, inplace=True)
merged_df = merged_df.drop(columns=[col for col in merged_df.columns if col.endswith('_y')])
merged_df.columns = [col.replace('_x', '') for col in merged_df.columns]
# XREF_SOURCESYSTEM2_y	XREF_KEYNAME2_y	XREF_VALUE2_y


# read our schools file
tw_schools = school_prettifier('outputs/schools/schools_0417_3.csv')

tw_merged_df = pd.merge(tw_schools, merged_df, on=['MASTERPROPERTIES_NCESSCHOOLID'], how='left')
print("total matched records")
print(tw_merged_df['MASTERPROPERTIES_ID'].notnull().sum())
print("result df length")
print(len(tw_merged_df))

# commented below line as we dont want to add our generated id to final dataset
#tw_merged_df['MASTERPROPERTIES_ID'] = tw_merged_df['MASTERPROPERTIES_ID'].fillna(tw_merged_df['MASTERPROPERTIES_ID_1'])

# using 3 as the xref value as ncessch is in xref 2
tw_merged_df['XREF_SOURCESYSTEM3'] = tw_merged_df['XREF_SOURCESYSTEM2_y']
tw_merged_df['XREF_KEYNAME3'] = tw_merged_df['XREF_KEYNAME2_y']
tw_merged_df['XREF_VALUE3'] = tw_merged_df['XREF_VALUE2_y']


tw_merged_df = tw_merged_df.drop(columns=[col for col in tw_merged_df.columns if col.endswith('_y')])
tw_merged_df.columns = [col.replace('_x', '') for col in tw_merged_df.columns]


tw_merged_update = tw_merged_df[tw_merged_df['MASTERPROPERTIES_ID'].notnull()]
tw_merged_insert = tw_merged_df[tw_merged_df['MASTERPROPERTIES_ID'].isnull()]
print(tw_merged_update.head())
print("========== ============")
print(tw_merged_insert.head())