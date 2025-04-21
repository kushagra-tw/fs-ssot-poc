import sys
sys.path.insert(0, "/Users/michaelbarnett/Desktop/clients/FirstStudent/fs-ssot-poc/")

import pandas as pd

import json

pd.set_option('display.max_columns', None)

def add_on_existing_db_ids(df, existing_db_df_path, intermediate_file_path):
        # merge intermediate with db export
        # existing_db_df = pd.read_csv('/Users/michaelbarnett/Desktop/clients/FirstStudent/fs-ssot-poc/domains/customer/DataFiles/school-export-2025-04-18-09-22.csv')
        existing_db_df = pd.read_csv(existing_db_df_path)
        # intermediate_file_df = pd.read_csv('/Users/michaelbarnett/Desktop/clients/FirstStudent/fs-ssot-poc/domains/customer/DataFiles/iterm_schools_20250331.csv')
        intermediate_file_df = pd.read_csv(intermediate_file_path)
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
        merged_df['focus_id_list'] = merged_df.apply(
            lambda row: json.loads(row['XREF_VALUE2']), axis=1
        )
        # XREF_SOURCESYSTEM2_y	XREF_KEYNAME2_y	XREF_VALUE2_y

        merged_df = merged_df.explode('focus_id_list')
        # print(merged_df['focus_id_list'].sort_values())


        # read our schools file
        # tw_schools = school_prettifier('outputs/schools/schools_0417_3.csv')

        tw_merged_df = pd.merge(df, merged_df, left_on=['FOCUS_ID'], right_on=['focus_id_list'], how='left')
        # tw_merged_df["clensed_nces_x"] = tw_merged_df.apply(
        #     lambda row: (row["MASTERPROPERTIES_NCESSCHOOLID_x"].zfill(13)[0:7] + row["MASTERPROPERTIES_NCESSCHOOLID_x"][-5:]) if len(row["MASTERPROPERTIES_NCESSCHOOLID_x"])>=12 else row["MASTERPROPERTIES_NCESSCHOOLID_x"], axis=1
        # )
        tw_merged_df["clensed_nces_y"] = tw_merged_df.apply(
            lambda row: str(row["MASTERPROPERTIES_NCESSCHOOLID_y"]).zfill(12) if len(row["MASTERPROPERTIES_NCESSCHOOLID_x"])>=12 else row["MASTERPROPERTIES_NCESSCHOOLID_x"], axis=1
        )
        # print(tw_merged_df.filter(items=["FOCUS_ID", "focus_id_list", "MASTERPROPERTIES_SCHOOLNAME_x", "MASTERPROPERTIES_ID", "clensed_nces_x", "MASTERPROPERTIES_NCESSCHOOLID_x", "MASTERPROPERTIES_NCESSCHOOLID_y", "MASTERPROPERTIES_ADDRESS_CITY_x"], axis=1)
        #     .loc[(tw_merged_df['focus_id_list']==tw_merged_df['focus_id_list'])]
        #     .loc[(tw_merged_df['clensed_nces_x']!=tw_merged_df['clensed_nces_y'])]
        # )

        creates = len(tw_merged_df
            .loc[(tw_merged_df['focus_id_list']!=tw_merged_df['focus_id_list'])] #i.e. no R-side focus id
        )
        updates = len(tw_merged_df
            .loc[(tw_merged_df['focus_id_list']==tw_merged_df['focus_id_list'])]
            .loc[(tw_merged_df['MASTERPROPERTIES_NCESSCHOOLID_x']!=tw_merged_df['MASTERPROPERTIES_NCESSCHOOLID_y'])]
        )

        print(f"Generating a file that will cause {creates} creates and {updates} updates.")

        # raise Exception("Hurrah!")
        # print("total matched records")
        # print(tw_merged_df['MASTERPROPERTIES_ID'].notnull().sum())
        # print("result df length")
        # print(len(tw_merged_df))

        # commented below line as we dont want to add our generated id to final dataset
        #tw_merged_df['MASTERPROPERTIES_ID'] = tw_merged_df['MASTERPROPERTIES_ID'].fillna(tw_merged_df['MASTERPROPERTIES_ID_1'])

        # using 3 as the xref value as ncessch is in xref 2
        tw_merged_df['XREF_SOURCESYSTEM3'] = tw_merged_df['XREF_SOURCESYSTEM2_y']
        tw_merged_df['XREF_KEYNAME3'] = tw_merged_df['XREF_KEYNAME2_y']
        tw_merged_df['XREF_VALUE3'] = tw_merged_df['XREF_VALUE2_y']


        tw_merged_df = tw_merged_df.drop(columns=[col for col in tw_merged_df.columns if col.endswith('_y')])
        tw_merged_df.columns = [col.replace('_x', '') for col in tw_merged_df.columns]

        return tw_merged_df


        # tw_merged_update = tw_merged_df[tw_merged_df['MASTERPROPERTIES_ID'].notnull()]
        # tw_merged_insert = tw_merged_df[tw_merged_df['MASTERPROPERTIES_ID'].isnull()]
        # print(tw_merged_update.head())
        # print("========== ============")
        # print(tw_merged_insert.head())