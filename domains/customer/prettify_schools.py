import sys
sys.path.insert(0, "/Users/michaelbarnett/Desktop/clients/FirstStudent/fs-ssot-poc/")

from domains.customer.Reader import read_data
from domains.customer.generate_delta import add_on_existing_db_ids
import pandas as pd

from random import randint

pd.set_option('display.max_columns', None)

def get_random_four_digits():
    return str(randint(0, 10000)).zfill(4)


def is_nan(field):
    return field != field

def school_prettifier(schools_file_path):
    school_type_to_code = {
        "Alternative School": "AS",
        "Career and Technical School": "CT",
        "Postsecondary": "PS",
        "Private": "PV",
        "Regular School": "PU",
        "Special Education School": "SE"
    }

    tw_data = read_data(
        schools_file_path)

    column_mapping = {
        "FOCUS_SCHOOL_ID": "FOCUS_ID",
        "NCES_NCESSCH": "MASTERPROPERTIES_NCESSCHOOLID",
        "NCES_LEAID": "MASTERPROPERTIES_NCESSCHOOLDISTRICTID",
        "NCES_SCH_NAME": "MASTERPROPERTIES_SCHOOLNAME",
        "NCES_NAME": "MASTERPROPERTIES_SCHOOLDISTRICTNAME",
        "NCES_SCH_TYPE_TEXT": "MASTERPROPERTIES_SCHOOLTYPE",
        "NCES_LEVEL": "MASTERPROPERTIES_SCHOOLLEVEL",
        "NCES_SY_STATUS_TEXT": "MASTERPROPERTIES_STARTYEARSTATUSCODE",
        "NCES_SCHOOL_YEAR": "MASTERPROPERTIES_SCHOOLYEAR",
        # "NCES_ZIP": "MASTERPROPERTIES_ADDRESS_POSTALCODE",
        "NCES_STATE": "MASTERPROPERTIES_ADDRESS_STATEPROVINCE",
        "NCES_CITY": "MASTERPROPERTIES_ADDRESS_CITY",
        "NCES_STREET": "MASTERPROPERTIES_ADDRESS_ADDRESSLINE1",
        "NCES_LAT": "MASTERPROPERTIES_ADDRESS_LATITUDE",
        "NCES_LON": "MASTERPROPERTIES_ADDRESS_LONGITUDE",
        "FOCUS_SCHOOL_CODE": "MASTERPROPERTIES_ERSCODE"
    }

    # consolidated = tw_data.filter(items=column_mapping.keys())
    renamed = tw_data.rename(columns=column_mapping)



    renamed["MASTERPROPERTIES_NCESDATASET"] = "STATIC_NCES_DOWNLOAD"
    # renamed["MASTERPROPERTIES_ERSCODE"] = ""
    renamed["MASTERPROPERTIES_ADDRESSTYPE"] = "Location"
    renamed["MASTERPROPERTIES_COUNTRY"] = "USA"
    renamed["MASTERPROPERTIES_ADDRESSLINE2"] = "USA"

    renamed["MASTERPROPERTIES_ADDRESS_POSTALCODE"] = renamed.apply(
            lambda row: row["NCES_ZIP"].zfill(5), axis=1)

    renamed["MASTERPROPERTIES_ID_1"] = renamed.apply(
            lambda row: f"{get_random_four_digits()}-{get_random_four_digits()}-{get_random_four_digits()}", axis=1)

    renamed["MASTERPROPERTIES_ID_2"] = renamed.apply(
        lambda row: f"US{school_type_to_code[row['MASTERPROPERTIES_SCHOOLTYPE']]}-{row['MASTERPROPERTIES_ID_1'][5:9]}-{row['MASTERPROPERTIES_ID_1'][10:14]}", axis=1)

    renamed["XREF_SOURCESYSTEM1"] = "nces"
    renamed["XREF_KEYNAME1"] = "schid"
    renamed["XREF_VALUE1"] = renamed.apply(
            lambda row: row["NCES_SCHID"], axis=1)

    renamed["XREF_SOURCESYSTEM2"] = "nces"
    renamed["XREF_KEYNAME2"] = "ncessch"
    renamed["XREF_VALUE2"] = renamed.apply(
            lambda row: row["MASTERPROPERTIES_NCESSCHOOLID"], axis=1)

    renamed["RELATION_ENTITY1"] = "customer"
    renamed["RELATION_TYPE1"] = renamed.apply(
            lambda row: "child" if not is_nan(row["MASTERPROPERTIES_NCESSCHOOLDISTRICTID"]) else "projection", axis=1)
    renamed["RELATION_ID1"] = renamed.apply(
            lambda row: row["MASTERPROPERTIES_NCESSCHOOLDISTRICTID"] if not is_nan(row["MASTERPROPERTIES_NCESSCHOOLDISTRICTID"]) else row["NCES_SCHID"], axis=1)


    for blank_column in [
        "TECHNICALPROPERTIES_CREATESYSTEM",
        "TECHNICALPROPERTIES_CREATETIMESTAMP",
        "TECHNICALPROPERTIES_UPDATESYSTEM",
        "TECHNICALPROPERTIES_UPDATETIMESTAMP",
        "TECHNICALPROPERTIES_DELETESYSTEM",
        "TECHNICALPROPERTIES_DELETETIMESTAMP",
        "TECHNICALPROPERTIES_DELETEFLAG",
        "TECHNICALPROPERTIES_VERSION",
        ]:

        renamed[blank_column] = ""

    renamed = add_on_existing_db_ids(
        df=renamed,
        existing_db_df_path='/Users/michaelbarnett/Desktop/clients/FirstStudent/fs-ssot-poc/domains/customer/DataFiles/school-export-2025-04-18-09-22.csv',
        intermediate_file_path='/Users/michaelbarnett/Desktop/clients/FirstStudent/fs-ssot-poc/domains/customer/DataFiles/iterm_schools_20250331.csv'
    )

    reordered = renamed.filter(items=[
        "TECHNICALPROPERTIES_CREATESYSTEM",
        "TECHNICALPROPERTIES_CREATETIMESTAMP",
        "TECHNICALPROPERTIES_UPDATESYSTEM",
        "TECHNICALPROPERTIES_UPDATETIMESTAMP",
        "TECHNICALPROPERTIES_DELETESYSTEM",
        "TECHNICALPROPERTIES_DELETETIMESTAMP",
        "TECHNICALPROPERTIES_DELETEFLAG",
        "TECHNICALPROPERTIES_VERSION",
        "MASTERPROPERTIES_ID_1",
        "MASTERPROPERTIES_ID_2",
        "FOCUS_ID",
        "MASTERPROPERTIES_ID",
        "MASTERPROPERTIES_NCESSCHOOLID",
        "MASTERPROPERTIES_NCESSCHOOLDISTRICTID",
        "MASTERPROPERTIES_SCHOOLNAME",
        "MASTERPROPERTIES_SCHOOLDISTRICTNAME",
        "MASTERPROPERTIES_SCHOOLTYPE",
        "MASTERPROPERTIES_SCHOOLLEVEL",
        "MASTERPROPERTIES_NCESDATASET",
        "MASTERPROPERTIES_STARTYEARSTATUSCODE",
        "MASTERPROPERTIES_SCHOOLYEAR",
        "MASTERPROPERTIES_ERSCODE",
        "MASTERPROPERTIES_ADDRESS_TYPE",
        "MASTERPROPERTIES_ADDRESS_POSTALCODE",
        "MASTERPROPERTIES_ADDRESS_COUNTRY"
        "MASTERPROPERTIES_ADDRESS_STATEPROVINCE",
        "MASTERPROPERTIES_ADDRESS_CITY",
        "MASTERPROPERTIES_ADDRESS_ADDRESSLINE1",
        "MASTERPROPERTIES_ADDRESS_ADDRESSLINE2",
        "MASTERPROPERTIES_ADDRESS_LATITUDE",
        "MASTERPROPERTIES_ADDRESS_LONGITUDE",
        "XREF_SOURCESYSTEM1",
        "XREF_KEYNAME1",
        "XREF_VALUE1",
        "XREF_SOURCESYSTEM2",
        "XREF_KEYNAME2",
        "XREF_VALUE2",
        "XREF_SOURCESYSTEM3",
        "XREF_KEYNAME3",
        "XREF_VALUE3",
        "RELATION_ENTITY1",
        "RELATION_TYPE1",
        "RELATION_ID1"
    ])

    print(reordered.head(10))
    return reordered
    #reordered.to_csv('20250410_tw_schools.csv')

school_prettifier('outputs/schools/schools_0421_1.csv')