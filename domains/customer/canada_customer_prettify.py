import pandas as pd # Assuming pandas is used by read_data or will be used
import numpy as np # For np.nan if truly null values are desired over empty strings

# def is_nan(field): # This function is already in the script
# return field != field

def odef_customer_prettifier_v2(customer_file_path):
    # Assuming read_data can handle CSVs directly or it's a pandas DataFrame
    odef_data = pd.read_csv(customer_file_path, encoding='utf-8-sig')

    # Clean up column names if necessary (e.g., remove leading/trailing spaces)
    odef_data.columns = odef_data.columns.str.strip()

    # Initialize a new DataFrame for the target structure
    # Updated target columns list with MASTERPROPERTIES_GOVTSCHOOLDISTRICTID
    target_columns = [
        "TECHNICALPROPERTIES_CREATESYSTEM", "TECHNICALPROPERTIES_CREATETIMESTAMP",
        "TECHNICALPROPERTIES_UPDATESYSTEM", "TECHNICALPROPERTIES_UPDATETIMESTAMP",
        "TECHNICALPROPERTIES_DELETESYSTEM", "TECHNICALPROPERTIES_DELETETIMESTAMP",
        "TECHNICALPROPERTIES_DELETEFLAG", "TECHNICALPROPERTIES_VERSION",
        "MASTERPROPERTIES_ID", "MASTERPROPERTIES_GOVTSCHOOLDISTRICTID", # Changed from NCESSCHOOLDISTRICTID
        "MASTERPROPERTIES_NAME", "MASTERPROPERTIES_RECORDTYPE",
        "MASTERPROPERTIES_STATUS", "MASTERPROPERTIES_ENTITYTYPE",
        "MASTERPROPERTIES_WEBSITE", "MASTERPROPERTIES_ADDRESS_TYPE",
        "MASTERPROPERTIES_ADDRESS_POSTALCODE", "MASTERPROPERTIES_ADDRESS_COUNTRY",
        "MASTERPROPERTIES_ADDRESS_STATEPROVINCE", "MASTERPROPERTIES_ADDRESS_CITY",
        "MASTERPROPERTIES_ADDRESSLINE1", "MASTERPROPERTIES_ADDRESSLINE2",
        "MASTERPROPERTIES_CONTACT_FULLNAME", "MASTERPROPERTIES_CONTACT_TITLE",
        "MASTERPROPERTIES_CONTACT_PHONE", "MASTERPROPERTIES_CONTACT_EMAIL",
        "MASTERPROPERTIES_CONTACT_ISFORMER", "XREF_SOURCESYSTEM1",
        "XREF_KEYNAME1", "XREF_VALUE1", "RELATION_ENTITY1",
        "RELATION_TYPE1", "RELATION_ID1"
    ]
    renamed = pd.DataFrame(columns=target_columns)

    # Direct Mappings from ODEF to Target
    renamed["MASTERPROPERTIES_NAME"] = odef_data["ODEF_authority_name"]
    renamed["MASTERPROPERTIES_ADDRESS_STATEPROVINCE"] = odef_data["ODEF_province_code"]
    renamed["MASTERPROPERTIES_ID"] = ''
    # New mapping for GOVTSCHOOLDISTRICTID
    renamed["MASTERPROPERTIES_GOVTSCHOOLDISTRICTID"] = odef_data["authority_hash_12"]


    # Hardcoded/Default Values
    renamed["MASTERPROPERTIES_RECORDTYPE"] = "Student Contract Record Type"
    renamed["MASTERPROPERTIES_STATUS"] = "Customer"
    renamed["MASTERPROPERTIES_ENTITYTYPE"] = "Regular School District" # Assumption: This is appropriate for ODEF entities
    renamed["MASTERPROPERTIES_ADDRESS_TYPE"] = "primary"
    renamed["MASTERPROPERTIES_ADDRESS_COUNTRY"] = "CAN"

    # XREF Fields - updated to use authority_hash_12
    renamed["XREF_SOURCESYSTEM1"] = "odef"
    renamed["XREF_KEYNAME1"] = "authority_hash" # Key name is the hash column
    renamed["XREF_VALUE1"] = odef_data["authority_hash_12"]

    # Relation Fields - updated to use authority_hash_12
    renamed["RELATION_ENTITY1"] = "school_district"
    renamed["RELATION_TYPE1"] = "projection"
    renamed["RELATION_ID1"] = odef_data["authority_hash_12"]

    # Fields to be kept null/empty with TODO comments as no source from ODEF
    # Technical Properties
    for col in [
        "TECHNICALPROPERTIES_CREATESYSTEM", "TECHNICALPROPERTIES_CREATETIMESTAMP",
        "TECHNICALPROPERTIES_UPDATESYSTEM", "TECHNICALPROPERTIES_UPDATETIMESTAMP",
        "TECHNICALPROPERTIES_DELETESYSTEM", "TECHNICALPROPERTIES_DELETETIMESTAMP",
        "TECHNICALPROPERTIES_DELETEFLAG", "TECHNICALPROPERTIES_VERSION"
    ]:
        renamed[col] = "" # Or np.nan if preferred for null

    # TODO: Website not available in ODEF_canada_customers_None.csv. Set to null.
    renamed["MASTERPROPERTIES_WEBSITE"] = np.nan # Or ""

    # TODO: Postal Code not available in ODEF_canada_customers_None.csv. Set to null.
    renamed["MASTERPROPERTIES_ADDRESS_POSTALCODE"] = np.nan # Or ""

    # TODO: City not available in ODEF_canada_customers_None.csv. Set to null.
    renamed["MASTERPROPERTIES_ADDRESS_CITY"] = np.nan # Or ""

    # TODO: Address Line 1 not available in ODEF_canada_customers_None.csv. Set to null.
    renamed["MASTERPROPERTIES_ADDRESSLINE1"] = np.nan # Or ""

    # TODO: Address Line 2 not available in ODEF_canada_customers_None.csv. Set to null.
    renamed["MASTERPROPERTIES_ADDRESSLINE2"] = np.nan # Or ""

    # Contact fields - all null as no source in ODEF
    contact_fields = [
        "MASTERPROPERTIES_CONTACT_FULLNAME", "MASTERPROPERTIES_CONTACT_TITLE",
        "MASTERPROPERTIES_CONTACT_PHONE", "MASTERPROPERTIES_CONTACT_EMAIL",
        "MASTERPROPERTIES_CONTACT_ISFORMER"
    ]
    for col in contact_fields:
        # TODO: Contact field ({col}) not available in ODEF_canada_customers_None.csv. Set to null.
        renamed[col] = np.nan # Or ""

    # Ensure order of columns is as per the original script (with the renamed column)
    reordered = renamed.filter(items=target_columns)

    # print(reordered.head())
    return reordered

# Example of how to call this new function:
# Make sure 'canada_customers_None.csv' is in the correct path and has the 'authority_hash_12' column
odef_customer_prettifier_v2('/Users/kirtanshah/PycharmProjects/fs-ssot-poc/customer/data/interim/canada_customers_None.csv') \
.to_csv('/Users/kirtanshah/PycharmProjects/fs-ssot-poc/customer/data/processed/pretty_canada_customers_v2.csv', index=False, encoding='utf-8')