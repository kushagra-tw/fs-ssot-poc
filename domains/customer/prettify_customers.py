import sys
sys.path.insert(0, "/Users/michaelbarnett/Desktop/clients/FirstStudent/fs-ssot-poc/")

from domains.customer.Reader import read_data

def is_nan(field):
    return field != field

def customer_prettifier(customer_file_path):

    tw_data = read_data(
        customer_file_path)
    
    # TODO: Ask how to get website???

    column_mapping = {
        "NCES_LEAID": "MASTERPROPERTIES_NCESSCHOOLDISTRICTID",
        "NCES_NAME": "MASTERPROPERTIES_NAME",
        "NCES_STATE.1": "MASTERPROPERTIES_ADDRESS_STATEPROVINCE",
        "NCES_CITY.1": "MASTERPROPERTIES_ADDRESS_CITY",
        "NCES_STREET.1": "MASTERPROPERTIES_ADDRESS_ADDRESSLINE1",
    }

    renamed = tw_data.rename(columns=column_mapping)

    renamed["MASTERPROPERTIES_ADDRESS_POSTALCODE"] = renamed.apply(
            lambda row: str(row["NCES_ZIP.1"]).zfill(5), axis=1)


    renamed["MASTERPROPERTIES_RECORDTYPE"] = "Student Contract Record Type"
    renamed["MASTERPROPERTIES_STATUS"] = "Customer"
    renamed["MASTERPROPERTIES_ENTITYTYPE"] = "Regular School District"
    renamed["MASTERPROPERTIES_ADDRESS_TYPE"] = "primary"
    renamed["MASTERPROPERTIES_ADDRESS_COUNTRY"] = "USA"

    renamed["XREF_SOURCESYSTEM1"] = "nces"
    renamed["XREF_KEYNAME1"] = renamed.apply(
            lambda row: "schid" if is_nan(row["MASTERPROPERTIES_NCESSCHOOLDISTRICTID"]) else "leaid", axis=1)
    renamed["XREF_VALUE1"] = renamed.apply(
            lambda row: row["NCES_SCHID"] if is_nan(row["MASTERPROPERTIES_NCESSCHOOLDISTRICTID"]) else row["MASTERPROPERTIES_NCESSCHOOLDISTRICTID"], axis=1)
    

    renamed["RELATION_ENTITY1"] = renamed.apply(
            lambda row: "school" if is_nan(row["MASTERPROPERTIES_NCESSCHOOLDISTRICTID"]) else "school_district", axis=1)
    renamed["RELATION_TYPE1"] = "projection"
    renamed["RELATION_ID1"] = renamed.apply(
            lambda row: row["NCES_SCHID"] if is_nan(row["MASTERPROPERTIES_NCESSCHOOLDISTRICTID"]) else row["MASTERPROPERTIES_NCESSCHOOLDISTRICTID"], axis=1)


    #TODO: what to do here??
    for blank_column in [
        "TECHNICALPROPERTIES_CREATESYSTEM",
        "TECHNICALPROPERTIES_CREATETIMESTAMP",
        "TECHNICALPROPERTIES_UPDATESYSTEM",
        "TECHNICALPROPERTIES_UPDATETIMESTAMP",
        "TECHNICALPROPERTIES_DELETESYSTEM",
        "TECHNICALPROPERTIES_DELETETIMESTAMP",
        "TECHNICALPROPERTIES_DELETEFLAG",
        "TECHNICALPROPERTIES_VERSION",
        "MASTERPROPERTIES_ADDRESSLINE2",
        "MASTERPROPERTIES_CONTACT_FULLNAME",
        "MASTERPROPERTIES_CONTACT_TITLE",
        "MASTERPROPERTIES_CONTACT_PHONE",
        "MASTERPROPERTIES_CONTACT_EMAIL",
        "MASTERPROPERTIES_CONTACT_ISFORMER",
        ]:

        renamed[blank_column] = ""


    reordered = renamed.filter(items=[
        "TECHNICALPROPERTIES_CREATESYSTEM",
        "TECHNICALPROPERTIES_CREATETIMESTAMP",
        "TECHNICALPROPERTIES_UPDATESYSTEM",
        "TECHNICALPROPERTIES_UPDATETIMESTAMP",
        "TECHNICALPROPERTIES_DELETESYSTEM",
        "TECHNICALPROPERTIES_DELETETIMESTAMP",
        "TECHNICALPROPERTIES_DELETEFLAG",
        "TECHNICALPROPERTIES_VERSION",
        "MASTERPROPERTIES_ID",
        "MASTERPROPERTIES_NAME",
        "MASTERPROPERTIES_RECORDTYPE",
        "MASTERPROPERTIES_STATUS",
        "MASTERPROPERTIES_ENTITYTYPE",
        "MASTERPROPERTIES_WEBSITE",
        "MASTERPROPERTIES_ADDRESS_TYPE",
        "MASTERPROPERTIES_ADDRESS_POSTALCODE",
        "MASTERPROPERTIES_ADDRESS_COUNTRY",
        "MASTERPROPERTIES_ADDRESS_STATEPROVINCE",
        "MASTERPROPERTIES_ADDRESS_CITY",
        "MASTERPROPERTIES_ADDRESSLINE1",
        "MASTERPROPERTIES_ADDRESSLINE2",
        "MASTERPROPERTIES_CONTACT_FULLNAME",
        "MASTERPROPERTIES_CONTACT_TITLE",
        "MASTERPROPERTIES_CONTACT_PHONE",
        "MASTERPROPERTIES_CONTACT_EMAIL",
        "MASTERPROPERTIES_CONTACT_ISFORMER",
        "XREF_SOURCESYSTEM1",
        "XREF_KEYNAME1",
        "XREF_VALUE1",
        "RELATION_ENTITY1",
        "RELATION_TYPE1",
        "RELATION_ID1"
    ])

    print(reordered.head())
    return reordered

customer_prettifier("outputs/customers/customers_0417_2.csv") \
    .to_csv('outputs/customers/pretty_customers_0417_2.csv')