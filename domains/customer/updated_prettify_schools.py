import sys

# It's good practice to make paths like this configurable or relative
# For now, keeping the original path structure
sys.path.insert(0, "/Users/michaelbarnett/Desktop/clients/FirstStudent/fs-ssot-poc/")

from domains.customer.Reader import read_data
from domains.customer.generate_delta import add_on_existing_db_ids
import pandas as pd
from random import randint
import os

pd.set_option('display.max_columns', None)


def get_random_four_digits():
    return str(randint(0, 9999)).zfill(4)  # Corrected randint to ensure 4 digits are possible (0-9999)


def is_nan(field):
    # Check if field is NaN (Not a Number)
    return field != field


def school_prettifier_govt(schools_file_path):  # Renamed function for clarity
    school_type_to_code = {
        "Alternative School": "AS",
        "Career and Technical School": "CT",
        "Postsecondary": "PS",
        "Private": "PV",
        "Regular School": "PU",
        "Special Education School": "SE"
    }

    tw_data = read_data(schools_file_path)

    column_mapping = {
        "FOCUS_SCHOOL_ID": "FOCUS_ID",
        "NCES_NCESSCH": "MASTERPROPERTIES_GOVTSCHOOLID",  # NCES -> GOVT
        "NCES_LEAID": "MASTERPROPERTIES_GOVTSCHOOLDISTRICTID",  # NCES -> GOVT
        "NCES_SCH_NAME": "MASTERPROPERTIES_SCHOOLNAME",
        "NCES_NAME": "MASTERPROPERTIES_SCHOOLDISTRICTNAME",
        "NCES_SCH_TYPE_TEXT": "MASTERPROPERTIES_SCHOOLTYPE",
        "NCES_LEVEL": "MASTERPROPERTIES_SCHOOLLEVEL",
        "NCES_SY_STATUS_TEXT": "MASTERPROPERTIES_STARTYEARSTATUSCODE",
        "NCES_SCHOOL_YEAR": "MASTERPROPERTIES_SCHOOLYEAR",
        "NCES_STATE": "MASTERPROPERTIES_ADDRESS_STATEPROVINCE",
        "NCES_CITY": "MASTERPROPERTIES_ADDRESS_CITY",
        "NCES_STREET": "MASTERPROPERTIES_ADDRESS_ADDRESSLINE1",
        "NCES_LAT": "MASTERPROPERTIES_ADDRESS_LATITUDE",
        "NCES_LON": "MASTERPROPERTIES_ADDRESS_LONGITUDE",
        "FOCUS_SCHOOL_CODE": "MASTERPROPERTIES_ERSCODE"
    }

    renamed = tw_data.rename(columns=column_mapping)

    # Assuming these paths are correct and accessible
    # Consider making these paths parameters to the function or configurable
    renamed = add_on_existing_db_ids(
        df=renamed,
        existing_db_df_path='/Users/michaelbarnett/Desktop/clients/FirstStudent/fs-ssot-poc/domains/customer/DataFiles/school-export-2025-04-22-15-54.csv',
        intermediate_file_path='/Users/michaelbarnett/Desktop/clients/FirstStudent/fs-ssot-poc/domains/customer/DataFiles/iterm_schools_20250331.csv'
    )

    renamed["MASTERPROPERTIES_GOVTDATASET"] = "STATIC_NCES_DOWNLOAD"  # NCES -> GOVT in column name
    # The value "STATIC_NCES_DOWNLOAD" is kept as is, assuming it's data.
    # If this value also needs to change to "STATIC_GOVT_DOWNLOAD", that would be an additional modification.

    renamed["MASTERPROPERTIES_ADDRESS_TYPE"] = "Location"
    renamed["MASTERPROPERTIES_ADDRESS_COUNTRY"] = "USA"

    renamed["MASTERPROPERTIES_ADDRESS_POSTALCODE"] = renamed.apply(
        lambda row: str(row["NCES_ZIP"]).zfill(5) if not is_nan(row["NCES_ZIP"]) else row["NCES_ZIP"],
        axis=1)  # Ensure NCES_ZIP is string

    # Apply transformation to the newly named GOVTSCHOOLDISTRICTID column
    renamed["MASTERPROPERTIES_GOVTSCHOOLDISTRICTID"] = renamed.apply(
        lambda row: str(int(row["MASTERPROPERTIES_GOVTSCHOOLDISTRICTID"])) if not is_nan(
            row["MASTERPROPERTIES_GOVTSCHOOLDISTRICTID"]) else row["MASTERPROPERTIES_GOVTSCHOOLDISTRICTID"], axis=1)

    renamed["MASTERPROPERTIES_ID_1"] = renamed.apply(
        lambda row: f"{get_random_four_digits()}-{get_random_four_digits()}-{get_random_four_digits()}", axis=1)

    renamed["MASTERPROPERTIES_ID_2"] = renamed.apply(
        lambda
            row: f"US{school_type_to_code.get(row['MASTERPROPERTIES_SCHOOLTYPE'], 'XX')}-{row['MASTERPROPERTIES_ID_1'][5:9]}-{row['MASTERPROPERTIES_ID_1'][10:14]}"
        if not is_nan(row['MASTERPROPERTIES_SCHOOLTYPE']) and row['MASTERPROPERTIES_SCHOOLTYPE'] in school_type_to_code
        else f"USXX-{row['MASTERPROPERTIES_ID_1'][5:9]}-{row['MASTERPROPERTIES_ID_1'][10:14]}",
        # Handle missing school types
        axis=1)

    renamed["XREF_SOURCESYSTEM1"] = "nces"  # Source system name kept as 'nces' as it refers to the origin
    renamed["XREF_KEYNAME1"] = "schid"
    renamed["XREF_VALUE1"] = renamed.apply(
        lambda row: row["NCES_SCHID"], axis=1)  # Uses original source column

    renamed["XREF_SOURCESYSTEM2"] = "nces"  # Source system name kept as 'nces'
    renamed["XREF_KEYNAME2"] = "ncessch"  # Key name kept as 'ncessch'
    renamed["XREF_VALUE2"] = renamed.apply(
        lambda row: row["MASTERPROPERTIES_GOVTSCHOOLID"], axis=1)  # Uses the new GOVT column

    renamed["RELATION_ENTITY1"] = "customer"
    renamed["RELATION_TYPE1"] = renamed.apply(
        lambda row: "child" if not is_nan(row["MASTERPROPERTIES_GOVTSCHOOLDISTRICTID"]) else "projection", axis=1)
    renamed["RELATION_ID1"] = renamed.apply(
        lambda row: row["MASTERPROPERTIES_GOVTSCHOOLDISTRICTID"] if not is_nan(
            row["MASTERPROPERTIES_GOVTSCHOOLDISTRICTID"]) else row["NCES_SCHID"], axis=1)

    blank_columns = [
        "TECHNICALPROPERTIES_CREATESYSTEM",
        "TECHNICALPROPERTIES_CREATETIMESTAMP",
        "TECHNICALPROPERTIES_UPDATESYSTEM",
        "TECHNICALPROPERTIES_UPDATETIMESTAMP",
        "TECHNICALPROPERTIES_DELETESYSTEM",
        "TECHNICALPROPERTIES_DELETETIMESTAMP",
        "TECHNICALPROPERTIES_DELETEFLAG",
        "TECHNICALPROPERTIES_VERSION",
    ]
    for blank_column in blank_columns:
        renamed[blank_column] = ""

    # Update the final list of columns to reflect GOVT changes
    final_column_order = [
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
        "MASTERPROPERTIES_ID",  # Assuming this is added by add_on_existing_db_ids
        "MASTERPROPERTIES_GOVTSCHOOLID",  # NCES -> GOVT
        "MASTERPROPERTIES_GOVTSCHOOLDISTRICTID",  # NCES -> GOVT
        "MASTERPROPERTIES_SCHOOLNAME",
        "MASTERPROPERTIES_SCHOOLDISTRICTNAME",
        "MASTERPROPERTIES_SCHOOLTYPE",
        "MASTERPROPERTIES_SCHOOLLEVEL",
        "MASTERPROPERTIES_GOVTDATASET",  # NCES -> GOVT
        "MASTERPROPERTIES_STARTYEARSTATUSCODE",
        "MASTERPROPERTIES_SCHOOLYEAR",
        "MASTERPROPERTIES_ERSCODE",
        "MASTERPROPERTIES_ADDRESS_TYPE",
        "MASTERPROPERTIES_ADDRESS_POSTALCODE",
        "MASTERPROPERTIES_ADDRESS_COUNTRY",
        "MASTERPROPERTIES_ADDRESS_STATEPROVINCE",
        "MASTERPROPERTIES_ADDRESS_CITY",
        "MASTERPROPERTIES_ADDRESS_ADDRESSLINE1",
        "MASTERPROPERTIES_ADDRESS_ADDRESSLINE2",  # This column was in the original list, ensure it exists or handle
        "MASTERPROPERTIES_ADDRESS_LATITUDE",
        "MASTERPROPERTIES_ADDRESS_LONGITUDE",
        "XREF_SOURCESYSTEM1",
        "XREF_KEYNAME1",
        "XREF_VALUE1",
        "XREF_SOURCESYSTEM2",
        "XREF_KEYNAME2",
        "XREF_VALUE2",
        "XREF_SOURCESYSTEM3",  # These XREF3 columns were in the original list
        "XREF_KEYNAME3",
        "XREF_VALUE3",
        "RELATION_ENTITY1",
        "RELATION_TYPE1",
        "RELATION_ID1"
    ]

    # Ensure all columns in final_column_order exist in 'renamed' DataFrame
    # Add missing columns as empty if necessary, to prevent errors during reordering
    for col in final_column_order:
        if col not in renamed.columns:
            renamed[col] = ""  # Or pd.NA or None, depending on desired empty value

    reordered = renamed.filter(items=final_column_order, axis=1)

    return reordered


if __name__ == "__main__":
    # Example of how to run the script
    # Ensure FILE_DATE_SUFFIX environment variable is set or provide a default
    file_date_suffix = os.environ.get("FILE_DATE_SUFFIX", "test_date")

    # Create dummy input file for testing if it doesn't exist
    input_file_path = f'outputs/schools/schools_{file_date_suffix}.csv'
    output_file_path = f'outputs/schools/pretty_schools_govt_{file_date_suffix}.csv'  # Added _govt to output

    # Create dummy directory if it doesn't exist
    os.makedirs('outputs/schools', exist_ok=True)

    # Create a dummy input CSV for the script to run if it's missing
    # This is just for basic script execution, content should match expected schema
    if not os.path.exists(input_file_path):
        print(f"Warning: Input file {input_file_path} not found. Creating a dummy file.")
        dummy_data = {
            "FOCUS_SCHOOL_ID": [1, 2], "NCES_NCESSCH": ["N001", "N002"], "NCES_LEAID": ["L01", "L02"],
            "NCES_SCH_NAME": ["School A", "School B"], "NCES_NAME": ["District A", "District B"],
            "NCES_SCH_TYPE_TEXT": ["Regular School", "Private"], "NCES_LEVEL": ["Elementary", "High"],
            "NCES_SY_STATUS_TEXT": ["Open", "Open"], "NCES_SCHOOL_YEAR": ["2023-2024", "2023-2024"],
            "NCES_STATE": ["CA", "NY"], "NCES_CITY": ["CityA", "CityB"], "NCES_STREET": ["1st St", "2nd Ave"],
            "NCES_LAT": [34.05, 40.71], "NCES_LON": [-118.24, -74.00], "FOCUS_SCHOOL_CODE": ["E001", "E002"],
            "NCES_ZIP": ["90001", "10001"], "NCES_SCHID": ["S001", "S002"]
        }
        pd.DataFrame(dummy_data).to_csv(input_file_path, index=False)

    # Assuming add_on_existing_db_ids and read_data are correctly implemented and accessible
    # For the script to run standalone without these domain functions, they would need to be mocked or simplified.
    # For now, this script structure assumes those imports work.
    try:
        print(f"Processing file: {input_file_path}")
        prettified_df = school_prettifier_govt(input_file_path)
        prettified_df.to_csv(output_file_path, index=False)
        print(f"Successfully created: {output_file_path}")
        print("\nFirst 5 rows of the prettified output:")
        print(prettified_df.head())
    except ImportError as e:
        print(f"ImportError: {e}. Make sure the domain modules are in the Python path.")
        print("This script snippet may not run standalone without its dependency 'domains.customer'.")
    except FileNotFoundError as e:
        print(f"FileNotFoundError: {e}. Check paths for data files.")
    except Exception as e:
        print(f"An error occurred: {e}")