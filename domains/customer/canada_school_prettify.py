import sys
import pandas as pd
from random import randint
import os
import re  # For text processing

# Placeholder for custom module imports, adjust if needed for your environment
# Example: If you have a central 'Reader' or 'generate_delta' module you want to use
# sys.path.insert(0, "/path/to/your/custom/modules/")
# from domains.customer.Reader import read_data
# from domains.customer.generate_delta import add_on_existing_db_ids # This will be skipped

pd.set_option('display.max_columns', None)  # Show all columns when printing DataFrames
pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('future.no_silent_downcasting', True) # Opt-in to future behavior


def get_random_four_digits():
    return str(randint(0, 9999)).zfill(4)


def is_nan(value):
    """
    Checks if a value is NaN (works for float NaN), None, or an empty/whitespace-only string.
    """
    if value is None:
        return True
    if isinstance(value, float) and value != value:  # Standard NaN check for float
        return True
    if isinstance(value, str) and not value.strip():  # Empty string or only whitespace
        return True
    return False


def coalesce_fields(row, primary_col, secondary_col):
    """
    Returns the value from the primary column if present and not 'nan-like'.
    Otherwise, returns the value from the secondary column if present and not 'nan-like'.
    Returns an empty string if neither is suitable.
    Ensures string values are stripped of leading/trailing whitespace.
    """
    primary_val_raw = row.get(primary_col)
    if not is_nan(primary_val_raw):
        return str(primary_val_raw).strip() if not (
                    isinstance(primary_val_raw, float) and primary_val_raw != primary_val_raw) else primary_val_raw

    secondary_val_raw = row.get(secondary_col)
    if not is_nan(secondary_val_raw):
        return str(secondary_val_raw).strip() if not (isinstance(secondary_val_raw,
                                                                 float) and secondary_val_raw != secondary_val_raw) else secondary_val_raw

    return ""  # Default if both are nan-like or missing


def format_canadian_postal_code(postal_code_val):
    """
    Formats a Canadian postal code string to 'A1A 1A1' format.
    Returns an empty string if input is nan-like, or original (as string) if not a typical format.
    """
    if is_nan(postal_code_val):
        return ""
    pc = str(postal_code_val).upper().replace("-", "").replace(" ", "")
    if len(pc) == 6 and pc.isalnum():  # Basic check
        return f"{pc[:3]} {pc[3:]}"
    return str(postal_code_val)  # Return original string if not a typical 6-char alphanumeric


def derive_school_type_from_isced_canada(row):
    """
    Derives a comma-separated string for MASTERPROPERTIES_SCHOOLTYPE based on ISCED flag columns.
    The ODEF_ISCED columns are expected to contain values like '0', '1', '0.0', '1.0', or be NaN/empty.
    """
    school_types = []
    isced_mapping = {
        "ODEF_ISCED010": "Early Childhood Educational Development",  # ISCED 010
        "ODEF_ISCED020": "Pre-primary Education",  # ISCED 020
        "ODEF_ISCED1": "Primary Education",  # ISCED 1
        "ODEF_ISCED2": "Lower Secondary Education",  # ISCED 2
        "ODEF_ISCED3": "Upper Secondary Education",  # ISCED 3
        "ODEF_ISCED4Plus": "Post-secondary Non-tertiary and Higher Education"  # ISCED 4+
    }
    isced_columns_in_order = [
        "ODEF_ISCED010", "ODEF_ISCED020", "ODEF_ISCED1",
        "ODEF_ISCED2", "ODEF_ISCED3", "ODEF_ISCED4Plus"
    ]

    for col_name in isced_columns_in_order:
        value_str = row.get(col_name)
        is_present = False
        if not is_nan(value_str):
            try:
                if abs(float(str(value_str)) - 1.0) < 0.001:
                    is_present = True
            except ValueError:
                pass

        if is_present:
            school_types.append(isced_mapping[col_name])

    if not school_types:
        return "Unknown or Not Specified"
    return ", ".join(school_types)


def school_prettifier_canada_odef_v4(schools_file_path: str) -> pd.DataFrame:  # Renamed function
    """
    Processes the Canadian schools data file to generate a prettified DataFrame.
    :param schools_file_path: Path to the input CSV file (e.g., canada_schools.csv).
    :return: A pandas DataFrame with the prettified school data.
    """
    try:
        canada_data = pd.read_csv(schools_file_path, low_memory=False, dtype=str, encoding='utf-8-sig')
        canada_data.columns = canada_data.columns.str.strip()
        for col in canada_data.select_dtypes(include=['object']).columns:
            canada_data[col] = canada_data[col].str.strip()
            canada_data[col] = canada_data[col].replace(['..', '.', 'N/A', 'NA', 'None', 'NULL', 'Null'], '',
                                                        regex=False)

    except FileNotFoundError:
        print(f"ERROR: Input file not found at '{schools_file_path}'. Please check the path.")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error reading or performing initial processing on Canadian schools file '{schools_file_path}': {e}")
        return pd.DataFrame()

    column_mapping = {
        "FOCUS_SCHOOL_ID": "FOCUS_ID",
        "ODEF_unique_id": "MASTERPROPERTIES_GOVTSCHOOLID",
        "FOCUS_SCHOOL_CODE": "MASTERPROPERTIES_ERSCODE",
    }
    actual_renames = {k: v for k, v in column_mapping.items() if k in canada_data.columns}
    renamed_df = canada_data.rename(columns=actual_renames)

    source_col_district_id = "authority_hash_12"
    if source_col_district_id in renamed_df.columns:
        renamed_df["MASTERPROPERTIES_GOVTSCHOOLDISTRICTID"] = renamed_df[source_col_district_id]
    else:
        print(
            f"WARNING: Source column '{source_col_district_id}' not found. MASTERPROPERTIES_GOVTSCHOOLDISTRICTID will be blank.")
        renamed_df["MASTERPROPERTIES_GOVTSCHOOLDISTRICTID"] = ""

    renamed_df["MASTERPROPERTIES_SCHOOLNAME"] = renamed_df.apply(
        lambda row: coalesce_fields(row, "ODEF_facility_name", "FOCUS_SCHOOL_NAME"), axis=1
    )
    renamed_df["MASTERPROPERTIES_SCHOOLDISTRICTNAME"] = renamed_df.apply(
        lambda row: coalesce_fields(row, "ODEF_authority_name", "FOCUS_SCHOOL_DISTRICT_NAME"), axis=1
    )

    renamed_df["MASTERPROPERTIES_ADDRESS_STATEPROVINCE"] = renamed_df.apply(
        lambda row: coalesce_fields(row, "ODEF_province_code", "FOCUS_STATE"), axis=1
    )
    renamed_df["MASTERPROPERTIES_ADDRESS_CITY"] = renamed_df.apply(
        lambda row: coalesce_fields(row, "ODEF_addressLocality", "FOCUS_CITY_NAME"), axis=1
    )
    focus_addr1_col_name = "FOCUS_ADRRESS_1" if "FOCUS_ADRRESS_1" in renamed_df.columns else "FOCUS_ADDRESS_1"
    renamed_df["MASTERPROPERTIES_ADDRESS_ADDRESSLINE1"] = renamed_df.apply(
        lambda row: coalesce_fields(row, "ODEF_streetAddress", focus_addr1_col_name), axis=1
    )

    renamed_df["MASTERPROPERTIES_ADDRESS_POSTALCODE"] = renamed_df.apply(
        lambda row: format_canadian_postal_code(coalesce_fields(row, "ODEF_postalCode", "FOCUS_POSTAL_CODE")), axis=1
    )

    def get_coord(row, odef_col, focus_col):
        val_str = coalesce_fields(row, odef_col, focus_col)
        if not is_nan(val_str):
            try:
                num_val = pd.to_numeric(val_str, errors='raise')
                return num_val
            except ValueError:
                return pd.NA
        return pd.NA

    renamed_df["MASTERPROPERTIES_ADDRESS_LATITUDE"] = renamed_df.apply(
        lambda row: get_coord(row, "ODEF_Latitude", "FOCUS_ADDRESS_LATITUDE"), axis=1)
    renamed_df["MASTERPROPERTIES_ADDRESS_LONGITUDE"] = renamed_df.apply(
        lambda row: get_coord(row, "ODEF_Longitude", "FOCUS_ADDRESS_LONGITUDE"), axis=1)

    renamed_df["MASTERPROPERTIES_GOVTDATASET"] = "STATIC_ODEF_CANADA"
    renamed_df["MASTERPROPERTIES_ADDRESS_TYPE"] = "Location"
    renamed_df["MASTERPROPERTIES_ADDRESS_COUNTRY"] = "CAN"

    renamed_df["MASTERPROPERTIES_SCHOOLTYPE"] = renamed_df.apply(derive_school_type_from_isced_canada, axis=1)

    # --- Fields explicitly set to NULL/Placeholder as per user decision ---

    # MASTERPROPERTIES_ID_2
    # TODO: If MASTERPROPERTIES_ID_2 is needed:
    # 1. Define how to derive a single representative two-letter code from the
    #    (potentially multiple) ISCED types listed in MASTERPROPERTIES_SCHOOLTYPE.
    #    For example, prioritize one ISCED level, or use a combination rule.
    # 2. Provide the corresponding school_type_to_code_canada mapping for these derived single codes.
    renamed_df["MASTERPROPERTIES_ID_2"] = ""  # Set to blank as requested (null equivalent)

    # MASTERPROPERTIES_SCHOOLLEVEL
    # TODO: Define School Level for Canadian schools from ODEF_min_grade and ODEF_max_grade.
    # Questions for user:
    # 1. Provide specific rules/categories for translating (ODEF_min_grade, ODEF_max_grade) to School Level.
    #    - Define "Elementary" (e.g., K-5, K-6, PreK-6)?
    #    - Define "Middle" (e.g., 6-8, 7-9)?
    #    - Define "Secondary"/"High School" (e.g., 9-12, 10-12)?
    #    - How to represent combined levels (e.g., "K-8", "K-12")?
    #    - Handle non-standard grade notations (e.g., "m" for Maternelle, "pre-k")?
    renamed_df["MASTERPROPERTIES_SCHOOLLEVEL"] = ""  # Set to blank as requested

    # MASTERPROPERTIES_STARTYEARSTATUSCODE
    # TODO: Define mapping from FOCUS_STATUS or other fields to this status code.
    # Questions for user:
    # 1. Are values in FOCUS_STATUS (e.g., "Active", "Inactive") directly equivalent/mappable for this target field?
    # 2. Is there another ODEF/FOCUS field better representing operational status for this specific target?
    renamed_df["MASTERPROPERTIES_STARTYEARSTATUSCODE"] = ""  # Set to blank as requested

    # MASTERPROPERTIES_SCHOOLYEAR
    # TODO: Determine source for academic School Year.
    # Questions for user:
    # 1. Is there a field in canada_schools.csv (ODEF/FOCUS) for academic school year (e.g., "2023-2024")?
    #    (ODEF_date_updated is record update timestamp, not academic year).
    # 2. If not, should this be left blank, or populated from filename, a script parameter, or processing date?
    renamed_df["MASTERPROPERTIES_SCHOOLYEAR"] = ""  # Set to blank as requested

    # MASTERPROPERTIES_ADDRESS_ADDRESSLINE2
    # TODO: Review this logic based on actual data quality and business rules for Address Line 2.
    # Previous logic (get_address_line2) is removed as per request to keep it null/TODO here.
    # User decision: "keep it null and keep the comments, TODO"
    renamed_df["MASTERPROPERTIES_ADDRESS_ADDRESSLINE2"] = ""  # Set to blank as requested

    # MASTERPROPERTIES_ID
    renamed_df["MASTERPROPERTIES_ID"] = ""  # Kept blank as per decision

    renamed_df["MASTERPROPERTIES_ID_1"] = renamed_df.apply(
        lambda row: f"{get_random_four_digits()}-{get_random_four_digits()}-{get_random_four_digits()}", axis=1)

    # --- XREF Fields ---
    renamed_df["XREF_SOURCESYSTEM1"] = "odef"
    renamed_df["XREF_KEYNAME1"] = "ODEF_unique_id"
    renamed_df["XREF_VALUE1"] = renamed_df.get("MASTERPROPERTIES_GOVTSCHOOLID", "")

    renamed_df["XREF_SOURCESYSTEM2"] = "focus_classic"
    renamed_df["XREF_KEYNAME2"] = "focus_id"
    renamed_df["XREF_VALUE2"] = renamed_df.get("FOCUS_ID", "")

    # XREF3 - Skipped, marked for review
    # TODO: Review if XREF3 is needed.
    # Consider if other ODEF identifiers like ODEF_source_id or ODEF_dguid should be captured here.
    renamed_df["XREF_SOURCESYSTEM3"] = ""
    renamed_df["XREF_KEYNAME3"] = ""
    renamed_df["XREF_VALUE3"] = ""

    # --- Relation Fields ---
    renamed_df["RELATION_ENTITY1"] = "customer"
    renamed_df["RELATION_TYPE1"] = "child"
    renamed_df["RELATION_ID1"] = renamed_df.get("MASTERPROPERTIES_GOVTSCHOOLDISTRICTID", "")
    renamed_df["RELATION_SOURCESYSTEM1"] = "odef"

    # --- Blank Technical Columns ---
    blank_technical_columns = [
        "TECHNICALPROPERTIES_CREATESYSTEM", "TECHNICALPROPERTIES_CREATETIMESTAMP",
        "TECHNICALPROPERTIES_UPDATESYSTEM", "TECHNICALPROPERTIES_UPDATETIMESTAMP",
        "TECHNICALPROPERTIES_DELETESYSTEM", "TECHNICALPROPERTIES_DELETETIMESTAMP",
        "TECHNICALPROPERTIES_DELETEFLAG", "TECHNICALPROPERTIES_VERSION",
    ]
    for col in blank_technical_columns:
        if col not in renamed_df.columns:
            renamed_df[col] = ""
        else:
            renamed_df[col] = ""

    final_column_order = [
        "TECHNICALPROPERTIES_CREATESYSTEM", "TECHNICALPROPERTIES_CREATETIMESTAMP",
        "TECHNICALPROPERTIES_UPDATESYSTEM", "TECHNICALPROPERTIES_UPDATETIMESTAMP",
        "TECHNICALPROPERTIES_DELETESYSTEM", "TECHNICALPROPERTIES_DELETETIMESTAMP",
        "TECHNICALPROPERTIES_DELETEFLAG", "TECHNICALPROPERTIES_VERSION",
        "MASTERPROPERTIES_ID_1", "MASTERPROPERTIES_ID_2", "FOCUS_ID", "MASTERPROPERTIES_ID",
        "MASTERPROPERTIES_GOVTSCHOOLID", "MASTERPROPERTIES_GOVTSCHOOLDISTRICTID",
        "MASTERPROPERTIES_SCHOOLNAME", "MASTERPROPERTIES_SCHOOLDISTRICTNAME",
        "MASTERPROPERTIES_SCHOOLTYPE", "MASTERPROPERTIES_SCHOOLLEVEL",
        "MASTERPROPERTIES_GOVTDATASET",
        "MASTERPROPERTIES_STARTYEARSTATUSCODE", "MASTERPROPERTIES_SCHOOLYEAR",
        "MASTERPROPERTIES_ERSCODE", "MASTERPROPERTIES_ADDRESS_TYPE",
        "MASTERPROPERTIES_ADDRESS_POSTALCODE", "MASTERPROPERTIES_ADDRESS_COUNTRY",
        "MASTERPROPERTIES_ADDRESS_STATEPROVINCE", "MASTERPROPERTIES_ADDRESS_CITY",
        "MASTERPROPERTIES_ADDRESS_ADDRESSLINE1", "MASTERPROPERTIES_ADDRESS_ADDRESSLINE2",
        "MASTERPROPERTIES_ADDRESS_LATITUDE", "MASTERPROPERTIES_ADDRESS_LONGITUDE",
        "XREF_SOURCESYSTEM1", "XREF_KEYNAME1", "XREF_VALUE1",
        "XREF_SOURCESYSTEM2", "XREF_KEYNAME2", "XREF_VALUE2",
        "XREF_SOURCESYSTEM3", "XREF_KEYNAME3", "XREF_VALUE3",
        "RELATION_ENTITY1", "RELATION_TYPE1", "RELATION_ID1", "RELATION_SOURCESYSTEM1"
    ]

    for col in final_column_order:
        if col not in renamed_df.columns:
            renamed_df[col] = ""

    reordered_df = renamed_df.filter(items=final_column_order, axis=1)
    return reordered_df


if __name__ == "__main__":
    # --- Configuration ---
    input_can_file_path = '/Users/kirtanshah/PycharmProjects/fs-ssot-poc/customer/data/interim/canada_schools.csv'  # Make sure this file is in the same directory or provide full path

    # Define output path
    # file_date_suffix = os.environ.get("FILE_DATE_SUFFIX", pd.Timestamp.now().strftime('%Y%m%d%H%M%S'))
    output_directory = '/Users/kirtanshah/PycharmProjects/fs-ssot-poc/customer/data/processed/'
    output_can_file_path = os.path.join(output_directory, f'pretty_schools_canada_odef_v4.csv')

    # # Create output directory if it doesn't exist
    # os.makedirs(output_directory, exist_ok=True)

    print(f"Attempting to process Canadian schools file: '{input_can_file_path}'")

    # Check if the input file exists before processing
    if not os.path.exists(input_can_file_path):
        print(f"ERROR: Input file '{input_can_file_path}' not found.")
        print("Please ensure the file exists in the correct location or update the path in the script.")
        sys.exit(1)  # Exit script if input file is critical and not found

    try:
        prettified_can_df = school_prettifier_canada_odef_v4(input_can_file_path)

        if not prettified_can_df.empty:
            prettified_can_df.to_csv(output_can_file_path, index=False, encoding='utf-8')
            print(f"Successfully created Canadian prettified file: '{output_can_file_path}'")
            print(f"\nProcessed {len(prettified_can_df)} rows.")
            # print("\nFirst 2 rows of the Canadian prettified output:")
            # print(prettified_can_df.head(2))
            # print("\nLast 2 rows of the Canadian prettified output:")
            # print(prettified_can_df.tail(2))
        else:
            print("Processing resulted in an empty DataFrame. Please check for errors or input file issues.")

    except Exception as e:
        print(f"An critical error occurred during Canadian school processing: {e}")
        import traceback

        print(traceback.format_exc())