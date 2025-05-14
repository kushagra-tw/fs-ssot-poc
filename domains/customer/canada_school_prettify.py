import sys
import pandas as pd
from random import randint
import os
import re # For text processing

# Placeholder for custom module imports, adjust if needed for your environment
# sys.path.insert(0, "/path/to/your/custom/modules/")
# from domains.customer.Reader import read_data # Assuming a generic CSV reader for now
# from domains.customer.generate_delta import add_on_existing_db_ids # This will be skipped

pd.set_option('display.max_columns', None)

def get_random_four_digits():
    return str(randint(0, 9999)).zfill(4)

def is_nan(value):
    """
    Checks if a value is NaN (works for float NaN) or None.
    Also considers empty strings or strings with only whitespace as 'nan-like' for some checks.
    """
    if value is None:
        return True
    if isinstance(value, float) and value != value: # Standard NaN check for float
        return True
    if isinstance(value, str) and not value.strip(): # Empty string
        return True
    return False

def coalesce_fields(row, primary_col, secondary_col):
    primary_val = row.get(primary_col)
    if not is_nan(primary_val):
        return primary_val
    secondary_val = row.get(secondary_col)
    if not is_nan(secondary_val):
        return secondary_val
    return "" # Or pd.NA or None, depending on desired empty value

def format_canadian_postal_code(postal_code_val):
    if is_nan(postal_code_val):
        return "" # Or pd.NA / None
    pc = str(postal_code_val).upper().replace("-", "").replace(" ", "")
    if len(pc) == 6 and pc.isalnum(): # Basic check, might need more robust validation
        return f"{pc[:3]} {pc[3:]}"
    return str(postal_code_val) # Return original (as string) if not a typical format or already formatted

# TODO: Review MASTERPROPERTIES_ADDRESS_ADDRESSLINE2 logic
# Ambiguity: Specifically for Address Line 2, should ODEF_postOfficeBoxNumber be the primary source,
# or FOCUS_ADDRESS_2?
# Decision: Use ODEF_ on priority followed by FOCUS_. Marking for review.
def get_address_line2(row):
    odef_val = row.get("ODEF_postOfficeBoxNumber")
    if not is_nan(odef_val):
        return str(odef_val) # Ensure it's string
    focus_val = row.get("FOCUS_ADDRESS_2")
    if not is_nan(focus_val):
        return str(focus_val)
    return ""


def school_prettifier_canada_odef_v2(schools_file_path):
    try:
        # Using pandas directly. Replace with your read_data if it offers specific advantages.
        canada_data = pd.read_csv(schools_file_path, low_memory=False, dtype=str) # Read all as string initially
        # Convert specific columns to numeric later if needed, after handling NaNs
    except Exception as e:
        print(f"Error reading Canadian schools file: {e}")
        return pd.DataFrame()

    # --- Column Mapping (Direct Renames or Primary Source Identification) ---
    # Target names are kept as "GOVT" for schema consistency with previous script versions,
    # despite data being Canadian.
    column_mapping = {
        "FOCUS_SCHOOL_ID": "FOCUS_ID",
        "ODEF_unique_id": "MASTERPROPERTIES_GOVTSCHOOLID",
        # authority_hash_12 will be used for MASTERPROPERTIES_GOVTSCHOOLDISTRICTID directly later
        # ODEF_facility_name and FOCUS_SCHOOL_NAME will be coalesced for MASTERPROPERTIES_SCHOOLNAME
        # ODEF_authority_name and FOCUS_SCHOOL_DISTRICT_NAME will be coalesced for MASTERPROPERTIES_SCHOOLDISTRICTNAME
        "FOCUS_SCHOOL_CODE": "MASTERPROPERTIES_ERSCODE",
        # Address components will be handled by coalesce logic
    }
    renamed_df = canada_data.rename(columns=column_mapping)

    # --- Populate fields based on decisions ---

    # MASTERPROPERTIES_GOVTSCHOOLDISTRICTID from authority_hash_12
    if "authority_hash_12" in renamed_df.columns:
        renamed_df["MASTERPROPERTIES_GOVTSCHOOLDISTRICTID"] = renamed_df["authority_hash_12"]
    else:
        print("WARNING: Column 'authority_hash_12' not found for MASTERPROPERTIES_GOVTSCHOOLDISTRICTID. Field will be blank.")
        renamed_df["MASTERPROPERTIES_GOVTSCHOOLDISTRICTID"] = ""


    # Coalesce School Name
    renamed_df["MASTERPROPERTIES_SCHOOLNAME"] = renamed_df.apply(
        lambda row: coalesce_fields(row, "ODEF_facility_name", "FOCUS_SCHOOL_NAME"), axis=1
    )

    # Coalesce School District Name
    renamed_df["MASTERPROPERTIES_SCHOOLDISTRICTNAME"] = renamed_df.apply(
        lambda row: coalesce_fields(row, "ODEF_authority_name", "FOCUS_SCHOOL_DISTRICT_NAME"), axis=1
    )

    # Address Components (ODEF priority, then FOCUS)
    renamed_df["MASTERPROPERTIES_ADDRESS_STATEPROVINCE"] = renamed_df.apply(
        lambda row: coalesce_fields(row, "ODEF_province_code", "FOCUS_STATE"), axis=1
    )
    renamed_df["MASTERPROPERTIES_ADDRESS_CITY"] = renamed_df.apply(
        lambda row: coalesce_fields(row, "ODEF_addressLocality", "FOCUS_CITY_NAME"), axis=1 # Or FOCUS_CITY if that's the correct one
    )
    renamed_df["MASTERPROPERTIES_ADDRESS_ADDRESSLINE1"] = renamed_df.apply(
        lambda row: coalesce_fields(row, "ODEF_streetAddress", "FOCUS_ADRRESS_1"), axis=1 # Note: FOCUS_ADRRESS_1 typo from CSV
    )
    # TODO: Review MASTERPROPERTIES_ADDRESS_ADDRESSLINE2 logic below based on actual data quality
    renamed_df["MASTERPROPERTIES_ADDRESS_ADDRESSLINE2"] = renamed_df.apply(get_address_line2, axis=1)

    renamed_df["MASTERPROPERTIES_ADDRESS_POSTALCODE"] = renamed_df.apply(
        lambda row: format_canadian_postal_code(coalesce_fields(row, "ODEF_postalCode", "FOCUS_POSTAL_CODE")), axis=1
    )
    # Lat/Lon - Assuming ODEF is preferred. If FOCUS can be a fallback, add coalesce.
    # For simplicity, directly taking ODEF if available, else check FOCUS.
    # Need to handle potential non-numeric values before converting to float.
    def get_coord(row, odef_col, focus_col):
        val = coalesce_fields(row, odef_col, focus_col)
        try:
            return pd.to_numeric(val)
        except ValueError:
            return pd.NA # Or None

    renamed_df["MASTERPROPERTIES_ADDRESS_LATITUDE"] = renamed_df.apply(lambda row: get_coord(row, "ODEF_Latitude", "FOCUS_ADDRESS_LATITUDE"), axis=1)
    renamed_df["MASTERPROPERTIES_ADDRESS_LONGITUDE"] = renamed_df.apply(lambda row: get_coord(row, "ODEF_Longitude", "FOCUS_ADDRESS_LONGITUDE"), axis=1)


    # Static assignments
    renamed_df["MASTERPROPERTIES_GOVTDATASET"] = "STATIC_ODEF_CANADA"
    renamed_df["MASTERPROPERTIES_ADDRESS_TYPE"] = "Location"
    renamed_df["MASTERPROPERTIES_ADDRESS_COUNTRY"] = "CAN"

    # --- Skipped/TODO Fields (as per user decisions) ---
    # MASTERPROPERTIES_SCHOOLTYPE
    # TODO: Define School Type for Canadian schools.
    # Ambiguity Questions:
    # 1. How to define "School Type"? From ODEF_facility_name keywords (e.g., "Elementary", "Secondary")?
    #    If so, provide keywords and corresponding types.
    # 2. Use ISCED classification (ODEF_ISCED1, ODEF_ISCED2) or flags (ODEF_french_immersion)?
    #    If so, what are the categories and derivation logic?
    # 3. How to map these Canadian types to two-letter codes for MASTERPROPERTIES_ID_2 (prefix "CA")?
    #    Provide school_type_to_code_canada mapping if this field is to be generated.
    renamed_df["MASTERPROPERTIES_SCHOOLTYPE"] = "TODO: Define Canadian School Type" # Placeholder

    # MASTERPROPERTIES_SCHOOLLEVEL
    # TODO: Define School Level for Canadian schools from ODEF_min_grade and ODEF_max_grade.
    # Ambiguity Questions:
    # 1. Provide specific rules/categories for translating (ODEF_min_grade, ODEF_max_grade) to School Level.
    #    - Define "Elementary" (e.g., K-5, K-6, PreK-6)?
    #    - Define "Middle" (e.g., 6-8, 7-9)?
    #    - Define "Secondary"/"High School" (e.g., 9-12, 10-12)?
    #    - How to represent combined levels (e.g., "K-8", "K-12")?
    #    - Handle non-standard grade notations (e.g., "m" for Maternelle, "pre-k")?
    renamed_df["MASTERPROPERTIES_SCHOOLLEVEL"] = "TODO: Define Canadian School Level" # Placeholder

    # MASTERPROPERTIES_STARTYEARSTATUSCODE
    # TODO: Define mapping from FOCUS_STATUS or other fields to this status code.
    # Ambiguity Questions:
    # 1. Are values in FOCUS_STATUS (e.g., "Active", "Inactive") directly equivalent/mappable?
    # 2. Is there another ODEF/FOCUS field better representing operational status?
    if "FOCUS_STATUS" in renamed_df.columns:
         renamed_df["MASTERPROPERTIES_STARTYEARSTATUSCODE"] = renamed_df["FOCUS_STATUS"] + " (TODO: Review Mapping)" # Temp use
    else:
        renamed_df["MASTERPROPERTIES_STARTYEARSTATUSCODE"] = "TODO: Review FOCUS_STATUS Mapping"

    # MASTERPROPERTIES_SCHOOLYEAR
    # TODO: Determine source for academic School Year.
    # Ambiguity Questions:
    # 1. Is there a field in canada_schools.csv (ODEF/FOCUS) for academic school year (e.g., "2023-2024")?
    #    (ODEF_date_updated is record update timestamp, not academic year).
    # 2. If not, leave blank, or populate from filename, parameter, or processing date?
    renamed_df["MASTERPROPERTIES_SCHOOLYEAR"] = "TODO: Define School Year Source" # Placeholder

    # MASTERPROPERTIES_ID (from add_on_existing_db_ids)
    # Decision: Ignore and keep blank for now.
    renamed_df["MASTERPROPERTIES_ID"] = ""

    # IDs generation
    renamed_df["MASTERPROPERTIES_ID_1"] = renamed_df.apply(
        lambda row: f"{get_random_four_digits()}-{get_random_four_digits()}-{get_random_four_digits()}", axis=1)

    # MASTERPROPERTIES_ID_2 is skipped because MASTERPROPERTIES_SCHOOLTYPE is skipped.
    # If SCHOOLTYPE is implemented, uncomment and adapt the following:
    # school_type_to_code_canada = { ... } # Define this mapping
    # renamed_df["MASTERPROPERTIES_ID_2"] = renamed_df.apply(
    # lambda row: f"CA{school_type_to_code_canada.get(row.get('MASTERPROPERTIES_SCHOOLTYPE', 'Unknown'), 'XX')}-{row['MASTERPROPERTIES_ID_1'][5:9]}-{row['MASTERPROPERTIES_ID_1'][10:14]}",
    # axis=1)
    renamed_df["MASTERPROPERTIES_ID_2"] = "TODO: Depends on School Type" # Placeholder


    # --- XREF Fields ---
    renamed_df["XREF_SOURCESYSTEM1"] = "odef"
    renamed_df["XREF_KEYNAME1"] = "ODEF_unique_id"
    renamed_df["XREF_VALUE1"] = renamed_df.get("MASTERPROPERTIES_GOVTSCHOOLID", "") # From ODEF_unique_id

    renamed_df["XREF_SOURCESYSTEM2"] = "focus_classic"
    renamed_df["XREF_KEYNAME2"] = "focus_id"
    renamed_df["XREF_VALUE2"] = renamed_df.get("FOCUS_ID", "") # Mapped from FOCUS_SCHOOL_ID

    # XREF3 - Skipped, marked for review
    # TODO: Review if XREF3 is needed.
    # Other potential ODEF IDs for XREF: ODEF_source_id, ODEF_dguid.
    renamed_df["XREF_SOURCESYSTEM3"] = "" #"TODO: Review XREF3"
    renamed_df["XREF_KEYNAME3"] = ""
    renamed_df["XREF_VALUE3"] = ""


    # --- Relation Fields ---
    renamed_df["RELATION_ENTITY1"] = "customer" # Assuming district is the customer
    renamed_df["RELATION_TYPE1"] = "child"     # As per user decision
    renamed_df["RELATION_ID1"] = renamed_df.get("MASTERPROPERTIES_GOVTSCHOOLDISTRICTID", "") # From authority_hash_12
    renamed_df["RELATION_SOURCESYSTEM1"] = "odef" # Source system of the RELATION_ID1


    # --- Blank Technical Columns ---
    blank_technical_columns = [
        "TECHNICALPROPERTIES_CREATESYSTEM", "TECHNICALPROPERTIES_CREATETIMESTAMP",
        "TECHNICALPROPERTIES_UPDATESYSTEM", "TECHNICALPROPERTIES_UPDATETIMESTAMP",
        "TECHNICALPROPERTIES_DELETESYSTEM", "TECHNICALPROPERTIES_DELETETIMESTAMP",
        "TECHNICALPROPERTIES_DELETEFLAG", "TECHNICALPROPERTIES_VERSION",
    ]
    for col in blank_technical_columns:
        renamed_df[col] = ""

    # --- Final Column Order ---
    # (Ensure all these columns exist in 'renamed_df' or are created as blank/placeholders)
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
        "RELATION_ENTITY1", "RELATION_TYPE1", "RELATION_ID1", "RELATION_SOURCESYSTEM1" # Added RELATION_SOURCESYSTEM1
    ]

    # Add any missing columns from the final_column_order as blank strings before filtering
    for col in final_column_order:
        if col not in renamed_df.columns:
            renamed_df[col] = "" # Or pd.NA as appropriate

    reordered_df = renamed_df.filter(items=final_column_order, axis=1)
    return reordered_df

if __name__ == "__main__":
    file_date_suffix = os.environ.get("FILE_DATE_SUFFIX", pd.Timestamp.now().strftime('%Y%m%d'))
    input_can_file_path = '/Users/kirtanshah/PycharmProjects/fs-ssot-poc/customer/data/interim/canada_schools.csv' # Ensure this file exists
    output_can_file_path = f'outputs/schools/pretty_schools_canada_odef_v2_{file_date_suffix}.csv'

    os.makedirs('outputs/schools', exist_ok=True)

    if not os.path.exists(input_can_file_path):
        print(f"ERROR: Input file {input_can_file_path} not found. Please create it or provide the correct path.")
        # Create a minimal dummy file if it doesn't exist, including newly referenced 'authority_hash_12'
        print(f"Creating a minimal dummy file for {input_can_file_path} for script structure testing.")
        dummy_data = {
            "FOCUS_SCHOOL_ID": ["F100", "F101"], "ODEF_unique_id": ["ODEFCAN001", "ODEFCAN002"],
            "authority_hash_12": ["AUTH#HASH1", "AUTH#HASH2"],
            "ODEF_facility_name": ["Maple Elementary", "Pine Secondary"], "FOCUS_SCHOOL_NAME": ["Maple Elem.", ""],
            "ODEF_authority_name": ["North District", "South District"], "FOCUS_SCHOOL_DISTRICT_NAME": ["", "South Dist."],
            "ODEF_province_code": ["ON", "BC"], "FOCUS_STATE": ["", "BC"],
            "ODEF_addressLocality": ["Ottawa", "Vancouver"], "FOCUS_CITY_NAME": ["", "Van"],
            "ODEF_streetAddress": ["1 Main St", "100 West Rd"], "FOCUS_ADRRESS_1": ["", ""], # Typo matches example
            "ODEF_postOfficeBoxNumber": [None, "PO Box 123"], "FOCUS_ADDRESS_2": ["Unit A", None],
            "ODEF_postalCode": ["K1A0B1", "V6B5A3"], "FOCUS_POSTAL_CODE": ["", "V6B 5A3"],
            "ODEF_Latitude": ["45.4215", "49.2827"], "ODEF_Longitude": ["-75.6972", "-123.1207"],
            "FOCUS_ADDRESS_LATITUDE": [None, None], "FOCUS_ADDRESS_LONGITUDE": [None, None],
            "FOCUS_SCHOOL_CODE": ["FS001", "FS002"],
            # Fields for skipped sections (can be empty or have sample values)
            "ODEF_min_grade": ["K","9"], "ODEF_max_grade": ["6","12"],
            "FOCUS_STATUS": ["Active", "Inactive"]
        }
        pd.DataFrame(dummy_data).to_csv(input_can_file_path, index=False)

    try:
        print(f"Processing Canadian schools file: {input_can_file_path}")
        prettified_can_df = school_prettifier_canada_odef_v2(input_can_file_path)

        if not prettified_can_df.empty:
            prettified_can_df.to_csv(output_can_file_path, index=False)
            print(f"Successfully created Canadian prettified file: {output_can_file_path}")
            print("\nFirst 5 rows of the Canadian prettified output:")
            print(prettified_can_df.head())
        else:
            print("Processing resulted in an empty DataFrame. Please check for errors or input file issues.")

    except Exception as e:
        print(f"An error occurred during Canadian school processing: {e}")
        import traceback
        print(traceback.format_exc())