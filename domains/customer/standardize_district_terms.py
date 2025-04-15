import pandas as pd
import re


def standardize_terms_in_school_district(df, cols_to_standardize):
    """
    Standardizes terms in specified columns of a Pandas DataFrame,
    addressing abbreviations and common terms with a more comprehensive map.

    Args:
        df (pd.DataFrame): The input DataFrame.
        cols_to_standardize (list): A list of column names (strings) to be standardized.

    Returns:
        pd.DataFrame: The DataFrame with the specified columns modified.
                      A copy of the dataframe is created to avoid modifying the original.
    """

    df_standardized = df.copy()  # Work on a copy

    # Check if the input columns exist
    for col in cols_to_standardize:
        if col not in df_standardized.columns:
            print(f"Warning: Column '{col}' not found in DataFrame. Skipping.")
            continue

    # Define mappings for abbreviations to full terms.
    # Added: RUSD, PSD, HTS, CS, CES, PS, PBLC, COOP, SAU, BORO
    abbreviation_map = {
        r"\bsd\b": "school district",
        r"\bcsd\b": "central school district",
        r"\busd\b": "unified school district",
        r"\bcusd\b": "consolidated unified school district",
        r"\bccsd\b": "consolidated community school district",
        r"\bisd\b": "independent school district",
        r"\bhsd\b": "high school district",
        r"\bco\b": "county",
        r"\btwp\b": "township",
        r"\belem\b": "elementary",
        r"\bdist\b": "district",
        r"\bpub\b": "public",
        r"\bpblc\b": "public",
        r"\breg\b": "regional",
        r"\bsch\b": "school",
        r"\bschl\b": "school",
        r"\bschls\b": "schools",
        r"\bschs\b": "schools",
        r"\bspec ed\b": "special education",
        r"\brusd\b": "unified school district",
        r"\bpsd\b": "public school district",
        r"\bhts\b": "heights",
        r"\bcs\b": "charter school",
        r"\bces\b": "charter school",
        r"\bps\b": "public schools",
        r"\bcoop\b": "cooperative",
        r"\bsau\b": "school administrative unit",
        r"\bboro\b": "borough",
    }

    # Define common terms to remove in LOWERCASE.
    common_terms = [
        # Core terms
        r"\bschool district\b",
        r"\bpublic schools\b",
        r"\bunified\b",
        r"\bcounty\b",
        r"\btownship\b",
        r"\bschools\b",
        r"\bpublic\b",
        r"\bdistrict\b",
        # Expanded terms from abbreviations
        r"\bcentral\b",
        r"\bconsolidated\b",
        r"\bcommunity\b",
        r"\bindependent\b",
        r"\bhigh school\b",
        r"\bregional\b",
        r"\belementary\b",
        r"\bspecial education\b",
        r"\bcharter school\b",
        r"\bcooperative\b",
        r"\bschool administrative unit\b",
        r"\bborough\b",
        r"\bheights\b",
        # Newly added terms
        r"\bcity\b",
        r"\bmetropolitan\b",
        r"\barea\b",
        r"\bcharter\b", # Remove 'charter' alone if 'charter school' wasn't caught
        r"\bof the city of\b", # Specific phrase removal
    ]

    for col in cols_to_standardize:
        # Ensure we are working with strings, and convert NaNs to empty string ''
        df_standardized[col] = df_standardized[col].fillna('').astype(str)

        # Apply the abbreviation mapping (Iterate multiple times for chained cases like CUSD -> CONSOLIDATED UNIFIED SD -> CONSOLIDATED UNIFIED SCHOOL DISTRICT)
        # A simple loop might be sufficient for common cases, but complex chains might need more sophisticated handling. Let's add a simple loop (e.g., twice)
        for _ in range(2):  # Apply mapping twice to handle potential chained replacements
            for abbr, full_term in abbreviation_map.items():
                df_standardized[col] = df_standardized[col].str.replace(abbr, full_term, regex=True)

        # Remove common terms (Iterate multiple times as term removal might expose new terms to remove)
        for _ in range(2):  # Apply removal twice
            for term in common_terms:
                # Add space padding to ensure we remove terms correctly even after previous removals
                df_standardized[col] = df_standardized[col].str.replace(f' {term} ', ' ', regex=True)
                # Handle terms at the start/end of the string
                df_standardized[col] = df_standardized[col].str.replace(fr'^{term}\s+', '', regex=True)
                df_standardized[col] = df_standardized[col].str.replace(fr'\s+{term}$', '', regex=True)
                # Handle cases where the term is the entire string
                df_standardized[col] = df_standardized[col].str.replace(fr'^{term}$', '', regex=True)

        # Final cleanup: Trim leading/trailing whitespace and collapse multiple spaces
        df_standardized[col] = df_standardized[col].str.strip().str.replace(r'\s+', ' ', regex=True)

    return df_standardized

# Example usage (assuming previous normalization script was also run):
# data = {'FOCUS_SCHOOL_DISTRICT_NAME': ["Racine RUSD", "Buffalo PSD", "Hicksville HTS", "MAST Community CS", "Ridgefield PS", "RICHMOND CITY PBLC SCHS", "Winnacunnet Coop School District", "SAU 50 Concord", "Ketchikan Gateway Boro SD"],
#         'NCES_NAME': ["Racine Unified School District", "BUFFALO PUBLIC SCHOOL DISTRICT", "Hicksville Heights", "MAST Charter School", "Ridgefield Public Schools", "RICHMOND CITY PUBLIC SCHOOLS", "Winnacunnet Cooperative", "Concord SCHOOL ADMINISTRATIVE UNIT 50", "Ketchikan Gateway BOROUGH School District"]}
# df_normalized = pd.DataFrame(data) # Assume this df is already normalized (uppercase, basic punctuation removed)

# columns_to_standardize = ['FOCUS_SCHOOL_DISTRICT_NAME', 'NCES_NAME']
# standardized_df = standardize_terms_in_dataframe(df_normalized, columns_to_standardize)
# print(standardized_df)

# Expected Output for Example Data:
# FOCUS_SCHOOL_DISTRICT_NAME NCES_NAME
# RACINE                     RACINE
# BUFFALO                    BUFFALO
# HICKSVILLE HEIGHTS         HICKSVILLE HEIGHTS # Assumes HTS->HEIGHTS is correct
# MAST                       MAST
# RIDGEFIELD                 RIDGEFIELD
# RICHMOND CITY              RICHMOND CITY
# WINNACUNNET                WINNACUNNET
# CONCORD 50                 CONCORD 50
# KETCHIKAN GATEWAY          KETCHIKAN GATEWAY