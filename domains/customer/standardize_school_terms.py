import pandas as pd
import re

def standardize_school_names(df, cols_to_standardize):
    """
    Standardizes school name strings in specified columns of a Pandas DataFrame
    by expanding abbreviations and removing common terms.

    Assumes input columns are already lowercase and punctuation has been removed.

    Args:
        df (pd.DataFrame): The input DataFrame.
        cols_to_standardize (list): A list of column names (strings) containing
                                     school names to be standardized.

    Returns:
        pd.DataFrame: The DataFrame with standardized school names in the
                      specified columns. Works on a copy.
    """
    df_standardized = df.copy()

    # Comprehensive map based on unique_combinations_school.txt and common usage
    abbreviation_map = {
        r'\belem\b': 'elementary',
        r'\bes\b': 'elementary school', # Order matters: handle 'es' after 'elem' potentially
        r'\bel sch\b': 'elementary school',
        r'\bk 8\b': 'k8', # Standardize grade variations
        r'\bjr sr\b': 'junior senior',
        r'\bjr\b': 'junior',
        r'\bsr\b': 'senior',
        r'\bms\b': 'middle school',
        r'\bmiddle\b': 'middle school', # Normalize 'middle' to 'middle school'
        r'\bhs\b': 'high school',
        r'\bhigh\b': 'high school', # Normalize 'high' to 'high school'
        r'\bshs\b': 'senior high school',
        r'\bjshs\b': 'junior senior high school',
        r'\bctr\b': 'center',
        r'\bsch\b': 'school',
        r'\bschl\b': 'school',
        r'\bschs\b': 'schools',
        r'\bschls\b': 'schools',
        r'\bcs\b': 'charter school', # Consider context if CS means Community School
        r'\bst\b': 'saint',
        r'\bmt\b': 'mount',
        r'\bdr\b': 'doctor',
        r'\bave\b': 'avenue',
        r'\bss\b': 'secondary school',
        r'\bps\b': 'public school', # Ambiguous: Could be public school or primary school
        r'\bacad\b': 'academy',
        r'\bvoc\b': 'vocational',
        r'\btech\b': 'technical',
        r'\bprep\b': 'preparatory',
        r'\binst\b': 'institute',
        r'\bluth\b': 'lutheran', # Handle common typo/abbrev
        r'\bluthern\b': 'lutheran' # Handle typo
        # Add other relevant mappings based on your data exploration
    }

    # Terms to potentially remove after abbreviation expansion
    # Order might matter slightly. Remove longer phrases first.
    # Consider carefully which terms to remove, as it might decrease uniqueness.
    # For matching, sometimes keeping terms like 'elementary' or 'high' is useful.
    # This list is aggressive in removal; you might want to prune it.
    terms_to_remove = [
        r'\belementary school\b',
        r'\bmiddle school\b',
        r'\bsenior high school\b',
        r'\bjúnior senior high school\b', # Use unicode or handle accents if needed
        r'\bhigh school\b',
        r'\bjunior high\b', # Add if 'jr' maps to 'junior high' elsewhere
        r'\bsecondary school\b',
        r'\bpublic school\b',
        r'\bcharter school\b',
        r'\belementary\b',
        r'\bjunior\b',
        r'\bsenior\b',
        r'\bupper\b',
        r'\blower\b',
        r'\bintermediate\b',
        r'\bmagnet\b',
        r'\bacademy\b',
        r'\bpreparatory\b',
        r'\binstitute\b',
        r'\bcenter\b',
        r'\bschools\b',
        r'\bschool\b',
        r'\bcampus\b',
        r'\bprogram\b',
        r'\bthe\b', # Remove 'the' article
        r'\bk8\b', # Remove standardized grade levels if desired
        r'\bcatholic\b',
        r'\blutheran\b',
        r'\bchristian\b',
        r'\bfriends\b',
        r'\bday\b',
        r'\bregional\b',
        r'\btechnical\b',
        r'\bvocational\b',
        r'\bcharter\b' # Remove charter alone if 'charter school' wasn't caught
        # Add district/county/etc. terms if they weren't handled earlier
    ]

    for col in cols_to_standardize:
        if col not in df_standardized.columns:
            print(f"Warning: Column '{col}' not found in DataFrame. Skipping.")
            continue

        # Ensure working with strings, handle potential NaN/None values
        df_standardized[col] = df_standardized[col].fillna('').astype(str)

        # 1. Expand abbreviations
        # Iterating once might be enough if order is considered, but twice can catch edge cases.
        for _ in range(2):
            for abbr, full in abbreviation_map.items():
                # Use regex=True for word boundary matching (\b)
                df_standardized[col] = df_standardized[col].str.replace(abbr, full, regex=True)

        # 2. Remove common terms (optional, adjust list as needed)
        # Iterate to catch terms revealed after abbreviation expansion or other removals
        for _ in range(2):
            for term in terms_to_remove:
                 # Pad with spaces for whole word matching, handle start/end cases
                df_standardized[col] = df_standardized[col].str.replace(f' {term} ', ' ', regex=True)
                df_standardized[col] = df_standardized[col].str.replace(fr'^{term}\s+', '', regex=True)
                df_standardized[col] = df_standardized[col].str.replace(fr'\s+{term}$', '', regex=True)
                df_standardized[col] = df_standardized[col].str.replace(fr'^{term}$', '', regex=True) # Handle if term is the whole string


        # 3. Final whitespace cleanup
        df_standardized[col] = df_standardized[col].str.strip()
        # Replace multiple spaces with a single space
        df_standardized[col] = df_standardized[col].str.replace(r'\s+', ' ', regex=True)

    return df_standardized

# # --- Example Usage ---
# # Create a sample DataFrame similar to your data (assuming lowercase and no punctuation)
# data = {
#     'FOCUS_SCHOOL_NAME_standardized': [
#         'lake spokane elem school', 'metcalf school', 'lewis es', 'longwood middle school',
#         'd89 westfield elementary', 'van sickle campus', 'janis dismus', 'isbell school',
#         'winnisquam high school', 'chestnut hill elementary', 'st paul luthern school',
#         'keshet day school', 'glenbard north hs', 'ava maria academy mt lebanon campus',
#         'sherwood es', 'iroquois jshs', 'paxton buckley loda jr sr high', 'bishop hendricken hs',
#         'hedrick middle school', 'mt tamalpais school', 'bridge valley elementary', 'seacoast charter school',
#         'central bucks east', 'conwell middle school', 'pine point', 'marshall street elementary'
#     ],
#     'NCES_SCH_NAME_standardized': [
#         'lake spokane elementary', 'thomas metcalf school', '', 'longwood middle school',
#         'westfield elem school', 'van sickle academy', 'janis e dismus middle school', 'isbell middle',
#         'winnisquam regional high school', 'chestnut hill el sch', '',
#         'keshet elmentary', 'glenbard north high school', 'ave maria academy mt lebanon campus',
#         'sherwood elementary', 'iroquois junior senior high school', 'paxton buckley loda high school', 'bishop hendricken high school',
#         'hedrick middle school', 'mount tamalpais school', 'bridge valley el sch', 'seacoast charter school',
#         'central bucks hs east', 'conwell russell ms', 'pine point school', 'marshall street el sch'
#     ]
# }
# df_input = pd.DataFrame(data)

# # Specify the columns to standardize
# columns_to_process = ['FOCUS_SCHOOL_NAME_standardized', 'NCES_SCH_NAME_standardized']

# # Apply the function
# df_output = standardize_school_names(df_input, columns_to_process)

# # Display the results
# print("Original DataFrame:")
# print(df_input)
# print("\nStandardized DataFrame:")
# print(df_output)