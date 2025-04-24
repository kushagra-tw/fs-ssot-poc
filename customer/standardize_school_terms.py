import pandas as pd
import re

def standardize_school_names(df, cols_to_standardize):
    """
    Standardizes school name strings in specified columns of a Pandas DataFrame
    by expanding abbreviations and removing common terms, using an expanded map
    based on re-analysis of unique_combinations_school.txt.

    Assumes input columns are already lowercase and punctuation (including slashes,
    parentheses) has been removed or handled appropriately beforehand.

    Args:
        df (pd.DataFrame): The input DataFrame.
        cols_to_standardize (list): A list of column names (strings) containing
                                     school names to be standardized.

    Returns:
        pd.DataFrame: The DataFrame with standardized school names in the
                      specified columns. Works on a copy.
    """
    df_standardized = df.copy()

    # Expanded abbreviation map based on re-analysis
    abbreviation_map = {
        # Basic Types
        r'\belem\b': 'elementary',
        r'\bel sch\b': 'elementary school', # Order after elem
        r'\bes\b': 'elementary school',    # Order after elem
        r'\bms\b': 'middle school',
        r'\bmiddle\b': 'middle school',   # Normalize base word
        r'\bhs\b': 'high school',
        r'\bhigh\b': 'high school',      # Normalize base word
        r'\bshs\b': 'senior high school',
        r'\bjshs\b': 'junior senior high school',
        # Jr/Sr variations (Assumes slash '/' was replaced with space or removed)
        r'\bjr sr\b': 'junior senior',
        r'\bjr\b': 'junior',
        r'\bsr\b': 'senior',
        # Other common school types/terms
        r'\bsch\b': 'school',
        r'\bschl\b': 'school',
        r'\bschs\b': 'schools',
        r'\bschls\b': 'schools',
        r'\bctr\b': 'center',
        r'\bcs\b': 'charter school', # Could be community school, adjust if needed
        r'\bacad\b': 'academy',
        r'\bvoc\b': 'vocational',
        r'\btech\b': 'technical',
        r'\bprep\b': 'preparatory',
        r'\binst\b': 'institute',
        r'\bkg\b': 'kindergarten', # Added
        r'\bk 8\b': 'k8',          # Normalize grade span
        r'\bk 12\b': 'k12',        # Normalize grade span
        # Locations/Titles/Orgs
        r'\bst\b': 'saint',
        r'\bmt\b': 'mount',
        r'\bdr\b': 'doctor',
        r'\bave\b': 'avenue',
        r'\bss\b': 'secondary school',
        r'\bps\b': 'public school',   # Or primary school? Check context
        r'\bcoop\b': 'cooperative',   # Added from district example if relevant
        # Typos/Variations seen
        r'\bluth\b': 'lutheran',
        r'\bluthern\b': 'lutheran',
        r'\belmentary\b': 'elementary' # Specific typo from file
        # Add more mappings as discovered...
    }

    # Expanded list of terms to remove (Review carefully - may remove useful info)
    # Removing longer phrases first can be slightly more robust
    terms_to_remove = [
        # Full types (often redundant after mapping)
        #r'\bjúnior senior high school\b', # Use unicode or handle accents if needed
        #r'\bsenior high school\b',
        #r'\bjunior high school\b', # If jr maps here
        #r'\bmiddle school\b',
        #r'\belementary school\b',
        #r'\bsecondary school\b',
        r'\bpublic school\b',
        #r'\bcharter school\b',
        # Base types/levels (use with caution)
        # r'\bhigh\b',             # Careful: removes 'high' from 'highland' if not bounded
        # r'\bmiddle\b',
        # r'\belementary\b',
        # r'\bjunior\b',
        # r'\bsenior\b',
        # r'\bupper\b',
        # r'\blower\b',
        # r'\bintermediate\b',
        # r'\bkindergarten\b',
        # r'\bk8\b',
        # r'\bk12\b',
        # Common descriptors & types
        #r'\bmagnet\b',
        r'\bacademy\b',
        r'\bpreparatory\b',
        r'\binstitute\b',
        #r'\bcenter\b',
        r'\bcampus\b',
        r'\bprogram\b',
        r'\bday\b',              # e.g., keshet day school
        r'\bschools\b',
        r'\bschool\b',           # Usually safe to remove after mapping types
        # Religious/Affiliation
        # r'\bcatholic\b',
        # r'\blutheran\b',
        # r'\bchristian\b',
        # r'\bfriends\b',          # e.g., buckingham friends school
        # r'\bchurch\b',           # e.g., bryn athyn church school
        # Organizational/Location/Misc
        r'\bregional\b',
        #r'\btechnical\b',
        #r'\bvocational\b',
        #r'\bcharter\b',          # If not part of 'charter school'
        r'\bcooperative\b',
        r'\bpublic\b',           # If not part of 'public school'
        r'\bof\b',               # Remove 'of' (e.g., 'school of the arts')
        r'\bthe\b',              # Remove 'the'
        r'\ban\b',               # Remove 'an'
        r'\bat\b',               # e.g., rise academy at van sickle
        # Specific phrases/qualifiers from file
        r'\bschool for\b',       # e.g., school for the deaf
        # r'\bdeaf\b',             # If always part of a descriptor phrase
        # r'\bblind\b',            # If relevant
        # r'\barts and sciences\b',
        # r'\bexpeditionary learning\b',
        r'\bacademic center\b',
        # r'\bearly learning center\b',
        # r'\bcontinuation\b',
        # r'\bpost secondary\b',
        # r'\bspecial education\b', # Added from district example if relevant
        # Potential Org/District remnants (Use cautiously)
        # r'\bksd\b', r'\bks\b', r'\bcvusd\b' # Uncomment/add carefully if needed
    ]

    for col in cols_to_standardize:
        
        if col not in df_standardized.columns:
            print(f"Warning: Column '{col}' not found in DataFrame. Skipping.")
            continue

        std_col = col

        df_standardized[std_col] = df_standardized.apply(lambda row: row[col].lower(), axis=1)


        # Ensure working with strings, handle potential NaN/None values
        df_standardized[std_col] = df_standardized[std_col].fillna('').astype(str)

        # Pre-processing within function (optional, depends on prior steps)
        # Example: Replace '/' if not handled before mapping 'jr/sr'
        # df_standardized[col] = df_standardized[col].str.replace('/', ' ', regex=False)

        # 1. Expand abbreviations (Multiple passes for potential chaining)
        for _ in range(1):
            for abbr, full in abbreviation_map.items():
                df_standardized[std_col] = df_standardized[std_col].str.replace(abbr, full, regex=True)

        # 2. Remove common terms (Multiple passes)
        for _ in range(2):
            for term in terms_to_remove:
                # Pad with spaces for whole word matching, handle start/end cases
                df_standardized[std_col] = df_standardized[std_col].str.replace(f' {term} ', ' ', regex=True)
                df_standardized[std_col] = df_standardized[std_col].str.replace(fr'^{term}\s+', '', regex=True)
                df_standardized[std_col] = df_standardized[std_col].str.replace(fr'\s+{term}$', '', regex=True)
                df_standardized[std_col] = df_standardized[std_col].str.replace(fr'^{term}$', '', regex=True) # Handle if term is the whole string

        # 3. Final whitespace cleanup
        df_standardized[std_col] = df_standardized[std_col].str.strip()
        df_standardized[std_col] = df_standardized[std_col].str.replace(r'\s+', ' ', regex=True)

        # 4. Optional: Handle possessive 's' (Use with extreme caution!)
        # This is risky as it might affect names like "st james's school" or plurals like "arts"
        # Consider only applying if fuzzy matching doesn't handle it well.
        # Example (apply carefully):
        # df_standardized[col] = df_standardized[col].apply(lambda x: re.sub(r"(\w+)s$", r"\1", x) if x.endswith('s') and not x.endswith('ss') else x)


    return df_standardized

# # --- Example Usage (using the same sample data) ---
# data = {
#     'FOCUS_SCHOOL_NAME_standardized': [
#         'lake spokane elem school', 'metcalf school', 'lewis es', 'longwood middle school',
#         'd89 westfield elementary', 'van sickle campus', 'janis dismus', 'isbell school',
#         'winnisquam high school', 'chestnut hill elementary', 'st paul luthern school',
#         'keshet day school', 'glenbard north hs', 'ava maria academy mt lebanon campus',
#         'sherwood es', 'iroquois jshs', 'paxton buckley loda jr sr high', 'bishop hendricken hs', # Assuming '/' replaced by space
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
# columns_to_process = ['FOCUS_SCHOOL_NAME_standardized', 'NCES_SCH_NAME_standardized']
# df_output_v2 = standardize_school_names_v2(df_input, columns_to_process)
# print("\nStandardized DataFrame (v2):")
# print(df_output_v2)