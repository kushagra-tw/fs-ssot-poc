import yaml
import pandas as pd
from pathlib import Path


# import os # Not needed for path resolution if avoiding env vars for paths

# --- Configuration Loading ---

def load_config(config_filename='config.yaml'):
    """
    Loads configuration from a YAML file located in the config directory
    at the project root.
    """
    try:
        # Get the absolute path to the current script file
        script_path = Path(__file__).resolve()
        # Get the directory containing the script (scripts/)
        script_dir = script_path.parent
        # Get the project root directory (one level up from scripts/)
        project_root = script_dir.parent
        # Construct the path to the config file within the config/ directory
        config_file_path = project_root / 'config' / config_filename

        print(f"Attempting to load configuration from: {config_file_path}")

        with open(config_file_path, 'r') as f:
            config = yaml.safe_load(f)  # Use safe_load for security
            print(f"Configuration loaded successfully.")
            return config

    except FileNotFoundError:
        print(f"Error: Configuration file not found at '{config_file_path}'")
        print("Please ensure 'config.yaml' exists in the 'config/' directory.")
        print("You may need to copy 'config.yaml.example' to 'config.yaml' and edit it.")
        return None
    except yaml.YAMLError as e:
        print(f"Error parsing YAML configuration file '{config_file_path}': {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred loading configuration: {e}")
        return None


# --- Main Script Logic ---

# Define project root early for resolving data paths later
# (Alternatively, pass project_root from load_config if needed elsewhere)
try:
    project_root = Path(__file__).resolve().parent.parent
except NameError:
    # Handle cases where __file__ might not be defined (e.g., interactive execution)
    project_root = Path('../src').resolve()
    print(f"Warning: __file__ not defined. Assuming project root is current directory: {project_root}")

# Load the configuration
config = load_config('config.yaml')  # Pass the filename

if config:
    try:
        # --- Construct Data Paths using pathlib ---
        # Get path strings directly from the loaded config
        raw_dir_str = config['paths']['raw_data_dir']
        processed_dir_str = config['paths']['processed_data_dir']
        interim_dir_str = config['paths']['interim_data_dir']

        # Convert strings to Path objects
        # Resolve paths relative to project_root if they are relative in config
        # Path() handles absolute paths correctly if they are used in config
        raw_dir = (project_root / raw_dir_str).resolve()
        processed_dir = (project_root / processed_dir_str).resolve()
        interim_dir = (project_root / interim_dir_str).resolve()

        # Construct full file paths
        customer_input_path = raw_dir / config['paths']['input_customers']
        pretty_customers_output_path = processed_dir / config['paths']['pretty_customers']
        # (... construct other paths ...)

        # Ensure output directories exist
        processed_dir.mkdir(parents=True, exist_ok=True)
        interim_dir.mkdir(parents=True, exist_ok=True)
        # (... ensure other dirs ...)

        print(f"\n--- Using configured paths ---")
        print(f"Project Root Context: {project_root}")
        print(f"Customer Input Path: {customer_input_path}")
        print(f"Pretty Customers Output Path: {pretty_customers_output_path}")

        # # --- Example Pandas Usage (remains the same) ---
        # try:
        #     df_customers = pd.read_csv(customer_input_path)
        #     print(f"Successfully read {len(df_customers)} rows from customer input.")
        #
        #     # --- Processing ---
        #     df_pretty_customers = df_customers.head(5).copy()
        #     df_pretty_customers['processed_timestamp'] = pd.Timestamp.now()
        #     # --- End Processing ---
        #
        #     print(f"\nAttempting to write to: {pretty_customers_output_path}")
        #     df_pretty_customers.to_csv(pretty_customers_output_path, index=False)
        #     print(f"Successfully wrote pretty customer data.")
        #
        # except FileNotFoundError:
        #     print(f"Error: Input file not found at {customer_input_path}")
        # except Exception as e:
        #     print(f"An error occurred during Pandas operations: {e}")

    except KeyError as e:
        print(f"Error: Missing key in configuration file: {e}")
    except Exception as e:
        print(f"An unexpected error occurred in the main script logic: {e}")

else:
    print("Exiting script due to configuration loading failure.")