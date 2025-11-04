"""scripts/data_preparation/prepare_customers.py.

This script reads customer data from the data/raw folder, cleans the data,
and writes the cleaned version to the data/prepared folder.

Tasks:
- Remove duplicates
- Handle missing values
- Remove outliers
- Ensure consistent formatting

"""

#####################################
# Import Modules at the Top
#####################################

# Import from Python Standard Library
import pathlib
import sys

# Import from external packages (requires a virtual environment)
import pandas as pd

# Ensure project root is in sys.path for local imports (now 2 parents are needed)
sys.path.append(str(pathlib.Path(__file__).resolve().parent.parent))

# Import local modules (e.g. utils_logger.py)
from utils_logger import logger, init_logger


# Simple DataScrubber class for this script
class DataScrubber:
    def __init__(self, df):
        self.df = df

    def remove_duplicate_records(self):
        return self.df.drop_duplicates()


# Constants
SCRIPTS_DATA_PREP_DIR: pathlib.Path = (
    pathlib.Path(__file__).resolve().parent
)  # Directory of the current script
SCRIPTS_DIR: pathlib.Path = SCRIPTS_DATA_PREP_DIR.parent
PROJECT_ROOT: pathlib.Path = SCRIPTS_DIR.parent.parent
DATA_DIR: pathlib.Path = PROJECT_ROOT / "data"
RAW_DATA_DIR: pathlib.Path = DATA_DIR / "raw"
PREPARED_DATA_DIR: pathlib.Path = DATA_DIR / "prepared"  # place to store prepared data


# Ensure the directories exist or create them
DATA_DIR.mkdir(exist_ok=True)
RAW_DATA_DIR.mkdir(exist_ok=True)
PREPARED_DATA_DIR.mkdir(exist_ok=True)

#####################################
# Define Functions - Reusable blocks of code / instructions
#####################################


def read_raw_data(file_name: str) -> pd.DataFrame:
    """Read raw data from CSV."""
    file_path: pathlib.Path = RAW_DATA_DIR.joinpath(file_name)
    try:
        logger.info(f"READING: {file_path}.")
        return pd.read_csv(file_path)
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return pd.DataFrame()  # Return an empty DataFrame if the file is not found
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame if any other error occurs


def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    """
    Save cleaned data to CSV.

    Args:
        df (pd.DataFrame): Cleaned DataFrame.
        file_name (str): Name of the output file.
    """
    logger.info(
        f"FUNCTION START: save_prepared_data with file_name={file_name}, dataframe shape={df.shape}"
    )
    file_path = PREPARED_DATA_DIR.joinpath(file_name)
    df.to_csv(file_path, index=False)
    logger.info(f"Data saved to {file_path}")


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate rows from the DataFrame.
    How do you decide if a row is duplicated?
    Which do you keep? Which do you delete?

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with duplicates removed.
    """
    logger.info(f"FUNCTION START: remove_duplicates with dataframe shape={df.shape}")

    # Let's delegate this to the DataScrubber class
    # First, create an instance of the DataScrubber class
    # by passing in the dataframe as an argument.
    df_scrubber = DataScrubber(df)

    # Now, call the method on our instance to remove duplicates.
    # This method will return a new dataframe with duplicates removed.
    df_deduped = df_scrubber.remove_duplicate_records()

    logger.info(f"Original dataframe shape: {df.shape}")
    logger.info(f"Deduped  dataframe shape: {df_deduped.shape}")
    return df_deduped


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle missing values by filling or dropping.
    This logic is specific to the actual data and business rules.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with missing values handled.
    """
    logger.info(f"FUNCTION START: handle_missing_values with dataframe shape={df.shape}")

    # Log missing values count before handling
    missing_before = df.isna().sum().sum()
    logger.info(f"Total missing values before handling: {missing_before}")

    # Fill or drop missing values based on business rules
    df["CustomerName"].fillna("Unknown", inplace=True)
    df.dropna(subset=["CustomerID"], inplace=True)

    # Log missing values count after handling
    missing_after = df.isna().sum().sum()
    logger.info(f"Total missing values after handling: {missing_after}")
    logger.info(f"{len(df)} records remaining after handling missing values.")
    return df


def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove outliers based on thresholds.
    This logic is very specific to the actual data and business rules.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with outliers removed.
    """
    logger.info(f"FUNCTION START: remove_outliers with dataframe shape={df.shape}")
    initial_count = len(df)

    # 1. CustomerID validation - should be positive integers
    logger.info("Checking CustomerID outliers...")
    before_customerid = len(df)
    df = df[df['CustomerID'] > 0]
    customerid_removed = before_customerid - len(df)
    logger.info(f"Removed {customerid_removed} rows with invalid CustomerID")

    # 2. TotalSpend validation - should be reasonable spending amounts
    logger.info("Checking TotalSpend outliers...")
    before_spend = len(df)
    df = df[(df['TotalSpend'] >= 0) & (df['TotalSpend'] <= 50000)]  # $0 to $50K range
    spend_removed = before_spend - len(df)
    logger.info(f"Removed {spend_removed} rows with TotalSpend outside $0-$50,000 range")

    # 3. CustomerSince date validation - should be realistic date range
    logger.info("Checking CustomerSince date outliers...")
    before_date = len(df)
    try:
        df['CustomerSince'] = pd.to_datetime(df['CustomerSince'])
        # Assume business started in 2015 and dates should not be in the future
        df = df[(df['CustomerSince'] >= '2015-01-01') & (df['CustomerSince'] <= pd.Timestamp.now())]
        date_removed = before_date - len(df)
        logger.info(f"Removed {date_removed} rows with invalid CustomerSince dates")
    except Exception as e:
        logger.warning(f"Could not process CustomerSince dates: {e}")
        date_removed = 0

    # 4. Region standardization and validation
    logger.info("Checking Region outliers...")
    before_region = len(df)
    # Standardize capitalization
    df['Region'] = df['Region'].str.title()
    # Define valid regions
    valid_regions = ['West', 'East', 'Central', 'North', 'South']
    df = df[df['Region'].isin(valid_regions)]
    region_removed = before_region - len(df)
    logger.info(f"Removed {region_removed} rows with invalid regions")

    # 5. CustomerStatus validation
    logger.info("Checking CustomerStatus outliers...")
    before_status = len(df)
    valid_statuses = ['Regular', 'Inactive', 'VIP', 'New']
    df = df[df['CustomerStatus'].isin(valid_statuses)]
    status_removed = before_status - len(df)
    logger.info(f"Removed {status_removed} rows with invalid customer status")

    removed_count = initial_count - len(df)
    logger.info(f"Total outliers removed: {removed_count} rows")
    logger.info(f"{len(df)} records remaining after removing outliers.")
    return df


#####################################
# Define Main Function - The main entry point of the script
#####################################


def main() -> None:
    """Process customer data."""
    # Initialize the logger first
    init_logger()

    logger.info("==================================")
    logger.info("STARTING prepare_customers_data.py")
    logger.info("==================================")

    logger.info(f"Root         : {PROJECT_ROOT}")
    logger.info(f"data/raw     : {RAW_DATA_DIR}")
    logger.info(f"data/prepared: {PREPARED_DATA_DIR}")
    logger.info(f"scripts      : {SCRIPTS_DIR}")

    input_file = "customers_data.csv"
    output_file = "customers_prepared.csv"

    # Read raw data
    df = read_raw_data(input_file)

    # Record original shape
    original_shape = df.shape

    # Log initial dataframe information
    logger.info(f"Initial dataframe columns: {', '.join(df.columns.tolist())}")
    logger.info(f"Initial dataframe shape: {df.shape}")

    # Clean column names
    original_columns = df.columns.tolist()
    df.columns = df.columns.str.strip()

    # Log if any column names changed
    changed_columns = [
        f"{old} -> {new}" for old, new in zip(original_columns, df.columns) if old != new
    ]
    if changed_columns:
        logger.info(f"Cleaned column names: {', '.join(changed_columns)}")

    # Remove duplicates
    df = remove_duplicates(df)

    # Handle missing values
    df = handle_missing_values(df)

    # Remove outliers
    df = remove_outliers(df)

    # Save prepared data
    save_prepared_data(df, output_file)

    logger.info("==================================")
    logger.info(f"Original shape: {df.shape}")
    logger.info(f"Cleaned shape:  {original_shape}")
    logger.info("==================================")
    logger.info("FINISHED prepare_customers_data.py")
    logger.info("==================================")


#####################################
# Conditional Execution Block
# Ensures the script runs only when executed directly
# This is a common Python convention.
#####################################

if __name__ == "__main__":
    main()
