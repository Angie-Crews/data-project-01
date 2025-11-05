"""
scripts/data_preparation/prepare_sales.py

This script reads data from the data/raw folder, cleans the data,
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
    """Simple data cleaning utilities."""

    def __init__(self, df):
        self.df = df

    def remove_duplicate_records(self):
        """Remove duplicate records from DataFrame."""
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

# TODO: Complete this by implementing functions based on the logic in the other scripts


def read_raw_data(file_name: str) -> pd.DataFrame:
    """
    Read raw data from CSV.

    Args:
        file_name (str): Name of the CSV file to read.

    Returns:
        pd.DataFrame: Loaded DataFrame.
    """
    logger.info(f"FUNCTION START: read_raw_data with file_name={file_name}")
    file_path = RAW_DATA_DIR.joinpath(file_name)
    logger.info(f"Reading data from {file_path}")
    df = pd.read_csv(file_path)
    logger.info(f"Loaded dataframe with {len(df)} rows and {len(df.columns)} columns")

    # Data Profiling - Understanding the dataset structure and content
    logger.info("=" * 50)
    logger.info("DATA PROFILING REPORT")
    logger.info("=" * 50)

    # 1. Column data types
    logger.info(f"Column datatypes:\n{df.dtypes}")

    # 2. Number of unique values per column
    logger.info(f"Number of unique values per column:\n{df.nunique()}")

    # 3. Basic statistical summary for numeric columns
    numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns.tolist()
    if numeric_cols:
        logger.info(f"Numeric columns found: {numeric_cols}")
        logger.info(f"Statistical summary for numeric columns:\n{df[numeric_cols].describe()}")

    # 4. Sample values for categorical columns
    categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
    if categorical_cols:
        logger.info(f"Categorical columns found: {categorical_cols}")
        for col in categorical_cols:
            unique_vals = df[col].unique()[:5]  # Show first 5 unique values
            logger.info(f"Sample values in '{col}': {unique_vals}")

    # 5. Check for potential data quality issues
    logger.info("DATA QUALITY CHECKS:")
    total_cells = len(df) * len(df.columns)
    missing_cells = df.isnull().sum().sum()
    logger.info(f"Total cells: {total_cells}")
    logger.info(f"Missing cells: {missing_cells}")
    logger.info(f"Data completeness: {((total_cells - missing_cells) / total_cells * 100):.1f}%")

    logger.info("=" * 50)
    logger.info("END DATA PROFILING REPORT")
    logger.info("=" * 50)

    return df


def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    """Save cleaned data to CSV.

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
    """Remove duplicate rows from the DataFrame.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with duplicates removed.
    """
    logger.info(f"FUNCTION START: remove_duplicates with dataframe shape={df.shape}")
    initial_count = len(df)

    # Enhanced duplicate detection for sales data
    logger.info("DUPLICATE DETECTION ANALYSIS:")

    # Check for exact duplicates (all columns identical)
    exact_duplicates = df.duplicated().sum()
    logger.info(f"Exact duplicates (all columns): {exact_duplicates}")

    # Check for TransactionID duplicates (most critical for sales)
    if "transactionid" in df.columns:
        transaction_duplicates = df.duplicated(subset=["transactionid"]).sum()
        logger.info(f"TransactionID duplicates: {transaction_duplicates}")

        if transaction_duplicates > 0:
            logger.warning(f"Found {transaction_duplicates} transactions with duplicate IDs!")
            duplicate_ids = df[df.duplicated(subset=["transactionid"], keep=False)][
                "transactionid"
            ].unique()
            logger.info(f"Duplicate TransactionIDs: {duplicate_ids}")

        # Remove duplicates based on TransactionID (keep first occurrence)
        df_deduped = df.drop_duplicates(subset=["transactionid"], keep="first")
        transaction_removed = initial_count - len(df_deduped)
        logger.info(f"Removed {transaction_removed} rows based on TransactionID duplicates")
    else:
        # Fallback to exact duplicates if transactionid column not found
        logger.info("TransactionID column not found, using exact duplicate removal")
        df_deduped = df.drop_duplicates()
        transaction_removed = initial_count - len(df_deduped)
        logger.info(f"Removed {transaction_removed} exact duplicate rows")

    # Check for potential same-customer same-day duplicates (informational only)
    if "customerid" in df_deduped.columns and "transactiondate" in df_deduped.columns:
        same_customer_day = df_deduped.duplicated(
            subset=["customerid", "transactiondate", "productid"], keep=False
        ).sum()
        if same_customer_day > 0:
            logger.warning(
                f"Found {same_customer_day} potential duplicate transactions (same customer, same day, same product)"
            )

    removed_count = initial_count - len(df_deduped)
    logger.info(f"Total duplicate rows removed: {removed_count}")
    logger.info(f"{len(df_deduped)} records remaining after removing duplicates.")
    return df_deduped


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Handle missing values by filling or dropping.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with missing values handled.
    """
    logger.info(f"FUNCTION START: handle_missing_values with dataframe shape={df.shape}")

    # Log missing values by column before handling
    missing_by_col = df.isna().sum()
    total_missing_before = missing_by_col.sum()
    logger.info(f"Missing values by column before handling:\n{missing_by_col}")
    logger.info(f"Total missing values: {total_missing_before}")

    if total_missing_before == 0:
        logger.info("✅ No missing values detected - data is complete!")
        return df

    # Enhanced missing value handling with business logic for sales data
    logger.info("MISSING VALUE HANDLING STRATEGY:")
    initial_rows = len(df)

    # 1. Critical columns - Drop rows if these are missing
    critical_columns = ["transactionid", "customerid", "productid"]
    for col in critical_columns:
        if col in df.columns:
            before_drop = len(df)
            df = df.dropna(subset=[col])
            dropped = before_drop - len(df)
            if dropped > 0:
                logger.warning(f"Dropped {dropped} rows missing critical column '{col}'")

    # 2. TransactionDate - Fill with most common date or interpolate
    if "transactiondate" in df.columns and df["transactiondate"].isna().any():
        missing_dates = df["transactiondate"].isna().sum()
        logger.info(f"Filling {missing_dates} missing transaction dates...")

        # Use the most common date as fallback
        if not df["transactiondate"].mode().empty:
            most_common_date = df["transactiondate"].mode()[0]
            logger.info(f"Using most common date: '{most_common_date}'")
            df["transactiondate"] = df["transactiondate"].fillna(most_common_date)
        else:
            # Fill with a default date if no mode available
            df["transactiondate"] = df["transactiondate"].fillna("1/1/2025")

    # 3. TotalAmount - Fill with median by product category or overall median
    if "totalamount" in df.columns and df["totalamount"].isna().any():
        missing_amounts = df["totalamount"].isna().sum()
        logger.info(f"Filling {missing_amounts} missing total amounts...")

        # Fill with overall median
        overall_median = df["totalamount"].median()
        df["totalamount"] = df["totalamount"].fillna(overall_median)
        logger.info(f"Used overall median amount: ${overall_median:.2f}")

    # 4. QuantitySold - Fill with median quantity
    if "quantitysold" in df.columns and df["quantitysold"].isna().any():
        missing_qty = df["quantitysold"].isna().sum()
        logger.info(f"Filling {missing_qty} missing quantities...")

        median_qty = df["quantitysold"].median()
        df["quantitysold"] = df["quantitysold"].fillna(median_qty)
        logger.info(f"Used median quantity: {median_qty}")

    # 5. StoreID - Fill with most common store
    if "storeid" in df.columns and df["storeid"].isna().any():
        missing_stores = df["storeid"].isna().sum()
        logger.info(f"Filling {missing_stores} missing store IDs...")

        if not df["storeid"].mode().empty:
            most_common_store = df["storeid"].mode()[0]
            df["storeid"] = df["storeid"].fillna(most_common_store)
            logger.info(f"Used most common store: {most_common_store}")

    # 6. CampaignID - Fill with 0 (no campaign)
    if "campaignid" in df.columns and df["campaignid"].isna().any():
        missing_campaigns = df["campaignid"].isna().sum()
        logger.info(f"Filling {missing_campaigns} missing campaign IDs...")
        df["campaignid"] = df["campaignid"].fillna(0)
        logger.info("Missing campaign IDs set to 0 (no campaign)")

    # 7. SalesRepresentative - Fill with "Unknown Rep"
    if "salesrepresentative" in df.columns and df["salesrepresentative"].isna().any():
        missing_reps = df["salesrepresentative"].isna().sum()
        logger.info(f"Filling {missing_reps} missing sales representatives...")
        df["salesrepresentative"] = df["salesrepresentative"].fillna("Unknown Rep")

    # Log final results
    missing_after = df.isna().sum()
    total_missing_after = missing_after.sum()
    rows_removed = initial_rows - len(df)

    logger.info("MISSING VALUE HANDLING RESULTS:")
    logger.info(f"Missing values by column after handling:\n{missing_after}")
    logger.info(f"Total missing values before: {total_missing_before}")
    logger.info(f"Total missing values after: {total_missing_after}")
    logger.info(f"Rows removed: {rows_removed}")
    logger.info(f"{len(df)} records remaining after handling missing values.")

    return df


def remove_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """Remove outliers based on thresholds.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with outliers removed.
    """
    logger.info(f"FUNCTION START: remove_outliers with dataframe shape={df.shape}")
    initial_count = len(df)

    logger.info("OUTLIER DETECTION ANALYSIS:")
    logger.info("=" * 40)

    # 1. TransactionID validation - should be positive and sequential
    if "transactionid" in df.columns:
        logger.info("Checking TransactionID outliers...")
        before_transaction_id = len(df)

        # Business rule: TransactionIDs should be positive
        df = df[df["transactionid"] > 0]
        invalid_transaction_id = before_transaction_id - len(df)
        logger.info(f"Removed {invalid_transaction_id} transactions with invalid ID (≤ 0)")

    # 2. TotalAmount outlier detection
    if "totalamount" in df.columns:
        logger.info("Checking TotalAmount outliers...")
        before_amount = len(df)

        # Convert TotalAmount to numeric first
        df["totalamount"] = pd.to_numeric(df["totalamount"], errors="coerce")

        # Business rule: Amounts should be positive
        df = df[df["totalamount"] > 0]
        negative_amounts = before_amount - len(df)
        logger.info(f"Removed {negative_amounts} transactions with non-positive amounts")

        # Statistical analysis: IQR method for extreme amounts
        if len(df) > 0:
            Q1 = df["totalamount"].quantile(0.25)
            Q3 = df["totalamount"].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 2.0 * IQR
            upper_bound = Q3 + 2.0 * IQR

            logger.info(f"Amount statistics: Q1=${Q1:.2f}, Q3=${Q3:.2f}, IQR=${IQR:.2f}")
            logger.info(f"Statistical outlier bounds: ${lower_bound:.2f} - ${upper_bound:.2f}")

            # Apply statistical bounds with business limits
            business_upper_limit = 50000.0  # $50,000 max reasonable transaction
            effective_upper = min(upper_bound, business_upper_limit)
            effective_lower = max(lower_bound, 0.01)  # Minimum $0.01

            before_stat = len(df)
            df = df[(df["totalamount"] >= effective_lower) & (df["totalamount"] <= effective_upper)]
            stat_outliers = before_stat - len(df)

            logger.info(f"Applied amount bounds: ${effective_lower:.2f} - ${effective_upper:.2f}")
            logger.info(f"Removed {stat_outliers} transactions with extreme amounts")

    # 3. QuantitySold outlier detection
    if "quantitysold" in df.columns:
        logger.info("Checking QuantitySold outliers...")
        before_qty = len(df)

        # Business rule: Quantities should be positive
        df = df[df["quantitysold"] > 0]
        non_positive_qty = before_qty - len(df)
        logger.info(f"Removed {non_positive_qty} transactions with non-positive quantities")

        # Statistical analysis for extremely high quantities
        if len(df) > 0:
            Q1 = df["quantitysold"].quantile(0.25)
            Q3 = df["quantitysold"].quantile(0.75)
            IQR = Q3 - Q1
            upper_bound = Q3 + 2.0 * IQR

            # Business upper limit: 1000 units max reasonable single transaction
            business_upper_limit = 1000
            effective_upper = min(upper_bound, business_upper_limit)

            logger.info(f"Quantity statistics: Q1={Q1:.0f}, Q3={Q3:.0f}, IQR={IQR:.0f}")
            logger.info(f"Quantity upper bound: {effective_upper:.0f} units")

            before_qty_stat = len(df)
            df = df[df["quantitysold"] <= effective_upper]
            qty_outliers = before_qty_stat - len(df)

            logger.info(f"Removed {qty_outliers} transactions with extremely high quantities")

    # 4. Date validation
    if "transactiondate" in df.columns:
        logger.info("Checking TransactionDate outliers...")
        before_date = len(df)

        # Convert to datetime if not already
        try:
            df["transactiondate"] = pd.to_datetime(df["transactiondate"])

            # Business rule: Dates should be within reasonable range (not future, not too old)
            current_date = pd.Timestamp.now()
            earliest_date = pd.Timestamp("2020-01-01")  # Reasonable business start

            df = df[
                (df["transactiondate"] >= earliest_date) & (df["transactiondate"] <= current_date)
            ]
            invalid_dates = before_date - len(df)
            logger.info(f"Removed {invalid_dates} transactions with invalid dates")

        except Exception as e:
            logger.warning(f"Could not validate dates: {e}")

    # Final summary
    removed_count = initial_count - len(df)

    logger.info("=" * 40)
    logger.info("OUTLIER REMOVAL SUMMARY:")
    logger.info(f"Initial records: {initial_count}")
    logger.info(f"Records after outlier removal: {len(df)}")
    logger.info(f"Total outliers removed: {removed_count}")
    logger.info(f"Data retention rate: {(len(df) / initial_count) * 100:.1f}%")
    logger.info("=" * 40)

    return df


def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """Validate data against business rules.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: Validated DataFrame.
    """
    logger.info(f"FUNCTION START: validate_data with dataframe shape={df.shape}")
    initial_count = len(df)

    logger.info("BUSINESS RULE VALIDATION:")
    logger.info("=" * 40)

    # 1. Transaction integrity validation
    if "totalamount" in df.columns and "quantitysold" in df.columns:
        logger.info("Validating transaction integrity...")

        # Calculate average unit price per transaction
        df["unit_price"] = df["totalamount"] / df["quantitysold"]

        # Flag transactions with extremely low or high unit prices
        low_price_threshold = 0.01
        high_price_threshold = 10000.0

        low_price_count = (df["unit_price"] < low_price_threshold).sum()
        high_price_count = (df["unit_price"] > high_price_threshold).sum()

        logger.info(f"Transactions with unit price < ${low_price_threshold}: {low_price_count}")
        logger.info(f"Transactions with unit price > ${high_price_threshold}: {high_price_count}")

        # Remove transactions with impossible unit prices
        before_price_check = len(df)
        df = df[
            (df["unit_price"] >= low_price_threshold) & (df["unit_price"] <= high_price_threshold)
        ]
        invalid_prices = before_price_check - len(df)
        logger.info(f"Removed {invalid_prices} transactions with invalid unit prices")

    # 2. CustomerID validation
    if "customerid" in df.columns:
        logger.info("Validating CustomerID business rules...")

        # Rule: CustomerIDs should be within expected range
        before_customer_range = len(df)
        df = df[(df["customerid"] >= 1000) & (df["customerid"] <= 9999)]
        invalid_customer_range = before_customer_range - len(df)
        logger.info(f"Removed {invalid_customer_range} transactions with invalid CustomerID range")

    # 3. ProductID validation
    if "productid" in df.columns:
        logger.info("Validating ProductID business rules...")

        # Rule: ProductIDs should be within expected range
        before_product_range = len(df)
        df = df[(df["productid"] >= 2000) & (df["productid"] <= 2999)]
        invalid_product_range = before_product_range - len(df)
        logger.info(f"Removed {invalid_product_range} transactions with invalid ProductID range")

    # 4. StoreID validation
    if "storeid" in df.columns:
        logger.info("Validating StoreID business rules...")

        # Rule: StoreIDs should be within expected range
        before_store_range = len(df)
        df = df[(df["storeid"] >= 401) & (df["storeid"] <= 499)]
        invalid_store_range = before_store_range - len(df)
        logger.info(f"Removed {invalid_store_range} transactions with invalid StoreID range")

    # 5. Sales Representative validation
    if "salesrepresentative" in df.columns:
        logger.info("Validating SalesRepresentative business rules...")

        # Rule: Sales rep names should be reasonable length
        before_rep_length = len(df)
        df = df[
            (df["salesrepresentative"].str.len() >= 2) & (df["salesrepresentative"].str.len() <= 50)
        ]
        invalid_rep_length = before_rep_length - len(df)
        logger.info(f"Removed {invalid_rep_length} transactions with invalid sales rep name length")

    # Final validation summary
    final_count = len(df)
    removed_count = initial_count - final_count

    logger.info("=" * 40)
    logger.info("VALIDATION SUMMARY:")
    logger.info(f"Initial records: {initial_count}")
    logger.info(f"Records after validation: {final_count}")
    logger.info(f"Records removed by validation: {removed_count}")
    logger.info(f"Validation pass rate: {(final_count / initial_count) * 100:.1f}%")
    logger.info("=" * 40)

    logger.info("Data validation complete")
    return df


def standardize_formats(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize the formatting of various columns.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with standardized formatting.
    """
    logger.info(f"FUNCTION START: standardize_formats with dataframe shape={df.shape}")

    logger.info("FORMAT STANDARDIZATION:")
    logger.info("=" * 40)

    # 1. TransactionDate standardization
    if "transactiondate" in df.columns:
        logger.info("Standardizing TransactionDate format...")

        # Handle date conversion with error handling for invalid dates
        try:
            # First, try to convert with mixed formats to handle inconsistencies
            df["transactiondate"] = pd.to_datetime(
                df["transactiondate"], format="mixed", errors="coerce"
            )

            # Remove rows with invalid dates (NaT values)
            initial_rows = len(df)
            df = df.dropna(subset=["transactiondate"])
            invalid_dates = initial_rows - len(df)

            if invalid_dates > 0:
                logger.warning(f"Removed {invalid_dates} rows with unparseable dates")

            # Format to consistent YYYY-MM-DD
            df["transactiondate"] = df["transactiondate"].dt.strftime("%Y-%m-%d")
            logger.info("Standardized date format to YYYY-MM-DD")

        except Exception as e:
            logger.error(f"Failed to standardize dates: {e}")
            # Fallback: keep original format if conversion fails
            logger.info(
                "Keeping original date format due to conversion issues"
            )  # 2. TotalAmount standardization
    if "totalamount" in df.columns:
        logger.info("Standardizing TotalAmount format...")

        # Round to 2 decimal places for currency
        df["totalamount"] = df["totalamount"].round(2)

        amount_stats = df["totalamount"].describe()
        logger.info(f"Amount range: ${amount_stats['min']:.2f} - ${amount_stats['max']:.2f}")
        logger.info(f"Average transaction: ${amount_stats['mean']:.2f}")

    # 3. QuantitySold standardization
    if "quantitysold" in df.columns:
        logger.info("Standardizing QuantitySold format...")

        # Ensure integers
        df["quantitysold"] = df["quantitysold"].astype(int)

        qty_stats = df["quantitysold"].describe()
        logger.info(f"Quantity range: {int(qty_stats['min'])} - {int(qty_stats['max'])} units")
        logger.info(f"Average quantity: {qty_stats['mean']:.1f} units")

    # 4. SalesRepresentative standardization
    if "salesrepresentative" in df.columns:
        logger.info("Standardizing SalesRepresentative format...")

        original_reps = df["salesrepresentative"].copy()

        # Standardize format and clean whitespace
        df["salesrepresentative"] = df["salesrepresentative"].str.title()
        df["salesrepresentative"] = df["salesrepresentative"].str.strip()

        reps_changed = (original_reps != df["salesrepresentative"]).sum()
        logger.info(f"Standardized {reps_changed} sales representative names")

        # Show sales rep distribution
        rep_counts = df["salesrepresentative"].value_counts()
        logger.info(f"Top sales reps: {dict(rep_counts.head(5))}")

    # 5. ID column standardization
    id_columns = ["transactionid", "customerid", "productid", "storeid", "campaignid"]
    for col in id_columns:
        if col in df.columns:
            # Ensure all IDs are integers
            df[col] = df[col].astype(int)

    # Final format validation
    logger.info("Final format validation...")

    # Remove any remaining whitespace in text columns
    for col in df.select_dtypes(include=["object"]).columns:
        if col != "transactiondate":  # Skip date column
            df[col] = df[col].str.strip()

    logger.info("=" * 40)
    logger.info("FORMAT STANDARDIZATION SUMMARY:")
    logger.info("✅ TransactionDate: YYYY-MM-DD format")
    logger.info("✅ TotalAmount: 2 decimal places")
    logger.info("✅ QuantitySold: Integer values")
    logger.info("✅ SalesRepresentative: Title case, trimmed")
    logger.info("✅ ID columns: Integer format")
    logger.info("=" * 40)

    logger.info("Completed standardizing formats")
    return df


#####################################
# Define Main Function - The main entry point of the script
#####################################


def main() -> None:
    """Process sales data."""
    # Initialize the logger first
    init_logger()

    logger.info("==================================")
    logger.info("STARTING prepare_sales_data.py")
    logger.info("==================================")

    logger.info(f"Root         : {PROJECT_ROOT}")
    logger.info(f"data/raw     : {RAW_DATA_DIR}")
    logger.info(f"data/prepared: {PREPARED_DATA_DIR}")
    logger.info(f"scripts      : {SCRIPTS_DIR}")

    input_file = "sales_data.csv"

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

    # Clean column names to lowercase
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

    # Remove duplicates
    df = remove_duplicates(df)

    # Handle missing values
    df = handle_missing_values(df)

    # Remove outliers
    df = remove_outliers(df)

    # Validate data
    df = validate_data(df)

    # Standardize formats
    df = standardize_formats(df)

    # Save prepared data
    save_prepared_data(df, "sales_prepared.csv")

    logger.info("==================================")
    logger.info(f"Original shape: {original_shape}")
    logger.info(f"Cleaned shape:  {df.shape}")
    logger.info("==================================")
    logger.info("FINISHED prepare_sales_data.py")
    logger.info("==")


#####################################
# Conditional Execution Block
# Ensures the script runs only when executed directly
# This is a common Python convention.
#####################################

if __name__ == "__main__":
    main()
