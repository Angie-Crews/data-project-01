"""
scripts/data_preparation/prepare_products.py

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

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with duplicates removed.
    """
    logger.info(f"FUNCTION START: remove_duplicates with dataframe shape={df.shape}")
    initial_count = len(df)

    # Enhanced duplicate detection for product data
    logger.info("DUPLICATE DETECTION ANALYSIS:")

    # Check for exact duplicates (all columns identical)
    exact_duplicates = df.duplicated().sum()
    logger.info(f"Exact duplicates (all columns): {exact_duplicates}")

    # Check for ProductID duplicates (most critical for products)
    if "productid" in df.columns:
        productid_duplicates = df.duplicated(subset=["productid"]).sum()
        logger.info(f"ProductID duplicates: {productid_duplicates}")

        if productid_duplicates > 0:
            logger.warning(f"Found {productid_duplicates} products with duplicate ProductIDs!")
            duplicate_ids = df[df.duplicated(subset=["productid"], keep=False)][
                "productid"
            ].unique()
            logger.info(f"Duplicate ProductIDs: {duplicate_ids}")

        # Remove duplicates based on ProductID (keep first occurrence)
        df_deduped = df.drop_duplicates(subset=["productid"], keep="first")
        productid_removed = initial_count - len(df_deduped)
        logger.info(f"Removed {productid_removed} rows based on ProductID duplicates")
    else:
        # Fallback to exact duplicates if productid column not found
        logger.info("ProductID column not found, using exact duplicate removal")
        df_deduped = df.drop_duplicates()
        productid_removed = initial_count - len(df_deduped)
        logger.info(f"Removed {productid_removed} exact duplicate rows")

    # Check for potential product name duplicates (informational only)
    if "productname" in df_deduped.columns:
        name_duplicates = df_deduped.duplicated(subset=["productname"]).sum()
        if name_duplicates > 0:
            logger.warning(
                f"Found {name_duplicates} products with duplicate names (but different IDs)"
            )
            duplicate_names = df_deduped[df_deduped.duplicated(subset=["productname"], keep=False)][
                "productname"
            ].unique()
            logger.info(f"Duplicate product names: {duplicate_names}")

    removed_count = initial_count - len(df_deduped)
    logger.info(f"Total duplicate rows removed: {removed_count}")
    logger.info(f"{len(df_deduped)} records remaining after removing duplicates.")
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

    # Log missing values by column before handling
    missing_by_col = df.isna().sum()
    total_missing_before = missing_by_col.sum()
    logger.info(f"Missing values by column before handling:\n{missing_by_col}")
    logger.info(f"Total missing values: {total_missing_before}")

    if total_missing_before == 0:
        logger.info("✅ No missing values detected - data is complete!")
        return df

    # Enhanced missing value handling with business logic
    logger.info("MISSING VALUE HANDLING STRATEGY:")
    initial_rows = len(df)

    # 1. Critical columns - Drop rows if these are missing (ProductID is essential)
    critical_columns = ["productid"]
    for col in critical_columns:
        if col in df.columns:
            before_drop = len(df)
            df = df.dropna(subset=[col])
            dropped = before_drop - len(df)
            if dropped > 0:
                logger.warning(f"Dropped {dropped} rows missing critical column '{col}'")

    # 2. ProductName - Fill with generic name based on category
    if "productname" in df.columns and df["productname"].isna().any():
        missing_names = df["productname"].isna().sum()
        logger.info(f"Filling {missing_names} missing product names...")

        if "productcategory" in df.columns:
            # Fill with category-based generic name
            df["productname"] = df["productname"].fillna(
                df["productcategory"].astype(str) + "-Product-" + df["productid"].astype(str)
            )
        else:
            # Fill with generic name using ProductID
            df["productname"] = df["productname"].fillna("Product-" + df["productid"].astype(str))

    # 3. ProductCategory - Fill with most common category or "Uncategorized"
    if "productcategory" in df.columns and df["productcategory"].isna().any():
        missing_categories = df["productcategory"].isna().sum()
        logger.info(f"Filling {missing_categories} missing product categories...")

        if not df["productcategory"].mode().empty:
            most_common_category = df["productcategory"].mode()[0]
            logger.info(f"Using most common category: '{most_common_category}'")
            df["productcategory"] = df["productcategory"].fillna(most_common_category)
        else:
            df["productcategory"] = df["productcategory"].fillna("Uncategorized")

    # 4. UnitPrice - Fill with median price by category, or overall median
    if "unitprice" in df.columns and df["unitprice"].isna().any():
        missing_prices = df["unitprice"].isna().sum()
        logger.info(f"Filling {missing_prices} missing unit prices...")

        if "productcategory" in df.columns:
            # Fill with median price per category
            df["unitprice"] = df.groupby("productcategory")["unitprice"].transform(
                lambda x: x.fillna(x.median())
            )

        # Fill any remaining NaNs with overall median
        overall_median = df["unitprice"].median()
        df["unitprice"] = df["unitprice"].fillna(overall_median)
        logger.info(
            f"Used overall median price: ${overall_median:.2f} for remaining missing values"
        )

    # 5. StockQuantity - Fill with 0 (out of stock) or category median
    if "stockquantity" in df.columns and df["stockquantity"].isna().any():
        missing_stock = df["stockquantity"].isna().sum()
        logger.info(f"Filling {missing_stock} missing stock quantities...")

        # Use median stock quantity by category
        if "productcategory" in df.columns:
            df["stockquantity"] = df.groupby("productcategory")["stockquantity"].transform(
                lambda x: x.fillna(x.median())
            )

        # Fill any remaining NaNs with 0 (assuming out of stock)
        df["stockquantity"] = df["stockquantity"].fillna(0)
        logger.info("Remaining missing stock quantities set to 0 (out of stock)")

    # 6. SupplierName - Fill with "Unknown Supplier"
    if "suppliername" in df.columns and df["suppliername"].isna().any():
        missing_suppliers = df["suppliername"].isna().sum()
        logger.info(f"Filling {missing_suppliers} missing supplier names...")
        df["suppliername"] = df["suppliername"].fillna("Unknown Supplier")

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

    logger.info("OUTLIER DETECTION ANALYSIS:")
    logger.info("=" * 40)

    # 1. ProductID validation - should be positive and within reasonable range
    if "productid" in df.columns:
        logger.info("Checking ProductID outliers...")
        before_productid = len(df)

        # Business rule: ProductIDs should be positive and within reasonable range
        df = df[df["productid"] > 0]
        invalid_productid = before_productid - len(df)
        logger.info(f"Removed {invalid_productid} products with invalid ProductID (≤ 0)")

    # 2. UnitPrice outlier detection - Combined business rules + statistical analysis
    if "unitprice" in df.columns:
        logger.info("Checking UnitPrice outliers...")
        before_price = len(df)

        # Business rule: Prices should be positive and reasonable
        df = df[df["unitprice"] > 0]  # No negative or zero prices
        negative_prices = before_price - len(df)
        logger.info(f"Removed {negative_prices} products with non-positive prices")

        # Statistical analysis: IQR method for extreme prices
        if len(df) > 0:
            Q1 = df["unitprice"].quantile(0.25)
            Q3 = df["unitprice"].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 2.0 * IQR  # Using 2.0 instead of 1.5 for less aggressive removal
            upper_bound = Q3 + 2.0 * IQR

            logger.info(f"Price statistics: Q1=${Q1:.2f}, Q3=${Q3:.2f}, IQR=${IQR:.2f}")
            logger.info(f"Statistical outlier bounds: ${lower_bound:.2f} - ${upper_bound:.2f}")

            # Apply statistical bounds but also business upper limit
            business_upper_limit = 10000.0  # $10,000 max reasonable price
            effective_upper = min(upper_bound, business_upper_limit)
            effective_lower = max(lower_bound, 0.01)  # Minimum $0.01

            before_stat = len(df)
            df = df[(df["unitprice"] >= effective_lower) & (df["unitprice"] <= effective_upper)]
            stat_outliers = before_stat - len(df)

            logger.info(f"Applied price bounds: ${effective_lower:.2f} - ${effective_upper:.2f}")
            logger.info(f"Removed {stat_outliers} products with extreme prices")

    # 3. StockQuantity outlier detection
    if "stockquantity" in df.columns:
        logger.info("Checking StockQuantity outliers...")
        before_stock = len(df)

        # Business rule: Stock quantities should be non-negative and reasonable
        df = df[df["stockquantity"] >= 0]  # No negative stock
        negative_stock = before_stock - len(df)
        logger.info(f"Removed {negative_stock} products with negative stock quantities")

        # Statistical analysis for extremely high stock quantities
        if len(df) > 0:
            Q1 = df["stockquantity"].quantile(0.25)
            Q3 = df["stockquantity"].quantile(0.75)
            IQR = Q3 - Q1
            upper_bound = Q3 + 2.0 * IQR  # Only check upper bound for stock

            # Business upper limit: 10,000 units max reasonable stock
            business_upper_limit = 10000
            effective_upper = min(upper_bound, business_upper_limit)

            logger.info(f"Stock statistics: Q1={Q1:.0f}, Q3={Q3:.0f}, IQR={IQR:.0f}")
            logger.info(f"Stock upper bound: {effective_upper:.0f} units")

            before_stock_stat = len(df)
            df = df[df["stockquantity"] <= effective_upper]
            stock_outliers = before_stock_stat - len(df)

            logger.info(f"Removed {stock_outliers} products with extremely high stock quantities")

    # 4. Category and Supplier validation
    if "productcategory" in df.columns:
        logger.info("Checking ProductCategory outliers...")
        before_category = len(df)

        # Remove rows where category is empty string or whitespace
        df = df[df["productcategory"].str.strip() != ""]
        empty_category = before_category - len(df)
        logger.info(f"Removed {empty_category} products with empty categories")

    if "suppliername" in df.columns:
        logger.info("Checking SupplierName outliers...")
        before_supplier = len(df)

        # Remove rows where supplier name is empty string or whitespace
        df = df[df["suppliername"].str.strip() != ""]
        empty_supplier = before_supplier - len(df)
        logger.info(f"Removed {empty_supplier} products with empty supplier names")

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


def standardize_formats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize the formatting of various columns.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with standardized formatting.
    """
    logger.info(f"FUNCTION START: standardize_formats with dataframe shape={df.shape}")

    logger.info("FORMAT STANDARDIZATION:")
    logger.info("=" * 40)

    # 1. ProductName Standardization
    if "productname" in df.columns:
        logger.info("Standardizing ProductName format...")

        # Before standardization stats
        original_names = df["productname"].copy()

        # Apply title case formatting for consistency
        df["productname"] = df["productname"].str.title()

        # Remove extra whitespace and standardize separators
        df["productname"] = df["productname"].str.strip()
        df["productname"] = df["productname"].str.replace(
            r'\s+', ' ', regex=True
        )  # Multiple spaces to single
        df["productname"] = df["productname"].str.replace(
            '_', '-'
        )  # Standardize separators to hyphens

        # Count how many names were changed
        names_changed = (original_names != df["productname"]).sum()
        logger.info(
            f"Standardized {names_changed} product names to title case and consistent separators"
        )

    # 2. ProductCategory Standardization
    if "productcategory" in df.columns:
        logger.info("Standardizing ProductCategory format...")

        original_categories = df["productcategory"].copy()

        # Standardize to title case for consistency
        df["productcategory"] = df["productcategory"].str.title()
        df["productcategory"] = df["productcategory"].str.strip()

        categories_changed = (original_categories != df["productcategory"]).sum()
        logger.info(f"Standardized {categories_changed} category names to title case")

        # Show category distribution
        category_counts = df["productcategory"].value_counts()
        logger.info(f"Category distribution: {dict(category_counts)}")

    # 3. UnitPrice Standardization (already done in validation, but ensure consistency)
    if "unitprice" in df.columns:
        logger.info("Standardizing UnitPrice format...")

        # Ensure all prices are rounded to 2 decimal places (currency standard)
        df["unitprice"] = df["unitprice"].round(2)

        # Price statistics after standardization
        price_stats = df["unitprice"].describe()
        logger.info(
            f"Price range after standardization: ${price_stats['min']:.2f} - ${price_stats['max']:.2f}"
        )
        logger.info(f"Average price: ${price_stats['mean']:.2f}")

    # 4. StockQuantity Standardization (ensure integers)
    if "stockquantity" in df.columns:
        logger.info("Standardizing StockQuantity format...")

        # Ensure all stock quantities are integers
        df["stockquantity"] = df["stockquantity"].astype(int)

        # Stock statistics
        stock_stats = df["stockquantity"].describe()
        logger.info(f"Stock range: {int(stock_stats['min'])} - {int(stock_stats['max'])} units")
        logger.info(f"Average stock: {int(stock_stats['mean'])} units")

    # 5. SupplierName Standardization
    if "suppliername" in df.columns:
        logger.info("Standardizing SupplierName format...")

        original_suppliers = df["suppliername"].copy()

        # Apply title case and clean whitespace
        df["suppliername"] = df["suppliername"].str.title()
        df["suppliername"] = df["suppliername"].str.strip()
        df["suppliername"] = df["suppliername"].str.replace(r'\s+', ' ', regex=True)

        suppliers_changed = (original_suppliers != df["suppliername"]).sum()
        logger.info(f"Standardized {suppliers_changed} supplier names to title case")

        # Show supplier distribution
        supplier_counts = df["suppliername"].value_counts()
        logger.info(f"Supplier distribution: {dict(supplier_counts)}")

    # 6. Cross-field format consistency checks
    logger.info("Performing format consistency checks...")

    # Check for any remaining data quality issues
    format_issues = 0

    # Check for unusual characters in text fields
    text_columns = ["productname", "productcategory", "suppliername"]
    for col in text_columns:
        if col in df.columns:
            # Check for non-standard characters (keep alphanumeric, spaces, hyphens, and common punctuation)
            unusual_chars = df[col].str.contains(r'[^A-Za-z0-9\s\-\.\,\&\(\)]', na=False)
            unusual_count = unusual_chars.sum()
            if unusual_count > 0:
                logger.warning(f"Found {unusual_count} records with unusual characters in {col}")
                format_issues += unusual_count

    # 7. Final format validation
    logger.info("Final format validation...")

    # Ensure no leading/trailing whitespace in any text column
    for col in df.select_dtypes(include=['object']).columns:
        before_strip = df[col].str.len().sum()
        df[col] = df[col].str.strip()
        after_strip = df[col].str.len().sum()
        whitespace_removed = before_strip - after_strip
        if whitespace_removed > 0:
            logger.info(f"Removed {whitespace_removed} whitespace characters from {col}")

    # Summary of standardization
    logger.info("=" * 40)
    logger.info("FORMAT STANDARDIZATION SUMMARY:")
    logger.info(f"✅ ProductName: Title case, consistent separators")
    logger.info(f"✅ ProductCategory: Title case, trimmed")
    logger.info(f"✅ UnitPrice: 2 decimal places, currency format")
    logger.info(f"✅ StockQuantity: Integer values")
    logger.info(f"✅ SupplierName: Title case, trimmed")
    logger.info(f"Format issues detected: {format_issues}")
    logger.info("=" * 40)

    logger.info("Completed standardizing formats")
    return df


def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate data against business rules.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: Validated DataFrame.
    """
    logger.info(f"FUNCTION START: validate_data with dataframe shape={df.shape}")
    initial_count = len(df)

    logger.info("BUSINESS RULE VALIDATION:")
    logger.info("=" * 40)

    # 1. ProductID Business Rules
    if "productid" in df.columns:
        logger.info("Validating ProductID business rules...")

        # Rule: ProductIDs should be unique (additional check after duplicate removal)
        duplicate_ids = df["productid"].duplicated().sum()
        if duplicate_ids > 0:
            logger.error(
                f"CRITICAL: Found {duplicate_ids} duplicate ProductIDs after deduplication!"
            )

        # Rule: ProductID should be within expected business range
        before_id_range = len(df)
        df = df[(df["productid"] >= 1000) & (df["productid"] <= 99999)]
        invalid_id_range = before_id_range - len(df)
        logger.info(
            f"Removed {invalid_id_range} products with ProductID outside business range (1000-99999)"
        )

    # 2. ProductName Business Rules
    if "productname" in df.columns:
        logger.info("Validating ProductName business rules...")

        # Rule: Product names should not be too short or too long
        before_name_length = len(df)
        df = df[(df["productname"].str.len() >= 2) & (df["productname"].str.len() <= 100)]
        invalid_name_length = before_name_length - len(df)
        logger.info(
            f"Removed {invalid_name_length} products with invalid name length (2-100 chars)"
        )

        # Rule: Product names should not contain only special characters or numbers
        if len(df) > 0:
            before_name_content = len(df)
            # Must contain at least one letter
            df = df[df["productname"].str.contains(r"[A-Za-z]", na=False)]
            invalid_name_content = before_name_content - len(df)
            logger.info(f"Removed {invalid_name_content} products with names containing no letters")

    # 3. ProductCategory Business Rules
    if "productcategory" in df.columns:
        logger.info("Validating ProductCategory business rules...")

        # Rule: Categories should be from approved list (based on data analysis)
        valid_categories = [
            "Electronics",
            "Clothing",
            "Home",
            "Office",
            "Books",
            "Sports",
            "Beauty",
        ]
        before_category = len(df)
        df = df[df["productcategory"].isin(valid_categories)]
        invalid_category = before_category - len(df)
        logger.info(f"Valid categories: {valid_categories}")
        logger.info(f"Removed {invalid_category} products with invalid categories")

    # 4. UnitPrice Business Rules
    if "unitprice" in df.columns:
        logger.info("Validating UnitPrice business rules...")

        # Rule: Prices should have reasonable precision (max 2 decimal places for currency)
        before_precision = len(df)
        df["unitprice"] = df["unitprice"].round(2)  # Round to 2 decimal places
        logger.info("Standardized all prices to 2 decimal places")

        # Rule: Check for suspiciously round numbers (might indicate placeholder data)
        round_prices = df[df["unitprice"] % 1 == 0]
        logger.info(
            f"Found {len(round_prices)} products with round-dollar prices (potential review needed)"
        )

        # Rule: Price should be reasonable for retail (not too cheap to be realistic)
        before_min_price = len(df)
        df = df[df["unitprice"] >= 1.0]  # Minimum $1.00
        invalid_min_price = before_min_price - len(df)
        logger.info(
            f"Removed {invalid_min_price} products with unrealistically low prices (<$1.00)"
        )

    # 5. StockQuantity Business Rules
    if "stockquantity" in df.columns:
        logger.info("Validating StockQuantity business rules...")

        # Rule: Stock quantities should be whole numbers
        before_stock_whole = len(df)
        df["stockquantity"] = df["stockquantity"].round().astype(int)
        logger.info("Ensured all stock quantities are whole numbers")

        # Rule: Identify products with critically low stock (business alert)
        low_stock = df[df["stockquantity"] <= 10]
        logger.warning(f"ALERT: {len(low_stock)} products with critically low stock (≤10 units)")

        # Rule: Identify products with zero stock (out of stock)
        zero_stock = df[df["stockquantity"] == 0]
        logger.info(f"INFO: {len(zero_stock)} products currently out of stock")

    # 6. SupplierName Business Rules
    if "suppliername" in df.columns:
        logger.info("Validating SupplierName business rules...")

        # Rule: Supplier names should be reasonable length
        before_supplier_length = len(df)
        df = df[(df["suppliername"].str.len() >= 2) & (df["suppliername"].str.len() <= 50)]
        invalid_supplier_length = before_supplier_length - len(df)
        logger.info(f"Removed {invalid_supplier_length} products with invalid supplier name length")

        # Rule: Check supplier diversity (business insight)
        supplier_counts = df["suppliername"].value_counts()
        logger.info(f"Supplier diversity: {len(supplier_counts)} unique suppliers")
        top_supplier = supplier_counts.index[0] if len(supplier_counts) > 0 else "None"
        top_supplier_count = supplier_counts.iloc[0] if len(supplier_counts) > 0 else 0
        logger.info(f"Top supplier: '{top_supplier}' with {top_supplier_count} products")

    # 7. Cross-Field Validation Rules
    logger.info("Performing cross-field validation...")

    # Rule: Electronic products should generally be more expensive than clothing
    if "productcategory" in df.columns and "unitprice" in df.columns:
        electronics = df[df["productcategory"] == "Electronics"]
        clothing = df[df["productcategory"] == "Clothing"]

        if len(electronics) > 0 and len(clothing) > 0:
            avg_electronics_price = electronics["unitprice"].mean()
            avg_clothing_price = clothing["unitprice"].mean()
            logger.info(f"Average Electronics price: ${avg_electronics_price:.2f}")
            logger.info(f"Average Clothing price: ${avg_clothing_price:.2f}")

            if avg_electronics_price < avg_clothing_price:
                logger.warning("BUSINESS ALERT: Electronics average price is lower than Clothing!")

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


def main() -> None:
    """Process product data."""
    # Initialize the logger first
    init_logger()

    logger.info("==================================")
    logger.info("STARTING prepare_products_data.py")
    logger.info("==================================")

    logger.info(f"Root         : {PROJECT_ROOT}")
    logger.info(f"data/raw     : {RAW_DATA_DIR}")
    logger.info(f"data/prepared: {PREPARED_DATA_DIR}")
    logger.info(f"scripts      : {SCRIPTS_DIR}")

    input_file = "products_data.csv"
    output_file = "products_prepared.csv"

    # Read raw data
    df = read_raw_data(input_file)

    # Read raw data
    df = read_raw_data(input_file)

    # Record original shape
    original_shape = df.shape

    # Log initial dataframe information
    logger.info(f"Initial dataframe columns: {', '.join(df.columns.tolist())}")
    logger.info(f"Initial dataframe shape: {df.shape}")

    # Clean column names
    original_columns = df.columns.tolist()
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

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

    # TODO:Remove outliers
    df = remove_outliers(df)

    # TODO: Validate data
    df = validate_data(df)

    # TODO: Standardize formats
    df = standardize_formats(df)

    # Save prepared data
    save_prepared_data(df, output_file)

    logger.info("==================================")
    logger.info(f"Original shape: {df.shape}")
    logger.info(f"Cleaned shape:  {original_shape}")
    logger.info("==================================")
    logger.info("FINISHED prepare_products_data.py")
    logger.info("==================================")


# -------------------
# Conditional Execution Block
# -------------------

if __name__ == "__main__":
    main()
