"""
Demo script to demonstrate DataScrubber functionality.

This script creates sample messy data and uses DataScrubber methods
to clean and transform it.
"""

import pandas as pd
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).resolve().parent))

from data_scrubber import DataScrubber


def create_sample_messy_data():
    """Create a sample DataFrame with common data quality issues."""
    data = {
        'name': ['  John Doe  ', 'jane smith', '  BOB JONES  ', 'Alice Brown', 'Charlie Wilson'],
        'email': [
            'JOHN@EMAIL.COM',
            '  jane@email.com  ',
            'bob@EMAIL.com',
            'alice@email.com',
            'charlie@email.com',
        ],
        'age': [25, 30, None, 35, 40],
        'salary': [50000, 65000, 75000, 80000, 150000],  # 150000 is an outlier
        'department': ['Sales', 'Sales', 'Engineering', 'Engineering', 'Management'],
        'hire_date': ['2020-01-15', '2019-06-20', '2021-03-10', '2018-11-05', '2015-02-28'],
    }

    # Add a duplicate row
    df = pd.DataFrame(data)
    df = pd.concat([df, df.iloc[[0]]], ignore_index=True)  # Duplicate first row

    return df


def main():
    """Demonstrate DataScrubber capabilities."""

    print("=" * 80)
    print("DATA SCRUBBER DEMONSTRATION")
    print("=" * 80)

    # Create sample messy data
    df = create_sample_messy_data()

    print("\n1. ORIGINAL MESSY DATA:")
    print("-" * 80)
    print(df)
    print(f"\nShape: {df.shape}")

    # Initialize DataScrubber
    scrubber = DataScrubber(df)

    # Check data consistency before cleaning
    print("\n2. DATA QUALITY CHECK (BEFORE CLEANING):")
    print("-" * 80)
    consistency_before = scrubber.check_data_consistency_before_cleaning()
    print(f"Null counts:\n{consistency_before['null_counts']}")
    print(f"\nDuplicate rows: {consistency_before['duplicate_count']}")

    # Inspect data
    print("\n3. DATA INSPECTION:")
    print("-" * 80)
    info_str, describe_str = scrubber.inspect_data()
    print("DataFrame Info:")
    print(info_str)
    print("\nStatistical Summary:")
    print(describe_str)

    # Step 1: Remove duplicates
    print("\n4. REMOVING DUPLICATES:")
    print("-" * 80)
    df = scrubber.remove_duplicate_records()
    print(f"After removing duplicates: {df.shape}")

    # Step 2: Handle missing values
    print("\n5. HANDLING MISSING VALUES:")
    print("-" * 80)
    print(f"Missing values before: {df.isnull().sum().sum()}")
    df = scrubber.handle_missing_data(fill_value=0)
    print(f"Missing values after: {df.isnull().sum().sum()}")

    # Step 3: Format name column to lowercase and trim
    print("\n6. FORMATTING NAMES (LOWERCASE + TRIM):")
    print("-" * 80)
    print("Before:", df['name'].tolist())
    df = scrubber.format_column_strings_to_lower_and_trim('name')
    print("After: ", df['name'].tolist())

    # Step 4: Format email column to uppercase and trim (using our fixed TODO!)
    print("\n7. FORMATTING EMAILS (UPPERCASE + TRIM):")
    print("-" * 80)
    print("Before:", df['email'].tolist())
    df = scrubber.format_column_strings_to_upper_and_trim('email')
    print("After: ", df['email'].tolist())

    # Step 5: Filter outliers (remove salaries > 100000)
    print("\n8. FILTERING OUTLIERS (SALARY):")
    print("-" * 80)
    print(f"Before filtering - Salary range: ${df['salary'].min()} to ${df['salary'].max()}")
    print(f"Rows before: {len(df)}")
    df = scrubber.filter_column_outliers('salary', lower_bound=0, upper_bound=100000)
    print(f"After filtering - Salary range: ${df['salary'].min()} to ${df['salary'].max()}")
    print(f"Rows after: {len(df)}")

    # Step 6: Parse dates
    print("\n9. PARSING DATES:")
    print("-" * 80)
    print(f"Before: hire_date type = {df['hire_date'].dtype}")
    df = scrubber.parse_dates_to_add_standard_datetime('hire_date')
    print(f"After: Added 'StandardDateTime' column")
    print(f"StandardDateTime type = {df['StandardDateTime'].dtype}")
    print(f"Sample dates:\n{df[['hire_date', 'StandardDateTime']].head()}")

    # Step 7: Rename columns
    print("\n10. RENAMING COLUMNS:")
    print("-" * 80)
    print(f"Before: {df.columns.tolist()}")
    df = scrubber.rename_columns({'name': 'employee_name', 'email': 'employee_email'})
    print(f"After:  {df.columns.tolist()}")

    # Step 8: Reorder columns
    print("\n11. REORDERING COLUMNS:")
    print("-" * 80)
    print(f"Before: {df.columns.tolist()}")
    new_order = [
        'employee_name',
        'employee_email',
        'age',
        'department',
        'salary',
        'hire_date',
        'StandardDateTime',
    ]
    df = scrubber.reorder_columns(new_order)
    print(f"After:  {df.columns.tolist()}")

    # Step 9: Convert data type
    print("\n12. CONVERTING DATA TYPES:")
    print("-" * 80)
    print(f"Before: age type = {df['age'].dtype}")
    df = scrubber.convert_column_to_new_data_type('age', int)
    print(f"After:  age type = {df['age'].dtype}")

    # Step 10: Drop columns
    print("\n13. DROPPING COLUMNS:")
    print("-" * 80)
    print(f"Before dropping: {df.columns.tolist()}")
    df = scrubber.drop_columns(['hire_date'])  # Drop original date column, keep parsed version
    print(f"After dropping:  {df.columns.tolist()}")

    # Final cleaned data
    print("\n14. FINAL CLEANED DATA:")
    print("-" * 80)
    print(df)
    print(f"\nFinal shape: {df.shape}")

    print("\n" + "=" * 80)
    print("DATA SCRUBBER DEMONSTRATION COMPLETE!")
    print("=" * 80)
    print("\nKey Achievements:")
    print("✅ Removed duplicate rows")
    print("✅ Handled missing values")
    print("✅ Standardized text formatting (uppercase/lowercase + trim)")
    print("✅ Filtered outliers")
    print("✅ Parsed dates to datetime format")
    print("✅ Renamed and reordered columns")
    print("✅ Converted data types")
    print("✅ Dropped unnecessary columns")
    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()
