"""Load prepared data into the Smart Store data warehouse (D4.2 Design)."""

import sqlite3
from pathlib import Path
import pandas as pd
from datetime import datetime
from utils_logger import logger, init_logger

# Initialize logger
init_logger()

# Project paths
SCRIPTS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPTS_DIR.parent.parent
PREPARED_DATA_DIR = PROJECT_ROOT / "data" / "prepared"
WAREHOUSE_DIR = PROJECT_ROOT / "data" / "warehouse"
WAREHOUSE_PATH = WAREHOUSE_DIR / "smart_store_dw.db"


def clear_warehouse_data():
    """Clear all existing data from warehouse tables (D4.2 naming)."""
    logger.info("Clearing existing warehouse data...")

    conn = sqlite3.connect(WAREHOUSE_PATH)
    cursor = conn.cursor()

    cursor.execute("DELETE FROM sales")
    cursor.execute("DELETE FROM customers")
    cursor.execute("DELETE FROM products")
    cursor.execute("DELETE FROM dates")

    conn.commit()
    conn.close()

    logger.info("✅ Warehouse data cleared")


def load_customers():
    """Load customers dimension from prepared data (D4.2 naming)."""
    logger.info("Loading customers dimension...")

    # Read prepared customer data
    customers_file = PREPARED_DATA_DIR / "customers_prepared.csv"
    df = pd.read_csv(customers_file)
    logger.info(f"Read {len(df)} customers from {customers_file}")

    # Remove any duplicates based on CustomerID
    df = df.drop_duplicates(subset=['CustomerID'], keep='first')
    logger.info(f"After deduplication: {len(df)} unique customers")

    # Connect to warehouse
    conn = sqlite3.connect(WAREHOUSE_PATH)

    # Map columns to dimension table (D4.2 snake_case naming)
    customers = pd.DataFrame(
        {
            'customer_id': df['CustomerID'],
            'name': df['CustomerName'],
            'email': df.get('Email', 'unknown@email.com'),  # Default if not present
            'region': df['Region'],
            'join_date': df['CustomerSince'],
            'customer_age': df.get('CustomerAge', 0),  # Default if not present
        }
    )

    # Load to database
    customers.to_sql('customers', conn, if_exists='append', index=False)
    logger.info(f"✅ Loaded {len(customers)} records into customers table")

    conn.close()


def load_products():
    """Load products dimension from prepared data (D4.2 naming)."""
    logger.info("Loading products dimension...")

    # Read prepared product data
    products_file = PREPARED_DATA_DIR / "products_prepared.csv"
    df = pd.read_csv(products_file)
    logger.info(f"Read {len(df)} products from {products_file}")

    # Connect to warehouse
    conn = sqlite3.connect(WAREHOUSE_PATH)

    # Map columns to dimension table (D4.2 snake_case naming)
    products = pd.DataFrame(
        {
            'product_id': df['productid'],
            'product_name': df['productname'],
            'category': df['productcategory'],
            'unit_price': df['unitprice'],
            'stock_level': df['stockquantity'],
            'product_size': df.get('ProductSize', 'Standard'),  # Default if not present
        }
    )

    # Load to database
    products.to_sql('products', conn, if_exists='append', index=False)
    logger.info(f"✅ Loaded {len(products)} records into products table")

    conn.close()


def load_dates():
    """Generate and load dates dimension from sales date range (D4.2 naming)."""
    logger.info("Loading dates dimension...")

    # Read sales data to get date range
    sales_file = PREPARED_DATA_DIR / "sales_prepared.csv"
    df = pd.read_csv(sales_file)

    # Convert to datetime
    df['TransactionDate'] = pd.to_datetime(df['transactiondate'])

    # Get min and max dates
    min_date = df['TransactionDate'].min()
    max_date = df['TransactionDate'].max()
    logger.info(f"Date range: {min_date.date()} to {max_date.date()}")

    # Generate all dates in range
    date_range = pd.date_range(start=min_date, end=max_date, freq='D')
    logger.info(f"Generating {len(date_range)} date records...")

    # Create date dimension (D4.2 snake_case naming)
    dates = pd.DataFrame(
        {
            'date_key': [int(d.strftime('%Y%m%d')) for d in date_range],
            'full_date': [d.strftime('%Y-%m-%d') for d in date_range],
            'year': [d.year for d in date_range],
            'quarter': [d.quarter for d in date_range],
            'month': [d.month for d in date_range],
            'month_name': [d.strftime('%B') for d in date_range],
            'day': [d.day for d in date_range],
            'day_of_week': [d.dayofweek for d in date_range],
            'day_name': [d.strftime('%A') for d in date_range],
            'is_weekend': [1 if d.dayofweek >= 5 else 0 for d in date_range],
        }
    )

    # Load to database
    conn = sqlite3.connect(WAREHOUSE_PATH)
    dates.to_sql('dates', conn, if_exists='append', index=False)
    logger.info(f"✅ Loaded {len(dates)} records into dates table")
    conn.close()


def load_sales():
    """Load sales fact table with foreign keys to dimensions (D4.2 naming)."""
    logger.info("Loading sales fact table...")

    # Read prepared sales data
    sales_file = PREPARED_DATA_DIR / "sales_prepared.csv"
    df = pd.read_csv(sales_file)
    logger.info(f"Read {len(df)} sales transactions from {sales_file}")

    # Connect to warehouse
    conn = sqlite3.connect(WAREHOUSE_PATH)

    # Get dimension lookups (D4.2 naming)
    customers_dim = pd.read_sql("SELECT customer_key, customer_id FROM customers", conn)
    products_dim = pd.read_sql("SELECT product_key, product_id FROM products", conn)

    # Convert transaction date to date_key format
    df['TransactionDate'] = pd.to_datetime(df['transactiondate'])
    df['date_key'] = df['TransactionDate'].dt.strftime('%Y%m%d').astype(int)

    # Convert IDs to matching types for merge
    df['customerid'] = df['customerid'].astype(str)
    df['productid'] = df['productid'].astype(str)
    customers_dim['customer_id'] = customers_dim['customer_id'].astype(str)
    products_dim['product_id'] = products_dim['product_id'].astype(str)

    # Merge to get foreign keys
    df = df.merge(customers_dim, left_on='customerid', right_on='customer_id', how='left')
    df = df.merge(products_dim, left_on='productid', right_on='product_id', how='left')

    # Check for unmatched records
    unmatched_customers = df['customer_key'].isna().sum()
    unmatched_products = df['product_key'].isna().sum()

    if unmatched_customers > 0:
        logger.warning(f"⚠️  {unmatched_customers} sales records have no matching customer")
    if unmatched_products > 0:
        logger.warning(f"⚠️  {unmatched_products} sales records have no matching product")

    # Remove records with missing keys
    df = df.dropna(subset=['customer_key', 'product_key'])
    logger.info(f"Loading {len(df)} valid sales records...")

    # Map to fact table (D4.2 snake_case naming)
    sales = pd.DataFrame(
        {
            'transaction_id': df['transactionid'],
            'customer_key': df['customer_key'].astype(int),
            'product_key': df['product_key'].astype(int),
            'date_key': df['date_key'],
            'quantity': df['quantitysold'],
            'sales_amount': df['totalamount'],
            'campaign_id': df['campaignid'],
            'payment_method': df.get('paymentmethod', 'Unknown'),  # Default if not present
        }
    )

    # Load to database
    sales.to_sql('sales', conn, if_exists='append', index=False)
    logger.info(f"✅ Loaded {len(sales)} records into sales table")

    conn.close()


def verify_load():
    """Verify data has been loaded correctly (D4.2 naming)."""
    logger.info("\n" + "=" * 80)
    logger.info("VERIFYING DATA LOAD (D4.2 Schema)")
    logger.info("=" * 80)

    conn = sqlite3.connect(WAREHOUSE_PATH)
    cursor = conn.cursor()

    # Count records in each table (D4.2 lowercase naming)
    tables = ['customers', 'products', 'dates', 'sales']

    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        logger.info(f"📊 {table}: {count:,} records")

    # Sample query: Top 5 sales with customer and product details (D4.2 naming)
    logger.info("\n" + "=" * 80)
    logger.info("SAMPLE QUERY: Top 5 Sales Transactions")
    logger.info("=" * 80)

    query = """
    SELECT
        s.transaction_id,
        c.name AS customer_name,
        p.product_name,
        s.quantity,
        s.sales_amount,
        d.full_date AS transaction_date
    FROM sales s
    JOIN customers c ON s.customer_key = c.customer_key
    JOIN products p ON s.product_key = p.product_key
    JOIN dates d ON s.date_key = d.date_key
    ORDER BY s.sales_amount DESC
    LIMIT 5
    """

    result = pd.read_sql(query, conn)
    logger.info(f"\n{result.to_string(index=False)}")

    conn.close()
    logger.info("\n" + "=" * 80)


def main():
    """Execute ETL process to load data warehouse."""
    logger.info("=" * 80)
    logger.info("SMART STORE DATA WAREHOUSE ETL PROCESS")
    logger.info("=" * 80)
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("")

    try:
        # Clear existing data first
        clear_warehouse_data()

        # Load dimensions first (D4.2 naming)
        load_customers()
        load_products()
        load_dates()

        # Load facts (depends on dimensions)
        load_sales()

        # Verify the load
        verify_load()

        logger.info("\n" + "=" * 80)
        logger.info("🎉 ETL PROCESS COMPLETED SUCCESSFULLY!")
        logger.info("=" * 80)
        logger.info(f"Warehouse location: {WAREHOUSE_PATH}")
        logger.info("Next: Run query_warehouse.py to execute analytical queries")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"❌ ETL process failed: {e}")
        raise


if __name__ == "__main__":
    main()
