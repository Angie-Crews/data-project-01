"""
Create SQLite Data Warehouse for Smart Store Analytics.

This module creates a dimensional data model with:
- Dimension tables: customers, products, dates
- Fact table: sales

The warehouse follows star schema design principles per D4.2 specification.
Naming conventions: lowercase table names, snake_case column names.
"""

import sqlite3
from pathlib import Path
from utils_logger import logger, init_logger

# Initialize logger
init_logger()

# Project paths
SCRIPTS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPTS_DIR.parent.parent
WAREHOUSE_DIR = PROJECT_ROOT / "data" / "warehouse"
WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)
WAREHOUSE_PATH = WAREHOUSE_DIR / "smart_store_dw.db"

logger.info(f"Project root: {PROJECT_ROOT}")
logger.info(f"Warehouse path: {WAREHOUSE_PATH}")


def create_warehouse_schema():
    """Create the data warehouse schema with dimension and fact tables.

    Schema Design (Professional BI Standard):
    - dim_customers: Customer dimension with join_date
    - dim_products: Product dimension with unit_price
    - dim_dates: Date dimension for time-based analysis
    - fact_sales: Sales fact table with foreign keys to dimensions
    """
    logger.info("Creating data warehouse schema...")

    conn = sqlite3.connect(WAREHOUSE_PATH)
    cursor = conn.cursor()

    # Drop existing tables and indexes if they exist (for clean rebuild)
    cursor.execute("DROP TABLE IF EXISTS fact_sales")
    cursor.execute("DROP TABLE IF EXISTS dim_customers")
    cursor.execute("DROP TABLE IF EXISTS dim_products")
    cursor.execute("DROP TABLE IF EXISTS dim_dates")
    cursor.execute("DROP INDEX IF EXISTS idx_customers_customer_id")
    cursor.execute("DROP INDEX IF EXISTS idx_products_product_id")
    cursor.execute("DROP INDEX IF EXISTS idx_dates_full_date")
    cursor.execute("DROP INDEX IF EXISTS idx_sales_customer_key")
    cursor.execute("DROP INDEX IF EXISTS idx_sales_product_key")
    cursor.execute("DROP INDEX IF EXISTS idx_sales_date_key")
    cursor.execute("DROP INDEX IF EXISTS idx_sales_transaction_id")

    logger.info("Creating dim_customers dimension table...")
    cursor.execute("""
        CREATE TABLE dim_customers (
            customer_key INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            region TEXT NOT NULL,
            join_date TEXT NOT NULL,
            customer_age INTEGER,
            load_date TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    logger.info("Creating dim_products dimension table...")
    cursor.execute("""
        CREATE TABLE dim_products (
            product_key INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id TEXT UNIQUE NOT NULL,
            product_name TEXT NOT NULL,
            category TEXT NOT NULL,
            unit_price REAL NOT NULL,
            stock_level INTEGER,
            product_size TEXT,
            load_date TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    logger.info("Creating dim_dates dimension table...")
    cursor.execute("""
        CREATE TABLE dim_dates (
            date_key INTEGER PRIMARY KEY,
            full_date TEXT UNIQUE NOT NULL,
            year INTEGER NOT NULL,
            quarter INTEGER NOT NULL,
            month INTEGER NOT NULL,
            month_name TEXT NOT NULL,
            day INTEGER NOT NULL,
            day_of_week INTEGER NOT NULL,
            day_name TEXT NOT NULL,
            is_weekend INTEGER NOT NULL
        )
    """)

    logger.info("Creating fact_sales fact table...")
    cursor.execute("""
        CREATE TABLE fact_sales (
            sale_id INTEGER PRIMARY KEY AUTOINCREMENT,
            transaction_id TEXT UNIQUE NOT NULL,
            customer_key INTEGER NOT NULL,
            product_key INTEGER NOT NULL,
            date_key INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            sales_amount REAL NOT NULL,
            campaign_id INTEGER,
            payment_method TEXT,
            load_date TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (customer_key) REFERENCES dim_customers(customer_key),
            FOREIGN KEY (product_key) REFERENCES dim_products(product_key),
            FOREIGN KEY (date_key) REFERENCES dim_dates(date_key)
        )
    """)

    # Create indexes for performance
    logger.info("Creating indexes for query optimization...")
    cursor.execute("CREATE INDEX idx_customers_customer_id ON dim_customers(customer_id)")
    cursor.execute("CREATE INDEX idx_products_product_id ON dim_products(product_id)")
    cursor.execute("CREATE INDEX idx_dates_full_date ON dim_dates(full_date)")
    cursor.execute("CREATE INDEX idx_sales_customer_key ON fact_sales(customer_key)")
    cursor.execute("CREATE INDEX idx_sales_product_key ON fact_sales(product_key)")
    cursor.execute("CREATE INDEX idx_sales_date_key ON fact_sales(date_key)")
    cursor.execute("CREATE INDEX idx_sales_transaction_id ON fact_sales(transaction_id)")

    conn.commit()
    conn.close()

    logger.info("✅ Data warehouse schema created successfully!")
    logger.info(f"Database location: {WAREHOUSE_PATH}")


def verify_schema():
    """Verify the created schema by listing all tables and their columns."""
    logger.info("\n" + "=" * 80)
    logger.info("VERIFYING DATA WAREHOUSE SCHEMA")
    logger.info("=" * 80)

    conn = sqlite3.connect(WAREHOUSE_PATH)
    cursor = conn.cursor()

    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = cursor.fetchall()

    logger.info(f"\nFound {len(tables)} tables:")
    for table in tables:
        table_name = table[0]
        logger.info(f"\n📊 Table: {table_name}")

        # Get column info
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()

        for col in columns:
            col_id, col_name, col_type, not_null, default_val, is_pk = col
            pk_marker = " [PRIMARY KEY]" if is_pk else ""
            nn_marker = " NOT NULL" if not_null else ""
            logger.info(f"   - {col_name}: {col_type}{pk_marker}{nn_marker}")

    # Get indexes
    logger.info("\n📇 Indexes created:")
    cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'")
    indexes = cursor.fetchall()
    for idx in indexes:
        logger.info(f"   - {idx[0]}")

    conn.close()
    logger.info("\n" + "=" * 80)


def main():
    """Main execution function."""
    logger.info("=" * 80)
    logger.info("SMART STORE DATA WAREHOUSE CREATION (D4.2 Design)")
    logger.info("=" * 80)
    logger.info("Creating dimensional data model with star schema design")
    logger.info("Schema: dim_customers, dim_products, dim_dates, fact_sales")
    logger.info("Naming: Professional BI standard (dim_* / fact_* prefixes)")
    logger.info("")

    create_warehouse_schema()
    verify_schema()

    logger.info("\n" + "=" * 80)
    logger.info("NEXT STEPS:")
    logger.info("=" * 80)
    logger.info("1. Run load_warehouse.py to populate the warehouse with prepared data")
    logger.info("2. Run query_warehouse.py to execute analytical queries")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
