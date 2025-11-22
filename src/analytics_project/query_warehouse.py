"""Query examples for the Smart Store data warehouse."""

import sqlite3
from pathlib import Path
import pandas as pd
from utils_logger import logger, init_logger

# Initialize logger
init_logger()

# Project paths
SCRIPTS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPTS_DIR.parent.parent
WAREHOUSE_DIR = PROJECT_ROOT / "data" / "warehouse"
WAREHOUSE_PATH = WAREHOUSE_DIR / "smart_store_dw.db"


def run_query(conn, query_name, query_sql):
    """Execute a query and display results."""
    logger.info(f"\n{'=' * 80}")
    logger.info(f"QUERY: {query_name}")
    logger.info('=' * 80)

    result = pd.read_sql(query_sql, conn)

    if len(result) > 0:
        logger.info(f"\n{result.to_string(index=False)}")
        logger.info(f"\nRows returned: {len(result)}")
    else:
        logger.info("No results found")

    return result


def main():
    """Execute analytical queries on the data warehouse."""
    logger.info("=" * 80)
    logger.info("SMART STORE DATA WAREHOUSE - ANALYTICAL QUERIES")
    logger.info("=" * 80)

    conn = sqlite3.connect(WAREHOUSE_PATH)

    # Query 1: Top 10 customers by total sales (Professional BI naming)
    query1 = """
    SELECT
        c.name AS customer_name,
        c.region,
        COUNT(s.sale_id) AS total_transactions,
        SUM(s.quantity) AS total_items_purchased,
        SUM(s.sales_amount) AS total_spent,
        ROUND(AVG(s.sales_amount), 2) AS avg_transaction_amount
    FROM fact_sales s
    JOIN dim_customers c ON s.customer_key = c.customer_key
    GROUP BY c.customer_key, c.name, c.region
    ORDER BY total_spent DESC
    LIMIT 10
    """
    run_query(conn, "Top 10 Customers by Total Sales", query1)

    # Query 2: Sales by product category (Professional BI naming)
    query2 = """
    SELECT
        p.category,
        COUNT(s.sale_id) AS total_sales,
        SUM(s.quantity) AS total_quantity,
        SUM(s.sales_amount) AS total_revenue,
        ROUND(AVG(s.sales_amount), 2) AS avg_sale_amount
    FROM fact_sales s
    JOIN dim_products p ON s.product_key = p.product_key
    GROUP BY p.category
    ORDER BY total_revenue DESC
    """
    run_query(conn, "Sales Performance by Product Category", query2)

    # Query 3: Top selling products (Professional BI naming)
    query3 = """
    SELECT
        p.product_name,
        p.category,
        p.unit_price,
        COUNT(s.sale_id) AS times_sold,
        SUM(s.quantity) AS total_quantity_sold,
        SUM(s.sales_amount) AS total_revenue
    FROM fact_sales s
    JOIN dim_products p ON s.product_key = p.product_key
    GROUP BY p.product_key, p.product_name, p.category, p.unit_price
    ORDER BY total_revenue DESC
    LIMIT 10
    """
    run_query(conn, "Top 10 Best-Selling Products", query3)

    # Query 4: Sales by region (Professional BI naming)
    query4 = """
    SELECT
        c.region,
        COUNT(DISTINCT c.customer_key) AS unique_customers,
        COUNT(s.sale_id) AS total_transactions,
        SUM(s.sales_amount) AS total_revenue,
        ROUND(AVG(s.sales_amount), 2) AS avg_transaction_amount
    FROM fact_sales s
    JOIN dim_customers c ON s.customer_key = c.customer_key
    GROUP BY c.region
    ORDER BY total_revenue DESC
    """
    run_query(conn, "Sales Performance by Region", query4)

    # Query 5: Campaign effectiveness (Professional BI naming)
    query5 = """
    SELECT
        CASE
            WHEN s.campaign_id = 0 THEN 'No Campaign'
            ELSE 'Campaign ' || s.campaign_id
        END AS campaign,
        COUNT(s.sale_id) AS total_sales,
        SUM(s.sales_amount) AS total_revenue,
        ROUND(AVG(s.sales_amount), 2) AS avg_sale_amount,
        SUM(s.quantity) AS total_items_sold
    FROM fact_sales s
    GROUP BY s.campaign_id
    ORDER BY total_revenue DESC
    """
    run_query(conn, "Campaign Effectiveness Analysis", query5)

    # Query 6: Payment method distribution (Professional BI naming)
    query6 = """
    SELECT
        s.payment_method,
        COUNT(s.sale_id) AS transaction_count,
        SUM(s.sales_amount) AS total_revenue,
        ROUND(AVG(s.sales_amount), 2) AS avg_transaction_amount,
        ROUND(100.0 * COUNT(s.sale_id) / (SELECT COUNT(*) FROM fact_sales), 2) AS percent_of_transactions
    FROM fact_sales s
    GROUP BY s.payment_method
    ORDER BY transaction_count DESC
    """
    run_query(conn, "Payment Method Distribution", query6)

    # Query 7: Customer purchase frequency (Professional BI naming)
    query7 = """
    SELECT
        CASE
            WHEN transaction_count = 1 THEN '1 purchase'
            WHEN transaction_count BETWEEN 2 AND 5 THEN '2-5 purchases'
            WHEN transaction_count BETWEEN 6 AND 10 THEN '6-10 purchases'
            ELSE '10+ purchases'
        END AS purchase_frequency,
        COUNT(*) AS customer_count,
        ROUND(AVG(total_spent), 2) AS avg_customer_value
    FROM (
        SELECT
            c.customer_key,
            COUNT(s.sale_id) AS transaction_count,
            SUM(s.sales_amount) AS total_spent
        FROM dim_customers c
        LEFT JOIN fact_sales s ON c.customer_key = s.customer_key
        GROUP BY c.customer_key
    )
    GROUP BY purchase_frequency
    ORDER BY MIN(transaction_count)
    """
    run_query(conn, "Customer Purchase Frequency Distribution", query7)

    # Query 8: High-value transactions (Professional BI naming)
    query8 = """
    SELECT
        s.transaction_id,
        c.name AS customer_name,
        p.product_name,
        p.category,
        s.quantity,
        s.sales_amount,
        d.full_date AS transaction_date
    FROM fact_sales s
    JOIN dim_customers c ON s.customer_key = c.customer_key
    JOIN dim_products p ON s.product_key = p.product_key
    JOIN dim_dates d ON s.date_key = d.date_key
    WHERE s.sales_amount > (SELECT AVG(sales_amount) * 2 FROM fact_sales)
    ORDER BY s.sales_amount DESC
    LIMIT 20
    """
    run_query(conn, "High-Value Transactions (Above 2x Average)", query8)

    conn.close()

    logger.info("\n" + "=" * 80)
    logger.info("QUERY EXECUTION COMPLETE")
    logger.info("=" * 80)
    logger.info(f"Database: {WAREHOUSE_PATH}")
    logger.info("All analytical queries executed successfully!")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()
