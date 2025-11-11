# Smart Store Data Warehouse Documentation

## Overview

The Smart Store Data Warehouse is a dimensional data model implemented in SQLite that enables analytical queries on customer, product, and sales data. The warehouse follows the **D4.2 naming convention** with lowercase table names and snake_case column names.

## D4.2 Naming Convention

### Why D4.2?

The project was initially implemented using PascalCase naming (e.g., `DimCustomer`, `FactSales`) but was refactored to follow the D4.2 industry standard for the following reasons:

- **Consistency**: Lowercase naming is standard in many SQL databases (PostgreSQL, MySQL)
- **Readability**: snake_case columns are easier to read (`customer_key` vs `CustomerKey`)
- **Industry Standard**: Aligns with modern data warehousing best practices
- **No Prefixes**: Removes redundant "Dim" and "Fact" prefixes from table names

### Before and After

**Old (PascalCase):**
```sql
SELECT c.Name, f.TotalAmount
FROM FactSales f
JOIN DimCustomer c ON f.CustomerKey = c.CustomerKey
```

**New (D4.2 lowercase):**
```sql
SELECT c.name, s.sales_amount
FROM sales s
JOIN customers c ON s.customer_key = c.customer_key
```

## Schema Design

### Star Schema Architecture

The warehouse uses a **star schema** with one fact table and three dimension tables:

```
         customers
              |
              |
         [dimension]
              |
    sales ---|--- products
   [fact]    |
              |
         [dimension]
              |
            dates
```

### Dimension Tables

#### 1. customers
Stores customer demographic information.

| Column | Type | Description |
|--------|------|-------------|
| customer_key | INTEGER | Primary key (surrogate) |
| customer_id | TEXT | Business key from source system |
| name | TEXT | Customer full name |
| email | TEXT | Customer email address |
| region | TEXT | Geographic region (East, West, North, South, Central) |
| join_date | TEXT | Date customer joined (ISO format) |
| customer_age | INTEGER | Customer age in years |

**Records**: 179 unique customers

#### 2. products
Stores product catalog information.

| Column | Type | Description |
|--------|------|-------------|
| product_key | INTEGER | Primary key (surrogate) |
| product_id | TEXT | Business key from source system |
| product_name | TEXT | Product name |
| category | TEXT | Product category (Home, Office, Electronics, Clothing) |
| unit_price | REAL | Product price per unit |
| stock_level | INTEGER | Current inventory level |
| product_size | TEXT | Size classification (Small, Medium, Large) |

**Records**: 100 products

#### 3. dates
Date dimension with calendar attributes.

| Column | Type | Description |
|--------|------|-------------|
| date_key | INTEGER | Primary key (surrogate) |
| full_date | TEXT | Full date (ISO format YYYY-MM-DD) |
| year | INTEGER | Year (2025) |
| quarter | INTEGER | Quarter (1-4) |
| month | INTEGER | Month number (1-12) |
| month_name | TEXT | Month name (January-December) |
| day | INTEGER | Day of month (1-31) |
| day_of_week | INTEGER | Day of week (0=Monday, 6=Sunday) |
| day_name | TEXT | Day name (Monday-Sunday) |
| is_weekend | INTEGER | 1 if weekend, 0 if weekday |

**Records**: 1 date (2025-05-04) - currently single date, expandable for full date range

### Fact Table

#### sales
Stores sales transactions with foreign keys to dimensions.

| Column | Type | Description |
|--------|------|-------------|
| sale_id | INTEGER | Primary key (auto-increment) |
| transaction_id | TEXT | Business transaction ID from source |
| customer_key | INTEGER | Foreign key to customers dimension |
| product_key | INTEGER | Foreign key to products dimension |
| date_key | INTEGER | Foreign key to dates dimension |
| quantity | INTEGER | Number of items purchased |
| sales_amount | REAL | Total transaction amount |
| campaign_id | INTEGER | Marketing campaign ID (0 = no campaign) |
| payment_method | TEXT | Payment type (currently Unknown) |

**Records**: 1,509 sales transactions

### Indexes

Performance indexes on all foreign keys and frequently queried columns:

```sql
-- Customer dimension indexes
CREATE INDEX idx_customers_id ON customers(customer_id)
CREATE INDEX idx_customers_region ON customers(region)

-- Product dimension indexes
CREATE INDEX idx_products_id ON products(product_id)
CREATE INDEX idx_products_category ON products(category)

-- Date dimension indexes
CREATE INDEX idx_dates_full_date ON dates(full_date)

-- Sales fact indexes
CREATE INDEX idx_sales_customer ON sales(customer_key)
CREATE INDEX idx_sales_product ON sales(product_key)
CREATE INDEX idx_sales_date ON sales(date_key)
CREATE INDEX idx_sales_transaction ON sales(transaction_id)
```

## ETL Process

### Overview

The ETL (Extract, Transform, Load) process loads prepared CSV files into the warehouse using Python and pandas.

### ETL Scripts

#### create_warehouse.py
Creates the database schema with all tables and indexes.

**Usage:**
```powershell
python src\analytics_project\create_warehouse.py
```

**Actions:**
- Creates SQLite database at `data/warehouse/smart_store_dw.db`
- Defines 4 tables (customers, products, dates, sales)
- Creates 14 indexes for query performance
- Validates schema with table count verification

#### load_warehouse.py
Executes the complete ETL pipeline to populate the warehouse.

**Usage:**
```powershell
python src\analytics_project\load_warehouse.py
```

**ETL Steps:**

1. **Clear existing data** - Removes all records from warehouse tables
2. **Load customers dimension**
   - Reads `data/prepared/customers_prepared.csv`
   - Deduplicates by customer_id
   - Maps columns to snake_case
   - Inserts 179 records
3. **Load products dimension**
   - Reads `data/prepared/products_prepared.csv`
   - Maps columns to snake_case
   - Inserts 100 records
4. **Load dates dimension**
   - Generates date range from sales data
   - Calculates calendar attributes (quarter, month_name, day_name, is_weekend)
   - Inserts 1 date record
5. **Load sales fact**
   - Reads `data/prepared/sales_prepared.csv`
   - Performs foreign key lookups to customers and products
   - Filters out 178 records with invalid customer references
   - Merges transaction data with dimension keys
   - Inserts 1,509 valid sales records
6. **Verify load**
   - Counts records in each table
   - Executes sample query showing top 5 sales

**Data Quality Notes:**
- 178 sales records excluded due to missing customer matches
- Handles inconsistent ID formats (CustomerID vs customerid) with type conversion
- Preserves referential integrity with foreign key validation

### Data Flow

```
Raw Data (CSV)
    ↓
Preparation Scripts (prepare_*.py)
    ↓
Prepared Data (CSV)
    ↓
ETL Process (load_warehouse.py)
    ↓
Data Warehouse (SQLite)
    ↓
Analytical Queries (query_warehouse.py)
```

## Analytical Queries

The warehouse supports 8 analytical queries demonstrating various business intelligence capabilities.

### Query 1: Top 10 Customers by Total Sales

**Purpose**: Identify highest-value customers for retention programs.

**SQL:**
```sql
SELECT
    c.name AS customer_name,
    c.region,
    COUNT(s.sale_id) AS total_transactions,
    SUM(s.quantity) AS total_items_purchased,
    SUM(s.sales_amount) AS total_spent,
    ROUND(AVG(s.sales_amount), 2) AS avg_transaction_amount
FROM sales s
JOIN customers c ON s.customer_key = c.customer_key
GROUP BY c.customer_key, c.name, c.region
ORDER BY total_spent DESC
LIMIT 10
```

**Sample Results:**
| customer_name | region | total_transactions | total_items_purchased | total_spent | avg_transaction_amount |
|---------------|--------|-------------------:|----------------------:|------------:|-----------------------:|
| Stephanie Garrison | East | 12 | 56 | 23,908.63 | 1,992.39 |
| David Brennan | North | 17 | 71 | 22,362.35 | 1,315.43 |
| Jessica Mora | South | 20 | 97 | 20,346.67 | 1,017.33 |

**Business Insight**: Top customer spent $23,909 across 12 transactions with high average order value ($1,992).

### Query 2: Sales Performance by Product Category

**Purpose**: Compare revenue and volume across product categories.

**SQL:**
```sql
SELECT
    p.category,
    COUNT(s.sale_id) AS total_sales,
    SUM(s.quantity) AS total_quantity,
    SUM(s.sales_amount) AS total_revenue,
    ROUND(AVG(s.sales_amount), 2) AS avg_sale_amount
FROM sales s
JOIN products p ON s.product_key = p.product_key
GROUP BY p.category
ORDER BY total_revenue DESC
```

**Sample Results:**
| category | total_sales | total_quantity | total_revenue | avg_sale_amount |
|----------|------------:|---------------:|--------------:|----------------:|
| Home | 461 | 2,296 | 476,825.63 | 1,034.33 |
| Clothing | 397 | 1,954 | 421,879.18 | 1,062.67 |
| Electronics | 308 | 1,543 | 421,545.73 | 1,368.65 |
| Office | 343 | 1,677 | 376,993.65 | 1,099.11 |

**Business Insight**: Home category leads in total revenue ($476,826) and volume (461 sales), but Electronics has highest average sale amount ($1,369).

### Query 3: Top 10 Best-Selling Products

**Purpose**: Identify star products for inventory and marketing focus.

**SQL:**
```sql
SELECT
    p.product_name,
    p.category,
    p.unit_price,
    COUNT(s.sale_id) AS times_sold,
    SUM(s.quantity) AS total_quantity_sold,
    SUM(s.sales_amount) AS total_revenue
FROM sales s
JOIN products p ON s.product_key = p.product_key
GROUP BY p.product_key, p.product_name, p.category, p.unit_price
ORDER BY total_revenue DESC
LIMIT 10
```

**Sample Results:**
| product_name | category | unit_price | times_sold | total_quantity_sold | total_revenue |
|--------------|----------|------------|------------|---------------------|---------------|
| Office-Doctor | Office | 832.26 | 20 | 94 | 40,780.75 |
| Office-Soon | Clothing | 926.51 | 21 | 104 | 40,117.89 |
| Office-Receive | Clothing | 619.36 | 27 | 137 | 39,515.16 |

**Business Insight**: "Office-Doctor" is top product with $40,781 revenue from 20 sales.

### Query 4: Sales Performance by Region

**Purpose**: Geographic analysis for regional strategy and resource allocation.

**SQL:**
```sql
SELECT
    c.region,
    COUNT(DISTINCT c.customer_key) AS unique_customers,
    COUNT(s.sale_id) AS total_transactions,
    SUM(s.sales_amount) AS total_revenue,
    ROUND(AVG(s.sales_amount), 2) AS avg_transaction_amount
FROM sales s
JOIN customers c ON s.customer_key = c.customer_key
GROUP BY c.region
ORDER BY total_revenue DESC
```

**Sample Results:**
| region | unique_customers | total_transactions | total_revenue | avg_transaction_amount |
|--------|------------------|-------------------:|--------------:|-----------------------:|
| East | 65 | 569 | 630,932.81 | 1,108.85 |
| North | 36 | 296 | 362,730.44 | 1,225.44 |
| West | 35 | 275 | 305,828.24 | 1,112.10 |

**Business Insight**: East region dominates with 65 customers and $630,933 revenue, but North has highest average transaction ($1,225).

### Query 5: Campaign Effectiveness Analysis

**Purpose**: Measure ROI of marketing campaigns.

**SQL:**
```sql
SELECT
    CASE
        WHEN s.campaign_id = 0 THEN 'No Campaign'
        ELSE 'Campaign ' || s.campaign_id
    END AS campaign,
    COUNT(s.sale_id) AS total_sales,
    SUM(s.sales_amount) AS total_revenue,
    ROUND(AVG(s.sales_amount), 2) AS avg_sale_amount,
    SUM(s.quantity) AS total_items_sold
FROM sales s
GROUP BY s.campaign_id
ORDER BY total_revenue DESC
```

**Sample Results:**
| campaign | total_sales | total_revenue | avg_sale_amount | total_items_sold |
|----------|------------:|--------------:|----------------:|-----------------:|
| Campaign 3 | 364 | 490,726.46 | 1,348.15 | 1,839 |
| No Campaign | 392 | 473,846.97 | 1,208.79 | 1,913 |
| Campaign 1 | 363 | 370,442.27 | 1,020.50 | 1,792 |

**Business Insight**: Campaign 3 generated highest revenue ($490,726) with highest average sale ($1,348), indicating strong campaign effectiveness.

### Query 6: Payment Method Distribution

**Purpose**: Understand customer payment preferences.

**SQL:**
```sql
SELECT
    s.payment_method,
    COUNT(s.sale_id) AS transaction_count,
    SUM(s.sales_amount) AS total_revenue,
    ROUND(AVG(s.sales_amount), 2) AS avg_transaction_amount,
    ROUND(100.0 * COUNT(s.sale_id) / (SELECT COUNT(*) FROM sales), 2) AS percent_of_transactions
FROM sales s
GROUP BY s.payment_method
ORDER BY transaction_count DESC
```

**Sample Results:**
| payment_method | transaction_count | total_revenue | avg_transaction_amount | percent_of_transactions |
|----------------|------------------:|--------------:|-----------------------:|------------------------:|
| Unknown | 1,509 | 1,697,244.19 | 1,124.75 | 100.0 |

**Business Insight**: Payment method data currently not captured - opportunity for data collection improvement.

### Query 7: Customer Purchase Frequency Distribution

**Purpose**: Segment customers by engagement level.

**SQL:**
```sql
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
    FROM customers c
    LEFT JOIN sales s ON c.customer_key = s.customer_key
    GROUP BY c.customer_key
)
GROUP BY purchase_frequency
ORDER BY MIN(transaction_count)
```

**Sample Results:**
| purchase_frequency | customer_count | avg_customer_value |
|-------------------|---------------:|-------------------:|
| 2-5 purchases | 30 | 4,303.97 |
| 6-10 purchases | 108 | 9,165.35 |
| 10+ purchases | 41 | 14,104.07 |

**Business Insight**: 41 high-frequency customers (10+ purchases) have average lifetime value of $14,104. 108 customers in moderate frequency tier (6-10 purchases).

### Query 8: High-Value Transactions

**Purpose**: Identify exceptional sales for analysis and potential fraud detection.

**SQL:**
```sql
SELECT
    s.transaction_id,
    c.name AS customer_name,
    p.product_name,
    p.category,
    s.quantity,
    s.sales_amount,
    d.full_date AS transaction_date
FROM sales s
JOIN customers c ON s.customer_key = c.customer_key
JOIN products p ON s.product_key = p.product_key
JOIN dates d ON s.date_key = d.date_key
WHERE s.sales_amount > (SELECT AVG(sales_amount) * 2 FROM sales)
ORDER BY s.sales_amount DESC
LIMIT 20
```

**Sample Results:**
| transaction_id | customer_name | product_name | category | quantity | sales_amount | transaction_date |
|----------------|---------------|--------------|----------|----------|--------------|------------------|
| 849 | Briana Alvarado | Office-Itself | Clothing | 6 | 4,049.82 | 2025-05-04 |
| 706 | William Benitez | Home-Assume | Office | 3 | 4,021.05 | 2025-05-04 |
| 1377 | Brittney Burnett | Clothing-Need | Clothing | 4 | 3,905.76 | 2025-05-04 |

**Business Insight**: 20 transactions exceed 2x average sale amount (threshold: ~$2,250). Highest transaction: $4,050 for 6 units of "Office-Itself".

## Running the Warehouse

### Complete Setup

Execute all warehouse operations in sequence:

```powershell
# 1. Create schema
python src\analytics_project\create_warehouse.py

# 2. Load data via ETL
python src\analytics_project\load_warehouse.py

# 3. Run analytical queries
python src\analytics_project\query_warehouse.py
```

### Expected Output

- **Schema Creation**: 9 tables created (4 dimensions/fact + sqlite_sequence + 4 old tables)
- **Data Load**: 179 customers, 100 products, 1 date, 1,509 sales
- **Query Execution**: 8 analytical queries with results

### Troubleshooting

**Issue**: "No such table: customers"
- **Solution**: Run `create_warehouse.py` first to create schema

**Issue**: "178 sales records have no matching customer"
- **Expected**: Data quality issue in source - some sales reference non-existent customers
- **Impact**: Maintains referential integrity by excluding invalid records

**Issue**: "Payment method shows 'Unknown'"
- **Expected**: Payment method not captured in current data
- **Future**: Enhance data collection to capture actual payment types

## Database Location

```
c:\Repos\smart-store-angie\data\warehouse\smart_store_dw.db
```

## Future Enhancements

1. **Date Dimension Expansion**: Generate full date range (2024-2026) instead of single date
2. **Payment Method Capture**: Collect actual payment types (Credit Card, PayPal, etc.)
3. **Customer Dimension**: Add more attributes (income level, loyalty tier)
4. **Product Dimension**: Add supplier, cost, profit margin columns
5. **Additional Fact Tables**: Returns, inventory movements, customer service interactions
6. **Incremental Loads**: Support delta loading instead of full refresh
7. **Data Quality**: Resolve 178 orphaned sales records

## References

- **D4.2 Design Standard**: [https://github.com/denisecase/smart-sales-docs/blob/main/D42_Design_DW.md](https://github.com/denisecase/smart-sales-docs/blob/main/D42_Design_DW.md)
- **Data Warehousing Guide**: [https://github.com/denisecase/smart-sales-docs/blob/main/D41_Data_Warehousing.md](https://github.com/denisecase/smart-sales-docs/blob/main/D41_Data_Warehousing.md)

---

**Last Updated**: November 10, 2025
**Version**: 2.0 (D4.2 Refactored)
