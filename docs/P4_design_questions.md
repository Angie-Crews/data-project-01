# Project 4: Data Warehouse Design Questions & Answers

**Student:** Angie Crews
**Project:** Smart Store Analytics
**Date:** November 10, 2025
**Repository:** <https://github.com/Angie-Crews/data-project-01>

---

## Data Warehouse Design Answers

### 1. What fact tables will you use in your DW?

**Answer:** `sales`

This is the single fact table that stores transactional sales data. It contains measures (quantity, sales_amount) and foreign keys to three dimension tables (customers, products, dates). The sales fact table contains 1,509 transactions.

---

### 2. What dimension tables will you use in your DW?

**Answer:** Three dimension tables:

1. **`customers`** - Customer demographic and geographic information (179 records)
2. **`products`** - Product catalog details (100 records)
3. **`dates`** - Calendar/time dimension with date attributes (1 record)

---

### 3. What additional columns did you add to Sales & what type is each column?

**Answer:** Beyond the basic transaction data from the source files, the sales fact table includes:

| Column | Type | Purpose |
|--------|------|---------|
| `sale_id` | INTEGER | Surrogate primary key (auto-increment) |
| `customer_key` | INTEGER | Foreign key to customers dimension |
| `product_key` | INTEGER | Foreign key to products dimension |
| `date_key` | INTEGER | Foreign key to dates dimension |
| `campaign_id` | INTEGER | Marketing campaign identifier |
| `payment_method` | TEXT | Payment type used for transaction |

These additions enable the star schema design with proper relationships and support analytical queries about campaigns and payment preferences.

---

### 4. What additional columns did you add to Products & what type is each column?

**Answer:** Beyond the basic product data, the products dimension includes:

| Column | Type | Purpose |
|--------|------|---------|
| `product_key` | INTEGER | Surrogate primary key (auto-increment) |
| `product_size` | TEXT | Size classification (Small, Medium, Large) |
| `stock_level` | INTEGER | Current inventory level |

The surrogate key (product_key) separates the warehouse identifier from the business key (product_id), allowing for slowly changing dimension management. Size and stock level support inventory and merchandising analysis.

---

### 5. What additional columns did you add to Customers & what type is each column?

**Answer:** Beyond the basic customer data, the customers dimension includes:

| Column | Type | Purpose |
|--------|------|---------|
| `customer_key` | INTEGER | Surrogate primary key (auto-increment) |
| `region` | TEXT | Geographic region (East, West, North, South, Central) |
| `join_date` | TEXT | Date customer joined (ISO format) |
| `customer_age` | INTEGER | Customer age in years |

The surrogate key enables tracking customer changes over time. Region, join date, and age support customer segmentation and cohort analysis.

---

### 6. Show your schema for your dimension tables:

**customers dimension:**
```sql
CREATE TABLE customers (
    customer_key INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id TEXT NOT NULL UNIQUE,
    name TEXT,
    email TEXT,
    region TEXT,
    join_date TEXT,
    customer_age INTEGER
)
```

**products dimension:**
```sql
CREATE TABLE products (
    product_key INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id TEXT NOT NULL UNIQUE,
    product_name TEXT,
    category TEXT,
    unit_price REAL,
    stock_level INTEGER,
    product_size TEXT
)
```

**dates dimension:**
```sql
CREATE TABLE dates (
    date_key INTEGER PRIMARY KEY AUTOINCREMENT,
    full_date TEXT NOT NULL UNIQUE,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name TEXT,
    day INTEGER,
    day_of_week INTEGER,
    day_name TEXT,
    is_weekend INTEGER
)
```

**Indexes for dimension tables:**
```sql
-- Customer dimension indexes
CREATE INDEX idx_customers_id ON customers(customer_id);
CREATE INDEX idx_customers_region ON customers(region);

-- Product dimension indexes
CREATE INDEX idx_products_id ON products(product_id);
CREATE INDEX idx_products_category ON products(category);

-- Date dimension indexes
CREATE INDEX idx_dates_full_date ON dates(full_date);
```

---

### 7. Show your schema for your fact table:

**sales fact table:**
```sql
CREATE TABLE sales (
    sale_id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_id TEXT NOT NULL,
    customer_key INTEGER NOT NULL,
    product_key INTEGER NOT NULL,
    date_key INTEGER NOT NULL,
    quantity INTEGER,
    sales_amount REAL,
    campaign_id INTEGER,
    payment_method TEXT,
    FOREIGN KEY (customer_key) REFERENCES customers(customer_key),
    FOREIGN KEY (product_key) REFERENCES products(product_key),
    FOREIGN KEY (date_key) REFERENCES dates(date_key)
)
```

**Indexes for fact table:**
```sql
-- Foreign key indexes for join performance
CREATE INDEX idx_sales_customer ON sales(customer_key);
CREATE INDEX idx_sales_product ON sales(product_key);
CREATE INDEX idx_sales_date ON sales(date_key);

-- Business key index for lookups
CREATE INDEX idx_sales_transaction ON sales(transaction_id);
```

---

### 8. Does each table have exactly one PRIMARY KEY?

**Answer:** Yes. Each table has exactly one primary key:

| Table | Primary Key | Type |
|-------|-------------|------|
| customers | `customer_key` | INTEGER AUTOINCREMENT |
| products | `product_key` | INTEGER AUTOINCREMENT |
| dates | `date_key` | INTEGER AUTOINCREMENT |
| sales | `sale_id` | INTEGER AUTOINCREMENT |

All primary keys are **surrogate keys** (auto-incrementing integers) that are separate from business keys (customer_id, product_id, transaction_id). This design:
- Ensures uniqueness and immutability
- Improves join performance with integer keys
- Supports slowly changing dimensions
- Separates warehouse identity from source system keys

---

### 9. What challenges did you encounter in the design process?

**Answer:** Six major challenges:

#### Challenge 1: Naming Convention Refactoring
**Problem:** Initially implemented with PascalCase naming (DimCustomer, FactSales, CustomerKey) following common SQL Server conventions, but needed to refactor to D4.2 lowercase standard (customers, sales, customer_key).

**Solution:** Systematically updated all three scripts (create_warehouse.py, load_warehouse.py, query_warehouse.py) to use lowercase table names and snake_case columns. Required careful testing at each step and updating all SQL queries.

**Impact:** Major refactoring effort but resulted in code that follows industry best practices and is consistent with modern data warehousing standards.

#### Challenge 2: Data Quality - Orphaned Sales Records
**Problem:** Discovered 178 sales records (out of 1,687 total) that referenced customer IDs not present in the customers dimension.

**Solution:** Implemented foreign key validation logic in the ETL process (load_warehouse.py) to:
- Merge sales with customers using pandas
- Filter out records where customer_key is NaN
- Log warning about excluded records
- Only load valid transactions

**Impact:** Maintained referential integrity in the warehouse while accepting 10.5% data loss. Documented as data quality issue for business stakeholders.

#### Challenge 3: Inconsistent Source Column Names
**Problem:** Prepared CSV files had mixed naming conventions:
- customers_prepared.csv: `CustomerID` (PascalCase)
- products_prepared.csv: `productid` (lowercase)
- sales_prepared.csv: `customerid` and `productid` (lowercase)

**Solution:** Added explicit type conversion with `astype(str)` during foreign key lookups to ensure consistent string comparison regardless of source format:
```python
df_merged = sales.merge(
    customers[['CustomerID', 'customer_key']],
    left_on=sales['customerid'].astype(str),
    right_on=customers['CustomerID'].astype(str)
)
```

**Impact:** Successful ETL execution despite inconsistent source data formatting.

#### Challenge 4: Single Date Limitation
**Problem:** All 1,509 sales transactions occurred on the same date (2025-05-04), resulting in a dates dimension with only 1 record.

**Solution:**
- Built complete date dimension structure with all calendar attributes (quarter, month_name, day_name, is_weekend)
- Documented limitation in warehouse documentation
- Designed for easy expansion when more dates are available

**Impact:** Limits time-based analysis (trends, seasonality) but demonstrates proper dimensional structure. Ready for future data with broader date ranges.

#### Challenge 5: Missing Payment Method Data
**Problem:** Source data didn't include actual payment methods, resulting in all 1,509 transactions showing payment_method as "Unknown".

**Solution:**
- Created payment_method column in sales table to support future data
- Documented gap in data_warehouse.md
- Identified as priority for data collection improvement
- Included payment method query (#6) to demonstrate intended analysis

**Impact:** Query infrastructure ready, but analysis blocked until source data improved.

#### Challenge 6: Star Schema vs Snowflake Decision
**Problem:** Needed to choose between star schema (denormalized dimensions) and snowflake schema (normalized dimensions).

**Solution:** Selected star schema based on:
- Simpler queries (fewer joins required)
- Better query performance for analytical workloads
- Easier for business users to understand
- Accepted trade-off of some data redundancy in dimensions

**Impact:** All 8 analytical queries execute efficiently with straightforward SQL. Documentation clearly explains the star schema design for stakeholders.

---

## Implementation Summary

### Files Created
- `src/analytics_project/create_warehouse.py` - Schema definition (92 lines)
- `src/analytics_project/load_warehouse.py` - ETL process (290 lines)
- `src/analytics_project/query_warehouse.py` - Analytical queries (195 lines)
- `docs/data_warehouse.md` - Comprehensive documentation (600+ lines)

### Database Structure
- **Location:** `data/warehouse/smart_store_dw.db`
- **Size:** SQLite database with 4 tables and 14 indexes
- **Records:** 179 customers + 100 products + 1 date + 1,509 sales = 1,789 total records

### Validation Results
All 8 analytical queries executed successfully:
1. ✅ Top 10 Customers by Total Sales
2. ✅ Sales Performance by Product Category
3. ✅ Top 10 Best-Selling Products
4. ✅ Sales Performance by Region
5. ✅ Campaign Effectiveness Analysis
6. ✅ Payment Method Distribution
7. ✅ Customer Purchase Frequency Distribution
8. ✅ High-Value Transactions (Above 2x Average)

### Key Business Insights
- Top customer: Stephanie Garrison ($23,909 spent)
- Best category: Home ($476,826 revenue)
- Top region: East ($630,933 revenue)
- Most effective campaign: Campaign 3 ($490,726 revenue)
- High-value segment: 41 customers with 10+ purchases ($14,104 avg lifetime value)

---

## References

- **D4.2 Design Standard:** <https://github.com/denisecase/smart-sales-docs/blob/main/D42_Design_DW.md>
- **Data Warehousing Guide:** <https://github.com/denisecase/smart-sales-docs/blob/main/D41_Data_Warehousing.md>
- **Project Repository:** <https://github.com/Angie-Crews/data-project-01>
- **Detailed Documentation:** [docs/data_warehouse.md](../docs/data_warehouse.md)

---

**Submitted:** November 10, 2025
