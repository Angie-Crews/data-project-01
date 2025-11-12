# Pro Analytics 02 Python Starter Repository

> Use this repo to start a professional Python project.

- Additional information: <https://github.com/denisecase/pro-analytics-02>
- Project organization: [STRUCTURE](./STRUCTURE.md)
- Build professional skills:
  - **Environment Management**: Every project in isolation
  - **Code Quality**: Automated checks for fewer bugs
  - **Documentation**: Use modern project documentation tools
  - **Testing**: Prove your code works
  - **Version Control**: Collaborate professionally

---

## WORKFLOW 1. Set Up Your Machine

Proper setup is critical.
Complete each step in the following guide and verify carefully.

- [SET UP MACHINE](./SET_UP_MACHINE.md)

---

## WORKFLOW 2. Set Up Your Project

After verifying your machine is set up, set up a new Python project by copying this template.
Complete each step in the following guide.

- [SET UP PROJECT](./SET_UP_PROJECT.md)

It includes the critical commands to set up your local environment (and activate it):

```shell
uv venv
uv python pin 3.12
uv sync --extra dev --extra docs --upgrade
uv run pre-commit install
uv run python --version
```

**Windows (PowerShell):**

```shell
.\.venv\Scripts\activate
```
---

## WORKFLOW 3. Daily Workflow

Please ensure that the prior steps have been verified before continuing.
When working on a project, we open just that project in VS Code.

### 3.1 Git Pull from GitHub

Always start with `git pull` to check for any changes made to the GitHub repo.

```shell
git pull
```

### 3.2 Run Checks as You Work

This mirrors real work where we typically:

1. Update dependencies (for security and compatibility).
2. Clean unused cached packages to free space.
3. Use `git add .` to stage all changes.
4. Run ruff and fix minor issues.
5. Update pre-commit periodically.
6. Run pre-commit quality checks on all code files (**twice if needed**, the first pass may fix things).
7. Run tests.

In VS Code, open your repository, then open a terminal (Terminal / New Terminal) and run the following commands one at a time to check the code.

```shell
uv sync --extra dev --extra docs --upgrade
uv cache clean
git add .
uvx ruff check --fix
uvx pre-commit autoupdate
uv run pre-commit run --all-files
git add .
uv run pytest
```

NOTE: The second `git add .` ensures any automatic fixes made by Ruff or pre-commit are included before testing or committing.

<details>
<summary>Click to see a note on best practices</summary>

`uvx` runs the latest version of a tool in an isolated cache, outside the virtual environment.
This keeps the project light and simple, but behavior can change when the tool updates.
For fully reproducible results, or when you need to use the local `.venv`, use `uv run` instead.

</details>

### 3.3 Build Project Documentation

Make sure you have current doc dependencies, then build your docs, fix any errors, and serve them locally to test.

```shell
uv run mkdocs build --strict
uv run mkdocs serve
```

- After running the serve command, the local URL of the docs will be provided. To open the site, press **CTRL and click** the provided link (at the same time) to view the documentation. On a Mac, use **CMD and click**.
- Press **CTRL c** (at the same time) to stop the hosting process.

### 3.4 Execute

This project includes demo code.
Run the demo Python modules to confirm everything is working.

In VS Code terminal, run:

```shell
uv run python -m analytics_project.demo_module_basics
uv run python -m analytics_project.demo_module_languages
uv run python -m analytics_project.demo_module_stats
uv run python -m analytics_project.demo_module_viz
```

You should see:

- Log messages in the terminal
- Greetings in several languages
- Simple statistics
- A chart window open (close the chart window to continue).

If this works, your project is ready! If not, check:

- Are you in the right folder? (All terminal commands are to be run from the root project folder.)
- Did you run the full `uv sync --extra dev --extra docs --upgrade` command?
- Are there any error messages? (ask for help with the exact error)

---

### 3.5 Git add-commit-push to GitHub

Anytime we make working changes to code is a good time to git add-commit-push to GitHub.

1. Stage your changes with git add.
2. Commit your changes with a useful message in quotes.
3. Push your work to GitHub.

```shell
git add .
git commit -m "describe your change in quotes"
git push -u origin main
```

This will trigger the GitHub Actions workflow and publish your documentation via GitHub Pages.

### 3.6 Modify and Debug

With a working version safe in GitHub, start making changes to the code.

Before starting a new session, remember to do a `git pull` and keep your tools updated.

Each time forward progress is made, remember to git add-commit-push.


### 3.7 Reading Raw Data into Pandas DataFrames

1. Read through the assignment
2. Opened project in VS Code
3. Added new source file:
   - Created `src/analytics_project/data_prep.py`
   - Retrieved from <https://github.com/denisecase/smart-sales-starter-files>
4. Executed the module
5. Git Add-Commit-Push to GitHub
6. Updated README.md file to record steps of process

### 3.8 Data Collection

Added a numeric and category column to existing raw CSV files and filled with fake data:

- `customers_data.csv`
- `products_data.csv`
- `sales_data.csv`

### 3.9 Data Cleaning & ETL Preparation

Created a new folder `src/analytics_project/data_preparation/` with individual files for each CSV:

- `prepare_customers.py`
- `prepare_products.py`
- `prepare_sales.py`

**Summary of the Process:** See the comprehensive documentation at
<https://github.com/Angie-Crews/smart-store-angie/blob/main/docs/data_preparation_pipeline.md>
### 3.10 Prepare Data for ETL

#### Created `data_scrubber.py` Utility

Created `src/analytics_project/data_scrubber.py` with reusable data cleaning methods.

**Completed TODO Task:**

- **Before:** `self.df[column] = self.df[column]`  # Does nothing!
- **After:** `self.df[column] = self.df[column].str.upper().str.strip()`  # ✅ Works!

**How It Works:**

- `.str.upper()` - Converts all text to UPPERCASE
  - Example: "hello world" → "HELLO WORLD"
- `.str.strip()` - Removes leading/trailing whitespace
  - Example: "  HELLO WORLD  " → "HELLO WORLD"
- **Chaining** - The methods are chained together, so both operations happen in sequence

#### Created Master Pipeline Orchestrator

Created `src/analytics_project/run_all_data_prep.py` to execute all three preparation scripts in one action:

```shell
python src\analytics_project\run_all_data_prep.py
```

This runs:

- `prepare_customers.py`
- `prepare_products.py`
- `prepare_sales.py`

---

## WORKFLOW 4. Data Warehouse Design & Implementation (Project 4)

After cleaning and preparing data in P3, the next step is designing and implementing a data warehouse for efficient data analysis and business intelligence.

### 4.1 Design Decisions

**Schema Type:** Star Schema
**Rationale:** Chosen for simpler queries, better performance, and easier understanding

**Tables:**
- **Fact Table:** `sales` (1,509 transactions)
- **Dimension Tables:** `customers` (179 records), `products` (100 records), `dates` (1 record)

**Naming Convention:** D4.2 Standard
- Lowercase table names (sales, customers, products, dates)
- Snake_case column names (customer_key, sales_amount, product_name)
- Reference: <https://github.com/denisecase/smart-sales-docs/blob/main/D42_Design_DW.md>

### 4.2 Implementation Files

Created three Python scripts in `src/analytics_project/`:

1. **`create_warehouse.py`** - Defines schema and creates database structure
2. **`load_warehouse.py`** - ETL process to populate warehouse from prepared CSV files
3. **`query_warehouse.py`** - 8 analytical queries demonstrating warehouse capabilities

### 4.3 Schema Details

**Fact Table: sales**
```sql
sale_id (INTEGER PK), transaction_id (TEXT),
customer_key (INTEGER FK), product_key (INTEGER FK), date_key (INTEGER FK),
quantity (INTEGER), sales_amount (REAL),
campaign_id (INTEGER), payment_method (TEXT)
```

**Dimension: customers**
```sql
customer_key (INTEGER PK), customer_id (TEXT),
name (TEXT), email (TEXT), region (TEXT),
join_date (TEXT), customer_age (INTEGER)
```

**Dimension: products**
```sql
product_key (INTEGER PK), product_id (TEXT),
product_name (TEXT), category (TEXT), unit_price (REAL),
stock_level (INTEGER), product_size (TEXT)
```

**Dimension: dates**
```sql
date_key (INTEGER PK), full_date (TEXT),
year (INTEGER), quarter (INTEGER), month (INTEGER), month_name (TEXT),
day (INTEGER), day_of_week (INTEGER), day_name (TEXT), is_weekend (INTEGER)
```

### 4.4 Running the Data Warehouse

Execute the complete workflow:

```powershell
# 1. Create schema
python src\analytics_project\create_warehouse.py

# 2. Load data via ETL
python src\analytics_project\load_warehouse.py

# 3. Run analytical queries
python src\analytics_project\query_warehouse.py
```

**Expected Results:**
- Schema: 4 tables created with 14 performance indexes
- Data Load: 179 customers, 100 products, 1 date, 1,509 sales
- Queries: 8 analytical reports (top customers, category performance, regional analysis, etc.)

### 4.5 Key Insights from Queries

- **Top Customer:** Stephanie Garrison ($23,909 total spent)
- **Best Category:** Home ($476,826 revenue)
- **Top Region:** East ($630,933 revenue, 65 customers)
- **Most Effective Campaign:** Campaign 3 ($490,726 revenue, $1,348 avg sale)
- **High-Value Customers:** 41 customers with 10+ purchases averaging $14,104 lifetime value

### 4.6 Challenges Encountered

1. **Naming Convention Refactoring** - Initially implemented PascalCase (DimCustomer, FactSales) but refactored to D4.2 lowercase standard. Required updating all three scripts and careful testing.

2. **Data Quality Issues** - 178 sales records (out of 1,687) referenced non-existent customers. Implemented foreign key validation to maintain referential integrity.

3. **Inconsistent Source Data** - CSV files had mixed column naming (CustomerID vs customerid), requiring type conversion in ETL.

4. **Single Date Limitation** - All transactions occurred on 2025-05-04, limiting time-based analysis but demonstrating structure for future expansion.

5. **Missing Payment Method Data** - Source data lacked actual payment types (all show "Unknown"), identified as future enhancement.

### 4.7 Documentation

Comprehensive warehouse documentation available at:
**[docs/data_warehouse.md](./docs/data_warehouse.md)**

Includes:
- Complete schema reference with all columns and data types
- ETL workflow and data flow diagrams
- All 8 analytical queries with SQL, sample results, and business insights
- Troubleshooting guide and future enhancements

### 4.8 Database Location

```
c:\Repos\smart-store-angie\data\warehouse\smart_store_dw.db
```

View in VS Code using SQLite Viewer extension.

---

