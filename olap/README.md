# OLAP Analysis - Regional Product Performance

## Business Goal
**Identify which product categories perform best in which regions to optimize inventory allocation, marketing spend, and regional strategy.**

### Why This Matters
Understanding the intersection of product category performance and geographic regions enables data-driven decisions about:
- Regional inventory optimization
- Targeted marketing campaigns
- Store expansion opportunities
- Category-specific promotions by region

---

## Data Sources

### Tables Used
1. **dim_customers** - Customer dimension with region information
2. **dim_products** - Product dimension with category classification
3. **dim_dates** - Time dimension for temporal analysis
4. **fact_sales** - Sales transactions with measures

### Columns Required

**From dim_customers:**
- `customer_key` (FK)
- `region` (Central, East, North, South, West)
- `name` (for customer drilldown)

**From dim_products:**
- `product_key` (FK)
- `category` (Clothing, Electronics, Home, Office)
- `product_name` (for product drilldown)

**From dim_dates:**
- `date_key` (FK)
- `year`, `quarter`, `month_name` (for time analysis)

**From fact_sales:**
- `sales_amount` (numeric metric)
- `quantity` (numeric metric)
- `customer_key`, `product_key`, `date_key` (foreign keys)

---

## Tools

**Primary Tool:** Power BI Desktop (November 2025, v2.149.911.0 64-bit)

**Why Power BI:**
- Built on existing P5 work with established data connections
- Interactive cross-filtering enables exploratory analysis
- Strong support for OLAP operations (slicing, dicing, drilldown)
- Hierarchies allow intuitive navigation from summary to detail
- Visual storytelling capabilities for business presentation

**Connection Method:** ODBC to SQLite data warehouse

---

## Workflow & Logic

### Descriptive Dimensions
1. **Region** - Geographic performance (5 regions)
2. **Product Category** - Product line analysis (4 categories)
3. **Time** - Temporal trends (year, quarter, month)
4. **Customer Segment** - Top customers by total spending

### Numeric Metrics
1. **Total Sales Amount** - `SUM(fact_sales.sales_amount)`
2. **Average Transaction Value** - `AVERAGE(fact_sales.sales_amount)`
3. **Total Quantity Sold** - `SUM(fact_sales.quantity)`
4. **Customer Count** - `DISTINCTCOUNT(fact_sales.customer_key)`
5. **Sales per Customer** - Total Sales / Customer Count

### Aggregations
- **SUM** - Total revenue, total quantity
- **AVERAGE** - Mean transaction value
- **COUNT DISTINCT** - Unique customer count
- **GROUP BY** - Region, Category, Time dimensions

---

## OLAP Operations Implemented

### 1. SLICE - Regional Analysis
**Operation:** Filter data by single dimension (Region)

**Implementation:**
- Region slicer for filtering
- KPI cards showing total sales, avg transaction, customer count
- Bar chart comparing sales across all 5 regions
- Table with regional performance metrics

**Purpose:** Identify which regions generate the most revenue

### 2. DICE - Region × Category Analysis
**Operation:** Multi-dimensional breakdown (Region AND Category)

**Implementation:**
- Matrix visual with Region (rows) × Category (columns)
- Conditional formatting creating heat map effect
- Dual slicers (Region + Category) for filtering
- Stacked bar chart showing category mix by region

**Purpose:** Discover which product categories perform best in which regions

### 3. DRILLDOWN - Hierarchical Navigation
**Operation:** Move from summary to detail levels

**Implementation:**
- **Region Hierarchy:** Region → Customer (top performers by region)
- **Category Hierarchy:** Category → Product (best-selling products by category)
- Matrix visual with expandable/collapsible levels
- Bar chart with drill-through capability

**Purpose:** Identify specific customers and products driving regional/category performance

### 4. TIME SERIES - Temporal Trends
**Operation:** Analyze performance over time dimensions

**Implementation:**
- Line chart showing quarterly sales by region
- Time hierarchy: Year → Quarter → Month
- Year-over-year comparison (if multi-year data available)

**Purpose:** Identify seasonal patterns and growth trends

---

## Analysis Pages Created

### Page 1: Regional Overview (SLICE)
- **Visuals:** KPI cards, Bar chart, Table, Region slicer
- **Insight:** Which region is the top performer overall?

### Page 2: Category Performance by Region (DICE)
- **Visuals:** Matrix with heat map, Stacked bar chart, Dual slicers
- **Insight:** Which category-region combinations are strongest?

### Page 3: Drilldown Analysis
- **Visuals:** Hierarchical bar chart, Matrix with hierarchies
- **Insight:** Who are the top customers by region? What products drive category sales?

### Page 4: Executive Dashboard
- **Visuals:** Combined KPIs, Trend lines, Summary insights
- **Insight:** High-level view with actionable recommendations

---

## Screenshots

Screenshots are saved in the `olap/` folder:
1. `P6_Screenshot1_Regional_Slice.jpg` - Regional overview with slicing
2. `P6_Screenshot2_Category_Dice.jpg` - Region × Category matrix (dicing)
3. `P6_Screenshot3_Drilldown.jpg` - Hierarchical drilldown analysis
4. `P6_Screenshot4_Executive_Dashboard.jpg` - Summary dashboard

---

## Results

### Key Findings

**Finding 1: East Region Dominates Revenue**
- East region generates **$630,932.81** in total sales, representing **37.2%** of company-wide revenue ($1.697M total)
- This single region outperforms all other regions by significant margins
- East has the highest customer concentration and purchasing power

**Finding 2: Home Category in East Region is Top Performer**
- The **Home × East** combination produces **$192,584.41** in sales
- This category-region intersection shows the darkest shading in the heat map matrix
- Home products have exceptional product-market fit in the East region

**Finding 3: Customer Concentration Risk**
- Top 10 customers account for significant revenue share:
  - **Stephanie Garrison**: $23,908.63 (1.4% of total revenue)
  - **David Brennan**: $22,362.35
  - **Jessica Mora**: $20,346.67
- High-value customer retention is critical to sustained performance

**Finding 4: Regional Performance Disparity**
- With East at $630.9K, other regions show substantial room for growth
- Analysis reveals untapped potential in underperforming regions
- Category performance varies significantly by geographic location

### Business Insights

**Insight 1: Geographic Strategy Optimization**
The East region's exceptional performance ($630.9K, 37% of total) suggests either superior market conditions, better operational execution, or ideal customer demographics. This concentration creates both opportunity (replicate success in other regions) and risk (over-dependence on single market).

**Insight 2: Product-Market Fit Varies by Region**
The Home category's dominance in East ($192.5K) indicates strong regional preferences. The heat map reveals that different categories resonate differently across regions, suggesting the need for localized inventory and marketing strategies rather than one-size-fits-all approaches.

**Insight 3: High-Value Customer Management**
The top 10 customers contribute over $200K combined. Stephanie Garrison alone represents nearly $24K in revenue. Customer relationship management and retention programs for these high-value accounts should be a strategic priority to protect revenue base and reduce churn risk.

---

## Suggested Business Actions

### Immediate Actions (0-30 days)

**Action 1: Increase Home Category Inventory in East Region**
Boost Home product stock levels in East by 25-30% to capitalize on proven demand ($192.5K sales). Prioritize fast-moving Home items identified in drilldown analysis to prevent stockouts and maximize revenue capture.

**Action 2: Launch VIP Customer Retention Program**
Implement personalized outreach to top 10 customers (Stephanie Garrison, David Brennan, Jessica Mora, etc.) offering exclusive benefits, early access to new products, and dedicated account management to reduce churn risk and increase lifetime value.

**Action 3: Conduct Regional Category Performance Audit**
Use heat map findings to identify underperforming category-region combinations. Investigate whether poor performance is due to inadequate inventory, weak marketing, or genuine lack of market demand to inform resource allocation decisions.

### Strategic Actions (30-90 days)

**Action 4: Replicate East Region Success Model**
Analyze East region's operational practices, customer demographics, and marketing strategies. Develop playbook to replicate successful elements in underperforming regions while accounting for local market differences.

**Action 5: Implement Regional Category Specialization**
Based on heat map analysis, tailor inventory mix by region. Expand high-performing categories in specific regions while reducing or eliminating low-performing category-region combinations to optimize inventory costs and turnover.

**Action 6: Develop Customer Segmentation Strategy**
Use drilldown hierarchy to segment customers by purchasing patterns and region. Create targeted marketing campaigns and pricing strategies for different segments to increase average transaction value and purchase frequency.

### Long-term Initiatives (90+ days)

**Action 7: Consider East Region Distribution Hub**
Given East's $630.9K performance (37% of revenue), evaluate feasibility of dedicated distribution center or expanded fulfillment capacity in East to reduce delivery times, improve customer experience, and support continued growth.

**Action 8: Geographic Expansion or Market Exit Analysis**
For persistently underperforming regions, conduct thorough cost-benefit analysis. Determine whether to invest in growth initiatives, maintain current operations, or strategically exit and reallocate resources to higher-potential markets.

**Action 9: Build Automated Regional Performance Monitoring**
Implement real-time Power BI dashboards with monthly KPI tracking. Set up automated alerts for significant deviations (e.g., East drops below 35% revenue share, top customer spending decreases >10%) to enable proactive management intervention.

---

## Challenges Encountered

### Challenge 1: Limited Time Range
**Issue:** Sales data primarily from single date (2025-05-04) despite expanded date dimension

**Solution:** Focused analysis on regional and categorical dimensions rather than temporal trends. Acknowledged limitation in findings.

### Challenge 2: KPI Card Aggregation Limitations
**Issue:** Attempted to create KPI cards showing "Top Region" and "Top Category" using First aggregation, but Power BI returned alphabetical first values ("Central") rather than highest by sales value ("East").

**Solution:** Removed problematic KPI cards and relied on bar charts which naturally sort by value. Bar charts on Page 1 clearly communicate regional rankings visually, providing more intuitive insights than single-value cards would have provided.

---

## Next Steps

- Continue monitoring regional performance monthly
- Implement A/B testing for category-specific regional promotions
- Expand analysis to include profitability margins by category-region
- Integrate customer satisfaction data for complete picture

---

## References

- Power BI Official Documentation: https://learn.microsoft.com/en-us/power-bi/fundamentals
- Smart Sales Analysis Goals: https://github.com/denisecase/smart-sales-analysis-goals
- OLAP Concepts Reference: https://github.com/denisecase/smart-sales-example
