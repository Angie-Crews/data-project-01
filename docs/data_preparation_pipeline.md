# Data Preparation Pipeline Documentation

## Overview

This document describes the comprehensive data preparation pipeline built for the Smart Store Analytics project. The pipeline processes three core datasets: customers, products, and sales transactions, ensuring data quality, consistency, and business rule compliance.

## Pipeline Architecture

### Core Components

1. **Data Profiling & Analysis**
2. **Duplicate Detection & Removal**
3. **Missing Value Handling**
4. **Outlier Detection & Removal**
5. **Business Rule Validation**
6. **Format Standardization**
7. **Audit Logging & Quality Reporting**

### File Structure

```
src/analytics_project/data_preparation/
├── prepare_customers.py    # Customer data processing
├── prepare_products.py     # Product data processing
└── prepare_sales.py        # Sales transaction processing

data/
├── raw/                    # Original source data
│   ├── customers_data.csv
│   ├── products_data.csv
│   └── sales_data.csv
└── prepared/              # Cleaned, validated data
    ├── customers_prepared.csv
    ├── products_prepared.csv
    └── sales_prepared.csv
```

## Data Processing Results Summary

### Customer Data (`prepare_customers.py`)
- **Input**: 201 rows, 6 columns
- **Output**: 180 rows, 6 columns
- **Data Retention**: 89.6%
- **Key Quality Issues Fixed**:
  - 21 records removed with invalid region data
  - Outlier detection using business rules and statistical analysis
  - Comprehensive data profiling and validation

### Product Data (`prepare_products.py`)
- **Input**: 100 rows, 6 columns
- **Output**: 100 rows, 6 columns
- **Data Retention**: 100%
- **Key Quality Improvements**:
  - Enhanced duplicate detection (2 products flagged with duplicate names)
  - 84 supplier names standardized to title case
  - Statistical outlier analysis with IQR method
  - Business rule validation for all product attributes

### Sales Data (`prepare_sales.py`)
- **Input**: 2,001 rows, 9 columns
- **Output**: 1,687 rows, 10 columns (added unit_price calculation)
- **Data Retention**: 84.4%
- **Major Data Quality Issues Resolved**:
  - 1 duplicate TransactionID removed
  - 285 transactions with non-positive amounts removed
  - 27 extreme outliers removed using statistical analysis
  - 1 invalid date format handled ("2023-13-01")
  - 1 missing CampaignID filled with default value

## Detailed Pipeline Components

### 1. Data Profiling & Analysis

**Purpose**: Understand dataset structure, quality, and characteristics before processing.

**Implementation**:
- Column data type analysis
- Unique value counts per column
- Statistical summaries for numeric fields
- Sample categorical values display
- Data completeness assessment

**Example Output**:
```
Column datatypes:
ProductID            int64
ProductName         object
ProductCategory     object
UnitPrice          float64
StockQuantity        int64
SupplierName        object

Data completeness: 100.0%
```

### 2. Duplicate Detection & Removal

**Business Logic**:
- **Products**: Remove based on ProductID (primary key)
- **Customers**: Remove based on CustomerID (primary key)
- **Sales**: Remove based on TransactionID (primary key)

**Advanced Features**:
- Cross-field duplicate detection (same customer, same day, same product)
- Duplicate name detection with different IDs (flagged for review)
- Configurable duplicate handling strategies

**Results**:
- Products: 0 duplicates removed, 2 flagged for review
- Customers: Enhanced detection implemented
- Sales: 1 duplicate TransactionID removed, 213 potential duplicates flagged

### 3. Missing Value Handling

**Strategy by Dataset**:

#### Customer Data
- Critical fields (CustomerID): Drop rows if missing
- CustomerName: Generate based on ID pattern
- Region: Use most common region or "Unknown"
- Numeric fields: Use median imputation

#### Product Data
- ProductID: Drop rows (critical field)
- ProductName: Generate category-based names
- Category: Use most common or "Uncategorized"
- Price: Use category median or overall median
- Stock: Set to 0 (out of stock) or category median
- Supplier: Set to "Unknown Supplier"

#### Sales Data
- TransactionID, CustomerID, ProductID: Drop rows (critical)
- Date: Use most common date or default
- Amount: Use overall median
- Quantity: Use median quantity
- CampaignID: Set to 0 (no campaign)
- Sales Rep: Set to "Unknown Rep"

### 4. Outlier Detection & Removal

**Methods Used**:

#### Statistical Analysis (IQR Method)
```python
Q1 = data.quantile(0.25)
Q3 = data.quantile(0.75)
IQR = Q3 - Q1
lower_bound = Q1 - 2.0 * IQR
upper_bound = Q3 + 2.0 * IQR
```

#### Business Rule Validation
- **Prices**: Must be positive, reasonable upper limits
- **Quantities**: Must be positive, reasonable maximums
- **IDs**: Must be within expected ranges
- **Dates**: Must be within business timeframe

**Results**:
- Products: 0 outliers (clean dataset)
- Customers: 21 invalid regions removed
- Sales: 312 outliers removed (285 invalid amounts + 27 extreme values)

### 5. Business Rule Validation

#### Product Validation Rules
- ProductID: Range 1000-99999
- ProductName: Length 2-100 characters, must contain letters
- Category: Must be from approved list
- Price: Minimum $1.00, maximum $10,000
- Stock: Non-negative integers
- Supplier: Length 2-50 characters

#### Customer Validation Rules
- CustomerID: Range 1000-9999
- Region: Must be valid business regions
- Spend amounts: Positive values only
- Date formats: Consistent formatting

#### Sales Validation Rules
- TransactionID: Positive, unique
- CustomerID: Range 1000-9999
- ProductID: Range 2000-2999
- StoreID: Range 401-499
- Unit price validation: $0.01 - $10,000 range
- Transaction integrity: Amount = Price × Quantity (within tolerance)

### 6. Format Standardization

#### Text Formatting
- **Names**: Title case formatting
- **Addresses/Regions**: Consistent capitalization
- **Suppliers**: Title case, trimmed whitespace
- **Categories**: Title case, standardized

#### Numeric Formatting
- **Prices**: 2 decimal places (currency standard)
- **Quantities**: Integer values
- **IDs**: Integer format, no decimals

#### Date Formatting
- **Standard Format**: YYYY-MM-DD
- **Error Handling**: Invalid dates removed or corrected
- **Consistency**: All dates use same format

### 7. Audit Logging & Quality Reporting

**Logging Features**:
- Function-level execution tracking
- Data transformation step logging
- Quality issue identification and counts
- Performance metrics (processing time)
- Error handling and warnings

**Quality Metrics Tracked**:
- Data retention rates
- Records removed by category
- Format standardization counts
- Validation pass/fail rates
- Outlier detection statistics

## Technical Implementation Details

### Dependencies
```python
import pandas as pd
import pathlib
import sys
from utils_logger import logger, init_logger
```

### Error Handling
- Graceful handling of data type conversion errors
- Invalid date format recovery
- Missing column handling
- Statistical calculation error management

### Performance Considerations
- Efficient pandas operations
- Minimal data copying
- Vectorized string operations
- Memory-conscious processing for large datasets

## Usage Instructions

### Running Individual Scripts
```bash
# Activate virtual environment
.venv\Scripts\Activate.ps1

# Run individual preparation scripts
python src\analytics_project\data_preparation\prepare_customers.py
python src\analytics_project\data_preparation\prepare_products.py
python src\analytics_project\data_preparation\prepare_sales.py
```

### Running Complete Pipeline
```bash
# Run all preparation scripts in sequence
python src\analytics_project\data_preparation\prepare_customers.py && python src\analytics_project\data_preparation\prepare_products.py && python src\analytics_project\data_preparation\prepare_sales.py
```

### Monitoring Pipeline Execution
- Check `project.log` for detailed execution logs
- Review console output for real-time processing status
- Examine prepared CSV files for data quality validation

## Data Quality Metrics

### Overall Pipeline Performance
- **Total Records Processed**: 2,302 (201 + 100 + 2,001)
- **Final Clean Records**: 1,967 (180 + 100 + 1,687)
- **Overall Data Retention**: 85.4%

### Quality Improvements Achieved
1. **Consistency**: Standardized formats across all datasets
2. **Completeness**: No missing values in final datasets
3. **Validity**: All records pass business rule validation
4. **Accuracy**: Outliers and data entry errors removed
5. **Integrity**: Cross-field validation ensures data coherence

### Business Value Delivered
- **Reliable Analytics**: Clean data enables accurate reporting
- **Regulatory Compliance**: Data meets quality standards
- **Operational Efficiency**: Automated quality checks reduce manual review
- **Scalability**: Pipeline can handle larger datasets with same quality assurance

## Maintenance and Monitoring

### Regular Tasks
1. **Log Review**: Monitor daily execution logs for errors
2. **Quality Metrics**: Track data retention rates over time
3. **Business Rule Updates**: Adjust validation rules as business evolves
4. **Performance Monitoring**: Track processing time for optimization

### Troubleshooting Common Issues
1. **Import Errors**: Ensure virtual environment is activated
2. **Path Issues**: Verify PROJECT_ROOT calculation is correct
3. **Data Format Changes**: Update parsing logic for new data formats
4. **Memory Issues**: Consider chunking for very large datasets

## Future Enhancements

### Potential Improvements
1. **Automated Scheduling**: Add cron/scheduled execution
2. **Data Validation Framework**: Implement more sophisticated validation rules
3. **Machine Learning**: Use ML for anomaly detection
4. **Real-time Processing**: Stream processing for live data
5. **Data Lineage**: Track data transformation history
6. **Performance Optimization**: Parallel processing for large datasets

### Scalability Considerations
- Database integration for larger datasets
- Distributed processing with Spark/Dask
- Cloud deployment options
- API integration for real-time data feeds

---

## Appendix

### Column Mappings

#### Customer Data Schema
| Original Column | Clean Column | Data Type | Validation Rules |
|----------------|--------------|-----------|------------------|
| CustomerID | customerid | int64 | Range: 1000-9999 |
| CustomerName | customername | object | Length: 2-100 chars |
| Region | region | object | Valid regions only |
| CustomerSince | customersince | object | Valid date format |
| TotalSpend | totalspend | float64 | Positive values |
| CustomerStatus | customerstatus | object | Valid status codes |

#### Product Data Schema
| Original Column | Clean Column | Data Type | Validation Rules |
|----------------|--------------|-----------|------------------|
| ProductID | productid | int64 | Range: 1000-99999 |
| ProductName | productname | object | Length: 2-100 chars |
| ProductCategory | productcategory | object | Approved categories |
| UnitPrice | unitprice | float64 | Range: $1.00-$10,000 |
| StockQuantity | stockquantity | int64 | Non-negative integers |
| SupplierName | suppliername | object | Length: 2-50 chars |

#### Sales Data Schema
| Original Column | Clean Column | Data Type | Validation Rules |
|----------------|--------------|-----------|------------------|
| TransactionID | transactionid | int64 | Positive, unique |
| TransactionDate | transactiondate | object | YYYY-MM-DD format |
| CustomerID | customerid | int64 | Range: 1000-9999 |
| ProductID | productid | int64 | Range: 2000-2999 |
| StoreID | storeid | int64 | Range: 401-499 |
| CampaignID | campaignid | int64 | 0-3 (0=no campaign) |
| TotalAmount | totalamount | float64 | Positive, 2 decimals |
| QuantitySold | quantitysold | int64 | Positive integers |
| SalesRepresentative | salesrepresentative | object | Length: 2-50 chars |
| unit_price | unit_price | float64 | Calculated field |

### Contact Information
For questions about this data preparation pipeline, please contact the analytics team or refer to the project documentation.

**Last Updated**: November 4, 2025
**Pipeline Version**: 1.0
**Documentation Version**: 1.0
