# Master Data Pipeline Usage Guide

## Overview

The Smart Store Analytics project includes a master orchestrator script that executes all data preparation scripts in sequence, providing automated data cleaning and validation across all datasets.

## Pipeline Components

### Individual Data Preparation Scripts

Located in `src/analytics_project/data_preparation/`:

1. **prepare_customers.py** - Customer data cleaning and validation
2. **prepare_products.py** - Product data cleaning and validation
3. **prepare_sales.py** - Sales transaction data cleaning and validation

### Master Orchestrator

**File**: `src/analytics_project/run_all_data_prep.py`

Executes all three preparation scripts in sequence with comprehensive logging and error handling.

## Running the Pipeline

### Option 1: Run All Scripts at Once (Recommended)

```bash
cd C:\Repos\smart-store-angie
python src\analytics_project\run_all_data_prep.py
```

**Execution Time**: ~5 seconds for all three datasets

### Option 2: Run Individual Scripts

```bash
# Customer data only
python src\analytics_project\data_preparation\prepare_customers.py

# Product data only
python src\analytics_project\data_preparation\prepare_products.py

# Sales data only
python src\analytics_project\data_preparation\prepare_sales.py
```

## Pipeline Results

### Data Processing Summary

| Dataset | Input Records | Output Records | Retention Rate | Key Issues Resolved |
|---------|--------------|----------------|----------------|-------------------|
| **Customers** | 201 | 180 | 89.6% | 21 invalid region records removed |
| **Products** | 100 | 100 | 100% | 84 supplier names standardized |
| **Sales** | 2,001 | 1,687 | 84.4% | 312 outliers, 1 duplicate, invalid dates |

### Output Files

All cleaned data is saved to `data/prepared/`:

- `customers_prepared.csv` - 180 validated customer records
- `products_prepared.csv` - 100 validated product records
- `sales_prepared.csv` - 1,687 validated transaction records

### Execution Log

Detailed execution logs are written to `project.log` in the project root.

## Pipeline Features

### Automated Processing
- Sequential execution of all data preparation scripts
- Continues processing even if one script fails
- Returns proper exit codes for automation/scheduling

### Comprehensive Logging
- Start/end timestamps for entire pipeline
- Individual script execution status
- Total execution time tracking
- Detailed error messages when failures occur

### Quality Assurance
- Success rate calculation
- Detailed status for each script
- Error capture and reporting
- Audit trail for compliance

## Master Orchestrator Output

### Successful Execution Example

```
================================================================================
STARTING MASTER DATA PREPARATION PIPELINE
================================================================================
Pipeline started at: 2025-11-05 17:46:59

================================================================================
EXECUTING: Customer Data Preparation
================================================================================
✅ prepare_customers.py completed successfully

================================================================================
EXECUTING: Product Data Preparation
================================================================================
✅ prepare_products.py completed successfully

================================================================================
EXECUTING: Sales Data Preparation
================================================================================
✅ prepare_sales.py completed successfully

================================================================================
PIPELINE EXECUTION SUMMARY
================================================================================
Pipeline completed at: 2025-11-05 17:47:04
Total execution time: 0:00:05.099434

✅ Customer Data Preparation
   Script: prepare_customers.py
   Status: SUCCESS

✅ Product Data Preparation
   Script: prepare_products.py
   Status: SUCCESS

✅ Sales Data Preparation
   Script: prepare_sales.py
   Status: SUCCESS

--------------------------------------------------------------------------------
Total Scripts: 3
Successful: 3
Failed: 0
Success Rate: 100.0%
================================================================================
🎉 Pipeline completed successfully!
```

## Automation and Scheduling

The master orchestrator can be integrated into automated workflows:

### Windows Task Scheduler
```bash
# Create scheduled task to run daily at 2 AM
schtasks /create /tn "DataPrepPipeline" /tr "python C:\Repos\smart-store-angie\src\analytics_project\run_all_data_prep.py" /sc daily /st 02:00
```

### PowerShell Script
```powershell
# Navigate to project directory
Set-Location C:\Repos\smart-store-angie

# Activate virtual environment
.venv\Scripts\Activate.ps1

# Run pipeline
python src\analytics_project\run_all_data_prep.py

# Check exit code
if ($LASTEXITCODE -eq 0) {
    Write-Host "Pipeline completed successfully"
} else {
    Write-Host "Pipeline failed - check logs"
}
```

## Error Handling

### Exit Codes
- **0**: All scripts executed successfully
- **1**: One or more scripts failed or critical error occurred

### Failure Scenarios
- Individual script failures are logged but don't stop the pipeline
- Missing scripts are reported with clear error messages
- Critical errors abort the pipeline with detailed error information

## Best Practices

### Regular Execution
- Run the pipeline after receiving new raw data files
- Schedule regular executions for ongoing data updates
- Review logs after each execution for data quality insights

### Monitoring
- Check `project.log` for detailed execution history
- Monitor success rates over time
- Investigate any scripts that consistently fail

### Maintenance
- Keep individual scripts focused on their specific datasets
- Update business rules as requirements change
- Add new validation rules based on discovered data issues

## Architecture Benefits

### Separation of Concerns
- Each dataset has its own specialized preparation script
- Dataset-specific business logic kept isolated
- Easier debugging and maintenance

### Scalability
- Easy to add new datasets by creating new preparation scripts
- Master orchestrator automatically handles new scripts
- No changes needed to existing scripts when adding new ones

### Production Ready
- Comprehensive error handling and logging
- Suitable for enterprise data warehouse pipelines
- Follows data engineering best practices

## Related Documentation

- **Data Preparation Pipeline**: Complete technical documentation of all cleaning processes
- **Quick Start and AI Development**: Development process and AI-assisted features
- **Individual Script Documentation**: See inline comments in each preparation script

---

**Last Updated**: November 5, 2025
**Pipeline Version**: 1.0
**Success Rate**: 100%
