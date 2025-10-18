# GCPBigQueryOrigin - Usage Guide

Component for extracting data from Google BigQuery with advanced capabilities.

---

## 🎯 Features

- ✅ Direct table reading or custom queries
- ✅ Pre and post extraction queries (`before_query`, `after_query`)
- ✅ Result limit for testing (`max_results`)
- ✅ Query validation without execution (`dry_run`)
- ✅ Parameterized queries
- ✅ Automatic cost estimation
- ✅ Detailed logging

---

## 📦 Installation
```bash
pip install google-cloud-bigquery google-auth db-dtypes
```

---

## 🚀 Basic Usage

### Example 1: Simple Query
```python
from src.google.cloud import GCPBigQueryOrigin
from src.core.base import Pipe
from src.core.common import Printer

# Create origin with query
origin = GCPBigQueryOrigin(
    name="sales_data",
    project_id="my-project",
    query="SELECT * FROM dataset.sales WHERE date >= '2024-01-01'"
)

# Connect and execute
pipe = Pipe("pipe1")
printer = Printer("output")

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

---

### Example 2: Direct Table Reading
```python
# Read full table without writing SELECT *
origin = GCPBigQueryOrigin(
    name="customers",
    project_id="my-project",
    table="dataset.customers"  # ✨ Simpler!
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

---

### Example 3: Limit for Testing
```python
# Extract only 100 rows for testing
origin = GCPBigQueryOrigin(
    name="sales_sample",
    project_id="my-project",
    table="dataset.sales",
    max_results=100  # ✨ Fast for development
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

---

## 🔧 Advanced Features

### Example 4: Dry Run (Validate and Estimate Costs)
```python
# Validate query WITHOUT executing
origin = GCPBigQueryOrigin(
    name="cost_check",
    project_id="my-project",
    query="SELECT * FROM `bigquery-public-data.usa_names.usa_1910_current`",
    dry_run=True  # ✨ Only validates and estimates cost
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()

# Output:
# ✅ Query is valid
# 📊 Estimated bytes processed: 6,432,432 bytes (6.13 MB)
# 💰 Estimated cost: $0.000038 USD
```

---

### Example 5: Before Query (Prepare Data)
```python
# Execute query BEFORE extraction
origin = GCPBigQueryOrigin(
    name="processed_sales",
    project_id="my-project",
    before_query="""
        -- Create temporary table with filtered data
        CREATE TEMP TABLE temp_sales AS
        SELECT * FROM dataset.raw_sales
        WHERE status = 'completed'
        AND date >= '2024-01-01';
    """,
    query="SELECT * FROM temp_sales WHERE amount > 100"
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

**Use cases for `before_query`:**
- Create temporary tables
- Call stored procedures
- Prepare data before extraction
- Clean staging areas

---

### Example 6: After Query (Auditing)
```python
# Execute query AFTER extraction
origin = GCPBigQueryOrigin(
    name="customer_extract",
    project_id="my-project",
    table="crm.customers",
    after_query="""
        -- Log the extraction
        INSERT INTO `my-project.audit.extraction_log` (
            table_name,
            extracted_at,
            record_count
        ) VALUES (
            'customers',
            CURRENT_TIMESTAMP(),
            (SELECT COUNT(*) FROM `my-project.crm.customers`)
        );
    """
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

**Use cases for `after_query`:**
- Audit logging
- Mark records as processed
- Update timestamps
- Clean temporary tables

---

### Example 7: Complete Workflow (Before + After)
```python
# Complete pipeline with preparation and cleanup
origin = GCPBigQueryOrigin(
    name="daily_sales_etl",
    project_id="my-project",
    
    # BEFORE: Prepare staging
    before_query="""
        -- Create staging table
        CREATE OR REPLACE TABLE `my-project.staging.daily_sales` AS
        SELECT 
            DATE(order_timestamp) as sale_date,
            product_id,
            customer_id,
            amount,
            region
        FROM `my-project.raw.orders`
        WHERE DATE(order_timestamp) = CURRENT_DATE()
        AND status = 'completed';
        
        -- Validate data
        CALL `my-project.procedures.validate_sales`();
    """,
    
    # MAIN QUERY
    query="SELECT * FROM `my-project.staging.daily_sales`",
    
    # AFTER: Log and cleanup
    after_query="""
        -- Log execution
        INSERT INTO `my-project.audit.etl_runs` (
            pipeline_name,
            run_timestamp,
            records_processed
        ) VALUES (
            'daily_sales_etl',
            CURRENT_TIMESTAMP(),
            (SELECT COUNT(*) FROM `my-project.staging.daily_sales`)
        );
        
        -- Clean temporary tables
        DROP TABLE IF EXISTS `my-project.staging.temp_processing`;
    """
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

---

### Example 8: Parameterized Queries
```python
from google.cloud import bigquery

# Secure query with parameters
origin = GCPBigQueryOrigin(
    name="filtered_sales",
    project_id="my-project",
    query="""
        SELECT * FROM dataset.sales 
        WHERE date >= @start_date 
        AND amount > @min_amount
    """,
    query_parameters=[
        bigquery.ScalarQueryParameter("start_date", "DATE", "2024-01-01"),
        bigquery.ScalarQueryParameter("min_amount", "FLOAT64", 100.0)
    ]
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

---

### Example 9: With Credentials and Location
```python
# Use service account and specific region
origin = GCPBigQueryOrigin(
    name="secure_extract",
    project_id="my-project",
    table="dataset.sensitive_data",
    credentials_path="/path/to/service-account.json",
    location="US",  # BigQuery region
    timeout=300,    # 5 minute timeout
    job_labels={
        "team": "data-engineering",
        "env": "production"
    }
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

---

## 📊 Example Output
```
GCPBigQueryOrigin 'daily_sales_etl' using default credentials
GCPBigQueryOrigin 'daily_sales_etl' BigQuery client initialized successfully

GCPBigQueryOrigin 'daily_sales_etl' executing before_query...
  Query preview: CREATE OR REPLACE TABLE `my-project.staging.daily_sales` AS...
✅ GCPBigQueryOrigin 'daily_sales_etl' before_query executed successfully
  - Bytes processed: 1,234,567 bytes (1.18 MB)
  - Rows affected: 5,432
  - Duration: 2.34s

======================================================================
GCPBigQueryOrigin 'daily_sales_etl' executing MAIN extraction query...
======================================================================
  - Project ID: my-project
  - Query: SELECT * FROM `my-project.staging.daily_sales`

GCPBigQueryOrigin 'daily_sales_etl' waiting for query completion...

======================================================================
GCPBigQueryOrigin 'daily_sales_etl' MAIN query results:
======================================================================
  📊 Results:
     - Rows returned: 5,432
     - Columns: 6
     - Column names: ['sale_date', 'product_id', 'customer_id', 'amount', ...]
  
  💾 Data processed:
     - Bytes processed: 234,567 bytes (0.22 GB)
     - Bytes billed: 234,567 bytes (0.22 GB)
  
  💰 Estimated cost: $0.001380 USD
  
  ⏱️  Job info:
     - Job ID: job_xyz123abc
     - Duration: 1.45s

GCPBigQueryOrigin 'daily_sales_etl' executing after_query...
  Query preview: INSERT INTO `my-project.audit.etl_runs` (...
✅ GCPBigQueryOrigin 'daily_sales_etl' after_query executed successfully
  - Bytes processed: 12,345 bytes (0.01 MB)
  - Rows affected: 1
  - Duration: 0.89s

======================================================================
✅ GCPBigQueryOrigin 'daily_sales_etl' pumped data through pipe 'pipe1'
======================================================================
```

---

## 📋 Complete Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | str | ✅ | - | Component name |
| `project_id` | str | ✅ | - | GCP project ID |
| `query` | str | * | None | SQL query to execute |
| `table` | str | * | None | Table in format `dataset.table` |
| `credentials_path` | str | ❌ | None | Path to service account JSON |
| `before_query` | str | ❌ | None | Query to execute BEFORE |
| `after_query` | str | ❌ | None | Query to execute AFTER |
| `max_results` | int | ❌ | None | Row limit to return |
| `use_legacy_sql` | bool | ❌ | False | Use legacy SQL |
| `query_parameters` | list | ❌ | [] | Query parameters |
| `location` | str | ❌ | None | BigQuery region |
| `job_labels` | dict | ❌ | {} | Job labels |
| `timeout` | float | ❌ | None | Timeout in seconds |
| `use_query_cache` | bool | ❌ | True | Use BigQuery cache |
| `dry_run` | bool | ❌ | False | Only validate without executing |

\* **Note**: You must provide `query` OR `table`, but not both.

---

## ✅ Best Practices

1. **Use `dry_run`** before running large queries to estimate costs
2. **Use `max_results`** in development for quick testing
3. **Use `before_query`** to prepare data and staging
4. **Use `after_query`** for auditing and cleanup
5. **Use `query_parameters`** instead of string concatenation (security)
6. **Use `table`** when you only need `SELECT *` (simpler)
7. **Specify `location`** if working with data in specific regions

---

## 🔗 See Also

- [GCPBigQueryDestination](./GCPBigQueryDestination.md) - For writing to BigQuery
- [Open-Stage Documentation](../README.md) - Complete documentation

---

**Open-Stage v2.4** - Enterprise ETL Framework