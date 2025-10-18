# GCPBigQueryDestination - Usage Guide

Component for loading data to Google BigQuery with advanced capabilities.

---

## 🎯 Features

- ✅ DataFrame loading to BigQuery
- ✅ Pre and post load queries (`before_query`, `after_query`)
- ✅ Time partitioning (by day, hour, month, year)
- ✅ Clustering for query optimization
- ✅ Automatic schema updates
- ✅ Write disposition control (TRUNCATE, APPEND, EMPTY)
- ✅ Configurable error tolerance
- ✅ Detailed logging with statistics

---

## 📦 Installation
```bash
pip install google-cloud-bigquery google-auth db-dtypes pandas
```

---

## 🚀 Basic Usage

### Example 1: Simple Load (APPEND)
```python
from src.google.cloud import GCPBigQueryDestination
from src.core.base import Pipe
from src.core.common import CSVOrigin

# Read data
origin = CSVOrigin("reader", filepath_or_buffer="sales.csv")

# BigQuery destination
destination = GCPBigQueryDestination(
    name="sales_loader",
    project_id="my-project",
    dataset="analytics",
    table="sales",
    write_disposition="WRITE_APPEND"  # Append data
)

# Connect and execute
pipe = Pipe("pipe1")
origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Example 2: Replace Table (TRUNCATE)
```python
# Replace all table data
destination = GCPBigQueryDestination(
    name="daily_report",
    project_id="my-project",
    dataset="reports",
    table="daily_summary",
    write_disposition="WRITE_TRUNCATE"  # ✨ Replace entire table
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Example 3: Only if Table is Empty (EMPTY)
```python
# Only load if table has no data
destination = GCPBigQueryDestination(
    name="initial_load",
    project_id="my-project",
    dataset="warehouse",
    table="customers",
    write_disposition="WRITE_EMPTY",  # ✨ Only if table is empty
    create_disposition="CREATE_IF_NEEDED"
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

## 🔧 Advanced Features

### Example 4: Before Query (Prepare Before Loading)
```python
# Execute query BEFORE loading data
destination = GCPBigQueryDestination(
    name="staged_load",
    project_id="my-project",
    dataset="warehouse",
    table="orders",
    before_query="""
        -- Create backup before loading
        CREATE OR REPLACE TABLE `my-project.warehouse.orders_backup` AS
        SELECT * FROM `my-project.warehouse.orders`;
        
        -- Truncate staging table
        TRUNCATE TABLE `my-project.staging.temp_orders`;
    """,
    write_disposition="WRITE_TRUNCATE"
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

**Use cases for `before_query`:**
- Create backups before loading
- Truncate specific tables
- Prepare staging areas
- Validate pre-conditions
- Clean old data

---

### Example 5: After Query (Auditing and Post-processing)
```python
# Execute query AFTER loading data
destination = GCPBigQueryDestination(
    name="customer_loader",
    project_id="my-project",
    dataset="crm",
    table="customers",
    write_disposition="WRITE_APPEND",
    after_query="""
        -- Log to audit
        INSERT INTO `my-project.audit.load_log` (
            table_name,
            loaded_at,
            record_count,
            loaded_by
        ) VALUES (
            'customers',
            CURRENT_TIMESTAMP(),
            (SELECT COUNT(*) FROM `my-project.crm.customers`),
            'open-stage-pipeline'
        );
        
        -- Update metadata
        UPDATE `my-project.crm.table_metadata`
        SET last_updated = CURRENT_TIMESTAMP()
        WHERE table_name = 'customers';
    """
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

**Use cases for `after_query`:**
- Audit logging
- Update metadata tables
- Execute post-load validations
- Call stored procedures
- Update materialized views

---

### Example 6: Complete Workflow (Before + After)
```python
# Complete pipeline with preparation and post-processing
destination = GCPBigQueryDestination(
    name="sales_etl",
    project_id="my-project",
    dataset="warehouse",
    table="sales_fact",
    
    # BEFORE: Prepare
    before_query="""
        -- Incremental backup
        CREATE OR REPLACE TABLE `my-project.warehouse.sales_fact_backup_{DATE}` AS
        SELECT * FROM `my-project.warehouse.sales_fact`;
        
        -- Prepare staging
        TRUNCATE TABLE `my-project.staging.sales_staging`;
        
        -- Mark load start
        UPDATE `my-project.control.etl_status`
        SET status = 'LOADING', start_time = CURRENT_TIMESTAMP()
        WHERE table_name = 'sales_fact';
    """,
    
    write_disposition="WRITE_TRUNCATE",
    
    # AFTER: Validate and log
    after_query="""
        -- Validate loaded data
        CALL `my-project.procedures.validate_sales_data`();
        
        -- Update dimensions
        CALL `my-project.procedures.refresh_sales_aggregates`();
        
        -- Log success
        INSERT INTO `my-project.audit.etl_runs` (
            pipeline_name,
            table_name,
            run_timestamp,
            records_loaded,
            status
        ) VALUES (
            'sales_etl',
            'sales_fact',
            CURRENT_TIMESTAMP(),
            (SELECT COUNT(*) FROM `my-project.warehouse.sales_fact`),
            'SUCCESS'
        );
        
        -- Update control
        UPDATE `my-project.control.etl_status`
        SET status = 'COMPLETED', end_time = CURRENT_TIMESTAMP()
        WHERE table_name = 'sales_fact';
    """
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Example 7: Time Partitioned Table
```python
# Create time-partitioned table for better performance
destination = GCPBigQueryDestination(
    name="events_loader",
    project_id="my-project",
    dataset="analytics",
    table="events",
    write_disposition="WRITE_APPEND",
    time_partitioning={
        'type': 'DAY',           # Partition by day
        'field': 'event_date'    # Date field
    }
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

**Partitioning types:**
- `'DAY'` - By day (recommended)
- `'HOUR'` - By hour
- `'MONTH'` - By month
- `'YEAR'` - By year

---

### Example 8: Table with Clustering
```python
# Optimize queries with clustering
destination = GCPBigQueryDestination(
    name="orders_loader",
    project_id="my-project",
    dataset="warehouse",
    table="orders",
    write_disposition="WRITE_APPEND",
    clustering_fields=['customer_id', 'product_id', 'region']  # Max 4 fields
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

**Clustering benefits:**
- Improves filtered query performance
- Reduces costs (only scans necessary blocks)
- Ideal for frequently filtered fields

---

### Example 9: Partitioning + Clustering
```python
# Combine partitioning and clustering for maximum optimization
destination = GCPBigQueryDestination(
    name="optimized_sales",
    project_id="my-project",
    dataset="warehouse",
    table="sales",
    write_disposition="WRITE_APPEND",
    time_partitioning={
        'type': 'DAY',
        'field': 'sale_date'
    },
    clustering_fields=['region', 'product_category', 'store_id']
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Example 10: Custom Schema
```python
from google.cloud import bigquery

# Define schema manually
custom_schema = [
    bigquery.SchemaField("customer_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("total_purchases", "FLOAT", mode="NULLABLE"),
]

destination = GCPBigQueryDestination(
    name="customers_loader",
    project_id="my-project",
    dataset="crm",
    table="customers",
    write_disposition="WRITE_APPEND",
    schema=custom_schema,  # Explicit schema
    autodetect=False       # Disable auto-detection
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Example 11: Automatic Schema Update
```python
# Allow automatic column addition
destination = GCPBigQueryDestination(
    name="flexible_loader",
    project_id="my-project",
    dataset="warehouse",
    table="evolving_table",
    write_disposition="WRITE_APPEND",
    schema_update_options=[
        'ALLOW_FIELD_ADDITION',      # Allow new columns
        'ALLOW_FIELD_RELAXATION'     # Relax restrictions (REQUIRED → NULLABLE)
    ]
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Example 12: With Credentials and Labels
```python
# Complete configuration with authentication and organization
destination = GCPBigQueryDestination(
    name="secure_loader",
    project_id="my-project",
    dataset="sensitive_data",
    table="pii_customers",
    write_disposition="WRITE_APPEND",
    credentials_path="/path/to/service-account.json",
    location="EU",  # Data in Europe
    job_labels={
        "team": "data-engineering",
        "env": "production",
        "pipeline": "customer-etl"
    }
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Example 13: Error Tolerance
```python
# Allow some records with errors
destination = GCPBigQueryDestination(
    name="lenient_loader",
    project_id="my-project",
    dataset="raw",
    table="web_logs",
    write_disposition="WRITE_APPEND",
    max_bad_records=100  # Tolerate up to 100 records with errors
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

## 📊 Example Output
```
GCPBigQueryDestination 'sales_etl' received data from pipe: 'pipe1'
GCPBigQueryDestination 'sales_etl' using default credentials
GCPBigQueryDestination 'sales_etl' BigQuery client initialized successfully

GCPBigQueryDestination 'sales_etl' executing before_query...
  Query preview: CREATE OR REPLACE TABLE `my-project.warehouse.sales_fact_backup` AS...
✅ GCPBigQueryDestination 'sales_etl' before_query executed successfully
  - Bytes processed: 2,345,678 bytes (2.24 MB)
  - Rows affected: 15,432
  - Duration: 3.12s

======================================================================
GCPBigQueryDestination 'sales_etl' loading data to BigQuery...
======================================================================
  - Table: my-project.warehouse.sales_fact
  - Write disposition: WRITE_TRUNCATE
  - Create disposition: CREATE_IF_NEEDED
  - DataFrame shape: (15432, 12)
  - DataFrame columns: ['sale_id', 'customer_id', 'product_id', 'amount', ...]
  - Clustering fields: ['region', 'product_category']
  - Time partitioning: {'type': 'DAY', 'field': 'sale_date'}

GCPBigQueryDestination 'sales_etl' starting load job...
GCPBigQueryDestination 'sales_etl' waiting for load job completion...

======================================================================
GCPBigQueryDestination 'sales_etl' LOAD completed successfully:
======================================================================
  📊 Load results:
     - Rows loaded: 15,432
     - Total rows in table: 15,432
     - Table schema fields: 12
  
  ⏱️  Job info:
     - Job ID: job_abc123xyz
     - Duration: 4.56s
     - Output rows: 15,432
  
  📋 Table info:
     - Created: 2024-01-15 10:30:00
     - Modified: 2025-01-18 14:25:33
     - Clustering: ['region', 'product_category']
     - Partitioning: DAY

GCPBigQueryDestination 'sales_etl' executing after_query...
  Query preview: CALL `my-project.procedures.validate_sales_data`();...
✅ GCPBigQueryDestination 'sales_etl' after_query executed successfully
  - Bytes processed: 45,678 bytes (0.04 MB)
  - Rows affected: 3
  - Duration: 1.89s

======================================================================
✅ GCPBigQueryDestination 'sales_etl' completed successfully
======================================================================
```

---

## 📋 Complete Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | str | ✅ | - | Component name |
| `project_id` | str | ✅ | - | GCP project ID |
| `dataset` | str | ✅ | - | Dataset name |
| `table` | str | ✅ | - | Table name |
| `write_disposition` | str | ✅ | - | Write mode |
| `credentials_path` | str | ❌ | None | Path to service account JSON |
| `before_query` | str | ❌ | None | Query to execute BEFORE |
| `after_query` | str | ❌ | None | Query to execute AFTER |
| `schema` | list | ❌ | None | Custom schema |
| `create_disposition` | str | ❌ | 'CREATE_IF_NEEDED' | Create table if not exists |
| `schema_update_options` | list | ❌ | [] | Update options |
| `clustering_fields` | list | ❌ | None | Clustering fields (max 4) |
| `time_partitioning` | dict | ❌ | None | Partitioning config |
| `location` | str | ❌ | None | BigQuery region |
| `job_labels` | dict | ❌ | {} | Job labels |
| `max_bad_records` | int | ❌ | 0 | Maximum error records |
| `autodetect` | bool | ❌ | True | Auto-detect schema |

---

## 🔧 write_disposition Values

| Value | Behavior |
|-------|----------|
| `WRITE_TRUNCATE` | Replace entire table (delete and recreate) |
| `WRITE_APPEND` | Append data to existing table |
| `WRITE_EMPTY` | Only write if table is empty (fails if has data) |

---

## 🔧 create_disposition Values

| Value | Behavior |
|-------|----------|
| `CREATE_IF_NEEDED` | Create table if not exists (default) |
| `CREATE_NEVER` | Fail if table doesn't exist |

---

## 🔧 Schema Update Options

| Option | Description |
|--------|-------------|
| `ALLOW_FIELD_ADDITION` | Allow adding new columns to schema |
| `ALLOW_FIELD_RELAXATION` | Allow changing REQUIRED to NULLABLE fields |

---

## ✅ Best Practices

1. **Use `WRITE_TRUNCATE`** for complete daily replacements
2. **Use `WRITE_APPEND`** for incremental loads
3. **Use `before_query`** to create backups before loading
4. **Use `after_query`** for validations and auditing
5. **Partition large tables** by date for better performance
6. **Use clustering** on frequently filtered fields
7. **Define explicit schema** for production (avoid auto-detect)
8. **Use `schema_update_options`** with caution in production
9. **Specify `location`** to comply with data regulations
10. **Add `job_labels`** for organization and tracking

---

## ⚠️ Important Considerations

### Partitioning
- Can only partition by **ONE** date/timestamp field
- Cannot change partitioning of existing table
- Consider scan cost vs. performance benefit

### Clustering
- Maximum **4 fields** for clustering
- Field order matters (most selective first)
- Only effective on large tables (>1GB)

### Schema Updates
- `ALLOW_FIELD_ADDITION` is safe
- `ALLOW_FIELD_RELAXATION` may cause issues if queries assume NOT NULL
- Cannot delete columns with schema updates

### Costs
- Load operations are **free**
- Charged for storage and queries
- Partitioning reduces query costs

---

## 🔗 See Also

- [GCPBigQueryOrigin](./GCPBigQueryOrigin.md) - For extracting from BigQuery
- [Open-Stage Documentation](../README.md) - Complete documentation

---

**Open-Stage v2.4** - Enterprise ETL Framework