# PostgresOrigin - Usage Guide

Component for extracting data from PostgreSQL with advanced capabilities.

---

## 🎯 Features

- ✅ Direct table reading or custom queries
- ✅ Pre and post extraction queries (`before_query`, `after_query`)
- ✅ Result limit for testing (`max_results`)
- ✅ Secure parameterized queries
- ✅ Configurable timeout
- ✅ Detailed logging with statistics

---

## 📦 Installation
```bash
pip install sqlalchemy psycopg2-binary pandas
```

---

## 🚀 Basic Usage

### Example 1: Simple Query
```python
from src.postgres.common import PostgresOrigin
from src.core.base import Pipe
from src.core.common import Printer

# Create origin with query
origin = PostgresOrigin(
    name="sales_data",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    query="SELECT * FROM sales WHERE date >= '2024-01-01'"
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
origin = PostgresOrigin(
    name="customers",
    host="localhost",
    database="crm",
    user="postgres",
    password="password",
    table="public.customers"  # ✨ Simpler!
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

---

### Example 3: Limit for Testing
```python
# Extract only 100 rows for testing
origin = PostgresOrigin(
    name="sales_sample",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    table="sales",
    max_results=100  # ✨ Fast for development
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

---

## 🔧 Advanced Features

### Example 4: Before Query (Prepare Data)
```python
# Execute query BEFORE extraction
origin = PostgresOrigin(
    name="processed_orders",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    before_query="""
        -- Create temporary table with filtered data
        CREATE TEMP TABLE temp_orders AS
        SELECT * FROM raw_orders
        WHERE status = 'completed'
        AND date >= '2024-01-01';
        
        -- Index for better performance
        CREATE INDEX idx_temp_orders_amount ON temp_orders(amount);
    """,
    query="SELECT * FROM temp_orders WHERE amount > 100"
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

**Use cases for `before_query`:**
- Create temporary tables
- Call stored procedures
- Prepare data before extraction
- Clean staging areas
- SET session variables
- Create temporary indexes

---

### Example 5: After Query (Auditing)
```python
# Execute query AFTER extraction
origin = PostgresOrigin(
    name="customer_extract",
    host="localhost",
    database="crm",
    user="postgres",
    password="password",
    table="customers",
    after_query="""
        -- Log the extraction
        INSERT INTO audit.extraction_log (
            table_name,
            extracted_at,
            record_count
        ) VALUES (
            'customers',
            NOW(),
            (SELECT COUNT(*) FROM customers)
        );
        
        -- Mark records as processed
        UPDATE customers
        SET last_extracted = NOW()
        WHERE last_extracted IS NULL;
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
- Update table statistics
- Notify completion

---

### Example 6: Complete Workflow (Before + After)
```python
# Complete pipeline with preparation and cleanup
origin = PostgresOrigin(
    name="daily_sales_etl",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    
    # BEFORE: Prepare staging
    before_query="""
        -- Create staging table
        DROP TABLE IF EXISTS staging.daily_sales;
        
        CREATE TABLE staging.daily_sales AS
        SELECT 
            DATE(order_timestamp) as sale_date,
            product_id,
            customer_id,
            amount,
            region
        FROM raw.orders
        WHERE DATE(order_timestamp) = CURRENT_DATE
        AND status = 'completed';
        
        -- Validate data
        DO $$
        BEGIN
            IF (SELECT COUNT(*) FROM staging.daily_sales) = 0 THEN
                RAISE EXCEPTION 'No sales data for today';
            END IF;
        END $$;
        
        -- Create indexes
        CREATE INDEX idx_staging_sales_date ON staging.daily_sales(sale_date);
    """,
    
    # MAIN QUERY
    query="SELECT * FROM staging.daily_sales ORDER BY sale_date, customer_id",
    
    # AFTER: Log and cleanup
    after_query="""
        -- Log execution
        INSERT INTO audit.etl_runs (
            pipeline_name,
            run_timestamp,
            records_processed,
            status
        ) VALUES (
            'daily_sales_etl',
            NOW(),
            (SELECT COUNT(*) FROM staging.daily_sales),
            'SUCCESS'
        );
        
        -- Update metadata
        UPDATE control.table_metadata
        SET last_extraction = NOW()
        WHERE table_name = 'daily_sales';
        
        -- Clean old temporary tables
        DROP TABLE IF EXISTS staging.temp_processing;
        
        -- Vacuum analyze for statistics
        ANALYZE staging.daily_sales;
    """
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

---

### Example 7: Parameterized Queries
```python
# Secure query with parameters (prevents SQL injection)
origin = PostgresOrigin(
    name="filtered_sales",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    query="""
        SELECT * FROM sales 
        WHERE date >= :start_date 
        AND amount > :min_amount
        AND region = :region
    """,
    query_parameters={
        'start_date': '2024-01-01',
        'min_amount': 100.0,
        'region': 'North'
    }
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

---

### Example 8: With Timeout
```python
# Execution time control for long queries
origin = PostgresOrigin(
    name="large_extract",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    query="SELECT * FROM huge_table WHERE date >= '2024-01-01'",
    timeout=300  # 5 minutes maximum
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

---

### Example 9: Reading with Explicit Schema
```python
# Specify schema when not 'public'
origin = PostgresOrigin(
    name="reporting_data",
    host="localhost",
    database="analytics",
    user="postgres",
    password="password",
    table="reports.monthly_summary",  # schema.table
    max_results=1000
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

---

### Example 10: With Session Variables
```python
# Configure session variables before extraction
origin = PostgresOrigin(
    name="custom_config",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    before_query="""
        -- Configure session variables
        SET work_mem = '256MB';
        SET statement_timeout = '300s';
        SET search_path = 'analytics, public';
        
        -- Create temporary table
        CREATE TEMP TABLE filtered_data AS
        SELECT * FROM large_table WHERE category = 'A';
    """,
    query="SELECT * FROM filtered_data"
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

---

## 📊 Example Output
```
PostgresOrigin 'daily_sales_etl' engine initialized successfully
Connection: postgres@localhost:5432/warehouse
  - Timeout: 300.0s

PostgresOrigin 'daily_sales_etl' executing before_query...
  Query preview: -- Create staging table
        DROP TABLE IF EXISTS staging.daily_sales;...
✅ PostgresOrigin 'daily_sales_etl' before_query executed successfully
  - Rows affected: 5,432
  - Duration: 2.34s

======================================================================
PostgresOrigin 'daily_sales_etl' executing MAIN extraction query...
======================================================================
  - Database: warehouse
  - Query: SELECT * FROM staging.daily_sales ORDER BY sale_date, customer_id
  - Parameters: ['start_date', 'region']

PostgresOrigin 'daily_sales_etl' waiting for query completion...

======================================================================
PostgresOrigin 'daily_sales_etl' MAIN query results:
======================================================================
  📊 Results:
     - Rows returned: 5,432
     - Columns: 5
     - Column names: ['sale_date', 'product_id', 'customer_id', 'amount', 'region']
  
  ⏱️  Query info:
     - Duration: 1.45s
  
  📋 Data types:
     - sale_date: object
     - product_id: int64
     - customer_id: int64
     - amount: float64
     - region: object

PostgresOrigin 'daily_sales_etl' executing after_query...
  Query preview: -- Log execution
        INSERT INTO audit.etl_runs (...
✅ PostgresOrigin 'daily_sales_etl' after_query executed successfully
  - Rows affected: 1
  - Duration: 0.45s

======================================================================
✅ PostgresOrigin 'daily_sales_etl' pumped data through pipe 'pipe1'
======================================================================
PostgresOrigin 'daily_sales_etl' connection closed
```

---

## 📋 Complete Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | str | ✅ | - | Component name |
| `host` | str | ✅ | - | PostgreSQL host |
| `port` | int | ❌ | 5432 | PostgreSQL port |
| `database` | str | ✅ | - | Database name |
| `user` | str | ✅ | - | PostgreSQL username |
| `password` | str | ✅ | - | Password |
| `query` | str | * | None | SQL query to execute |
| `table` | str | * | None | Table in format `table` or `schema.table` |
| `before_query` | str | ❌ | None | Query to execute BEFORE |
| `after_query` | str | ❌ | None | Query to execute AFTER |
| `max_results` | int | ❌ | None | Row limit to return |
| `timeout` | float | ❌ | None | Timeout in seconds |
| `query_parameters` | dict | ❌ | {} | Query parameters |

\* **Note**: You must provide `query` OR `table`, but not both.

---

## 🔐 Supported Table Formats

```python
# Format 1: Table name only (uses default schema)
table="customers"
# Generates: SELECT * FROM "customers"

# Format 2: Explicit schema
table="public.customers"
# Generates: SELECT * FROM "public"."customers"

# Format 3: Non-public schema
table="analytics.sales_summary"
# Generates: SELECT * FROM "analytics"."sales_summary"
```

---

## ✅ Best Practices

1. **Use `table`** when you only need `SELECT *` (simpler)
2. **Use `max_results`** in development for quick testing
3. **Use `before_query`** to prepare data and staging
4. **Use `after_query`** for auditing and cleanup
5. **Use `query_parameters`** instead of string concatenation (security)
6. **Specify `timeout`** for long queries
7. **Use temporary tables** in `before_query` for complex transformations
8. **Index temporary tables** if you'll filter/sort on them
9. **Clean resources** in `after_query` (DROP TEMP TABLES)
10. **Use transactions** when necessary in before/after queries

---

## ⚠️ Important Considerations

### Temporary Tables
- TEMP tables are automatically deleted when connection closes
- Use `CREATE TEMP TABLE` for intermediate data
- Only visible to current session

### Query Parameters
- Use `:param_name` in query
- Provide values in `query_parameters` as dictionary
- Prevents SQL injection using parameters

### Timeout
- Applies to both initial connection and queries
- Useful for long queries or to avoid locks
- If query exceeds timeout, raises exception

### Performance
- `ANALYZE` tables after large loads in `after_query`
- Use `EXPLAIN ANALYZE` in development to optimize
- Consider temporary indexes in staging

### Schemas
- Default schema is `public`
- Specify schema explicitly: `schema.table`
- Use `SET search_path` in `before_query` if needed

---

## 🔗 See Also

- [PostgresDestination](./PostgresDestination.md) - For writing to PostgreSQL
- [Open-Stage Documentation](../README.md) - Complete documentation

---

**Open-Stage v2.4** - Enterprise ETL Framework