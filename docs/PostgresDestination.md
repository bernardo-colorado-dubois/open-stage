# PostgresDestination - Usage Guide

Component for loading data to PostgreSQL with advanced capabilities.

---

## 🎯 Features

- ✅ DataFrame loading to PostgreSQL
- ✅ Pre and post load queries (`before_query`, `after_query`)
- ✅ Write disposition control (FAIL, REPLACE, APPEND)
- ✅ Configurable timeout
- ✅ Optimized loading with chunks and multi-insert
- ✅ Detailed logging with statistics

---

## 📦 Installation
```bash
pip install sqlalchemy psycopg2-binary pandas
```

---

## 🚀 Basic Usage

### Example 1: Simple Load (APPEND)
```python
from src.postgres.common import PostgresDestination
from src.core.base import Pipe
from src.core.common import CSVOrigin

# Read data
origin = CSVOrigin("reader", filepath_or_buffer="sales.csv")

# PostgreSQL destination
destination = PostgresDestination(
    name="sales_loader",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    table="sales",
    schema="public",
    if_exists="append"  # Append data
)

# Connect and execute
pipe = Pipe("pipe1")
origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Example 2: Replace Table (REPLACE)
```python
# Replace all table data
destination = PostgresDestination(
    name="daily_report",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    table="daily_summary",
    schema="reports",
    if_exists="replace"  # ✨ Replace entire table
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Example 3: Only if Table Doesn't Exist (FAIL)
```python
# Fail if table already exists (initial load)
destination = PostgresDestination(
    name="initial_load",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    table="customers",
    schema="public",
    if_exists="fail"  # ✨ Error if table exists
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

## 🔧 Advanced Features

### Example 4: Before Query (Prepare Before Loading)
```python
# Execute query BEFORE loading data
destination = PostgresDestination(
    name="staged_load",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    table="orders",
    schema="public",
    before_query="""
        -- Create backup before loading
        DROP TABLE IF EXISTS public.orders_backup;
        CREATE TABLE public.orders_backup AS
        SELECT * FROM public.orders;
        
        -- Truncate staging table
        TRUNCATE TABLE staging.temp_orders;
        
        -- Prepare sequences
        SELECT setval('orders_id_seq', COALESCE(MAX(id), 1))
        FROM public.orders;
    """,
    if_exists="replace"
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
- Reset sequences
- Temporarily disable triggers

---

### Example 5: After Query (Auditing and Post-processing)
```python
# Execute query AFTER loading data
destination = PostgresDestination(
    name="customer_loader",
    host="localhost",
    database="crm",
    user="postgres",
    password="password",
    table="customers",
    schema="public",
    if_exists="append",
    after_query="""
        -- Log to audit
        INSERT INTO audit.load_log (
            table_name,
            loaded_at,
            record_count,
            loaded_by
        ) VALUES (
            'customers',
            NOW(),
            (SELECT COUNT(*) FROM public.customers),
            'open-stage-pipeline'
        );
        
        -- Update metadata
        UPDATE control.table_metadata
        SET 
            last_updated = NOW(),
            row_count = (SELECT COUNT(*) FROM public.customers)
        WHERE table_name = 'customers';
        
        -- Refresh materialized view
        REFRESH MATERIALIZED VIEW reports.customer_summary;
        
        -- Analyze for statistics
        ANALYZE public.customers;
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
- ANALYZE to update statistics
- Create additional indexes
- Re-enable triggers

---

### Example 6: Complete Workflow (Before + After)
```python
# Complete pipeline with preparation and post-processing
destination = PostgresDestination(
    name="sales_etl",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    table="sales_fact",
    schema="public",
    
    # BEFORE: Prepare
    before_query="""
        -- Incremental backup with timestamp
        DROP TABLE IF EXISTS public.sales_fact_backup;
        CREATE TABLE public.sales_fact_backup AS
        SELECT *, NOW() as backup_timestamp
        FROM public.sales_fact;
        
        -- Prepare staging
        TRUNCATE TABLE staging.sales_staging;
        
        -- Mark load start in control
        INSERT INTO control.etl_status (
            table_name, status, start_time
        ) VALUES (
            'sales_fact', 'LOADING', NOW()
        )
        ON CONFLICT (table_name) DO UPDATE
        SET status = 'LOADING', start_time = NOW();
        
        -- Disable triggers for better performance
        ALTER TABLE public.sales_fact DISABLE TRIGGER ALL;
    """,
    
    if_exists="replace",
    timeout=600,  # 10 minutes
    
    # AFTER: Validate and log
    after_query="""
        -- Re-enable triggers
        ALTER TABLE public.sales_fact ENABLE TRIGGER ALL;
        
        -- Validate loaded data
        DO $$
        DECLARE
            invalid_count INTEGER;
        BEGIN
            SELECT COUNT(*) INTO invalid_count
            FROM public.sales_fact
            WHERE amount < 0 OR customer_id IS NULL;
            
            IF invalid_count > 0 THEN
                RAISE EXCEPTION 'Found % invalid records', invalid_count;
            END IF;
        END $$;
        
        -- Update aggregates
        REFRESH MATERIALIZED VIEW CONCURRENTLY reports.sales_by_region;
        REFRESH MATERIALIZED VIEW CONCURRENTLY reports.sales_by_product;
        
        -- Log success
        INSERT INTO audit.etl_runs (
            pipeline_name,
            table_name,
            run_timestamp,
            records_loaded,
            status,
            duration_seconds
        ) VALUES (
            'sales_etl',
            'sales_fact',
            NOW(),
            (SELECT COUNT(*) FROM public.sales_fact),
            'SUCCESS',
            EXTRACT(EPOCH FROM (NOW() - (
                SELECT start_time FROM control.etl_status 
                WHERE table_name = 'sales_fact'
            )))
        );
        
        -- Update control
        UPDATE control.etl_status
        SET 
            status = 'COMPLETED',
            end_time = NOW(),
            record_count = (SELECT COUNT(*) FROM public.sales_fact)
        WHERE table_name = 'sales_fact';
        
        -- VACUUM ANALYZE to optimize
        VACUUM ANALYZE public.sales_fact;
    """
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Example 7: With Timeout
```python
# Time control for large loads
destination = PostgresDestination(
    name="large_load",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    table="big_table",
    schema="public",
    if_exists="append",
    timeout=1800  # 30 minutes maximum
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Example 8: Load to Non-Public Schema
```python
# Load data to specific schema
destination = PostgresDestination(
    name="analytics_load",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    table="monthly_summary",
    schema="analytics",  # Specific schema
    if_exists="append"
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Example 9: With Pre and Post Validations
```python
# Validate before and after loading
destination = PostgresDestination(
    name="validated_load",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    table="products",
    schema="public",
    
    before_query="""
        -- Validate available space
        DO $$
        DECLARE
            free_space BIGINT;
        BEGIN
            SELECT pg_database_size(current_database()) INTO free_space;
            -- Custom validations here
        END $$;
        
        -- Verify destination table exists
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'products'
            ) THEN
                RAISE EXCEPTION 'Table public.products does not exist';
            END IF;
        END $$;
    """,
    
    if_exists="append",
    
    after_query="""
        -- Validate referential integrity
        DO $$
        DECLARE
            orphan_count INTEGER;
        BEGIN
            SELECT COUNT(*) INTO orphan_count
            FROM public.products p
            WHERE NOT EXISTS (
                SELECT 1 FROM public.categories c 
                WHERE c.id = p.category_id
            );
            
            IF orphan_count > 0 THEN
                RAISE WARNING 'Found % products with invalid category', orphan_count;
            END IF;
        END $$;
        
        -- Verify duplicates
        DO $$
        DECLARE
            dup_count INTEGER;
        BEGIN
            SELECT COUNT(*) - COUNT(DISTINCT sku) INTO dup_count
            FROM public.products;
            
            IF dup_count > 0 THEN
                RAISE EXCEPTION 'Found % duplicate SKUs', dup_count;
            END IF;
        END $$;
    """
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Example 10: Load with Optimized Indexes
```python
# Optimize load by removing/recreating indexes
destination = PostgresDestination(
    name="optimized_load",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    table="transactions",
    schema="public",
    
    before_query="""
        -- Save index definitions
        CREATE TEMP TABLE temp_indexes AS
        SELECT indexdef
        FROM pg_indexes
        WHERE schemaname = 'public' AND tablename = 'transactions';
        
        -- Drop indexes for faster loading
        DROP INDEX IF EXISTS idx_transactions_date;
        DROP INDEX IF EXISTS idx_transactions_customer;
        DROP INDEX IF EXISTS idx_transactions_amount;
    """,
    
    if_exists="append",
    
    after_query="""
        -- Recreate indexes
        CREATE INDEX idx_transactions_date 
            ON public.transactions(transaction_date);
        CREATE INDEX idx_transactions_customer 
            ON public.transactions(customer_id);
        CREATE INDEX idx_transactions_amount 
            ON public.transactions(amount) WHERE amount > 1000;
        
        -- ANALYZE after creating indexes
        ANALYZE public.transactions;
    """
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

## 📊 Example Output
```
PostgresDestination 'sales_etl' received data from pipe: 'pipe1'
PostgresDestination 'sales_etl' engine initialized successfully
Connection: postgres@localhost:5432/warehouse
  - Timeout: 600.0s

PostgresDestination 'sales_etl' executing before_query...
  Query preview: -- Incremental backup with timestamp
        DROP TABLE IF EXISTS public.sales_fact_backup;...
✅ PostgresDestination 'sales_etl' before_query executed successfully
  - Rows affected: 15,432
  - Duration: 3.12s

======================================================================
PostgresDestination 'sales_etl' loading data to PostgreSQL...
======================================================================
  - Database: warehouse
  - Schema: public
  - Table: sales_fact
  - Full table reference: public.sales_fact
  - If exists: replace
  - DataFrame shape: (15432, 12)
  - DataFrame columns: ['sale_id', 'customer_id', 'product_id', 'amount', ...]

PostgresDestination 'sales_etl' starting load operation...

======================================================================
PostgresDestination 'sales_etl' LOAD completed successfully:
======================================================================
  📊 Load results:
     - Rows loaded: 15,432
     - Columns loaded: 12
     - Table: public.sales_fact
  
  ⏱️  Load info:
     - Duration: 4.56s
     - Write mode: replace
  
  📋 Data types:
     - sale_id: int64
     - customer_id: int64
     - product_id: int64
     - amount: float64
     - sale_date: object
     - region: object
     - ...

PostgresDestination 'sales_etl' executing after_query...
  Query preview: -- Re-enable triggers
        ALTER TABLE public.sales_fact ENABLE TRIGGER ALL;...
✅ PostgresDestination 'sales_etl' after_query executed successfully
  - Rows affected: 3
  - Duration: 2.34s

======================================================================
✅ PostgresDestination 'sales_etl' completed successfully
======================================================================
PostgresDestination 'sales_etl' connection closed
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
| `table` | str | ✅ | - | Table name |
| `schema` | str | ❌ | 'public' | PostgreSQL schema |
| `if_exists` | str | ❌ | 'append' | Write mode |
| `before_query` | str | ❌ | None | Query to execute BEFORE |
| `after_query` | str | ❌ | None | Query to execute AFTER |
| `timeout` | float | ❌ | None | Timeout in seconds |

---

## 🔧 if_exists Values

| Value | Behavior |
|-------|----------|
| `append` | Append data to existing table (default) |
| `replace` | Drop and recreate table with new data |
| `fail` | Error if table already exists |

---

## ✅ Best Practices

1. **Use `append`** for incremental loads
2. **Use `replace`** for complete daily replacements
3. **Use `before_query`** to create backups before loading
4. **Use `after_query`** for validations and auditing
5. **Disable triggers** in `before_query` for better performance
6. **Enable triggers** in `after_query` after loading
7. **Execute `ANALYZE`** in `after_query` to update statistics
8. **Use `VACUUM`** after large loads
9. **Specify `timeout`** for large loads
10. **Validate data** in `after_query` before confirming success
11. **Drop indexes** before large loads and recreate after
12. **Use transactions** in before/after queries when necessary

---

## ⚠️ Important Considerations

### Performance
- Load uses `chunksize=1000` to process in batches
- Uses `method='multi'` for optimized INSERTs
- Consider disabling triggers on large loads
- Drop indexes before loading and recreate after
- Execute `VACUUM ANALYZE` after large loads

### Schemas
- Default schema is `public`
- Specify schema explicitly if using other schemas
- User must have permissions on destination schema

### Write Modes
- `append`: Faster, adds data
- `replace`: Drops entire table and recreates (DROP + CREATE)
- `fail`: Useful for initial loads (prevents overwriting)

### Timeout
- Applies to both initial connection and queries
- Useful for very large loads
- If operation exceeds timeout, raises exception

### Transactions
- Each operation (before, load, after) uses its own transaction
- If `after_query` fails, data is ALREADY loaded
- Use explicit transactions in before/after if you need rollback

### Permissions
- User needs CREATE/INSERT/UPDATE/DELETE permissions
- For `replace` needs DROP TABLE permissions
- For non-public schemas, needs permissions on that schema

---

## 🔍 Troubleshooting

### Error: "Table already exists"
```python
# Solution 1: Use append
if_exists="append"

# Solution 2: Use replace
if_exists="replace"

# Solution 3: Drop table in before_query
before_query="DROP TABLE IF EXISTS schema.table;"
```

### Error: "Permission denied for schema"
```sql
-- Verify permissions
SELECT has_schema_privilege('username', 'schema_name', 'CREATE');

-- Grant permissions
GRANT CREATE, USAGE ON SCHEMA schema_name TO username;
```

### Error: "Timeout exceeded"
```python
# Increase timeout
timeout=1800  # 30 minutes

# Or split load into smaller chunks
```

### Very Slow Load
```python
# Optimize with before_query and after_query
before_query="""
    -- Drop indexes
    DROP INDEX IF EXISTS idx_table_field;
    
    -- Disable triggers
    ALTER TABLE schema.table DISABLE TRIGGER ALL;
"""

after_query="""
    -- Recreate indexes
    CREATE INDEX idx_table_field ON schema.table(field);
    
    -- Enable triggers
    ALTER TABLE schema.table ENABLE TRIGGER ALL;
    
    -- Update statistics
    ANALYZE schema.table;
"""
```

---

## 🔗 See Also

- [PostgresOrigin](./PostgresOrigin.md) - For extracting from PostgreSQL
- [Open-Stage Documentation](../README.md) - Complete documentation

---

**Open-Stage v2.4** - Enterprise ETL Framework