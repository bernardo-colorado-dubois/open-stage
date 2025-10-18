# MySQLDestination - Usage Guide

Component for loading data to MySQL with advanced capabilities.

---

## 🎯 Features

- ✅ DataFrame loading to MySQL
- ✅ Pre and post load queries (`before_query`, `after_query`)
- ✅ Write disposition control (FAIL, REPLACE, APPEND)
- ✅ Configurable timeout
- ✅ Optimized loading with chunks
- ✅ Detailed logging with statistics

---

## 📦 Installation
```bash
pip install sqlalchemy pymysql pandas
```

---

## 🚀 Basic Usage

### Example 1: Simple Load (APPEND)
```python
from src.mysql.common import MySQLDestination
from src.core.base import Pipe
from src.core.common import CSVOrigin

# Read data
origin = CSVOrigin("reader", filepath_or_buffer="sales.csv")

# MySQL destination
destination = MySQLDestination(
    name="sales_loader",
    host="localhost",
    database="warehouse",
    user="root",
    password="password",
    table="sales",
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
destination = MySQLDestination(
    name="daily_report",
    host="localhost",
    database="warehouse",
    user="root",
    password="password",
    table="daily_summary",
    if_exists="replace"  # ✨ Replace entire table
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Example 3: Only if Table Doesn't Exist (FAIL)
```python
# Fail if table already exists (initial load)
destination = MySQLDestination(
    name="initial_load",
    host="localhost",
    database="warehouse",
    user="root",
    password="password",
    table="customers",
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
destination = MySQLDestination(
    name="staged_load",
    host="localhost",
    database="warehouse",
    user="root",
    password="password",
    table="orders",
    before_query="""
        -- Create backup before loading
        DROP TABLE IF EXISTS orders_backup;
        CREATE TABLE orders_backup AS
        SELECT * FROM orders;
        
        -- Truncate staging table
        TRUNCATE TABLE staging_orders;
        
        -- Prepare auto_increment
        ALTER TABLE orders AUTO_INCREMENT = 1;
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
- Reset AUTO_INCREMENT
- Temporarily disable triggers
- Create indexes that will be needed

---

### Example 5: After Query (Auditing and Post-processing)
```python
# Execute query AFTER loading data
destination = MySQLDestination(
    name="customer_loader",
    host="localhost",
    database="crm",
    user="root",
    password="password",
    table="customers",
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
            (SELECT COUNT(*) FROM customers),
            'open-stage-pipeline'
        );
        
        -- Update metadata
        UPDATE control.table_metadata
        SET 
            last_updated = NOW(),
            row_count = (SELECT COUNT(*) FROM customers)
        WHERE table_name = 'customers';
        
        -- Analyze for statistics
        ANALYZE TABLE customers;
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
- ANALYZE to update statistics
- Create additional indexes
- Re-enable triggers
- Update summary tables

---

### Example 6: Complete Workflow (Before + After)
```python
# Complete pipeline with preparation and post-processing
destination = MySQLDestination(
    name="sales_etl",
    host="localhost",
    database="warehouse",
    user="root",
    password="password",
    table="sales_fact",
    
    # BEFORE: Prepare
    before_query="""
        -- Incremental backup with timestamp
        DROP TABLE IF EXISTS sales_fact_backup;
        CREATE TABLE sales_fact_backup AS
        SELECT *, NOW() as backup_timestamp
        FROM sales_fact;
        
        -- Prepare staging
        TRUNCATE TABLE staging_sales;
        
        -- Mark load start in control
        INSERT INTO control.etl_status (
            table_name, status, start_time
        ) VALUES (
            'sales_fact', 'LOADING', NOW()
        )
        ON DUPLICATE KEY UPDATE
            status = 'LOADING',
            start_time = NOW();
        
        -- Disable triggers for better performance
        SET @DISABLE_TRIGGERS = 1;
    """,
    
    if_exists="replace",
    timeout=600,  # 10 minutes
    
    # AFTER: Validate and log
    after_query="""
        -- Re-enable triggers
        SET @DISABLE_TRIGGERS = 0;
        
        -- Validate loaded data
        SET @invalid_count = (
            SELECT COUNT(*) FROM sales_fact
            WHERE amount < 0 OR customer_id IS NULL
        );
        
        -- Update aggregates
        CALL refresh_sales_aggregates();
        
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
            (SELECT COUNT(*) FROM sales_fact),
            'SUCCESS',
            TIMESTAMPDIFF(SECOND, (
                SELECT start_time FROM control.etl_status 
                WHERE table_name = 'sales_fact'
            ), NOW())
        );
        
        -- Update control
        UPDATE control.etl_status
        SET 
            status = 'COMPLETED',
            end_time = NOW(),
            record_count = (SELECT COUNT(*) FROM sales_fact)
        WHERE table_name = 'sales_fact';
        
        -- Optimize table
        OPTIMIZE TABLE sales_fact;
        ANALYZE TABLE sales_fact;
    """
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Example 7: With Timeout
```python
# Time control for large loads
destination = MySQLDestination(
    name="large_load",
    host="localhost",
    database="warehouse",
    user="root",
    password="password",
    table="big_table",
    if_exists="append",
    timeout=1800  # 30 minutes maximum
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Example 8: With Pre and Post Validations
```python
# Validate before and after loading
destination = MySQLDestination(
    name="validated_load",
    host="localhost",
    database="warehouse",
    user="root",
    password="password",
    table="products",
    
    before_query="""
        -- Validate available space
        SET @free_space = (
            SELECT SUM(data_free) 
            FROM information_schema.tables 
            WHERE table_schema = 'warehouse'
        );
        
        -- Verify destination table exists
        SET @table_exists = (
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'warehouse' 
            AND table_name = 'products'
        );
        
        -- Check if table exists
        SELECT IF(@table_exists = 0, 
            'Table does not exist', 
            'Table exists') as validation;
    """,
    
    if_exists="append",
    
    after_query="""
        -- Validate referential integrity
        SET @orphan_count = (
            SELECT COUNT(*) FROM products p
            WHERE NOT EXISTS (
                SELECT 1 FROM categories c 
                WHERE c.id = p.category_id
            )
        );
        
        -- Verify duplicates
        SET @dup_count = (
            SELECT COUNT(*) - COUNT(DISTINCT sku) 
            FROM products
        );
        
        -- Log validation results
        INSERT INTO audit.validation_log (
            table_name,
            validated_at,
            orphan_records,
            duplicate_records
        ) VALUES (
            'products',
            NOW(),
            @orphan_count,
            @dup_count
        );
    """
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Example 9: Load with Optimized Indexes
```python
# Optimize load by removing/recreating indexes
destination = MySQLDestination(
    name="optimized_load",
    host="localhost",
    database="warehouse",
    user="root",
    password="password",
    table="transactions",
    
    before_query="""
        -- Save index definitions to temporary table
        CREATE TEMPORARY TABLE temp_indexes AS
        SELECT 
            table_name,
            index_name,
            GROUP_CONCAT(column_name ORDER BY seq_in_index) as columns
        FROM information_schema.statistics
        WHERE table_schema = 'warehouse' 
        AND table_name = 'transactions'
        AND index_name != 'PRIMARY'
        GROUP BY table_name, index_name;
        
        -- Drop indexes for faster loading
        ALTER TABLE transactions 
            DROP INDEX idx_transactions_date,
            DROP INDEX idx_transactions_customer,
            DROP INDEX idx_transactions_amount;
    """,
    
    if_exists="append",
    
    after_query="""
        -- Recreate indexes
        ALTER TABLE transactions
            ADD INDEX idx_transactions_date (transaction_date),
            ADD INDEX idx_transactions_customer (customer_id),
            ADD INDEX idx_transactions_amount (amount);
        
        -- Optimize and analyze
        OPTIMIZE TABLE transactions;
        ANALYZE TABLE transactions;
    """
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Example 10: Load with Partitioning
```python
# Prepare partitioned table before loading
destination = MySQLDestination(
    name="partitioned_load",
    host="localhost",
    database="warehouse",
    user="root",
    password="password",
    table="sales_partitioned",
    
    before_query="""
        -- Create partitioned table if not exists
        CREATE TABLE IF NOT EXISTS sales_partitioned (
            id INT AUTO_INCREMENT,
            sale_date DATE NOT NULL,
            amount DECIMAL(10,2),
            customer_id INT,
            PRIMARY KEY (id, sale_date)
        )
        PARTITION BY RANGE (YEAR(sale_date)) (
            PARTITION p2023 VALUES LESS THAN (2024),
            PARTITION p2024 VALUES LESS THAN (2025),
            PARTITION p2025 VALUES LESS THAN (2026),
            PARTITION p_future VALUES LESS THAN MAXVALUE
        );
    """,
    
    if_exists="append",
    
    after_query="""
        -- Analyze partitions
        ANALYZE TABLE sales_partitioned;
        
        -- Log partition info
        INSERT INTO audit.partition_log
        SELECT 
            'sales_partitioned',
            partition_name,
            table_rows,
            NOW()
        FROM information_schema.partitions
        WHERE table_schema = 'warehouse'
        AND table_name = 'sales_partitioned';
    """
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

## 📊 Example Output
```
MySQLDestination 'sales_etl' received data from pipe: 'pipe1'
MySQLDestination 'sales_etl' engine initialized successfully
Connection: root@localhost:3306/warehouse
  - Timeout: 600.0s

MySQLDestination 'sales_etl' executing before_query...
  Query preview: -- Incremental backup with timestamp
        DROP TABLE IF EXISTS sales_fact_backup;...
✅ MySQLDestination 'sales_etl' before_query executed successfully
  - Rows affected: 15,432
  - Duration: 3.12s

======================================================================
MySQLDestination 'sales_etl' loading data to MySQL...
======================================================================
  - Database: warehouse
  - Table: sales_fact
  - If exists: replace
  - DataFrame shape: (15432, 12)
  - DataFrame columns: ['sale_id', 'customer_id', 'product_id', 'amount', ...]

MySQLDestination 'sales_etl' starting load operation...

======================================================================
MySQLDestination 'sales_etl' LOAD completed successfully:
======================================================================
  📊 Load results:
     - Rows loaded: 15,432
     - Columns loaded: 12
     - Table: sales_fact
  
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

MySQLDestination 'sales_etl' executing after_query...
  Query preview: -- Re-enable triggers
        SET @DISABLE_TRIGGERS = 0;...
✅ MySQLDestination 'sales_etl' after_query executed successfully
  - Rows affected: 3
  - Duration: 2.34s

======================================================================
✅ MySQLDestination 'sales_etl' completed successfully
======================================================================
MySQLDestination 'sales_etl' connection closed
```

---

## 📋 Complete Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | str | ✅ | - | Component name |
| `host` | str | ✅ | - | MySQL host |
| `port` | int | ❌ | 3306 | MySQL port |
| `database` | str | ✅ | - | Database name |
| `user` | str | ✅ | - | MySQL username |
| `password` | str | ✅ | - | Password |
| `table` | str | ✅ | - | Table name |
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
7. **Execute `ANALYZE TABLE`** in `after_query` to update statistics
8. **Use `OPTIMIZE TABLE`** after large loads
9. **Specify `timeout`** for large loads
10. **Validate data** in `after_query` before confirming success
11. **Drop indexes** before large loads and recreate after
12. **Use transactions** in before/after queries when necessary
13. **Consider partitioning** for very large tables

---

## ⚠️ Important Considerations

### Performance
- Load uses `chunksize=1000` to process in batches
- Consider disabling indexes on large loads
- Drop indexes before loading and recreate after
- Execute `OPTIMIZE TABLE` after large loads
- Use `ANALYZE TABLE` to update statistics

### Write Modes
- `append`: Faster, adds data
- `replace`: Drops entire table and recreates (DROP + CREATE)
- `fail`: Useful for initial loads (prevents overwriting)

### Timeout
- Applies to both initial connection and queries
- Useful for very large loads
- If operation exceeds timeout, raises exception
- MySQL uses `connect_timeout` parameter

### Transactions
- Each operation (before, load, after) uses its own transaction
- If `after_query` fails, data is ALREADY loaded
- Use explicit transactions in before/after if you need rollback

### Permissions
- User needs CREATE/INSERT/UPDATE/DELETE permissions
- For `replace` needs DROP TABLE permissions
- Check permissions: `SHOW GRANTS FOR 'username'@'host';`

### MySQL-Specific Features
- Use `AUTO_INCREMENT` for auto-incrementing primary keys
- Use `TRUNCATE TABLE` instead of `DELETE FROM` for performance
- Use `OPTIMIZE TABLE` to reclaim space after large deletes
- Use `ANALYZE TABLE` to update table statistics
- Session variables: `SET @variable = value;`

---

## 🔍 Troubleshooting

### Error: "Table already exists"
```python
# Solution 1: Use append
if_exists="append"

# Solution 2: Use replace
if_exists="replace"

# Solution 3: Drop table in before_query
before_query="DROP TABLE IF EXISTS table_name;"
```

### Error: "Access denied"
```sql
-- Verify permissions
SHOW GRANTS FOR 'username'@'host';

-- Grant permissions
GRANT CREATE, INSERT, UPDATE, DELETE ON database.* TO 'username'@'host';
FLUSH PRIVILEGES;
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
    ALTER TABLE table_name DROP INDEX idx_name;
    
    -- Disable foreign key checks
    SET FOREIGN_KEY_CHECKS = 0;
"""

after_query="""
    -- Recreate indexes
    ALTER TABLE table_name ADD INDEX idx_name(field);
    
    -- Enable foreign key checks
    SET FOREIGN_KEY_CHECKS = 1;
    
    -- Update statistics
    ANALYZE TABLE table_name;
    OPTIMIZE TABLE table_name;
"""
```

### Error: "Packet too large"
```sql
-- Check max_allowed_packet
SHOW VARIABLES LIKE 'max_allowed_packet';

-- Increase in MySQL config or session
SET GLOBAL max_allowed_packet = 67108864;  -- 64MB
```

### Error: "Deadlock found"
```python
# Use timeout and retry logic
# Or execute during low-traffic periods
# Or lock tables in before_query
before_query="""
    LOCK TABLES table_name WRITE;
"""

after_query="""
    UNLOCK TABLES;
"""
```

---

## 📗 MySQL vs PostgreSQL Differences

| Feature | MySQL | PostgreSQL |
|---------|-------|------------|
| **Write modes** | fail, replace, append | fail, replace, append |
| **Truncate** | `TRUNCATE TABLE` | `TRUNCATE TABLE` |
| **Analyze** | `ANALYZE TABLE` | `ANALYZE table` |
| **Optimize** | `OPTIMIZE TABLE` | `VACUUM` |
| **Auto-increment** | `AUTO_INCREMENT` | `SERIAL` |
| **Insert method** | Basic chunks | Multi-insert |
| **Timeout param** | `connect_timeout` | `connect_timeout` |
| **Lock tables** | `LOCK TABLES` | `LOCK TABLE` |
| **Temp tables** | `TEMPORARY` keyword | `TEMP` keyword |

---

## 📗 See Also

- [MySQLOrigin](./MySQLOrigin.md) - For extracting from MySQL
- [PostgresDestination](./PostgresDestination.md) - PostgreSQL equivalent
- [Open-Stage Documentation](../README.md) - Complete documentation

---

**Open-Stage v2.4** - Enterprise ETL Framework