# MySQLOrigin - Usage Guide

Component for extracting data from MySQL with advanced capabilities.

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
pip install sqlalchemy pymysql pandas
```

---

## 🚀 Basic Usage

### Example 1: Simple Query
```python
from src.mysql.common import MySQLOrigin
from src.core.base import Pipe
from src.core.common import Printer

# Create origin with query
origin = MySQLOrigin(
    name="sales_data",
    host="localhost",
    database="warehouse",
    user="root",
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
origin = MySQLOrigin(
    name="customers",
    host="localhost",
    database="crm",
    user="root",
    password="password",
    table="customers"  # ✨ Simpler!
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

---

### Example 3: Limit for Testing
```python
# Extract only 100 rows for testing
origin = MySQLOrigin(
    name="sales_sample",
    host="localhost",
    database="warehouse",
    user="root",
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
origin = MySQLOrigin(
    name="processed_orders",
    host="localhost",
    database="warehouse",
    user="root",
    password="password",
    before_query="""
        -- Create temporary table with filtered data
        CREATE TEMPORARY TABLE temp_orders AS
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
origin = MySQLOrigin(
    name="customer_extract",
    host="localhost",
    database="crm",
    user="root",
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
origin = MySQLOrigin(
    name="daily_sales_etl",
    host="localhost",
    database="warehouse",
    user="root",
    password="password",
    
    # BEFORE: Prepare staging
    before_query="""
        -- Create staging table
        DROP TEMPORARY TABLE IF EXISTS staging_daily_sales;
        
        CREATE TEMPORARY TABLE staging_daily_sales AS
        SELECT 
            DATE(order_timestamp) as sale_date,
            product_id,
            customer_id,
            amount,
            region
        FROM raw_orders
        WHERE DATE(order_timestamp) = CURDATE()
        AND status = 'completed';
        
        -- Validate data
        SET @row_count = (SELECT COUNT(*) FROM staging_daily_sales);
        
        -- Create index
        CREATE INDEX idx_staging_sales_date 
            ON staging_daily_sales(sale_date);
    """,
    
    # MAIN QUERY
    query="SELECT * FROM staging_daily_sales ORDER BY sale_date, customer_id",
    
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
            (SELECT COUNT(*) FROM staging_daily_sales),
            'SUCCESS'
        );
        
        -- Update metadata
        UPDATE control.table_metadata
        SET last_extraction = NOW()
        WHERE table_name = 'daily_sales';
        
        -- Clean old temporary tables
        DROP TEMPORARY TABLE IF EXISTS temp_processing;
    """
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

---

### Example 7: Parameterized Queries
```python
# Secure query with parameters (prevents SQL injection)
origin = MySQLOrigin(
    name="filtered_sales",
    host="localhost",
    database="warehouse",
    user="root",
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
origin = MySQLOrigin(
    name="large_extract",
    host="localhost",
    database="warehouse",
    user="root",
    password="password",
    query="SELECT * FROM huge_table WHERE date >= '2024-01-01'",
    timeout=300  # 5 minutes maximum
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

---

### Example 9: Reading with Database Prefix
```python
# Specify database when reading cross-database
origin = MySQLOrigin(
    name="cross_db_data",
    host="localhost",
    database="warehouse",  # Default database for connection
    user="root",
    password="password",
    table="analytics.monthly_summary"  # database.table format
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

---

### Example 10: With Session Variables
```python
# Configure session variables before extraction
origin = MySQLOrigin(
    name="custom_config",
    host="localhost",
    database="warehouse",
    user="root",
    password="password",
    before_query="""
        -- Configure session variables
        SET SESSION sql_mode = 'STRICT_TRANS_TABLES';
        SET SESSION max_execution_time = 300000;  -- 5 minutes in milliseconds
        
        -- Create temporary table
        CREATE TEMPORARY TABLE filtered_data AS
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
MySQLOrigin 'daily_sales_etl' engine initialized successfully
Connection: root@localhost:3306/warehouse
  - Timeout: 300.0s

MySQLOrigin 'daily_sales_etl' executing before_query...
  Query preview: -- Create staging table
        DROP TEMPORARY TABLE IF EXISTS staging_daily_sales;...
✅ MySQLOrigin 'daily_sales_etl' before_query executed successfully
  - Rows affected: 5,432
  - Duration: 2.34s

======================================================================
MySQLOrigin 'daily_sales_etl' executing MAIN extraction query...
======================================================================
  - Database: warehouse
  - Query: SELECT * FROM staging_daily_sales ORDER BY sale_date, customer_id
  - Parameters: ['start_date', 'region']

======================================================================
MySQLOrigin 'daily_sales_etl' MAIN query results:
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

MySQLOrigin 'daily_sales_etl' executing after_query...
  Query preview: -- Log execution
        INSERT INTO audit.etl_runs (...
✅ MySQLOrigin 'daily_sales_etl' after_query executed successfully
  - Rows affected: 1
  - Duration: 0.45s

======================================================================
✅ MySQLOrigin 'daily_sales_etl' pumped data through pipe 'pipe1'
======================================================================
MySQLOrigin 'daily_sales_etl' connection closed
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
| `query` | str | * | None | SQL query to execute |
| `table` | str | * | None | Table in format `table` or `database.table` |
| `before_query` | str | ❌ | None | Query to execute BEFORE |
| `after_query` | str | ❌ | None | Query to execute AFTER |
| `max_results` | int | ❌ | None | Row limit to return |
| `timeout` | float | ❌ | None | Timeout in seconds |
| `query_parameters` | dict | ❌ | {} | Query parameters |

\* **Note**: You must provide `query` OR `table`, but not both.

---

## 📝 Supported Table Formats

```python
# Format 1: Table name only
table="customers"
# Generates: SELECT * FROM `customers`

# Format 2: Explicit database
table="warehouse.customers"
# Generates: SELECT * FROM `warehouse`.`customers`

# Format 3: Cross-database query
table="analytics.sales_summary"
# Generates: SELECT * FROM `analytics`.`sales_summary`
```

---

## ✅ Best Practices

1. **Use `table`** when you only need `SELECT *` (simpler)
2. **Use `max_results`** in development for quick testing
3. **Use `before_query`** to prepare data and staging
4. **Use `after_query`** for auditing and cleanup
5. **Use `query_parameters`** instead of string concatenation (security)
6. **Specify `timeout`** for long queries
7. **Use TEMPORARY tables** in `before_query` for complex transformations
8. **Index temporary tables** if you'll filter/sort on them
9. **Clean resources** in `after_query` (DROP TEMPORARY TABLES)
10. **Use transactions** when necessary in before/after queries

---

## ⚠️ Important Considerations

### Temporary Tables
- TEMPORARY tables are automatically deleted when connection closes
- Use `CREATE TEMPORARY TABLE` for intermediate data
- Only visible to current session
- MySQL syntax: `DROP TEMPORARY TABLE IF EXISTS table_name;`

### Query Parameters
- Use `:param_name` in query
- Provide values in `query_parameters` as dictionary
- Prevents SQL injection using parameters

### Timeout
- Applies to both initial connection and queries
- Useful for long queries or to avoid locks
- If query exceeds timeout, raises exception
- MySQL uses `connect_timeout` in connection string

### Performance
- Use `ANALYZE TABLE` after large loads in `after_query`
- Use `EXPLAIN` in development to optimize
- Consider temporary indexes in staging

### MySQL-Specific Features
- Use `CURDATE()` instead of `CURRENT_DATE`
- Use `NOW()` instead of `CURRENT_TIMESTAMP()` (both work, but NOW() is more common)
- TEMPORARY tables don't need schema prefix
- Session variables: `SET SESSION variable = value;`

### Cross-Database Queries
- Can reference tables from different databases: `database.table`
- User must have permissions on all referenced databases
- Connection is established to the database specified in `database` parameter

---

## 🔍 Troubleshooting

### Error: "Can't connect to MySQL server"
```python
# Verify MySQL is running
# Check host and port are correct
# Check firewall rules
```

### Error: "Access denied for user"
```python
# Verify username and password
# Check user permissions:
mysql> SHOW GRANTS FOR 'username'@'host';

# Grant permissions if needed:
mysql> GRANT SELECT ON database.* TO 'username'@'host';
```

### Error: "Unknown database"
```python
# Check database exists:
mysql> SHOW DATABASES;

# Create database if needed:
mysql> CREATE DATABASE database_name;
```

### Error: "Table doesn't exist"
```python
# Check table exists:
mysql> SHOW TABLES FROM database_name;

# Check exact table name (MySQL is case-sensitive on Linux)
```

### Error: "Query timeout"
```python
# Increase timeout
timeout=600  # 10 minutes

# Or optimize query with indexes
# Or split into smaller chunks
```

### Very Slow Query
```python
# Use EXPLAIN to analyze
before_query="EXPLAIN SELECT * FROM large_table WHERE condition;"

# Create indexes in before_query
before_query="""
    CREATE INDEX idx_temp_field ON temp_table(field);
    ANALYZE TABLE temp_table;
"""
```

---

## 📗 MySQL vs PostgreSQL Differences

| Feature | MySQL | PostgreSQL |
|---------|-------|------------|
| **Temporary tables** | `CREATE TEMPORARY TABLE` | `CREATE TEMP TABLE` |
| **Current date** | `CURDATE()` | `CURRENT_DATE` |
| **Current timestamp** | `NOW()` | `NOW()` or `CURRENT_TIMESTAMP` |
| **Table quoting** | Backticks \`table\` | Double quotes "table" |
| **Auto-increment** | `AUTO_INCREMENT` | `SERIAL` |
| **String concat** | `CONCAT()` | `\|\|` or `CONCAT()` |
| **Timeout param** | `connect_timeout` | `connect_timeout` |
| **Schema support** | Uses databases | Uses schemas within DB |

---

## 📗 See Also

- [MySQLDestination](./MySQLDestination.md) - For writing to MySQL
- [PostgresOrigin](./PostgresOrigin.md) - PostgreSQL equivalent
- [Open-Stage Documentation](../README.md) - Complete documentation

---

**Open-Stage v2.4** - Enterprise ETL Framework