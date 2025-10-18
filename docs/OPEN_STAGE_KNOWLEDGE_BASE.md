# Open-Stage - Complete Knowledge Base
## Version 2.4 - January 2025

---

# PROJECT OVERVIEW

**Open-Stage** is an enterprise-grade ETL (Extract, Transform, Load) platform built in Python, inspired by IBM DataStage. It implements a pipes and filters architecture that enables the creation of modular, scalable data processing pipelines with multi-model generative AI capabilities.

## Key Statistics
- **Total Components**: 29 (5 base + 24 specialized)
- **License**: MIT
- **Authors**: Bernardo Colorado Dubois, Saul Hernandez Cordova
- **Python Version**: 3.8+
- **Current Version**: 2.4
- **Code Style**: 2-space indentation

## Core Features
- 29 Modular Components (5 base + 24 specialized)
- Multiple Data Sources: CSV, MySQL, PostgreSQL, BigQuery, REST APIs, In-Memory DataFrames
- AI-Powered Transformations: OpenAI (GPT-4o, GPT-4-Turbo), Claude (Anthropic), Gemini (Google), DeepSeek
- Robust Validations and intelligent error handling
- Method Chaining for fluent syntax
- Extensible Architecture by provider and component type
- **Advanced BigQuery Support**: before_query, after_query, partitioning, clustering, dry_run
- **Advanced PostgreSQL Support**: before_query, after_query, timeout, query_parameters ✨
- **Advanced MySQL Support**: before_query, after_query, timeout, query_parameters ✨ NEW v2.4

---

# PROJECT STRUCTURE
```
project/
├── LICENSE                    # MIT License
├── README.md
├── requirements.txt
├── src/
│   ├── core/
│   │   ├── __init__.py
│   │   ├── base.py                    # Base classes (5)
│   │   │   ├── DataPackage           # Encapsulates data + metadata
│   │   │   ├── Pipe                  # Connects components
│   │   │   ├── Origin                # Abstract class for sources (0→1)
│   │   │   ├── Destination           # Abstract class for sinks (1→0)
│   │   │   └── Node                  # Abstract class for transformers
│   │   └── common.py                  # Generic components (15)
│   │       ├── Generator             # Origin: Sequential data
│   │       ├── CSVOrigin             # Origin: CSV files
│   │       ├── APIRestOrigin         # Origin: REST APIs
│   │       ├── OpenOrigin            # Origin: In-memory DataFrames
│   │       ├── Printer               # Destination: Console output
│   │       ├── CSVDestination        # Destination: CSV files
│   │       ├── Funnel                # Node/Router: N→1 combine
│   │       ├── Switcher              # Node/Router: 1→N conditional
│   │       ├── Copy                  # Node/Router: 1→N duplicate
│   │       ├── Filter                # Node/Transformer: Row filtering (9 ops)
│   │       ├── Aggregator            # Node/Transformer: Aggregations
│   │       ├── DeleteColumns         # Node/Transformer: Remove columns
│   │       ├── RemoveDuplicates      # Node/Transformer: Deduplication
│   │       ├── Joiner                # Node/Transformer: Joins (2→1)
│   │       └── Transformer           # Node/Transformer: Custom functions
│   ├── postgres/
│   │   ├── __init__.py
│   │   └── common.py                  # PostgreSQL connectors (2) ✨ ENHANCED v2.4
│   │       ├── PostgresOrigin        # Origin: PostgreSQL queries
│   │       └── PostgresDestination   # Destination: PostgreSQL tables
│   ├── mysql/
│   │   ├── __init__.py
│   │   └── common.py                  # MySQL connectors (2) ✨ ENHANCED v2.4
│   │       ├── MySQLOrigin           # Origin: MySQL queries
│   │       └── MySQLDestination      # Destination: MySQL tables
│   ├── google/
│   │   ├── __init__.py
│   │   ├── cloud.py                   # GCP BigQuery services (2)
│   │   │   ├── GCPBigQueryOrigin     # Origin: BigQuery queries ✨ ENHANCED
│   │   │   └── GCPBigQueryDestination # Destination: BigQuery load ✨ ENHANCED
│   │   └── gemini.py                  # Google AI (1)
│   │       └── GeminiPromptTransformer # Node: Gemini transformations
│   ├── anthropic/
│   │   ├── __init__.py
│   │   └── claude.py                  # Anthropic AI (1)
│   │       └── AnthropicPromptTransformer # Node: Claude transformations
│   ├── deepseek/
│   │   ├── __init__.py
│   │   └── deepseek.py                # DeepSeek AI (1)
│   │       └── DeepSeekPromptTransformer # Node: DeepSeek transformations
│   └── open_ai/
│       ├── __init__.py
│       └── chat_gpt.py                # OpenAI AI (1)
│           └── OpenAIPromptTransformer # Node: GPT transformations
```

---

# COMPONENTS CATALOG

## Origins (Data Sources) - 0→1

### MySQLOrigin ✨ ENHANCED v2.4
**Module**: `src/mysql/common.py`
**Purpose**: Queries MySQL databases with advanced features.

**Dependencies**: `sqlalchemy`, `pymysql`

**Parameters**:
- `name`: str - Component name
- `host`: str - MySQL host
- `port`: int = 3306 - MySQL port
- `database`: str - Database name
- `user`: str - Username
- `password`: str - Password
- `query`: str (optional) - SQL query to execute (required if table not provided)
- `table`: str (optional) - Table reference in format 'table' or 'database.table' (required if query not provided)
- **`before_query`: str = None** - ✨ SQL query to execute BEFORE extraction
- **`after_query`: str = None** - ✨ SQL query to execute AFTER extraction
- **`max_results`: int = None** - ✨ Maximum rows to return (adds LIMIT)
- **`timeout`: float = None** - ✨ Query timeout in seconds
- **`query_parameters`: dict = None** - ✨ Dictionary of query parameters for parameterized queries

**Connection String**: `mysql+pymysql://user:pass@host:port/database`

**Example**:
```python
mysql = MySQLOrigin(
  name="mysql_reader",
  host="localhost",
  database="company_db",
  user="root",
  password="password",
  query="SELECT * FROM customers WHERE active = 1"
)
```

**New Features v2.4**:
- **before_query**: Create temp tables, call procedures, prepare data before extraction
- **after_query**: Audit logging, cleanup, post-processing after extraction
- **table**: Direct table read without writing SELECT *
- **max_results**: Limit rows for testing without modifying query
- **timeout**: Control query execution time
- **query_parameters**: Secure parameterized queries using :param_name syntax
- **Enhanced logging**: Duration, rows returned, data types, detailed statistics

**Example with advanced features**:
```python
mysql = MySQLOrigin(
  name="daily_sales",
  host="localhost",
  database="warehouse",
  user="root",
  password="password",
  before_query="""
    CREATE TEMPORARY TABLE staging_sales AS
    SELECT * FROM raw_sales WHERE date = CURDATE();
  """,
  query="SELECT * FROM staging_sales WHERE amount > :min_amount",
  query_parameters={'min_amount': 100.0},
  max_results=1000,
  timeout=300,
  after_query="""
    INSERT INTO audit.extraction_log (table_name, extracted_at)
    VALUES ('staging_sales', NOW());
  """
)
```

**Validations**:
- host, database, user, password cannot be empty
- Must specify either query OR table (not both)
- port must be positive
- max_results must be positive if specified
- timeout must be positive if specified
- before_query and after_query cannot be empty strings if specified

**Error Handling**:
- Connection refused → Check if MySQL is running
- Authentication failed → Check credentials
- Database not found → Check if database exists
- SQL syntax errors → Shows problematic query
- Timeout errors → Shows configured timeout value

---

### PostgresOrigin ✨ ENHANCED v2.4
**Module**: `src/postgres/common.py`
**Purpose**: Queries PostgreSQL databases with advanced features.

**Dependencies**: `sqlalchemy`, `psycopg2-binary`

**Parameters**:
- `name`: str - Component name
- `host`: str - PostgreSQL host
- `port`: int = 5432 - PostgreSQL port
- `database`: str - Database name
- `user`: str - Username
- `password`: str - Password
- `query`: str (optional) - SQL query to execute (required if table not provided)
- `table`: str (optional) - Table reference in format 'table' or 'schema.table' (required if query not provided)
- **`before_query`: str = None** - ✨ SQL query to execute BEFORE extraction
- **`after_query`: str = None** - ✨ SQL query to execute AFTER extraction
- **`max_results`: int = None** - ✨ Maximum rows to return (adds LIMIT)
- **`timeout`: float = None** - ✨ Query timeout in seconds
- **`query_parameters`: dict = None** - ✨ Dictionary of query parameters for parameterized queries

**Connection String**: `postgresql+psycopg2://user:pass@host:port/database`

**Example**:
```python
pg = PostgresOrigin(
  name="pg_reader",
  host="localhost",
  database="analytics_db",
  user="postgres",
  password="password",
  query="SELECT * FROM sales WHERE date >= '2024-01-01'"
)
```

---

### GCPBigQueryOrigin ✨ ENHANCED
**Module**: `src/google/cloud.py`
**Purpose**: Queries Google BigQuery with advanced features.

**Dependencies**: `google-cloud-bigquery`, `google-auth`, `db-dtypes`

**Parameters**:
- `name`: str - Component name
- `project_id`: str - GCP project ID
- `query`: str (optional) - BigQuery SQL query (required if table not provided)
- `table`: str (optional) - Table reference in format 'dataset.table' or 'project.dataset.table'
- `credentials_path`: str = None - Path to service account JSON (optional)
- **`before_query`: str = None** - ✨ SQL query to execute BEFORE extraction
- **`after_query`: str = None** - ✨ SQL query to execute AFTER extraction
- **`max_results`: int = None** - ✨ Maximum rows to return (for testing)
- **`use_legacy_sql`: bool = False** - ✨ Use legacy SQL syntax
- **`query_parameters`: list = None** - ✨ Query parameters for parameterized queries
- **`location`: str = None** - ✨ BigQuery location (e.g., 'US', 'EU')
- **`job_labels`: dict = {}** - ✨ Labels for the BigQuery job
- **`timeout`: float = None** - ✨ Query timeout in seconds
- **`use_query_cache`: bool = True** - ✨ Use cached results if available
- **`dry_run`: bool = False** - ✨ Validate query and estimate cost without executing

---

## Destinations (Data Sinks) - 1→0

### MySQLDestination ✨ ENHANCED v2.4
**Module**: `src/mysql/common.py`
**Purpose**: Writes data to MySQL tables with advanced features.

**Dependencies**: `sqlalchemy`, `pymysql`

**Parameters**:
- `name`: str - Component name
- `host`: str - MySQL host
- `port`: int = 3306 - MySQL port
- `database`: str - Database name
- `user`: str - Username
- `password`: str - Password
- `table`: str - Table name
- `if_exists`: str = 'append' - Behavior if table exists
- **`before_query`: str = None** - ✨ SQL query to execute BEFORE loading data
- **`after_query`: str = None** - ✨ SQL query to execute AFTER loading data
- **`timeout`: float = None** - ✨ Query timeout in seconds

**if_exists options**:
- `'fail'` - Error if table exists
- `'replace'` - Drop and recreate table
- `'append'` - Add rows to existing table

**Example**:
```python
mysql_dest = MySQLDestination(
  name="mysql_writer",
  host="localhost",
  database="dulceria",
  user="root",
  password="password",
  table="compras_importantes",
  if_exists="append"
)
```

**New Features v2.4**:
- **before_query**: Create backups, truncate tables, prepare staging before load
- **after_query**: Audit logging, data validation, update statistics after load
- **timeout**: Control connection and query execution time
- **Enhanced logging**: Rows loaded, duration, data types, detailed statistics

**Example with advanced features**:
```python
mysql_dest = MySQLDestination(
  name="sales_loader",
  host="localhost",
  database="warehouse",
  user="root",
  password="password",
  table="sales_fact",
  before_query="""
    -- Create backup
    DROP TABLE IF EXISTS sales_fact_backup;
    CREATE TABLE sales_fact_backup AS SELECT * FROM sales_fact;
    
    -- Disable foreign key checks
    SET FOREIGN_KEY_CHECKS = 0;
  """,
  if_exists="replace",
  timeout=600,
  after_query="""
    -- Re-enable foreign key checks
    SET FOREIGN_KEY_CHECKS = 1;
    
    -- Update statistics
    ANALYZE TABLE sales_fact;
    
    -- Log the load
    INSERT INTO audit.load_log (table_name, loaded_at, record_count)
    VALUES ('sales_fact', NOW(), (SELECT COUNT(*) FROM sales_fact));
  """
)
```

**Optimizations**:
- Writes in chunks of 1000 rows
- No index included (index=False)

**Validations**:
- host, database, user, password, table cannot be empty
- port must be positive
- if_exists must be one of: 'fail', 'replace', 'append'
- before_query and after_query cannot be empty strings if specified
- timeout must be positive if specified

**Error Handling**:
- Connection refused → Check if MySQL is running
- Authentication failed → Check credentials
- Database not found → Check if database exists
- Table already exists → Use if_exists='replace' or 'append'
- Timeout errors → Shows configured timeout value

---

### PostgresDestination ✨ ENHANCED v2.4
**Module**: `src/postgres/common.py`
**Purpose**: Writes data to PostgreSQL tables with advanced features.

**Dependencies**: `sqlalchemy`, `psycopg2-binary`

**Parameters**:
- `name`: str - Component name
- `host`: str - PostgreSQL host
- `port`: int = 5432 - PostgreSQL port
- `database`: str - Database name
- `user`: str - Username
- `password`: str - Password
- `table`: str - Table name
- `schema`: str = 'public' - PostgreSQL schema
- `if_exists`: str = 'append' - Behavior if table exists
- **`before_query`: str = None** - ✨ SQL query to execute BEFORE loading data
- **`after_query`: str = None** - ✨ SQL query to execute AFTER loading data
- **`timeout`: float = None** - ✨ Query timeout in seconds

---

### GCPBigQueryDestination ✨ ENHANCED
**Module**: `src/google/cloud.py`
**Purpose**: Loads data to Google BigQuery with advanced features.

**Dependencies**: `google-cloud-bigquery`, `google-auth`

**Parameters**:
- `name`: str - Component name
- `project_id`: str - GCP project ID
- `dataset`: str - BigQuery dataset
- `table`: str - Table name
- `write_disposition`: str - Write mode
- `credentials_path`: str = None - Path to service account JSON (optional)
- **`before_query`: str = None** - ✨ SQL query to execute BEFORE loading
- **`after_query`: str = None** - ✨ SQL query to execute AFTER loading
- **`schema`: list = None** - ✨ BigQuery schema (auto-detected if None)
- **`create_disposition`: str = 'CREATE_IF_NEEDED'** - ✨ Table creation behavior
- **`schema_update_options`: list = []** - ✨ Schema update options
- **`clustering_fields`: list = None** - ✨ Fields for clustering (max 4)
- **`time_partitioning`: dict = None** - ✨ Time partitioning configuration
- **`location`: str = None** - ✨ BigQuery location
- **`job_labels`: dict = {}** - ✨ Labels for the BigQuery job
- **`max_bad_records`: int = 0** - ✨ Maximum bad records to tolerate
- **`autodetect`: bool = True** - ✨ Auto-detect schema from DataFrame

---

# DESIGN PRINCIPLES

1. **Pipes and Filters Architecture**: Modular, reusable components with clear separation of concerns
2. **Robust Validations**: Validation at construction time, data type validation, connectivity validation
3. **Error Handling**: Try-catch in critical operations, detailed error logging, graceful recovery
4. **Default Connectivity**: Origin (0→1), Destination (1→0), Node (flexible via override)
5. **Method Chaining**: Fluent syntax via `set_destination()` returning Node
6. **Lazy Initialization**: External clients initialized on demand
7. **Immediate Processing**: `sink()` automatically calls `pump()`
8. **Resource Cleanup**: DataFrames cleaned post-processing, connections closed appropriately
9. **2-Space Indentation**: Consistent code style throughout the project
10. **Enhanced Logging**: Structured logging with emojis (✅ ❌ 📊 ⏱️ 📋) for better readability ✨

---

# EXAMPLE PATTERNS

## Pattern 1: Simple ETL
```python
origin = CSVOrigin("reader", filepath_or_buffer="data.csv")
transform = Filter("filter", "age", ">=", 18)
destination = CSVDestination("writer", path_or_buf="output.csv", index=False)

pipe1, pipe2 = Pipe("p1"), Pipe("p2")

origin.add_output_pipe(pipe1).set_destination(transform)
transform.add_output_pipe(pipe2).set_destination(destination)

origin.pump()
```

## Pattern 2: MySQL with Advanced Features ✨ NEW v2.4
```python
# Extract with before_query
mysql_origin = MySQLOrigin(
  name="sales_extract",
  host="localhost",
  database="warehouse",
  user="root",
  password="password",
  before_query="""
    CREATE TEMPORARY TABLE staging AS
    SELECT * FROM raw_sales WHERE date = CURDATE();
  """,
  query="SELECT * FROM staging WHERE amount > :min_amount",
  query_parameters={'min_amount': 100.0},
  max_results=10000,
  timeout=300,
  after_query="""
    INSERT INTO audit.extraction_log (table_name, extracted_at)
    VALUES ('staging', NOW());
  """
)

# Transform
filter_node = Filter("high_value", "amount", ">", 1000)

# Load with before/after queries
mysql_dest = MySQLDestination(
  name="sales_load",
  host="localhost",
  database="warehouse",
  user="root",
  password="password",
  table="sales_fact",
  before_query="""
    -- Create backup
    DROP TABLE IF EXISTS sales_fact_backup;
    CREATE TABLE sales_fact_backup AS SELECT * FROM sales_fact;
    -- Disable foreign key checks
    SET FOREIGN_KEY_CHECKS = 0;
  """,
  if_exists="replace",
  timeout=600,
  after_query="""
    -- Re-enable foreign key checks
    SET FOREIGN_KEY_CHECKS = 1;
    -- Update statistics
    ANALYZE TABLE sales_fact;
    -- Log load
    INSERT INTO audit.load_log (table_name, loaded_at)
    VALUES ('sales_fact', NOW());
  """
)

# Connect
pipe1, pipe2 = Pipe("extract"), Pipe("load")
mysql_origin.add_output_pipe(pipe1).set_destination(filter_node)
filter_node.add_output_pipe(pipe2).set_destination(mysql_dest)

# Execute
mysql_origin.pump()
```

## Pattern 3: PostgreSQL with Advanced Features ✨ v2.4
```python
# Extract with before_query
pg_origin = PostgresOrigin(
  name="sales_extract",
  host="localhost",
  database="warehouse",
  user="postgres",
  password="password",
  before_query="""
    CREATE TEMP TABLE staging AS
    SELECT * FROM raw_sales WHERE date = CURRENT_DATE;
  """,
  query="SELECT * FROM staging WHERE amount > :min_amount",
  query_parameters={'min_amount': 100.0},
  max_results=10000,
  timeout=300,
  after_query="""
    INSERT INTO audit.extraction_log (table_name, extracted_at)
    VALUES ('staging', NOW());
  """
)

# Transform
filter_node = Filter("high_value", "amount", ">", 1000)

# Load with before/after queries
pg_dest = PostgresDestination(
  name="sales_load",
  host="localhost",
  database="warehouse",
  user="postgres",
  password="password",
  table="sales_fact",
  schema="public",
  before_query="""
    -- Create backup
    CREATE TABLE sales_fact_backup AS SELECT * FROM sales_fact;
    -- Disable triggers
    ALTER TABLE sales_fact DISABLE TRIGGER ALL;
  """,
  if_exists="replace",
  timeout=600,
  after_query="""
    -- Re-enable triggers
    ALTER TABLE sales_fact ENABLE TRIGGER ALL;
    -- Refresh views
    REFRESH MATERIALIZED VIEW reports.sales_summary;
    -- Log load
    INSERT INTO audit.load_log (table_name, loaded_at)
    VALUES ('sales_fact', NOW());
    -- Update statistics
    ANALYZE sales_fact;
  """
)

# Connect
pipe1, pipe2 = Pipe("extract"), Pipe("load")
pg_origin.add_output_pipe(pipe1).set_destination(filter_node)
filter_node.add_output_pipe(pipe2).set_destination(pg_dest)

# Execute
pg_origin.pump()
```

## Pattern 4: BigQuery with Advanced Features ✨ EXISTING
```python
# Extract with before_query and dry_run
bq_origin = GCPBigQueryOrigin(
  name="sales_extract",
  project_id="my-project",
  before_query="""
    CREATE TEMP TABLE staging AS
    SELECT * FROM raw_sales WHERE date = CURRENT_DATE()
  """,
  query="SELECT * FROM staging",
  dry_run=False,
  max_results=10000
)

# Transform
filter_node = Filter("high_value", "amount", ">", 1000)

# Load with partitioning and after_query
bq_dest = GCPBigQueryDestination(
  name="sales_load",
  project_id="my-project",
  dataset="warehouse",
  table="sales",
  write_disposition="WRITE_APPEND",
  time_partitioning={'type': 'DAY', 'field': 'sale_date'},
  clustering_fields=['region', 'category'],
  after_query="""
    INSERT INTO audit.load_log (table_name, loaded_at, record_count)
    VALUES ('sales', CURRENT_TIMESTAMP(), (SELECT COUNT(*) FROM warehouse.sales))
  """
)

# Connect
pipe1, pipe2 = Pipe("extract"), Pipe("load")
bq_origin.add_output_pipe(pipe1).set_destination(filter_node)
filter_node.add_output_pipe(pipe2).set_destination(bq_dest)

# Execute
bq_origin.pump()
```

---

# ROADMAP

## Completed (✅)
- MySQL Origin and Destination ✨ **ENHANCED v2.4**
- PostgreSQL Origin and Destination ✨ **ENHANCED v2.4**
- BigQuery Origin and Destination with advanced features ✨
- OpenOrigin for in-memory DataFrames
- OpenAI Transformer (GPT-4o, GPT-4-Turbo, GPT-3.5-Turbo)
- Anthropic Transformer (Claude)
- Google Transformer (Gemini)
- DeepSeek Transformer
- **GCPBigQueryOrigin enhancements**: before_query, after_query, dry_run, max_results ✨
- **GCPBigQueryDestination enhancements**: before_query, after_query, partitioning, clustering ✨
- **PostgresOrigin enhancements**: before_query, after_query, timeout, max_results, query_parameters, table ✨
- **PostgresDestination enhancements**: before_query, after_query, timeout ✨
- **MySQLOrigin enhancements**: before_query, after_query, timeout, max_results, query_parameters, table ✨ **NEW v2.4**
- **MySQLDestination enhancements**: before_query, after_query, timeout ✨ **NEW v2.4**
- **29 total components**

## In Progress (🚧)
- [ ] Documentation improvements (expanding guides for remaining components)

## Pending AI Providers
- [ ] Mistral AI Transformer
- [ ] Cohere Transformer
- [ ] Llama Transformer (via Ollama/local)
- [ ] Azure OpenAI Transformer

## Potential Components

### Origins
- [ ] MariaDB, MongoDB, Kafka Consumer
- [ ] S3 (AWS), Azure Blob Storage, Snowflake
- [ ] Excel, Parquet, JSON, XML, SFTP

### Destinations
- [ ] MariaDB, MongoDB, Kafka Producer
- [ ] S3 (AWS), Azure Blob Storage, Snowflake
- [ ] Excel, Parquet, JSON, XML, SFTP

### Transformers
- [ ] Sort, Pivot, Unpivot, Window Functions
- [ ] Lookup, Merge, Split, Sample
- [ ] Normalize, Encode

---

# NOTES FOR FUTURE DEVELOPMENT

## When creating new Origins:
1. Inherit from `Origin` class
2. Use default 0→1 connectivity (no override needed for simple cases)
3. Initialize external clients in `_initialize_client()` (lazy initialization)
4. Implement `pump()` method
5. Add comprehensive error handling
6. Close connections in `finally` block
7. Use 2-space indentation
8. **Consider adding `before_query` and `after_query` parameters for advanced workflows** ✨
9. **Consider adding `timeout`, `max_results`, and `query_parameters` for flexibility** ✨
10. **Use structured logging with separators and emojis** ✨
11. **Implement `_execute_query()` method for before/after query execution** ✨
12. **Implement `_build_query()` method if supporting both query and table parameters** ✨

## When creating new Destinations:
1. Inherit from `Destination` class
2. Use default 1→0 connectivity (no override needed for simple cases)
3. Initialize external clients in `_initialize_client()` (lazy initialization)
4. Implement `sink()` method
5. Add comprehensive error handling
6. Close connections in `finally` block
7. Use 2-space indentation
8. **Consider adding `before_query` and `after_query` parameters for advanced workflows** ✨
9. **Consider adding `timeout` parameter for control** ✨
10. **Consider performance optimizations like partitioning and clustering** ✨
11. **Use structured logging with separators and emojis** ✨
12. **Implement `_execute_query()` method for before/after query execution** ✨

## Database Enhancement Pattern (v2.4) ✨
The PostgreSQL and MySQL components serve as reference patterns for enhancing database connectors:

**Origin enhancements**:
- `before_query`: Create temp tables, call functions, set session variables
- `after_query`: Audit logging, mark records as processed, cleanup
- `table`: Simplified table read (supports 'table' or 'schema.table' / 'database.table')
- `max_results`: Add LIMIT automatically for testing
- `timeout`: Control connection and query execution time
- `query_parameters`: Parameterized queries using :param_name syntax (dict)

**Destination enhancements**:
- `before_query`: Create backups, truncate tables, disable triggers/foreign keys
- `after_query`: Validate data, refresh views, re-enable triggers, ANALYZE/OPTIMIZE
- `timeout`: Control connection and query execution time

**Logging improvements**:
- Structured format with `{'='*70}` separators
- Emojis: ✅ ❌ 📊 ⏱️ 📋 for visual clarity
- Duration tracking with `time.time()`
- Detailed statistics (rows, columns, data types)
- Separate sections for before/main/after queries

**Error handling**:
- Specific error messages for common failures
- Timeout detection and reporting
- Query preview in error messages

**MySQL vs PostgreSQL differences**:
- MySQL uses backticks for identifiers: \`table\`
- PostgreSQL uses double quotes: "table"
- MySQL: `TEMPORARY TABLE`, PostgreSQL: `TEMP TABLE`
- MySQL: `CURDATE()` / `NOW()`, PostgreSQL: `CURRENT_DATE` / `NOW()`
- MySQL: `ANALYZE TABLE`, PostgreSQL: `ANALYZE table`
- MySQL: `OPTIMIZE TABLE`, PostgreSQL: `VACUUM`
- MySQL: `SET FOREIGN_KEY_CHECKS = 0`, PostgreSQL: `ALTER TABLE DISABLE TRIGGER ALL`

This pattern should be applied to MariaDB and other database connectors.

---

**Version**: 2.4  
**Date**: January 2025  
**Status**: Production Ready ✅  
**Latest Updates**: 
- Enhanced MySQLOrigin with before_query, after_query, timeout, max_results, query_parameters, table ✨ **NEW**
- Enhanced MySQLDestination with before_query, after_query, timeout ✨ **NEW**
- Enhanced PostgresOrigin with before_query, after_query, timeout, max_results, query_parameters, table ✨
- Enhanced PostgresDestination with before_query, after_query, timeout ✨
- Enhanced logging with structured format and emojis ✨
- Added database enhancement pattern for reference ✨

---

**END OF KNOWLEDGE BASE**