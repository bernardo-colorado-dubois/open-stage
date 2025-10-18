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
│   │   └── common.py                  # PostgreSQL connectors (2)
│   │       ├── PostgresOrigin        # Origin: PostgreSQL queries
│   │       └── PostgresDestination   # Destination: PostgreSQL tables
│   ├── mysql/
│   │   ├── __init__.py
│   │   └── common.py                  # MySQL connectors (2)
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

## File Organization Rules

### Core Module (`src/core/`)
- **base.py**: Contains the 5 fundamental base classes (DataPackage, Pipe, Origin, Destination, Node)
- **common.py**: Contains generic components that don't require external dependencies (15 components)

### Database Modules
- **Pattern**: `src/{database_name}/common.py`
- **Examples**: `src/postgres/`, `src/mysql/`
- **Contains**: Origin and Destination for each database type
- **Dependencies**: Database-specific drivers (psycopg2, pymysql, etc.)

### Cloud Provider Modules
- **Pattern**: `src/{provider}/`
- **Example**: `src/google/`
- **Files**:
  - `cloud.py`: Cloud service connectors (BigQuery, etc.)
  - `{service}.py`: AI services (gemini.py, etc.)

### AI Provider Modules
- **Pattern**: `src/{ai_provider}/{service}.py`
- **Examples**: 
  - `src/anthropic/claude.py`
  - `src/google/gemini.py`
  - `src/deepseek/deepseek.py`
  - `src/open_ai/chat_gpt.py`
- **Contains**: AI Prompt Transformers

### Module Naming Conventions
- **Origin classes**: End with `Origin` (e.g., `MySQLOrigin`)
- **Destination classes**: End with `Destination` (e.g., `MySQLDestination`)
- **AI Transformers**: End with `PromptTransformer` (e.g., `OpenAIPromptTransformer`)
- **Generic components**: Descriptive names (e.g., `Filter`, `Aggregator`)

---

# ARCHITECTURE

## Base Classes (5)

### 1. DataPackage
**Purpose**: Encapsulates data and metadata from a pipe.
```python
class DataPackage:
  def __init__(self, pipe_name: str, df: pd.DataFrame) -> None:
    self.pipe_name = pipe_name
    self.df = df
  
  def get_pipe_name(self) -> str:
    return self.pipe_name
  
  def get_df(self) -> pd.DataFrame:
    return self.df
```

### 2. Pipe
**Purpose**: Connects components and transports data.
```python
class Pipe:
  def __init__(self, name: str) -> None:
    self.name = name
    self.origin = None
    self.destination = None
  
  def set_destination(self, destination):
    self.destination = destination
    self.destination.add_input_pipe(self)
    if isinstance(destination, Node):
      return destination  # Enables method chaining
  
  def flow(self, df: pd.DataFrame) -> None:
    data_package = DataPackage(self.name, df)
    self.destination.sink(data_package)
```

### 3. Origin (Abstract Class)
**Connectivity**: 0 → 1 (by default)
```python
class Origin:
  def __init__(self):
    self.outputs = {}
  
  @abstractmethod
  def add_output_pipe(self, pipe: Pipe) -> Pipe:
    if len(self.outputs.keys()) == 0:
      self.outputs[pipe.get_name()] = pipe
      pipe.set_origin(self)
      return pipe
  
  @abstractmethod
  def pump(self):
    pass
```

### 4. Destination (Abstract Class)
**Connectivity**: 1 → 0 (by default)
```python
class Destination:
  def __init__(self):
    self.inputs = {}
  
  @abstractmethod
  def add_input_pipe(self, pipe: Pipe) -> None:
    if len(self.inputs.keys()) == 0:
      self.inputs[pipe.get_name()] = pipe
  
  @abstractmethod
  def sink(self, data_package: DataPackage) -> None:
    pass
```

### 5. Node (Abstract Class)
**Connectivity**: Flexible (inherits from Origin and Destination)
```python
class Node(Origin, Destination):
  def __init__(self):
    Origin.__init__(self)
    Destination.__init__(self)
```

---

# COMPONENTS CATALOG

## Origins (Data Sources) - 0→1

### Generator
**Module**: `src/core/common.py`
**Purpose**: Generates sequential numeric data for testing.

**Parameters**:
- `name`: str - Component name
- `length`: int - Number of integers to generate

**Example**:
```python
gen = Generator("gen1", 100)
pipe = Pipe("pipe1")
gen.add_output_pipe(pipe).set_destination(destination)
gen.pump()
```

---

### OpenOrigin
**Module**: `src/core/common.py`
**Purpose**: Accepts a pandas DataFrame directly for in-memory processing.

**Parameters**:
- `name`: str - Component name
- `df`: pd.DataFrame - Pandas DataFrame to use as data source

**Example**:
```python
import pandas as pd

data = {'id': [1, 2, 3], 'name': ['Alice', 'Bob', 'Charlie']}
df = pd.DataFrame(data)

origin = OpenOrigin(name="my_data", df=df)
pipe = Pipe("pipe1")
origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

**Use Cases**:
- Testing pipelines quickly
- Processing data already loaded in memory
- Prototyping and proof of concepts
- Working with data from APIs or custom sources
- Jupyter notebooks and interactive analysis

---

### CSVOrigin
**Module**: `src/core/common.py`
**Purpose**: Reads CSV files using pandas.

**Parameters**:
- `name`: str - Component name
- `**kwargs` - All pandas.read_csv() arguments

**Example**:
```python
csv = CSVOrigin(
  name="reader",
  filepath_or_buffer="data.csv",
  sep=",",
  encoding="utf-8"
)
```

---

### APIRestOrigin
**Module**: `src/core/common.py`
**Purpose**: Consumes REST APIs and converts JSON to DataFrame.

**Parameters**:
- `name`: str - Component name
- `path`: str = '.' - Path to navigate in JSON response
- `fields`: list = None - Fields to extract
- `**kwargs` - All requests.request() arguments

**Example**:
```python
api = APIRestOrigin(
  name="api_reader",
  path="data.users",
  fields=["id", "name"],
  url="https://api.example.com/users",
  method="GET",
  headers={"Authorization": "Bearer token"}
)
```

---

### MySQLOrigin
**Module**: `src/mysql/common.py`
**Purpose**: Queries MySQL databases.

**Dependencies**: `sqlalchemy`, `pymysql`

**Parameters**:
- `name`: str - Component name
- `host`: str - MySQL host
- `port`: int = 3306 - MySQL port
- `database`: str - Database name
- `user`: str - Username
- `password`: str - Password
- `query`: str - SQL query

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

**Validations**:
- host, database, user, password, query cannot be empty
- port must be positive

**Error Handling**:
- Connection refused → Check if MySQL is running
- Authentication failed → Check credentials
- Unknown database → Check if database exists
- SQL syntax errors → Shows problematic query

---

### PostgresOrigin
**Module**: `src/postgres/common.py`
**Purpose**: Queries PostgreSQL databases.

**Dependencies**: `sqlalchemy`, `psycopg2-binary`

**Parameters**:
- `name`: str - Component name
- `host`: str - PostgreSQL host
- `port`: int = 5432 - PostgreSQL port
- `database`: str - Database name
- `user`: str - Username
- `password`: str - Password
- `query`: str - SQL query

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

**New Features**:
- **before_query**: Create temp tables, call procedures, prepare data before extraction
- **after_query**: Audit logging, cleanup, post-processing after extraction
- **table**: Direct table read without writing SELECT *
- **max_results**: Limit rows for testing without modifying query
- **dry_run**: Validate SQL and estimate processing costs
- **query_parameters**: Secure parameterized queries
- **Enhanced logging**: Bytes processed, costs, cache hits, job duration

**Example 1: Basic usage with table**:
```python
bq = GCPBigQueryOrigin(
  name="bq_reader",
  project_id="my-project",
  table="dataset.customers",
  max_results=1000
)
```

**Example 2: With before_query**:
```python
bq = GCPBigQueryOrigin(
  name="bq_reader",
  project_id="my-project",
  before_query="""
    CREATE TEMP TABLE staging AS
    SELECT * FROM raw_data WHERE valid = true
  """,
  query="SELECT * FROM staging",
  dry_run=False
)
```

**Example 3: Dry run for cost estimation**:
```python
bq = GCPBigQueryOrigin(
  name="cost_check",
  project_id="my-project",
  query="SELECT * FROM `bigquery-public-data.usa_names.usa_1910_current`",
  dry_run=True  # Only validates and estimates cost
)
```

**Example 4: Parameterized query**:
```python
from google.cloud import bigquery

bq = GCPBigQueryOrigin(
  name="filtered",
  project_id="my-project",
  query="SELECT * FROM dataset.sales WHERE date >= @start_date",
  query_parameters=[
    bigquery.ScalarQueryParameter("start_date", "DATE", "2024-01-01")
  ]
)
```

**Use Cases**:
- **before_query**: Create staging tables, call validation procedures, prepare temp data
- **after_query**: Log extraction metadata, cleanup temp tables, update control tables
- **dry_run**: Estimate query costs before running expensive operations
- **max_results**: Quick testing without processing full datasets
- **table**: Simplified syntax for full table reads

---

## Destinations (Data Sinks) - 1→0

### Printer
**Module**: `src/core/common.py`
**Purpose**: Displays data to console for debugging.

**Parameters**:
- `name`: str - Component name

**Example**:
```python
printer = Printer("output")
pipe.set_destination(printer)
```

---

### CSVDestination
**Module**: `src/core/common.py`
**Purpose**: Writes DataFrame to CSV files.

**Parameters**:
- `name`: str - Component name
- `**kwargs` - All pandas.to_csv() arguments

**Example**:
```python
csv_dest = CSVDestination(
  name="writer",
  path_or_buf="output.csv",
  index=False,
  sep=";"
)
```

---

### MySQLDestination
**Module**: `src/mysql/common.py`
**Purpose**: Writes data to MySQL tables.

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

**Optimizations**:
- Writes in chunks of 1000 rows
- No index included (index=False)

---

### PostgresDestination
**Module**: `src/postgres/common.py`
**Purpose**: Writes data to PostgreSQL tables.

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

**Key Difference from MySQL**: PostgreSQL uses schemas (default: 'public')

**Example**:
```python
pg_dest = PostgresDestination(
  name="pg_writer",
  host="localhost",
  database="analytics_db",
  user="postgres",
  password="password",
  table="sales_summary",
  schema="reports",
  if_exists="replace"
)
```

**Optimizations**:
- Writes in chunks of 1000 rows
- Uses `method='multi'` for faster INSERTs
- No index included (index=False)

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

**write_disposition options**:
- `'WRITE_TRUNCATE'` - Replace table (truncate before load)
- `'WRITE_APPEND'` - Add to table
- `'WRITE_EMPTY'` - Only if table empty

**create_disposition options**:
- `'CREATE_IF_NEEDED'` - Create table if not exists (default)
- `'CREATE_NEVER'` - Fail if table doesn't exist

**schema_update_options**:
- `'ALLOW_FIELD_ADDITION'` - Allow adding new columns
- `'ALLOW_FIELD_RELAXATION'` - Allow REQUIRED → NULLABLE

**New Features**:
- **before_query**: Create backups, truncate tables, prepare staging before load
- **after_query**: Audit logging, data validation, refresh views after load
- **time_partitioning**: Partition by DAY, HOUR, MONTH, or YEAR for performance
- **clustering_fields**: Cluster by up to 4 fields to optimize queries
- **schema_update_options**: Automatically update schema when DataFrame changes
- **Enhanced logging**: Rows loaded, table metadata, partition/cluster info

**Example 1: Basic load with partitioning**:
```python
bq_dest = GCPBigQueryDestination(
  name="bq_writer",
  project_id="my-project",
  dataset="warehouse",
  table="sales",
  write_disposition="WRITE_APPEND",
  time_partitioning={
    'type': 'DAY',
    'field': 'sale_date'
  }
)
```

**Example 2: With clustering**:
```python
bq_dest = GCPBigQueryDestination(
  name="bq_writer",
  project_id="my-project",
  dataset="warehouse",
  table="events",
  write_disposition="WRITE_APPEND",
  clustering_fields=['region', 'product_category', 'user_id']
)
```

**Example 3: With before_query and after_query**:
```python
bq_dest = GCPBigQueryDestination(
  name="bq_writer",
  project_id="my-project",
  dataset="warehouse",
  table="customers",
  write_disposition="WRITE_TRUNCATE",
  before_query="""
    -- Create backup
    CREATE OR REPLACE TABLE `my-project.warehouse.customers_backup` AS
    SELECT * FROM `my-project.warehouse.customers`;
  """,
  after_query="""
    -- Log the load
    INSERT INTO `my-project.audit.load_log` (
      table_name, loaded_at, record_count
    ) VALUES (
      'customers',
      CURRENT_TIMESTAMP(),
      (SELECT COUNT(*) FROM `my-project.warehouse.customers`)
    );
  """
)
```

**Example 4: Schema updates allowed**:
```python
bq_dest = GCPBigQueryDestination(
  name="flexible_load",
  project_id="my-project",
  dataset="warehouse",
  table="evolving_table",
  write_disposition="WRITE_APPEND",
  schema_update_options=[
    'ALLOW_FIELD_ADDITION',
    'ALLOW_FIELD_RELAXATION'
  ]
)
```

**Use Cases**:
- **before_query**: Backup tables, prepare staging, truncate before full refresh
- **after_query**: Validate data quality, update metadata, refresh materialized views
- **time_partitioning**: Improve query performance and reduce costs on large tables
- **clustering**: Optimize queries that filter on specific columns
- **schema_update_options**: Handle evolving schemas in production

---

## Routers (Flow Control) - N↔M

### Funnel
**Module**: `src/core/common.py`
**Connectivity**: N → 1 (override of add_input_pipe)
**Purpose**: Combines multiple data streams into one.

**Parameters**:
- `name`: str - Component name

**Example**:
```python
funnel = Funnel("combiner")
pipe1.set_destination(funnel)
pipe2.set_destination(funnel)
pipe3.set_destination(funnel)
funnel.add_output_pipe(output_pipe)
```

**Behavior**:
- Waits for all inputs before processing
- Concatenates DataFrames (pd.concat)
- Validates column consistency

---

### Switcher
**Module**: `src/core/common.py`
**Connectivity**: 1 → N (override of add_output_pipe)
**Purpose**: Routes data conditionally based on field values.

**Parameters**:
- `name`: str - Component name
- `field`: str - Field to evaluate
- `mapping`: dict - Maps field values to pipe names
- `fail_on_unmatch`: bool = False - Error on unmapped values

**Example**:
```python
switcher = Switcher(
  "router",
  field="category",
  mapping={
    "A": "pipe_a",
    "B": "pipe_b",
    "C": "pipe_c"
  },
  fail_on_unmatch=True
)
```

---

### Copy
**Module**: `src/core/common.py`
**Connectivity**: 1 → N (override of add_output_pipe)
**Purpose**: Duplicates data stream to multiple destinations.

**Parameters**:
- `name`: str - Component name

**Example**:
```python
copy = Copy("duplicator")
pipe.set_destination(copy)
copy.add_output_pipe(pipe1).set_destination(dest1)
copy.add_output_pipe(pipe2).set_destination(dest2)
```

---

## Transformers - 1→1

### Filter
**Module**: `src/core/common.py`
**Purpose**: Filters DataFrame rows based on conditions.

**Parameters**:
- `name`: str - Component name
- `field`: str - Field to filter
- `condition`: str - Operator
- `value_or_values`: any - Value(s) for comparison

**Supported conditions** (9):
- Comparison: `<`, `>`, `<=`, `>=`, `!=`, `=`
- Membership: `in`, `not in`
- Range: `between` (requires list with 2 values)

**Examples**:
```python
# Comparison
Filter("adults", "age", ">=", 18)

# Membership
Filter("valid_types", "type", "in", ["A", "B", "C"])

# Range
Filter("price_range", "price", "between", [100, 500])
```

---

### Aggregator
**Module**: `src/core/common.py`
**Purpose**: Aggregates data by grouping.

**Parameters**:
- `name`: str - Component name
- `key`: str - Field to group by (GROUP BY)
- `agg_field_name`: str - Output field name
- `agg_type`: str - Aggregation type
- `field_to_agg`: str = None - Field to aggregate (not needed for 'count')

**Aggregation types**:
`sum`, `count`, `mean`, `median`, `min`, `max`, `std`, `var`, `nunique`, `first`, `last`

**Examples**:
```python
# Count
Aggregator("counter", "category", "total_count", "count")

# Sum
Aggregator("summer", "category", "total_sales", "sum", "sales")
```

---

### DeleteColumns
**Module**: `src/core/common.py`
**Purpose**: Removes specified columns from DataFrame.

**Parameters**:
- `name`: str - Component name
- `columns`: list - List of column names to delete

**Example**:
```python
DeleteColumns("cleanup", ["temp_col", "id", "unused"])
```

---

### RemoveDuplicates
**Module**: `src/core/common.py`
**Purpose**: Deduplicates rows based on key field.

**Parameters**:
- `name`: str - Component name
- `key`: str - Key field for partitioning
- `sort_by`: str - Field to sort by
- `orientation`: str - Sort direction (`'ASC'` or `'DESC'`)
- `retain`: str - Which record to keep (`'FIRST'` or `'LAST'`)

**Example**:
```python
RemoveDuplicates(
  name="dedup",
  key="user_id",
  sort_by="timestamp",
  orientation="DESC",
  retain="FIRST"  # Keeps most recent
)
```

**Process**:
1. Sort by `sort_by` field
2. Identify duplicates by `key`
3. Retain FIRST or LAST occurrence

---

### Joiner
**Module**: `src/core/common.py`
**Connectivity**: 2 → 1 (override of add_input_pipe)
**Purpose**: Joins two DataFrames.

**Parameters**:
- `name`: str - Component name
- `left`: str - Name of left pipe
- `right`: str - Name of right pipe
- `key`: str - Join key field
- `join_type`: str - Type of join

**Join types**: `'left'`, `'right'`, `'inner'`

**Example**:
```python
joiner = Joiner(
  name="join_users_orders",
  left="users_pipe",
  right="orders_pipe",
  key="user_id",
  join_type="left"
)
```

---

### Transformer
**Module**: `src/core/common.py`
**Purpose**: Applies custom transformation functions.

**Parameters**:
- `name`: str - Component name
- `transformer_function`: callable - Function that receives and returns DataFrame

**Example**:
```python
def my_transform(df):
  df['total'] = df['price'] * df['quantity']
  return df

Transformer("calculator", my_transform)
```

---

## AI Transformers - 1→1

### OpenAIPromptTransformer
**Module**: `src/open_ai/chat_gpt.py`
**Purpose**: Transforms data using OpenAI GPT models.

**Dependencies**: `openai`

**Parameters**:
- `name`: str - Component name
- `model`: str - OpenAI model name
- `api_key`: str - OpenAI API key
- `prompt`: str - Transformation instructions
- `max_tokens`: int = 16000 - Max output tokens

**Popular models**: 
- `gpt-4o` - Most capable, multimodal (recommended)
- `gpt-4-turbo` - Fast, high intelligence
- `gpt-4` - High intelligence
- `gpt-3.5-turbo` - Fast and economical
- `gpt-4o-mini` - Cost-effective for simpler tasks

**Format**: CSV (optimized for token efficiency)

**Example**:
```python
openai = OpenAIPromptTransformer(
  name="sentiment_analyzer",
  model="gpt-4o",
  api_key="sk-...",
  prompt="Add sentiment analysis column (positive, negative, neutral)",
  max_tokens=16000
)
```

**Features**:
- CSV format for token efficiency
- Uses OpenAI SDK
- Automatic truncation handling (finish_reason: "length")
- Token usage tracking (prompt_tokens, completion_tokens, total_tokens)
- System instructions for format adherence
- temperature=0.0 for maximum instruction adherence
- Handles markdown code block removal
- Comprehensive error handling

**Context Window**: Up to 128K tokens (GPT-4o, GPT-4-Turbo)

**Use Cases**:
- Sentiment analysis
- Text classification and categorization
- Entity extraction
- Data enrichment
- Content generation
- Translation
- Summarization

---

### AnthropicPromptTransformer
**Module**: `src/anthropic/claude.py`
**Purpose**: Transforms data using Claude AI.

**Dependencies**: `anthropic`

**Parameters**:
- `name`: str - Component name
- `model`: str - Claude model name
- `api_key`: str - Anthropic API key
- `prompt`: str - Transformation instructions
- `max_tokens`: int = 16000 - Max output tokens

**Popular models**: `claude-sonnet-4-5-20250929`

**Format**: CSV (optimized for token efficiency)

**Example**:
```python
claude = AnthropicPromptTransformer(
  name="sentiment_analyzer",
  model="claude-sonnet-4-5-20250929",
  api_key="sk-ant-...",
  prompt="Add a sentiment_score column (positive, negative, neutral)",
  max_tokens=16000
)
```

**Features**:
- CSV format for token efficiency
- Automatic truncation handling
- Token usage tracking
- System instructions for format adherence

---

### GeminiPromptTransformer
**Module**: `src/google/gemini.py`
**Purpose**: Transforms data using Google Gemini AI.

**Dependencies**: `google-generativeai`

**Parameters**:
- `name`: str - Component name
- `model`: str - Gemini model name
- `api_key`: str - Google API key
- `prompt`: str - Transformation instructions
- `max_tokens`: int = 16000 - Max output tokens

**Popular models**: `gemini-2.0-flash-exp`

**Example**:
```python
gemini = GeminiPromptTransformer(
  name="ai_cleaner",
  model="gemini-2.0-flash-exp",
  api_key="...",
  prompt="Standardize names to Title Case",
  max_tokens=16000
)
```

**Features**:
- CSV format
- temperature=0.0 for adherence
- system_instruction support (passed to GenerativeModel constructor)

---

### DeepSeekPromptTransformer
**Module**: `src/deepseek/deepseek.py`
**Purpose**: Transforms data using DeepSeek AI.

**Dependencies**: `openai` (SDK)

**Parameters**:
- `name`: str - Component name
- `model`: str - DeepSeek model name
- `api_key`: str - DeepSeek API key
- `prompt`: str - Transformation instructions
- `max_tokens`: int = 8192 - Max output tokens (limit: 1-8192)

**Popular models**: `deepseek-chat`, `deepseek-coder`

**Base URL**: `https://api.deepseek.com`

**Example**:
```python
deepseek = DeepSeekPromptTransformer(
  name="ai_enricher",
  model="deepseek-chat",
  api_key="sk-...",
  prompt="Add sentiment analysis",
  max_tokens=8192
)
```

**Features**:
- Uses OpenAI SDK
- Automatic token adjustment
- Context limit: 131,072 tokens

---

## AI Transformers Comparison Table

| Feature | OpenAI | Anthropic | Gemini | DeepSeek |
|---------|--------|-----------|--------|----------|
| **SDK Used** | openai | anthropic | google-generativeai | openai |
| **Base URL** | Default | Default | Default | api.deepseek.com |
| **Max Tokens Default** | 16000 | 16000 | 16000 | 8192 |
| **Temperature** | 0.0 | N/A | 0.0 | 0.0 |
| **Truncation Reason** | "length" | "max_tokens" | N/A | "length" |
| **Token Tracking** | ✅ prompt/completion | ✅ input/output | ✅ prompt/candidates | ✅ prompt/completion |
| **Context Window** | 128K | 200K | 2M | 131K |
| **Best For** | General purpose | Long context | Fast inference | Coding tasks |
| **Pricing** | $$$ | $$$$ | $$ | $ |

---

# CONNECTIVITY RULES

| Component Type | Inputs | Outputs | Override | Notes |
|----------------|--------|---------|----------|-------|
| **Origins** | 0 | 1 | No | Default behavior from Origin class |
| **Destinations** | 1 | 0 | No | Default behavior from Destination class |
| **Funnel** | N | 1 | Yes (inputs) | Router that combines streams |
| **Switcher** | 1 | N | Yes (outputs) | Router that splits conditionally |
| **Copy** | 1 | N | Yes (outputs) | Router that duplicates |
| **Transformers** | 1 | 1 | No | Default Node behavior |
| **Joiner** | 2 | 1 | Yes (inputs) | Special transformer |
| **AI Transformers** | 1 | 1 | No | Default Node behavior |

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

---

# DEPENDENCIES
```txt
pandas>=1.3.0
requests>=2.25.0
sqlalchemy>=1.4.0
psycopg2-binary>=2.9.0
pymysql>=1.0.0
google-cloud-bigquery>=3.0.0
google-auth>=2.0.0
db-dtypes>=1.0.0
anthropic>=0.18.0
google-generativeai>=0.3.0
openai>=1.0.0
python-dotenv>=0.19.0
```

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

## Pattern 2: In-Memory Processing
```python
import pandas as pd

df = pd.DataFrame({'id': [1, 2, 3], 'value': [10, 20, 30]})

origin = OpenOrigin("memory_data", df=df)
printer = Printer("output")

pipe = Pipe("p1")
origin.add_output_pipe(pipe).set_destination(printer)

origin.pump()
```

## Pattern 3: Database Migration
```python
mysql_origin = MySQLOrigin("source", host="...", database="...", user="...", password="...", query="...")
pg_dest = PostgresDestination("target", host="...", database="...", user="...", password="...", table="...", schema="public")

pipe = Pipe("migration")
mysql_origin.add_output_pipe(pipe).set_destination(pg_dest)
mysql_origin.pump()
```

## Pattern 4: AI Transformation with OpenAI
```python
csv_origin = CSVOrigin("reader", filepath_or_buffer="data.csv")
ai_transform = OpenAIPromptTransformer("ai", model="gpt-4o", api_key="...", prompt="...")
csv_dest = CSVDestination("writer", path_or_buf="output.csv", index=False)

pipe1, pipe2 = Pipe("in"), Pipe("out")

csv_origin.add_output_pipe(pipe1).set_destination(ai_transform)
ai_transform.add_output_pipe(pipe2).set_destination(csv_dest)

csv_origin.pump()
```

## Pattern 5: BigQuery with Advanced Features ✨ NEW
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

## Pattern 6: Multi-Model AI Comparison
```python
# Use Copy to duplicate data for multiple AI models
origin = CSVOrigin("reader", filepath_or_buffer="data.csv")
copy = Copy("duplicator")
origin.add_output_pipe(Pipe("in")).set_destination(copy)

# Process with different AI models in parallel
openai = OpenAIPromptTransformer("gpt", model="gpt-4o", api_key="...", prompt="...")
claude = AnthropicPromptTransformer("claude", model="claude-sonnet-4-5-20250929", api_key="...", prompt="...")
gemini = GeminiPromptTransformer("gemini", model="gemini-2.0-flash-exp", api_key="...", prompt="...")

copy.add_output_pipe(Pipe("to_gpt")).set_destination(openai)
copy.add_output_pipe(Pipe("to_claude")).set_destination(claude)
copy.add_output_pipe(Pipe("to_gemini")).set_destination(gemini)

# Consolidate results
funnel = Funnel("consolidator")
openai.add_output_pipe(Pipe("gpt_out")).set_destination(funnel)
claude.add_output_pipe(Pipe("claude_out")).set_destination(funnel)
gemini.add_output_pipe(Pipe("gemini_out")).set_destination(funnel)

destination = CSVDestination("results", path_or_buf="comparison.csv", index=False)
funnel.add_output_pipe(Pipe("final")).set_destination(destination)

origin.pump()
```

---

# MERMAID DIAGRAM COLOR SCHEME

Use these colors consistently in all diagrams:

- **Origins** (🔵): `fill:#4A90E2,stroke:#2E5C8A,stroke-width:2px,color:#fff`
- **Transformers** (🔴): `fill:#FF6B6B,stroke:#C92A2A,stroke-width:2px,color:#fff`
- **Routers** (🟡): `fill:#F5A623,stroke:#C17D11,stroke-width:2px,color:#fff`
- **Destinations** (🟢): `fill:#7ED321,stroke:#5FA319,stroke-width:2px,color:#fff`

## Mermaid Example
```mermaid
flowchart LR
    Origin[CSV Origin]:::origin
    Transform[Filter]:::transformer
    Router[Copy]:::router
    Dest[MySQL Destination]:::destination
    
    Origin --> Transform
    Transform --> Router
    Router --> Dest
    
    classDef origin fill:#4A90E2,stroke:#2E5C8A,stroke-width:2px,color:#fff
    classDef transformer fill:#FF6B6B,stroke:#C92A2A,stroke-width:2px,color:#fff
    classDef router fill:#F5A623,stroke:#C17D11,stroke-width:2px,color:#fff
    classDef destination fill:#7ED321,stroke:#5FA319,stroke-width:2px,color:#fff
```

## Color Reference Table

| Component Type | Color Name | Hex Code | RGB | Usage |
|----------------|-----------|----------|-----|-------|
| **Origins** | Blue | `#4A90E2` | `rgb(74, 144, 226)` | Data sources (CSV, MySQL, API, etc.) |
| **Transformers** | Red | `#FF6B6B` | `rgb(255, 107, 107)` | Data transformations (Filter, Aggregator, etc.) |
| **Routers** | Orange | `#F5A623` | `rgb(245, 166, 35)` | Flow control (Funnel, Switcher, Copy) |
| **Destinations** | Green | `#7ED321` | `rgb(126, 211, 33)` | Data sinks (MySQL, PostgreSQL, CSV, etc.) |

## Border Colors

| Component Type | Stroke Color | Hex Code | RGB |
|----------------|-------------|----------|-----|
| **Origins** | Dark Blue | `#2E5C8A` | `rgb(46, 92, 138)` |
| **Transformers** | Dark Red | `#C92A2A` | `rgb(201, 42, 42)` |
| **Routers** | Dark Orange | `#C17D11` | `rgb(193, 125, 17)` |
| **Destinations** | Dark Green | `#5FA319` | `rgb(95, 163, 25)` |

---

# ROADMAP

## Completed (✅)
- MySQL Origin and Destination
- PostgreSQL Origin and Destination
- BigQuery Origin and Destination with advanced features ✨
- OpenOrigin for in-memory DataFrames
- OpenAI Transformer (GPT-4o, GPT-4-Turbo, GPT-3.5-Turbo)
- Anthropic Transformer (Claude)
- Google Transformer (Gemini)
- DeepSeek Transformer
- **GCPBigQueryOrigin enhancements**: before_query, after_query, dry_run, max_results ✨
- **GCPBigQueryDestination enhancements**: before_query, after_query, partitioning, clustering ✨
- **29 total components**

## Pending AI Providers
- [ ] Mistral AI Transformer
- [ ] Cohere Transformer
- [ ] Llama Transformer (via Ollama/local)
- [ ] Azure OpenAI Transformer

## Potential Database Enhancements
- [ ] Apply before_query/after_query to PostgresOrigin/PostgresDestination
- [ ] Apply before_query/after_query to MySQLOrigin/MySQLDestination
- [ ] Add partitioning support to PostgreSQL
- [ ] Add clustering support to other databases

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

### Validators
- [ ] Schema Validator
- [ ] Data Quality Validator
- [ ] Business Rules Validator

### Utilities
- [ ] Logger, Profiler, Cache, Checkpoint

---

# LICENSE

MIT License

Copyright (c) 2025 Bernardo Colorado Dubois and Saul Hernandez Cordova

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

---

# NOTES FOR FUTURE DEVELOPMENT

## Code Style Guidelines
- **Indentation**: Always use 2 spaces (no tabs)
- **Type Hints**: Use `pd.DataFrame` instead of importing `DataFrame` separately
- **Imports**: Import pandas as `import pandas as pd`
- **Consistency**: Follow existing patterns in base classes

## When creating new Origins:
1. Inherit from `Origin` class
2. Use default 0→1 connectivity (no override needed for simple cases)
3. Initialize external clients in `_initialize_client()` (lazy initialization)
4. Implement `pump()` method
5. Add comprehensive error handling
6. Close connections in `finally` block
7. Use 2-space indentation
8. **Consider adding `before_query` and `after_query` parameters for advanced workflows**

## When creating new Destinations:
1. Inherit from `Destination` class
2. Use default 1→0 connectivity (no override needed for simple cases)
3. Initialize external clients in `_initialize_client()` (lazy initialization)
4. Implement `sink()` method
5. Add comprehensive error handling
6. Close connections in `finally` block
7. Use 2-space indentation
8. **Consider adding `before_query` and `after_query` parameters for advanced workflows**
9. **Consider performance optimizations like partitioning and clustering**

## When creating new Transformers (1→1):
1. Inherit from `Node` class
2. Use default 1→1 connectivity (no override needed)
3. Store received DataFrame in `self.received_df`
4. Call `self.pump()` at end of `sink()`
5. Clear `self.received_df = None` after processing
6. Use 2-space indentation

## When creating new Routers (N↔M):
1. Inherit from `Node` class
2. Override `add_input_pipe()` and/or `add_output_pipe()` as needed
3. Implement custom routing logic in `pump()`
4. Use 2-space indentation

## When creating new AI Transformers:
1. Inherit from `Node` class
2. Use default 1→1 connectivity (no override needed)
3. Initialize AI client in `_initialize_client()` (lazy initialization)
4. Use CSV format for input/output (token efficiency)
5. Set temperature=0.0 for instruction adherence
6. Implement truncation handling
7. Track token usage
8. Remove markdown code blocks from responses
9. Store received DataFrame in `self.received_df`
10. Call `self.pump()` at end of `sink()`
11. Clear `self.received_df = None` after processing
12. Use 2-space indentation

## File Organization:
- Core components: `src/core/`
- Database-specific: `src/{database_name}/common.py`
- Cloud provider-specific: `src/{provider}/`
- AI provider-specific: `src/{ai_provider}/`

---

## Testing Checklist for New Components

### For Origins:
- [✅] Test connection with valid credentials
- [✅] Test connection with invalid credentials
- [✅] Test with empty result set
- [✅] Test with large result set (1000+ rows)
- [✅] Test error handling for network issues
- [✅] Verify resource cleanup (connections closed)
- [✅] **Test before_query and after_query if applicable**
- [✅] **Test dry_run mode if applicable**

### For Destinations:
- [✅] Test write with valid data
- [✅] Test write with empty DataFrame
- [✅] Test write with large DataFrame (1000+ rows)
- [✅] Test all write modes (fail, replace, append)
- [✅] Test error handling for permission issues
- [✅] Verify resource cleanup (connections closed)
- [✅] **Test before_query and after_query if applicable**
- [✅] **Test partitioning and clustering if applicable**

### For Transformers:
- [✅] Test with valid input data
- [✅] Test with edge cases (empty, single row, large dataset)
- [✅] Test error handling for invalid data
- [✅] Verify output schema is correct
- [✅] Test that original data is not modified
- [✅] Verify resource cleanup

### For AI Transformers:
- [✅] Test with valid API key
- [✅] Test with invalid API key
- [✅] Test with small dataset (<10 rows)
- [✅] Test with large dataset (100+ rows)
- [✅] Test truncation handling (set low max_tokens)
- [✅] Verify token usage is logged correctly
- [✅] Test CSV parsing works correctly
- [✅] Test markdown code block removal
- [✅] Verify all original columns are preserved
- [✅] Test in complete pipeline (Origin → AI → Destination)

---

**Version**: 2.4  
**Date**: January 2025  
**Status**: Production Ready ✅  
**Latest Updates**: 
- Enhanced GCPBigQueryOrigin with before_query, after_query, dry_run, max_results ✨
- Enhanced GCPBigQueryDestination with before_query, after_query, partitioning, clustering ✨

---

**END OF KNOWLEDGE BASE**