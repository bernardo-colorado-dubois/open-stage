# src/google/cloud.py

from src.core.base import DataPackage, Pipe, Origin, Destination
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from typing import Optional, Dict, List


class GCPBigQueryOrigin(Origin):
  """
  GCPBigQueryOrigin - Enhanced BigQuery Data Source
  ==================================================
  Reads data from Google BigQuery with advanced features including
  pre/post query execution.
  
  Connectivity: 0 → 1 (inherits from Origin)
  
  Parameters
  ----------
  name : str
    Component name
  project_id : str
    GCP project ID
  query : str, optional
    SQL query to execute (required if table is not provided)
  table : str, optional
    Table reference in format 'dataset.table' or 'project.dataset.table'
    (required if query is not provided)
  credentials_path : str, optional
    Path to service account JSON file (uses default credentials if None)
  before_query : str, optional
    SQL query to execute BEFORE the main extraction query.
    Useful for: creating temp tables, calling procedures, data preparation.
    Default: None
  after_query : str, optional
    SQL query to execute AFTER the main extraction query.
    Useful for: audit logging, updating flags, cleanup operations.
    Default: None
  max_results : int, optional
    Maximum number of rows to return (useful for testing)
  use_legacy_sql : bool, default=False
    Use legacy SQL syntax instead of standard SQL
  query_parameters : list, optional
    List of query parameters for parameterized queries
  location : str, optional
    BigQuery location (e.g., 'US', 'EU', 'asia-northeast1')
  job_labels : dict, optional
    Labels to attach to the BigQuery job
  timeout : float, optional
    Query timeout in seconds (default: None = no timeout)
  use_query_cache : bool, default=True
    Use cached results if available
  dry_run : bool, default=False
    Validate query and estimate cost without executing
  
  Examples
  --------
  # Example 1: Simple query
  >>> origin = GCPBigQueryOrigin(
  ...     name="sales_data",
  ...     project_id="my-project",
  ...     query="SELECT * FROM dataset.sales WHERE date >= '2024-01-01'"
  ... )
  
  # Example 2: Direct table read with limit
  >>> origin = GCPBigQueryOrigin(
  ...     name="customers",
  ...     project_id="my-project",
  ...     table="dataset.customers",
  ...     max_results=1000
  ... )
  
  # Example 3: With before_query (create temp table)
  >>> origin = GCPBigQueryOrigin(
  ...     name="processed_sales",
  ...     project_id="my-project",
  ...     before_query='''
  ...         CREATE TEMP TABLE temp_sales AS
  ...         SELECT * FROM dataset.raw_sales
  ...         WHERE status = 'completed'
  ...     ''',
  ...     query="SELECT * FROM temp_sales WHERE amount > 100"
  ... )
  
  # Example 4: With after_query (audit logging)
  >>> origin = GCPBigQueryOrigin(
  ...     name="customers",
  ...     project_id="my-project",
  ...     table="dataset.customers",
  ...     after_query='''
  ...         UPDATE dataset.processing_log
  ...         SET last_extracted = CURRENT_TIMESTAMP(),
  ...             record_count = (SELECT COUNT(*) FROM dataset.customers)
  ...         WHERE table_name = 'customers'
  ...     '''
  ... )
  
  # Example 5: Complete workflow with before and after
  >>> origin = GCPBigQueryOrigin(
  ...     name="daily_report",
  ...     project_id="my-project",
  ...     before_query='''
  ...         -- Prepare staging table
  ...         CREATE OR REPLACE TABLE dataset.staging_daily_sales AS
  ...         SELECT 
  ...             DATE(order_timestamp) as order_date,
  ...             product_id,
  ...             SUM(amount) as total_amount
  ...         FROM dataset.raw_orders
  ...         WHERE DATE(order_timestamp) = CURRENT_DATE()
  ...         GROUP BY 1, 2;
  ...         
  ...         -- Call stored procedure for validation
  ...         CALL dataset.validate_daily_data();
  ...     ''',
  ...     query="SELECT * FROM dataset.staging_daily_sales",
  ...     after_query='''
  ...         -- Mark as processed
  ...         INSERT INTO dataset.extraction_audit (
  ...             table_name,
  ...             extracted_at,
  ...             record_count
  ...         ) VALUES (
  ...             'staging_daily_sales',
  ...             CURRENT_TIMESTAMP(),
  ...             (SELECT COUNT(*) FROM dataset.staging_daily_sales)
  ...         );
  ...         
  ...         -- Cleanup temp tables
  ...         DROP TABLE IF EXISTS dataset.temp_processing;
  ...     '''
  ... )
  
  # Example 6: Parameterized query with before_query
  >>> from google.cloud import bigquery
  >>> origin = GCPBigQueryOrigin(
  ...     name="filtered_sales",
  ...     project_id="my-project",
  ...     before_query="CREATE TEMP TABLE date_range AS SELECT DATE('2024-01-01') as start_date",
  ...     query="SELECT * FROM dataset.sales WHERE date >= @start_date",
  ...     query_parameters=[
  ...         bigquery.ScalarQueryParameter("start_date", "DATE", "2024-01-01")
  ...     ]
  ... )
  
  # Example 7: Dry run with before_query
  >>> origin = GCPBigQueryOrigin(
  ...     name="cost_check",
  ...     project_id="my-project",
  ...     before_query="CREATE TEMP TABLE filtered AS SELECT * FROM huge_table WHERE category = 'A'",
  ...     query="SELECT COUNT(*) FROM filtered",
  ...     dry_run=True  # Validates query and estimates cost
  ... )
  """
  
  def __init__(
    self, 
    name: str, 
    project_id: str, 
    query: Optional[str] = None,
    table: Optional[str] = None,
    credentials_path: Optional[str] = None,
    before_query: Optional[str] = None,
    after_query: Optional[str] = None,
    max_results: Optional[int] = None,
    use_legacy_sql: bool = False,
    query_parameters: Optional[List] = None,
    location: Optional[str] = None,
    job_labels: Optional[Dict[str, str]] = None,
    timeout: Optional[float] = None,
    use_query_cache: bool = True,
    dry_run: bool = False
  ):
    super().__init__()
    self.name = name
    self.project_id = project_id
    self.query = query
    self.table = table
    self.credentials_path = credentials_path
    self.before_query = before_query
    self.after_query = after_query
    self.max_results = max_results
    self.use_legacy_sql = use_legacy_sql
    self.query_parameters = query_parameters or []
    self.location = location
    self.job_labels = job_labels or {}
    self.timeout = timeout
    self.use_query_cache = use_query_cache
    self.dry_run = dry_run
    self.client = None
    
    # Validar que project_id no esté vacío
    if not project_id or not project_id.strip():
      raise ValueError(f"GCPBigQueryOrigin '{self.name}': project_id cannot be empty")
    
    # Validar que se proporcione query O table (pero no ambos)
    if query and table:
      raise ValueError(f"GCPBigQueryOrigin '{self.name}': cannot specify both 'query' and 'table'. Use one or the other.")
    
    if not query and not table:
      raise ValueError(f"GCPBigQueryOrigin '{self.name}': must specify either 'query' or 'table'")
    
    # Validar query si se proporciona
    if query and (not query.strip()):
      raise ValueError(f"GCPBigQueryOrigin '{self.name}': query cannot be empty")
    
    # Validar table si se proporciona
    if table and (not table.strip()):
      raise ValueError(f"GCPBigQueryOrigin '{self.name}': table cannot be empty")
    
    # Validar before_query si se proporciona
    if before_query is not None and (not before_query.strip()):
      raise ValueError(f"GCPBigQueryOrigin '{self.name}': before_query cannot be empty string")
    
    # Validar after_query si se proporciona
    if after_query is not None and (not after_query.strip()):
      raise ValueError(f"GCPBigQueryOrigin '{self.name}': after_query cannot be empty string")
    
    # Validar max_results si se proporciona
    if max_results is not None and max_results <= 0:
      raise ValueError(f"GCPBigQueryOrigin '{self.name}': max_results must be positive, got {max_results}")
    
    # Validar timeout si se proporciona
    if timeout is not None and timeout <= 0:
      raise ValueError(f"GCPBigQueryOrigin '{self.name}': timeout must be positive, got {timeout}")
  
  def add_output_pipe(self, pipe: Pipe) -> Pipe:
    """
    Add output pipe (only 1 allowed by default)
    
    Parameters
    ----------
    pipe : Pipe
      Output pipe to connect
      
    Returns
    -------
    Pipe
      The connected pipe (enables method chaining)
    """
    # Solo permite 1 salida
    if len(self.outputs.keys()) == 0:
      self.outputs[pipe.get_name()] = pipe
      pipe.set_origin(self)
      return pipe
    else:
      raise ValueError(f"GCPBigQueryOrigin '{self.name}' can only have 1 output")
  
  def _initialize_client(self):
    """
    Inicializa el cliente de BigQuery con o sin credenciales
    """
    try:
      if self.credentials_path:
        # Usar service account file
        print(f"GCPBigQueryOrigin '{self.name}' using credentials from: {self.credentials_path}")
        credentials = service_account.Credentials.from_service_account_file(self.credentials_path)
        self.client = bigquery.Client(
          project=self.project_id, 
          credentials=credentials,
          location=self.location
        )
      else:
        # Usar default credentials
        print(f"GCPBigQueryOrigin '{self.name}' using default credentials")
        self.client = bigquery.Client(
          project=self.project_id,
          location=self.location
        )
      
      print(f"GCPBigQueryOrigin '{self.name}' BigQuery client initialized successfully")
      if self.location:
        print(f"  - Location: {self.location}")
      
    except Exception as e:
      raise ValueError(f"GCPBigQueryOrigin '{self.name}' failed to initialize BigQuery client: {str(e)}")
  
  def _execute_query(self, sql_query: str, description: str) -> None:
    """
    Ejecuta una query arbitraria (para before_query y after_query)
    
    Parameters
    ----------
    sql_query : str
      SQL query to execute
    description : str
      Description for logging (e.g., "before_query", "after_query")
    """
    try:
      print(f"\nGCPBigQueryOrigin '{self.name}' executing {description}...")
      print(f"  Query preview: {sql_query[:200]}{'...' if len(sql_query) > 200 else ''}")
      
      # Crear job config básico
      job_config = bigquery.QueryJobConfig()
      job_config.use_legacy_sql = self.use_legacy_sql
      
      # Ejecutar la query
      query_job = self.client.query(sql_query, job_config=job_config)
      
      # Esperar a que termine
      if self.timeout:
        query_job.result(timeout=self.timeout)
      else:
        query_job.result()
      
      print(f"✅ GCPBigQueryOrigin '{self.name}' {description} executed successfully")
      
      # Mostrar estadísticas si están disponibles
      if query_job.total_bytes_processed:
        print(f"  - Bytes processed: {query_job.total_bytes_processed:,} bytes ({query_job.total_bytes_processed / (1024**2):.2f} MB)")
      
      if hasattr(query_job, 'num_dml_affected_rows') and query_job.num_dml_affected_rows is not None:
        print(f"  - Rows affected: {query_job.num_dml_affected_rows:,}")
      
      if query_job.ended and query_job.created:
        duration = (query_job.ended - query_job.created).total_seconds()
        print(f"  - Duration: {duration:.2f}s")
      
    except bigquery.exceptions.BadRequest as e:
      print(f"❌ Error: GCPBigQueryOrigin '{self.name}' invalid query in {description}: {str(e)}")
      print(f"Query: {sql_query}")
      raise
    except Exception as e:
      print(f"❌ Error: GCPBigQueryOrigin '{self.name}' failed to execute {description}: {str(e)}")
      print(f"Query: {sql_query[:500]}{'...' if len(sql_query) > 500 else ''}")
      raise
  
  def _build_query(self) -> str:
    """
    Construye la query final (desde tabla o query directa)
    """
    if self.table:
      # Construir query desde nombre de tabla
      # Soporta formatos: 'dataset.table' o 'project.dataset.table'
      parts = self.table.split('.')
      
      if len(parts) == 2:
        # dataset.table
        table_ref = f"`{self.project_id}.{self.table}`"
      elif len(parts) == 3:
        # project.dataset.table
        table_ref = f"`{self.table}`"
      else:
        raise ValueError(f"GCPBigQueryOrigin '{self.name}': invalid table format '{self.table}'. Use 'dataset.table' or 'project.dataset.table'")
      
      # Construir SELECT *
      query = f"SELECT * FROM {table_ref}"
      
      # Agregar LIMIT si se especificó max_results
      if self.max_results:
        query += f" LIMIT {self.max_results}"
      
      return query
    else:
      # Usar query proporcionada
      query = self.query
      
      # Si hay max_results y la query no tiene LIMIT, agregarlo
      if self.max_results and 'LIMIT' not in query.upper():
        query += f" LIMIT {self.max_results}"
      
      return query
  
  def _create_job_config(self) -> bigquery.QueryJobConfig:
    """
    Crea la configuración del job de BigQuery
    """
    job_config = bigquery.QueryJobConfig()
    
    # Legacy SQL
    job_config.use_legacy_sql = self.use_legacy_sql
    
    # Query parameters
    if self.query_parameters:
      job_config.query_parameters = self.query_parameters
    
    # Job labels
    if self.job_labels:
      job_config.labels = self.job_labels
    
    # Query cache
    job_config.use_query_cache = self.use_query_cache
    
    # Dry run
    if self.dry_run:
      job_config.dry_run = True
    
    return job_config
  
  def _estimate_cost(self, total_bytes_processed: int) -> float:
    """
    Estima el costo de la query basado en bytes procesados
    BigQuery pricing: $6.25 per TB (as of 2024)
    """
    bytes_per_tb = 1024 ** 4  # 1 TB in bytes
    cost_per_tb = 6.25
    
    cost = (total_bytes_processed / bytes_per_tb) * cost_per_tb
    return cost
  
  def pump(self) -> None:
    """
    Execute query and pump data through output pipe
    
    Execution flow:
    1. Initialize BigQuery client
    2. Execute before_query (if provided)
    3. Execute main extraction query
    4. Execute after_query (if provided)
    5. Pump data through output pipe
    """
    try:
      # Inicializar cliente si no existe
      if self.client is None:
        self._initialize_client()
      
      # ================================================================
      # STEP 1: BEFORE QUERY (Pre-processing)
      # ================================================================
      if self.before_query:
        if self.dry_run:
          print(f"\n⚠️  Skipping before_query execution (dry run mode)")
        else:
          self._execute_query(self.before_query, "before_query")
      
      # ================================================================
      # STEP 2: MAIN EXTRACTION QUERY
      # ================================================================
      
      # Construir query
      final_query = self._build_query()
      
      print(f"\n{'='*70}")
      print(f"GCPBigQueryOrigin '{self.name}' executing MAIN extraction query...")
      print(f"{'='*70}")
      print(f"  - Project ID: {self.project_id}")
      if self.table:
        print(f"  - Table: {self.table}")
      print(f"  - Query: {final_query[:150]}{'...' if len(final_query) > 150 else ''}")
      if self.max_results:
        print(f"  - Max results: {self.max_results:,}")
      if self.use_legacy_sql:
        print(f"  - Using legacy SQL")
      if self.dry_run:
        print(f"  - DRY RUN MODE: Query will not be executed")
      
      # Crear job config
      job_config = self._create_job_config()
      
      # Ejecutar la query
      query_job = self.client.query(final_query, job_config=job_config)
      
      # Si es dry run, solo mostrar estadísticas
      if self.dry_run:
        print(f"\n{'='*70}")
        print(f"GCPBigQueryOrigin '{self.name}' DRY RUN results:")
        print(f"{'='*70}")
        print(f"  ✅ Query is valid")
        print(f"  📊 Estimated bytes processed: {query_job.total_bytes_processed:,} bytes ({query_job.total_bytes_processed / (1024**3):.2f} GB)")
        
        estimated_cost = self._estimate_cost(query_job.total_bytes_processed)
        print(f"  💰 Estimated cost: ${estimated_cost:.6f} USD")
        
        if query_job.total_bytes_processed == 0:
          print(f"  ⚡ Query will use cached results (no cost)")
        
        print(f"\n⚠️  No data was extracted (dry run mode)")
        print(f"⚠️  before_query and after_query were NOT executed (dry run mode)")
        return
      
      # Esperar a que termine y obtener resultados
      print(f"\nGCPBigQueryOrigin '{self.name}' waiting for query completion...")
      
      if self.timeout:
        print(f"  - Timeout: {self.timeout}s")
        job_result = query_job.result(timeout=self.timeout)
      else:
        job_result = query_job.result()
      
      # Convertir a DataFrame
      df = job_result.to_dataframe()
      
      # Obtener estadísticas del job
      total_bytes_processed = query_job.total_bytes_processed
      total_bytes_billed = query_job.total_bytes_billed
      cache_hit = query_job.cache_hit
      
      print(f"\n{'='*70}")
      print(f"GCPBigQueryOrigin '{self.name}' MAIN query results:")
      print(f"{'='*70}")
      print(f"  📊 Results:")
      print(f"     - Rows returned: {len(df):,}")
      print(f"     - Columns: {len(df.columns)}")
      print(f"     - Column names: {list(df.columns)}")
      
      print(f"  💾 Data processed:")
      print(f"     - Bytes processed: {total_bytes_processed:,} bytes ({total_bytes_processed / (1024**3):.2f} GB)")
      print(f"     - Bytes billed: {total_bytes_billed:,} bytes ({total_bytes_billed / (1024**3):.2f} GB)")
      
      if cache_hit:
        print(f"  ⚡ Cache hit: Yes (no cost incurred)")
      else:
        estimated_cost = self._estimate_cost(total_bytes_billed)
        print(f"  💰 Estimated cost: ${estimated_cost:.6f} USD")
      
      print(f"  ⏱️  Job info:")
      print(f"     - Job ID: {query_job.job_id}")
      if query_job.ended:
        duration = (query_job.ended - query_job.created).total_seconds()
        print(f"     - Duration: {duration:.2f}s")
      
      print(f"  📋 Data types:")
      for col, dtype in df.dtypes.items():
        print(f"     - {col}: {dtype}")
      
      # ================================================================
      # STEP 3: AFTER QUERY (Post-processing)
      # ================================================================
      if self.after_query:
        self._execute_query(self.after_query, "after_query")
      
      # ================================================================
      # STEP 4: PUMP DATA
      # ================================================================
      
      # Verificar que tenemos una salida configurada
      if len(self.outputs) > 0:
        output_pipe = list(self.outputs.values())[0]
        output_pipe.flow(df)
        print(f"\n{'='*70}")
        print(f"✅ GCPBigQueryOrigin '{self.name}' pumped data through pipe '{output_pipe.get_name()}'")
        print(f"{'='*70}")
      else:
        print(f"\n{'='*70}")
        print(f"⚠️  Warning: GCPBigQueryOrigin '{self.name}' has no output pipe configured")
        print(f"{'='*70}")
        
    except bigquery.exceptions.BadRequest as e:
      print(f"\n{'='*70}")
      print(f"❌ Error: GCPBigQueryOrigin '{self.name}' invalid query: {str(e)}")
      print(f"{'='*70}")
      print(f"Query: {final_query}")
      raise
    except bigquery.exceptions.NotFound as e:
      print(f"\n{'='*70}")
      print(f"❌ Error: GCPBigQueryOrigin '{self.name}' resource not found: {str(e)}")
      print(f"{'='*70}")
      print(f"Check if project '{self.project_id}' exists and you have access")
      if self.table:
        print(f"Check if table '{self.table}' exists")
      raise
    except bigquery.exceptions.Forbidden as e:
      print(f"\n{'='*70}")
      print(f"❌ Error: GCPBigQueryOrigin '{self.name}' permission denied: {str(e)}")
      print(f"{'='*70}")
      print(f"Check if you have BigQuery permissions for project '{self.project_id}'")
      raise
    except bigquery.exceptions.GoogleCloudError as e:
      print(f"\n{'='*70}")
      print(f"❌ Error: GCPBigQueryOrigin '{self.name}' BigQuery error: {str(e)}")
      print(f"{'='*70}")
      raise
    except Exception as e:
      print(f"\n{'='*70}")
      print(f"❌ Error: GCPBigQueryOrigin '{self.name}' unexpected error: {str(e)}")
      print(f"{'='*70}")
      print(f"  - Project: {self.project_id}")
      if self.credentials_path:
        print(f"  - Credentials path: {self.credentials_path}")
      if self.table:
        print(f"  - Table: {self.table}")
      else:
        print(f"  - Query: {self.query[:200]}{'...' if len(self.query) > 200 else ''}")
      raise

      
# src/google/cloud.py (continuación)

class GCPBigQueryDestination(Destination):
  """
  GCPBigQueryDestination - Enhanced BigQuery Data Sink
  =====================================================
  Writes data to Google BigQuery with advanced features including
  pre/post query execution.
  
  Connectivity: 1 → 0 (inherits from Destination)
  
  Parameters
  ----------
  name : str
    Component name
  project_id : str
    GCP project ID
  dataset : str
    BigQuery dataset name
  table : str
    Table name
  write_disposition : str
    Write mode: 'WRITE_TRUNCATE', 'WRITE_APPEND', or 'WRITE_EMPTY'
  credentials_path : str, optional
    Path to service account JSON file (uses default credentials if None)
  before_query : str, optional
    SQL query to execute BEFORE loading data.
    Useful for: truncating tables, creating staging, data preparation.
    Default: None
  after_query : str, optional
    SQL query to execute AFTER loading data.
    Useful for: audit logging, post-processing, triggers, cleanup.
    Default: None
  schema : list, optional
    BigQuery schema (list of SchemaField objects). Auto-detected if None.
  create_disposition : str, default='CREATE_IF_NEEDED'
    Table creation: 'CREATE_IF_NEEDED' or 'CREATE_NEVER'
  schema_update_options : list, optional
    Schema update options: ['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION']
  clustering_fields : list, optional
    Fields to use for clustering (max 4 fields)
  time_partitioning : dict, optional
    Time partitioning config: {'type': 'DAY', 'field': 'date_column'}
  location : str, optional
    BigQuery location (e.g., 'US', 'EU', 'asia-northeast1')
  job_labels : dict, optional
    Labels to attach to the BigQuery job
  max_bad_records : int, default=0
    Maximum number of bad records to tolerate
  autodetect : bool, default=True
    Auto-detect schema from DataFrame
  
  Examples
  --------
  # Example 1: Simple load
  >>> dest = GCPBigQueryDestination(
  ...     name="sales_loader",
  ...     project_id="my-project",
  ...     dataset="analytics",
  ...     table="sales",
  ...     write_disposition="WRITE_APPEND"
  ... )
  
  # Example 2: With before_query (truncate before load)
  >>> dest = GCPBigQueryDestination(
  ...     name="daily_load",
  ...     project_id="my-project",
  ...     dataset="reports",
  ...     table="daily_summary",
  ...     before_query='''
  ...         TRUNCATE TABLE `my-project.reports.daily_summary`
  ...     ''',
  ...     write_disposition="WRITE_APPEND"
  ... )
  
  # Example 3: With after_query (audit logging)
  >>> dest = GCPBigQueryDestination(
  ...     name="customer_loader",
  ...     project_id="my-project",
  ...     dataset="crm",
  ...     table="customers",
  ...     write_disposition="WRITE_APPEND",
  ...     after_query='''
  ...         INSERT INTO `my-project.audit.load_log` (
  ...             table_name,
  ...             loaded_at,
  ...             record_count
  ...         ) VALUES (
  ...             'customers',
  ...             CURRENT_TIMESTAMP(),
  ...             (SELECT COUNT(*) FROM `my-project.crm.customers`)
  ...         )
  ...     '''
  ... )
  
  # Example 4: Partitioned and clustered table
  >>> dest = GCPBigQueryDestination(
  ...     name="events_loader",
  ...     project_id="my-project",
  ...     dataset="analytics",
  ...     table="events",
  ...     write_disposition="WRITE_APPEND",
  ...     time_partitioning={
  ...         'type': 'DAY',
  ...         'field': 'event_date'
  ...     },
  ...     clustering_fields=['user_id', 'event_type']
  ... )
  
  # Example 5: Complete workflow with before and after
  >>> dest = GCPBigQueryDestination(
  ...     name="processed_orders",
  ...     project_id="my-project",
  ...     dataset="warehouse",
  ...     table="orders",
  ...     before_query='''
  ...         -- Create backup before load
  ...         CREATE OR REPLACE TABLE `my-project.warehouse.orders_backup` AS
  ...         SELECT * FROM `my-project.warehouse.orders`;
  ...         
  ...         -- Prepare staging
  ...         TRUNCATE TABLE `my-project.staging.temp_orders`;
  ...     ''',
  ...     write_disposition="WRITE_TRUNCATE",
  ...     after_query='''
  ...         -- Update metadata
  ...         UPDATE `my-project.warehouse.table_metadata`
  ...         SET 
  ...             last_updated = CURRENT_TIMESTAMP(),
  ...             row_count = (SELECT COUNT(*) FROM `my-project.warehouse.orders`)
  ...         WHERE table_name = 'orders';
  ...         
  ...         -- Run data quality checks
  ...         CALL `my-project.procedures.validate_orders`();
  ...     '''
  ... )
  """
  
  def __init__(
    self,
    name: str,
    project_id: str,
    dataset: str,
    table: str,
    write_disposition: str,
    credentials_path: Optional[str] = None,
    before_query: Optional[str] = None,
    after_query: Optional[str] = None,
    schema: Optional[List] = None,
    create_disposition: str = 'CREATE_IF_NEEDED',
    schema_update_options: Optional[List[str]] = None,
    clustering_fields: Optional[List[str]] = None,
    time_partitioning: Optional[Dict] = None,
    location: Optional[str] = None,
    job_labels: Optional[Dict[str, str]] = None,
    max_bad_records: int = 0,
    autodetect: bool = True
  ):
    super().__init__()
    self.name = name
    self.project_id = project_id
    self.dataset = dataset
    self.table = table
    self.write_disposition = write_disposition
    self.credentials_path = credentials_path
    self.before_query = before_query
    self.after_query = after_query
    self.schema = schema
    self.create_disposition = create_disposition
    self.schema_update_options = schema_update_options or []
    self.clustering_fields = clustering_fields
    self.time_partitioning = time_partitioning
    self.location = location
    self.job_labels = job_labels or {}
    self.max_bad_records = max_bad_records
    self.autodetect = autodetect
    self.client = None
    
    # Validar que project_id no esté vacío
    if not project_id or not project_id.strip():
      raise ValueError(f"GCPBigQueryDestination '{self.name}': project_id cannot be empty")
    
    # Validar que dataset no esté vacío
    if not dataset or not dataset.strip():
      raise ValueError(f"GCPBigQueryDestination '{self.name}': dataset cannot be empty")
    
    # Validar que table no esté vacío
    if not table or not table.strip():
      raise ValueError(f"GCPBigQueryDestination '{self.name}': table cannot be empty")
    
    # Validar write_disposition
    valid_dispositions = ['WRITE_TRUNCATE', 'WRITE_APPEND', 'WRITE_EMPTY']
    if write_disposition not in valid_dispositions:
      raise ValueError(f"GCPBigQueryDestination '{self.name}': write_disposition must be one of {valid_dispositions}, got '{write_disposition}'")
    
    # Validar create_disposition
    valid_create = ['CREATE_IF_NEEDED', 'CREATE_NEVER']
    if create_disposition not in valid_create:
      raise ValueError(f"GCPBigQueryDestination '{self.name}': create_disposition must be one of {valid_create}, got '{create_disposition}'")
    
    # Validar before_query si se proporciona
    if before_query is not None and (not before_query.strip()):
      raise ValueError(f"GCPBigQueryDestination '{self.name}': before_query cannot be empty string")
    
    # Validar after_query si se proporciona
    if after_query is not None and (not after_query.strip()):
      raise ValueError(f"GCPBigQueryDestination '{self.name}': after_query cannot be empty string")
    
    # Validar clustering_fields (máximo 4)
    if clustering_fields and len(clustering_fields) > 4:
      raise ValueError(f"GCPBigQueryDestination '{self.name}': clustering_fields cannot have more than 4 fields, got {len(clustering_fields)}")
    
    # Validar max_bad_records
    if max_bad_records < 0:
      raise ValueError(f"GCPBigQueryDestination '{self.name}': max_bad_records must be >= 0, got {max_bad_records}")
  
  def add_input_pipe(self, pipe: Pipe) -> None:
    """
    Add input pipe (only 1 allowed by default)
    
    Parameters
    ----------
    pipe : Pipe
      Input pipe to connect
    """
    # Solo permite 1 entrada
    if len(self.inputs.keys()) == 0:
      self.inputs[pipe.get_name()] = pipe
    else:
      raise ValueError(f"GCPBigQueryDestination '{self.name}' can only have 1 input")
  
  def _initialize_client(self):
    """
    Inicializa el cliente de BigQuery con o sin credenciales
    """
    try:
      if self.credentials_path:
        # Usar service account file
        print(f"GCPBigQueryDestination '{self.name}' using credentials from: {self.credentials_path}")
        credentials = service_account.Credentials.from_service_account_file(self.credentials_path)
        self.client = bigquery.Client(
          project=self.project_id, 
          credentials=credentials,
          location=self.location
        )
      else:
        # Usar default credentials
        print(f"GCPBigQueryDestination '{self.name}' using default credentials")
        self.client = bigquery.Client(
          project=self.project_id,
          location=self.location
        )
      
      print(f"GCPBigQueryDestination '{self.name}' BigQuery client initialized successfully")
      if self.location:
        print(f"  - Location: {self.location}")
      
    except Exception as e:
      raise ValueError(f"GCPBigQueryDestination '{self.name}' failed to initialize BigQuery client: {str(e)}")
  
  def _execute_query(self, sql_query: str, description: str) -> None:
    """
    Ejecuta una query arbitraria (para before_query y after_query)
    
    Parameters
    ----------
    sql_query : str
      SQL query to execute
    description : str
      Description for logging (e.g., "before_query", "after_query")
    """
    try:
      print(f"\nGCPBigQueryDestination '{self.name}' executing {description}...")
      print(f"  Query preview: {sql_query[:200]}{'...' if len(sql_query) > 200 else ''}")
      
      # Crear job config básico
      job_config = bigquery.QueryJobConfig()
      
      # Ejecutar la query
      query_job = self.client.query(sql_query, job_config=job_config)
      
      # Esperar a que termine
      query_job.result()
      
      print(f"✅ GCPBigQueryDestination '{self.name}' {description} executed successfully")
      
      # Mostrar estadísticas si están disponibles
      if query_job.total_bytes_processed:
        print(f"  - Bytes processed: {query_job.total_bytes_processed:,} bytes ({query_job.total_bytes_processed / (1024**2):.2f} MB)")
      
      if hasattr(query_job, 'num_dml_affected_rows') and query_job.num_dml_affected_rows is not None:
        print(f"  - Rows affected: {query_job.num_dml_affected_rows:,}")
      
      if query_job.ended and query_job.created:
        duration = (query_job.ended - query_job.created).total_seconds()
        print(f"  - Duration: {duration:.2f}s")
      
    except bigquery.exceptions.BadRequest as e:
      print(f"❌ Error: GCPBigQueryDestination '{self.name}' invalid query in {description}: {str(e)}")
      print(f"Query: {sql_query}")
      raise
    except Exception as e:
      print(f"❌ Error: GCPBigQueryDestination '{self.name}' failed to execute {description}: {str(e)}")
      print(f"Query: {sql_query[:500]}{'...' if len(sql_query) > 500 else ''}")
      raise
  
  def _create_job_config(self) -> bigquery.LoadJobConfig:
    """
    Crea la configuración del job de carga
    """
    job_config = bigquery.LoadJobConfig()
    
    # Write disposition
    job_config.write_disposition = self.write_disposition
    
    # Create disposition
    job_config.create_disposition = self.create_disposition
    
    # Schema
    if self.schema:
      job_config.schema = self.schema
      job_config.autodetect = False
    else:
      job_config.autodetect = self.autodetect
    
    # Schema update options
    if self.schema_update_options:
      job_config.schema_update_options = self.schema_update_options
    
    # Clustering
    if self.clustering_fields:
      job_config.clustering_fields = self.clustering_fields
    
    # Time partitioning
    if self.time_partitioning:
      partition_type = self.time_partitioning.get('type', 'DAY')
      partition_field = self.time_partitioning.get('field')
      
      if partition_field:
        job_config.time_partitioning = bigquery.TimePartitioning(
          type_=partition_type,
          field=partition_field
        )
      else:
        job_config.time_partitioning = bigquery.TimePartitioning(type_=partition_type)
    
    # Job labels
    if self.job_labels:
      job_config.labels = self.job_labels
    
    # Max bad records
    job_config.max_bad_records = self.max_bad_records
    
    return job_config
  
  def sink(self, data_package: DataPackage) -> None:
    """
    Load data to BigQuery
    
    Execution flow:
    1. Initialize BigQuery client
    2. Execute before_query (if provided)
    3. Load DataFrame to BigQuery
    4. Execute after_query (if provided)
    """
    print(f"GCPBigQueryDestination '{self.name}' received data from pipe: '{data_package.get_pipe_name()}'")
    df = data_package.get_df()
    
    try:
      # Inicializar cliente si no existe
      if self.client is None:
        self._initialize_client()
      
      # ================================================================
      # STEP 1: BEFORE QUERY (Pre-processing)
      # ================================================================
      if self.before_query:
        self._execute_query(self.before_query, "before_query")
      
      # ================================================================
      # STEP 2: LOAD DATA TO BIGQUERY
      # ================================================================
      
      # Construir la referencia completa de la tabla
      table_id = f"{self.project_id}.{self.dataset}.{self.table}"
      
      print(f"\n{'='*70}")
      print(f"GCPBigQueryDestination '{self.name}' loading data to BigQuery...")
      print(f"{'='*70}")
      print(f"  - Table: {table_id}")
      print(f"  - Write disposition: {self.write_disposition}")
      print(f"  - Create disposition: {self.create_disposition}")
      print(f"  - DataFrame shape: {df.shape}")
      print(f"  - DataFrame columns: {list(df.columns)}")
      if self.clustering_fields:
        print(f"  - Clustering fields: {self.clustering_fields}")
      if self.time_partitioning:
        print(f"  - Time partitioning: {self.time_partitioning}")
      if self.schema_update_options:
        print(f"  - Schema update options: {self.schema_update_options}")
      
      # Configurar el job de carga
      job_config = self._create_job_config()
      
      # Cargar DataFrame a BigQuery
      print(f"\nGCPBigQueryDestination '{self.name}' starting load job...")
      job = self.client.load_table_from_dataframe(
        df, 
        table_id, 
        job_config=job_config
      )
      
      # Esperar a que termine el job
      print(f"GCPBigQueryDestination '{self.name}' waiting for load job completion...")
      job.result()  # Espera a que termine
      
      # Obtener información de la tabla después de la carga
      table = self.client.get_table(table_id)
      
      print(f"\n{'='*70}")
      print(f"GCPBigQueryDestination '{self.name}' LOAD completed successfully:")
      print(f"{'='*70}")
      print(f"  📊 Load results:")
      print(f"     - Rows loaded: {len(df):,}")
      print(f"     - Total rows in table: {table.num_rows:,}")
      print(f"     - Table schema fields: {len(table.schema)}")
      
      print(f"  ⏱️  Job info:")
      print(f"     - Job ID: {job.job_id}")
      if job.ended and job.created:
        duration = (job.ended - job.created).total_seconds()
        print(f"     - Duration: {duration:.2f}s")
      
      if hasattr(job, 'output_rows') and job.output_rows:
        print(f"     - Output rows: {job.output_rows:,}")
      
      print(f"  📋 Table info:")
      print(f"     - Created: {table.created}")
      print(f"     - Modified: {table.modified}")
      if table.clustering_fields:
        print(f"     - Clustering: {table.clustering_fields}")
      if table.time_partitioning:
        print(f"     - Partitioning: {table.time_partitioning.type_}")
      
      # Mostrar errores si los hay
      if job.errors:
        print(f"\n⚠️  Warnings/Errors during load:")
        for error in job.errors:
          print(f"     - {error}")
      
      # ================================================================
      # STEP 3: AFTER QUERY (Post-processing)
      # ================================================================
      if self.after_query:
        self._execute_query(self.after_query, "after_query")
      
      print(f"\n{'='*70}")
      print(f"✅ GCPBigQueryDestination '{self.name}' completed successfully")
      print(f"{'='*70}")
      
    except Exception as e:
      # Verificar tipos específicos de error
      error_msg = str(e).lower()
      
      print(f"\n{'='*70}")
      if "not found" in error_msg:
        print(f"❌ Error: GCPBigQueryDestination '{self.name}' table or dataset not found: {str(e)}")
        print(f"Table: {self.project_id}.{self.dataset}.{self.table}")
        print(f"Check if dataset '{self.dataset}' exists")
      elif "forbidden" in error_msg or "permission" in error_msg:
        print(f"❌ Error: GCPBigQueryDestination '{self.name}' permission denied: {str(e)}")
        print(f"Check if you have write permissions for table '{self.project_id}.{self.dataset}.{self.table}'")
      elif "schema" in error_msg or "field" in error_msg:
        print(f"❌ Error: GCPBigQueryDestination '{self.name}' schema mismatch: {str(e)}")
        print(f"DataFrame columns: {list(df.columns)}")
        print(f"DataFrame dtypes: {dict(df.dtypes)}")
      elif "already exists" in error_msg:
        print(f"❌ Error: GCPBigQueryDestination '{self.name}' table already exists: {str(e)}")
        print(f"Write disposition was: {self.write_disposition}")
      else:
        print(f"❌ Error: GCPBigQueryDestination '{self.name}' unexpected error: {str(e)}")
        print(f"Table: {self.project_id}.{self.dataset}.{self.table}")
        print(f"Credentials path: {self.credentials_path}")
        print(f"DataFrame shape: {df.shape}")
      print(f"{'='*70}")
      raise