# src/google/bigquery.py

import logging
from open_stage.core.base import DataPackage, Origin, Destination, SingleInputMixin, SingleOutputMixin
from google.cloud import bigquery
from google.oauth2 import service_account
from typing import Optional, Dict, List

logger = logging.getLogger(__name__)


class GCPBigQueryOrigin(SingleOutputMixin, Origin):
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
  after_query : str, optional
    SQL query to execute AFTER the main extraction query.
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
    Query timeout in seconds
  use_query_cache : bool, default=True
    Use cached results if available
  dry_run : bool, default=False
    Validate query and estimate cost without executing
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

    if not project_id or not project_id.strip():
      raise ValueError(f"GCPBigQueryOrigin '{self.name}': project_id cannot be empty")
    if query and table:
      raise ValueError(f"GCPBigQueryOrigin '{self.name}': cannot specify both 'query' and 'table'.")
    if not query and not table:
      raise ValueError(f"GCPBigQueryOrigin '{self.name}': must specify either 'query' or 'table'")
    if query and not query.strip():
      raise ValueError(f"GCPBigQueryOrigin '{self.name}': query cannot be empty")
    if table and not table.strip():
      raise ValueError(f"GCPBigQueryOrigin '{self.name}': table cannot be empty")
    if before_query is not None and not before_query.strip():
      raise ValueError(f"GCPBigQueryOrigin '{self.name}': before_query cannot be empty string")
    if after_query is not None and not after_query.strip():
      raise ValueError(f"GCPBigQueryOrigin '{self.name}': after_query cannot be empty string")
    if max_results is not None and max_results <= 0:
      raise ValueError(f"GCPBigQueryOrigin '{self.name}': max_results must be positive, got {max_results}")
    if timeout is not None and timeout <= 0:
      raise ValueError(f"GCPBigQueryOrigin '{self.name}': timeout must be positive, got {timeout}")

  def _initialize_client(self):
    try:
      if self.credentials_path:
        logger.debug("GCPBigQueryOrigin '%s' using credentials from: %s", self.name, self.credentials_path)
        credentials = service_account.Credentials.from_service_account_file(self.credentials_path)
        self.client = bigquery.Client(project=self.project_id, credentials=credentials, location=self.location)
      else:
        logger.debug("GCPBigQueryOrigin '%s' using default credentials", self.name)
        self.client = bigquery.Client(project=self.project_id, location=self.location)
      logger.info("GCPBigQueryOrigin '%s' BigQuery client initialized (project=%s, location=%s)",
                  self.name, self.project_id, self.location)
    except Exception as e:
      raise ValueError(f"GCPBigQueryOrigin '{self.name}' failed to initialize BigQuery client: {str(e)}")

  def _execute_query(self, sql_query: str, description: str) -> None:
    try:
      logger.info("GCPBigQueryOrigin '%s' executing %s", self.name, description)
      logger.debug("Query preview: %s", sql_query[:200])
      job_config = bigquery.QueryJobConfig(use_legacy_sql=self.use_legacy_sql)
      query_job = self.client.query(sql_query, job_config=job_config)
      if self.timeout:
        query_job.result(timeout=self.timeout)
      else:
        query_job.result()
      duration = (query_job.ended - query_job.created).total_seconds() if query_job.ended else None
      logger.info("GCPBigQueryOrigin '%s' %s completed%s",
                  self.name, description, f" in {duration:.2f}s" if duration else "")
      if query_job.total_bytes_processed:
        logger.debug("%s: %s bytes processed, %s rows affected",
                     description,
                     f"{query_job.total_bytes_processed:,}",
                     query_job.num_dml_affected_rows if hasattr(query_job, 'num_dml_affected_rows') else "n/a")
    except bigquery.exceptions.BadRequest as e:
      logger.error("GCPBigQueryOrigin '%s' invalid query in %s: %s", self.name, description, e)
      raise
    except Exception as e:
      logger.error("GCPBigQueryOrigin '%s' failed to execute %s: %s", self.name, description, e)
      raise

  def _build_query(self) -> str:
    if self.table:
      parts = self.table.split('.')
      if len(parts) == 2:
        table_ref = f"`{self.project_id}.{self.table}`"
      elif len(parts) == 3:
        table_ref = f"`{self.table}`"
      else:
        raise ValueError(f"GCPBigQueryOrigin '{self.name}': invalid table format '{self.table}'")
      query = f"SELECT * FROM {table_ref}"
      if self.max_results:
        query += f" LIMIT {self.max_results}"
      return query
    else:
      query = self.query
      if self.max_results and 'LIMIT' not in query.upper():
        query += f" LIMIT {self.max_results}"
      return query

  def _create_job_config(self) -> bigquery.QueryJobConfig:
    job_config = bigquery.QueryJobConfig()
    job_config.use_legacy_sql = self.use_legacy_sql
    if self.query_parameters:
      job_config.query_parameters = self.query_parameters
    if self.job_labels:
      job_config.labels = self.job_labels
    job_config.use_query_cache = self.use_query_cache
    if self.dry_run:
      job_config.dry_run = True
    return job_config

  def _estimate_cost(self, total_bytes_processed: int) -> float:
    return (total_bytes_processed / 1024 ** 4) * 6.25

  def pump(self) -> None:
    try:
      if self.client is None:
        self._initialize_client()

      if self.before_query:
        if self.dry_run:
          logger.warning("GCPBigQueryOrigin '%s' skipping before_query (dry run)", self.name)
        else:
          self._execute_query(self.before_query, "before_query")

      final_query = self._build_query()
      logger.info("GCPBigQueryOrigin '%s' executing main query (project=%s%s%s)",
                  self.name, self.project_id,
                  f", table={self.table}" if self.table else "",
                  f", max_results={self.max_results}" if self.max_results else "")
      logger.debug("Query: %s", final_query[:200])

      job_config = self._create_job_config()
      query_job = self.client.query(final_query, job_config=job_config)

      if self.dry_run:
        logger.info("GCPBigQueryOrigin '%s' DRY RUN — query valid, estimated %.2f GB, cost ~$%.6f USD",
                    self.name,
                    query_job.total_bytes_processed / (1024 ** 3),
                    self._estimate_cost(query_job.total_bytes_processed))
        return

      job_result = query_job.result(timeout=self.timeout) if self.timeout else query_job.result()
      df = job_result.to_dataframe()

      duration = (query_job.ended - query_job.created).total_seconds() if query_job.ended else None
      cache_hit = query_job.cache_hit
      cost_info = "cache hit (no cost)" if cache_hit else f"~${self._estimate_cost(query_job.total_bytes_billed):.6f} USD"
      logger.info("GCPBigQueryOrigin '%s' query returned %d rows, %d columns%s | %s",
                  self.name, len(df), len(df.columns),
                  f" in {duration:.2f}s" if duration else "", cost_info)
      logger.debug("Columns: %s | job_id: %s", list(df.columns), query_job.job_id)

      if self.after_query:
        self._execute_query(self.after_query, "after_query")

      if len(self.outputs) > 0:
        output_pipe = list(self.outputs.values())[0]
        output_pipe.flow(df)
        logger.debug("GCPBigQueryOrigin '%s' pumped data through pipe '%s'", self.name, output_pipe.get_name())
      else:
        logger.warning("GCPBigQueryOrigin '%s' has no output pipe configured", self.name)

    except bigquery.exceptions.BadRequest as e:
      logger.error("GCPBigQueryOrigin '%s' invalid query: %s", self.name, e)
      raise
    except bigquery.exceptions.NotFound as e:
      logger.error("GCPBigQueryOrigin '%s' resource not found: %s (project=%s, table=%s)",
                   self.name, e, self.project_id, self.table)
      raise
    except bigquery.exceptions.Forbidden as e:
      logger.error("GCPBigQueryOrigin '%s' permission denied: %s", self.name, e)
      raise
    except Exception as e:
      logger.error("GCPBigQueryOrigin '%s' failed: %s", self.name, e)
      raise


class GCPBigQueryDestination(SingleInputMixin, Destination):
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
  after_query : str, optional
    SQL query to execute AFTER loading data.
  schema : list, optional
    BigQuery schema (list of SchemaField objects). Auto-detected if None.
  create_disposition : str, default='CREATE_IF_NEEDED'
  schema_update_options : list, optional
  clustering_fields : list, optional
  time_partitioning : dict, optional
  location : str, optional
  job_labels : dict, optional
  max_bad_records : int, default=0
  autodetect : bool, default=True
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

    if not project_id or not project_id.strip():
      raise ValueError(f"GCPBigQueryDestination '{self.name}': project_id cannot be empty")
    if not dataset or not dataset.strip():
      raise ValueError(f"GCPBigQueryDestination '{self.name}': dataset cannot be empty")
    if not table or not table.strip():
      raise ValueError(f"GCPBigQueryDestination '{self.name}': table cannot be empty")
    valid_dispositions = ['WRITE_TRUNCATE', 'WRITE_APPEND', 'WRITE_EMPTY']
    if write_disposition not in valid_dispositions:
      raise ValueError(f"GCPBigQueryDestination '{self.name}': write_disposition must be one of {valid_dispositions}")
    valid_create = ['CREATE_IF_NEEDED', 'CREATE_NEVER']
    if create_disposition not in valid_create:
      raise ValueError(f"GCPBigQueryDestination '{self.name}': create_disposition must be one of {valid_create}")
    if before_query is not None and not before_query.strip():
      raise ValueError(f"GCPBigQueryDestination '{self.name}': before_query cannot be empty string")
    if after_query is not None and not after_query.strip():
      raise ValueError(f"GCPBigQueryDestination '{self.name}': after_query cannot be empty string")
    if clustering_fields and len(clustering_fields) > 4:
      raise ValueError(f"GCPBigQueryDestination '{self.name}': clustering_fields cannot exceed 4 fields")
    if max_bad_records < 0:
      raise ValueError(f"GCPBigQueryDestination '{self.name}': max_bad_records must be >= 0")

  def _initialize_client(self):
    try:
      if self.credentials_path:
        logger.debug("GCPBigQueryDestination '%s' using credentials from: %s", self.name, self.credentials_path)
        credentials = service_account.Credentials.from_service_account_file(self.credentials_path)
        self.client = bigquery.Client(project=self.project_id, credentials=credentials, location=self.location)
      else:
        logger.debug("GCPBigQueryDestination '%s' using default credentials", self.name)
        self.client = bigquery.Client(project=self.project_id, location=self.location)
      logger.info("GCPBigQueryDestination '%s' BigQuery client initialized (project=%s, location=%s)",
                  self.name, self.project_id, self.location)
    except Exception as e:
      raise ValueError(f"GCPBigQueryDestination '{self.name}' failed to initialize BigQuery client: {str(e)}")

  def _execute_query(self, sql_query: str, description: str) -> None:
    try:
      logger.info("GCPBigQueryDestination '%s' executing %s", self.name, description)
      logger.debug("Query preview: %s", sql_query[:200])
      job_config = bigquery.QueryJobConfig()
      query_job = self.client.query(sql_query, job_config=job_config)
      query_job.result()
      duration = (query_job.ended - query_job.created).total_seconds() if query_job.ended else None
      logger.info("GCPBigQueryDestination '%s' %s completed%s",
                  self.name, description, f" in {duration:.2f}s" if duration else "")
    except bigquery.exceptions.BadRequest as e:
      logger.error("GCPBigQueryDestination '%s' invalid query in %s: %s", self.name, description, e)
      raise
    except Exception as e:
      logger.error("GCPBigQueryDestination '%s' failed to execute %s: %s", self.name, description, e)
      raise

  def _create_job_config(self) -> bigquery.LoadJobConfig:
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = self.write_disposition
    job_config.create_disposition = self.create_disposition
    if self.schema:
      job_config.schema = self.schema
      job_config.autodetect = False
    else:
      job_config.autodetect = self.autodetect
    if self.schema_update_options:
      job_config.schema_update_options = self.schema_update_options
    if self.clustering_fields:
      job_config.clustering_fields = self.clustering_fields
    if self.time_partitioning:
      partition_type = self.time_partitioning.get('type', 'DAY')
      partition_field = self.time_partitioning.get('field')
      job_config.time_partitioning = (
        bigquery.TimePartitioning(type_=partition_type, field=partition_field)
        if partition_field else bigquery.TimePartitioning(type_=partition_type)
      )
    if self.job_labels:
      job_config.labels = self.job_labels
    job_config.max_bad_records = self.max_bad_records
    return job_config

  def sink(self, data_package: DataPackage) -> None:
    logger.debug("GCPBigQueryDestination '%s' received data from pipe '%s'",
                 self.name, data_package.get_pipe_name())
    df = data_package.get_df()
    table_id = f"{self.project_id}.{self.dataset}.{self.table}"

    try:
      if self.client is None:
        self._initialize_client()

      if self.before_query:
        self._execute_query(self.before_query, "before_query")

      logger.info("GCPBigQueryDestination '%s' loading %d rows to %s (disposition=%s)",
                  self.name, len(df), table_id, self.write_disposition)
      logger.debug("Columns: %s", list(df.columns))

      job_config = self._create_job_config()
      job = self.client.load_table_from_dataframe(df, table_id, job_config=job_config)
      job.result()

      table = self.client.get_table(table_id)
      duration = (job.ended - job.created).total_seconds() if job.ended else None
      logger.info("GCPBigQueryDestination '%s' load complete: %d rows loaded, %d total in table%s",
                  self.name, len(df), table.num_rows, f" in {duration:.2f}s" if duration else "")
      logger.debug("job_id: %s", job.job_id)

      if job.errors:
        for error in job.errors:
          logger.warning("GCPBigQueryDestination '%s' load warning: %s", self.name, error)

      if self.after_query:
        self._execute_query(self.after_query, "after_query")

    except Exception as e:
      error_msg = str(e).lower()
      if "not found" in error_msg:
        logger.error("GCPBigQueryDestination '%s' table/dataset not found: %s (table=%s)",
                     self.name, e, table_id)
      elif "forbidden" in error_msg or "permission" in error_msg:
        logger.error("GCPBigQueryDestination '%s' permission denied: %s", self.name, e)
      elif "schema" in error_msg or "field" in error_msg:
        logger.error("GCPBigQueryDestination '%s' schema mismatch: %s | columns=%s",
                     self.name, e, list(df.columns))
      else:
        logger.error("GCPBigQueryDestination '%s' failed: %s (table=%s)", self.name, e, table_id)
      raise
