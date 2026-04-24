# src/postgres/common.py

import logging
from sqlalchemy import create_engine, text
import pandas as pd
from open_stage.core.base import Origin, Pipe, Destination, DataPackage, SingleInputMixin, SingleOutputMixin
from typing import Optional, List, Dict
import time

logger = logging.getLogger(__name__)


class PostgresOrigin(SingleOutputMixin, Origin):
  """
  PostgresOrigin - Enhanced PostgreSQL Data Source
  ================================================
  Reads data from PostgreSQL databases with advanced features including
  pre/post query execution.

  Connectivity: 0 → 1 (inherits from Origin)

  Parameters
  ----------
  name : str
    Component name
  host : str
    PostgreSQL host
  port : int, default=5432
  database : str
  user : str
  password : str
  query : str, optional
    SQL query to execute (required if table is not provided)
  table : str, optional
    Table reference in format 'table' or 'schema.table'
  before_query : str, optional
    SQL to execute BEFORE extraction (temp tables, session vars, etc.)
  after_query : str, optional
    SQL to execute AFTER extraction (audit logging, cleanup, etc.)
  max_results : int, optional
  timeout : float, optional
  query_parameters : dict, optional
  """

  def __init__(
    self,
    name: str,
    host: str,
    port: int = 5432,
    database: str = None,
    user: str = None,
    password: str = None,
    query: Optional[str] = None,
    table: Optional[str] = None,
    before_query: Optional[str] = None,
    after_query: Optional[str] = None,
    max_results: Optional[int] = None,
    timeout: Optional[float] = None,
    query_parameters: Optional[Dict] = None
  ):
    super().__init__()
    self.name = name
    self.host = host
    self.port = port
    self.database = database
    self.user = user
    self.password = password
    self.query = query
    self.table = table
    self.before_query = before_query
    self.after_query = after_query
    self.max_results = max_results
    self.timeout = timeout
    self.query_parameters = query_parameters or {}
    self.engine = None

    if not host or not host.strip():
      raise ValueError(f"PostgresOrigin '{self.name}': host cannot be empty")
    if not database or not database.strip():
      raise ValueError(f"PostgresOrigin '{self.name}': database cannot be empty")
    if not user or not user.strip():
      raise ValueError(f"PostgresOrigin '{self.name}': user cannot be empty")
    if not password or not password.strip():
      raise ValueError(f"PostgresOrigin '{self.name}': password cannot be empty")
    if query and table:
      raise ValueError(f"PostgresOrigin '{self.name}': cannot specify both 'query' and 'table'.")
    if not query and not table:
      raise ValueError(f"PostgresOrigin '{self.name}': must specify either 'query' or 'table'")
    if query and not query.strip():
      raise ValueError(f"PostgresOrigin '{self.name}': query cannot be empty")
    if table and not table.strip():
      raise ValueError(f"PostgresOrigin '{self.name}': table cannot be empty")
    if before_query is not None and not before_query.strip():
      raise ValueError(f"PostgresOrigin '{self.name}': before_query cannot be empty string")
    if after_query is not None and not after_query.strip():
      raise ValueError(f"PostgresOrigin '{self.name}': after_query cannot be empty string")
    if port <= 0:
      raise ValueError(f"PostgresOrigin '{self.name}': port must be positive, got {port}")
    if max_results is not None and max_results <= 0:
      raise ValueError(f"PostgresOrigin '{self.name}': max_results must be positive, got {max_results}")
    if timeout is not None and timeout <= 0:
      raise ValueError(f"PostgresOrigin '{self.name}': timeout must be positive, got {timeout}")

  def _create_connection_string(self):
    return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

  def _initialize_engine(self):
    try:
      connect_args = {}
      if self.timeout:
        connect_args['connect_timeout'] = int(self.timeout)
      self.engine = create_engine(self._create_connection_string(), connect_args=connect_args)
      logger.info("PostgresOrigin '%s' connected to %s@%s:%s/%s",
                  self.name, self.user, self.host, self.port, self.database)
    except Exception as e:
      raise ValueError(f"PostgresOrigin '{self.name}' failed to initialize engine: {str(e)}")

  def _execute_query(self, sql_query: str, description: str) -> None:
    try:
      logger.info("PostgresOrigin '%s' executing %s", self.name, description)
      logger.debug("Query preview: %s", sql_query[:200])
      start_time = time.time()
      with self.engine.connect() as connection:
        result = connection.execute(text(sql_query))
        connection.commit()
        duration = time.time() - start_time
        logger.info("PostgresOrigin '%s' %s completed in %.2fs%s",
                    self.name, description, duration,
                    f" ({result.rowcount:,} rows affected)" if result.rowcount >= 0 else "")
    except Exception as e:
      logger.error("PostgresOrigin '%s' failed to execute %s: %s", self.name, description, e)
      raise

  def _build_query(self) -> str:
    if self.table:
      parts = self.table.split('.')
      if len(parts) == 1:
        table_ref = f'"{self.table}"'
      elif len(parts) == 2:
        schema, table = parts
        table_ref = f'"{schema}"."{table}"'
      else:
        raise ValueError(f"PostgresOrigin '{self.name}': invalid table format '{self.table}'")
      query = f"SELECT * FROM {table_ref}"
      if self.max_results:
        query += f" LIMIT {self.max_results}"
      return query
    else:
      query = self.query
      if self.max_results and 'LIMIT' not in query.upper():
        query += f" LIMIT {self.max_results}"
      return query

  def pump(self) -> None:
    try:
      if self.engine is None:
        self._initialize_engine()

      if self.before_query:
        self._execute_query(self.before_query, "before_query")

      final_query = self._build_query()
      logger.info("PostgresOrigin '%s' executing main query (db=%s%s%s)",
                  self.name, self.database,
                  f", table={self.table}" if self.table else "",
                  f", max_results={self.max_results}" if self.max_results else "")
      logger.debug("Query: %s", final_query[:200])

      start_time = time.time()
      if self.query_parameters:
        df = pd.read_sql(text(final_query), self.engine, params=self.query_parameters)
      else:
        df = pd.read_sql(final_query, self.engine)
      duration = time.time() - start_time

      logger.info("PostgresOrigin '%s' query returned %d rows, %d columns in %.2fs",
                  self.name, len(df), len(df.columns), duration)
      logger.debug("Columns: %s", list(df.columns))

      if self.after_query:
        self._execute_query(self.after_query, "after_query")

      if len(self.outputs) > 0:
        output_pipe = list(self.outputs.values())[0]
        output_pipe.flow(df)
        logger.debug("PostgresOrigin '%s' pumped data through pipe '%s'", self.name, output_pipe.get_name())
      else:
        logger.warning("PostgresOrigin '%s' has no output pipe configured", self.name)

    except Exception as e:
      error_msg = str(e).lower()
      if "could not connect" in error_msg or "connection refused" in error_msg:
        logger.error("PostgresOrigin '%s' connection failed — is server running at %s:%s? %s",
                     self.name, self.host, self.port, e)
      elif "password authentication failed" in error_msg or "authentication" in error_msg:
        logger.error("PostgresOrigin '%s' authentication failed (user='%s'): %s", self.name, self.user, e)
      elif "database" in error_msg and "does not exist" in error_msg:
        logger.error("PostgresOrigin '%s' database '%s' not found: %s", self.name, self.database, e)
      elif "timeout" in error_msg or "timed out" in error_msg:
        logger.error("PostgresOrigin '%s' query timed out (timeout=%ss): %s", self.name, self.timeout, e)
      else:
        logger.error("PostgresOrigin '%s' failed: %s", self.name, e)
      raise

    finally:
      if self.engine is not None:
        self.engine.dispose()
        logger.debug("PostgresOrigin '%s' connection closed", self.name)


class PostgresDestination(SingleInputMixin, Destination):
  """
  PostgresDestination - Enhanced PostgreSQL Data Sink
  ====================================================
  Writes data to PostgreSQL databases with advanced features including
  pre/post query execution.

  Connectivity: 1 → 0 (inherits from Destination)

  Parameters
  ----------
  name : str
  host : str
  port : int, default=5432
  database : str
  user : str
  password : str
  table : str
  schema : str, default='public'
  if_exists : str, default='append'
    Write mode: 'fail', 'replace', or 'append'
  before_query : str, optional
  after_query : str, optional
  timeout : float, optional
  """

  def __init__(
    self,
    name: str,
    host: str,
    port: int = 5432,
    database: str = None,
    user: str = None,
    password: str = None,
    table: str = None,
    schema: str = 'public',
    if_exists: str = 'append',
    before_query: Optional[str] = None,
    after_query: Optional[str] = None,
    timeout: Optional[float] = None
  ):
    super().__init__()
    self.name = name
    self.host = host
    self.port = port
    self.database = database
    self.user = user
    self.password = password
    self.table = table
    self.schema = schema
    self.if_exists = if_exists
    self.before_query = before_query
    self.after_query = after_query
    self.timeout = timeout
    self.engine = None

    if not host or not host.strip():
      raise ValueError(f"PostgresDestination '{self.name}': host cannot be empty")
    if not database or not database.strip():
      raise ValueError(f"PostgresDestination '{self.name}': database cannot be empty")
    if not user or not user.strip():
      raise ValueError(f"PostgresDestination '{self.name}': user cannot be empty")
    if not password or not password.strip():
      raise ValueError(f"PostgresDestination '{self.name}': password cannot be empty")
    if not table or not table.strip():
      raise ValueError(f"PostgresDestination '{self.name}': table cannot be empty")
    if not schema or not schema.strip():
      raise ValueError(f"PostgresDestination '{self.name}': schema cannot be empty")
    if port <= 0:
      raise ValueError(f"PostgresDestination '{self.name}': port must be positive, got {port}")
    valid_if_exists = ['fail', 'replace', 'append']
    if if_exists not in valid_if_exists:
      raise ValueError(f"PostgresDestination '{self.name}': if_exists must be one of {valid_if_exists}")
    if before_query is not None and not before_query.strip():
      raise ValueError(f"PostgresDestination '{self.name}': before_query cannot be empty string")
    if after_query is not None and not after_query.strip():
      raise ValueError(f"PostgresDestination '{self.name}': after_query cannot be empty string")
    if timeout is not None and timeout <= 0:
      raise ValueError(f"PostgresDestination '{self.name}': timeout must be positive, got {timeout}")

  def _create_connection_string(self):
    return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

  def _initialize_engine(self):
    try:
      connect_args = {}
      if self.timeout:
        connect_args['connect_timeout'] = int(self.timeout)
      self.engine = create_engine(self._create_connection_string(), connect_args=connect_args)
      logger.info("PostgresDestination '%s' connected to %s@%s:%s/%s",
                  self.name, self.user, self.host, self.port, self.database)
    except Exception as e:
      raise ValueError(f"PostgresDestination '{self.name}' failed to initialize engine: {str(e)}")

  def _execute_query(self, sql_query: str, description: str) -> None:
    try:
      logger.info("PostgresDestination '%s' executing %s", self.name, description)
      logger.debug("Query preview: %s", sql_query[:200])
      start_time = time.time()
      with self.engine.connect() as connection:
        result = connection.execute(text(sql_query))
        connection.commit()
        duration = time.time() - start_time
        logger.info("PostgresDestination '%s' %s completed in %.2fs%s",
                    self.name, description, duration,
                    f" ({result.rowcount:,} rows affected)" if result.rowcount >= 0 else "")
    except Exception as e:
      logger.error("PostgresDestination '%s' failed to execute %s: %s", self.name, description, e)
      raise

  def sink(self, data_package: DataPackage) -> None:
    logger.debug("PostgresDestination '%s' received data from pipe '%s'",
                 self.name, data_package.get_pipe_name())
    df = data_package.get_df()
    table_ref = f"{self.schema}.{self.table}"

    try:
      if self.engine is None:
        self._initialize_engine()

      if self.before_query:
        self._execute_query(self.before_query, "before_query")

      logger.info("PostgresDestination '%s' loading %d rows to %s (if_exists=%s)",
                  self.name, len(df), table_ref, self.if_exists)
      logger.debug("Columns: %s", list(df.columns))

      start_time = time.time()
      df.to_sql(
        name=self.table,
        con=self.engine,
        schema=self.schema,
        if_exists=self.if_exists,
        index=False,
        chunksize=1000,
        method='multi'
      )
      duration = time.time() - start_time
      logger.info("PostgresDestination '%s' load complete: %d rows in %.2fs", self.name, len(df), duration)

      if self.after_query:
        self._execute_query(self.after_query, "after_query")

    except Exception as e:
      error_msg = str(e).lower()
      if "could not connect" in error_msg or "connection refused" in error_msg:
        logger.error("PostgresDestination '%s' connection failed — is server running at %s:%s? %s",
                     self.name, self.host, self.port, e)
      elif "password authentication failed" in error_msg or "authentication" in error_msg:
        logger.error("PostgresDestination '%s' authentication failed (user='%s'): %s", self.name, self.user, e)
      elif "already exists" in error_msg:
        logger.error("PostgresDestination '%s' table already exists (use if_exists='replace'/'append'): %s",
                     self.name, e)
      elif "permission denied" in error_msg:
        logger.error("PostgresDestination '%s' permission denied for schema '%s': %s",
                     self.name, self.schema, e)
      elif "timeout" in error_msg or "timed out" in error_msg:
        logger.error("PostgresDestination '%s' query timed out (timeout=%ss): %s", self.name, self.timeout, e)
      else:
        logger.error("PostgresDestination '%s' failed: %s (table=%s, shape=%s)",
                     self.name, e, table_ref, df.shape)
      raise

    finally:
      if self.engine is not None:
        self.engine.dispose()
        logger.debug("PostgresDestination '%s' connection closed", self.name)
