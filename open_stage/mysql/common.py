# src/mysql/common.py

import logging
from sqlalchemy import create_engine, text
import pandas as pd
from open_stage.core.base import Origin, Pipe, Destination, DataPackage, SingleInputMixin, SingleOutputMixin
from typing import Optional, Dict
import time

logger = logging.getLogger(__name__)


class MySQLOrigin(SingleOutputMixin, Origin):
  """
  MySQLOrigin - Enhanced MySQL Data Source
  =========================================
  Reads data from MySQL databases with advanced features including
  pre/post query execution.

  Connectivity: 0 → 1 (inherits from Origin)

  Parameters
  ----------
  name : str
  host : str
  port : int, default=3306
  database : str
  user : str
  password : str
  query : str, optional
    SQL query to execute (required if table is not provided)
  table : str, optional
    Table reference in format 'table' or 'database.table'
  before_query : str, optional
  after_query : str, optional
  max_results : int, optional
  timeout : float, optional
  query_parameters : dict, optional
  """

  def __init__(
    self,
    name: str,
    host: str,
    port: int = 3306,
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
      raise ValueError(f"MySQLOrigin '{self.name}': host cannot be empty")
    if not database or not database.strip():
      raise ValueError(f"MySQLOrigin '{self.name}': database cannot be empty")
    if not user or not user.strip():
      raise ValueError(f"MySQLOrigin '{self.name}': user cannot be empty")
    if not password or not password.strip():
      raise ValueError(f"MySQLOrigin '{self.name}': password cannot be empty")
    if query and table:
      raise ValueError(f"MySQLOrigin '{self.name}': cannot specify both 'query' and 'table'.")
    if not query and not table:
      raise ValueError(f"MySQLOrigin '{self.name}': must specify either 'query' or 'table'")
    if query and not query.strip():
      raise ValueError(f"MySQLOrigin '{self.name}': query cannot be empty")
    if table and not table.strip():
      raise ValueError(f"MySQLOrigin '{self.name}': table cannot be empty")
    if before_query is not None and not before_query.strip():
      raise ValueError(f"MySQLOrigin '{self.name}': before_query cannot be empty string")
    if after_query is not None and not after_query.strip():
      raise ValueError(f"MySQLOrigin '{self.name}': after_query cannot be empty string")
    if port <= 0:
      raise ValueError(f"MySQLOrigin '{self.name}': port must be positive, got {port}")
    if max_results is not None and max_results <= 0:
      raise ValueError(f"MySQLOrigin '{self.name}': max_results must be positive, got {max_results}")
    if timeout is not None and timeout <= 0:
      raise ValueError(f"MySQLOrigin '{self.name}': timeout must be positive, got {timeout}")

  def _create_connection_string(self):
    connection_string = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    if self.timeout:
      connection_string += f"?connect_timeout={int(self.timeout)}"
    return connection_string

  def _initialize_engine(self):
    try:
      self.engine = create_engine(self._create_connection_string())
      logger.info("MySQLOrigin '%s' connected to %s@%s:%s/%s",
                  self.name, self.user, self.host, self.port, self.database)
    except Exception as e:
      raise ValueError(f"MySQLOrigin '{self.name}' failed to initialize engine: {str(e)}")

  def _execute_query(self, sql_query: str, description: str) -> None:
    try:
      logger.info("MySQLOrigin '%s' executing %s", self.name, description)
      logger.debug("Query preview: %s", sql_query[:200])
      start_time = time.time()
      with self.engine.connect() as connection:
        result = connection.execute(text(sql_query))
        connection.commit()
        duration = time.time() - start_time
        logger.info("MySQLOrigin '%s' %s completed in %.2fs%s",
                    self.name, description, duration,
                    f" ({result.rowcount:,} rows affected)" if result.rowcount >= 0 else "")
    except Exception as e:
      logger.error("MySQLOrigin '%s' failed to execute %s: %s", self.name, description, e)
      raise

  def _build_query(self) -> str:
    if self.table:
      parts = self.table.split('.')
      if len(parts) == 1:
        table_ref = f'`{self.table}`'
      elif len(parts) == 2:
        db, table = parts
        table_ref = f'`{db}`.`{table}`'
      else:
        raise ValueError(f"MySQLOrigin '{self.name}': invalid table format '{self.table}'")
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
      logger.info("MySQLOrigin '%s' executing main query (db=%s%s%s)",
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

      logger.info("MySQLOrigin '%s' query returned %d rows, %d columns in %.2fs",
                  self.name, len(df), len(df.columns), duration)
      logger.debug("Columns: %s", list(df.columns))

      if self.after_query:
        self._execute_query(self.after_query, "after_query")

      if len(self.outputs) > 0:
        output_pipe = list(self.outputs.values())[0]
        output_pipe.flow(df)
        logger.debug("MySQLOrigin '%s' pumped data through pipe '%s'", self.name, output_pipe.get_name())
      else:
        logger.warning("MySQLOrigin '%s' has no output pipe configured", self.name)

    except Exception as e:
      error_msg = str(e).lower()
      if "can't connect" in error_msg or "connection refused" in error_msg:
        logger.error("MySQLOrigin '%s' connection failed — is server running at %s:%s? %s",
                     self.name, self.host, self.port, e)
      elif "access denied" in error_msg or "authentication" in error_msg:
        logger.error("MySQLOrigin '%s' authentication failed (user='%s'): %s", self.name, self.user, e)
      elif "unknown database" in error_msg:
        logger.error("MySQLOrigin '%s' database '%s' not found: %s", self.name, self.database, e)
      elif "timeout" in error_msg or "timed out" in error_msg:
        logger.error("MySQLOrigin '%s' query timed out (timeout=%ss): %s", self.name, self.timeout, e)
      else:
        logger.error("MySQLOrigin '%s' failed: %s", self.name, e)
      raise

    finally:
      if self.engine is not None:
        self.engine.dispose()
        logger.debug("MySQLOrigin '%s' connection closed", self.name)


class MySQLDestination(SingleInputMixin, Destination):
  """
  MySQLDestination - Enhanced MySQL Data Sink
  ============================================
  Writes data to MySQL databases with advanced features including
  pre/post query execution.

  Connectivity: 1 → 0 (inherits from Destination)

  Parameters
  ----------
  name : str
  host : str
  port : int, default=3306
  database : str
  user : str
  password : str
  table : str
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
    port: int = 3306,
    database: str = None,
    user: str = None,
    password: str = None,
    table: str = None,
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
    self.if_exists = if_exists
    self.before_query = before_query
    self.after_query = after_query
    self.timeout = timeout
    self.engine = None

    if not host or not host.strip():
      raise ValueError(f"MySQLDestination '{self.name}': host cannot be empty")
    if not database or not database.strip():
      raise ValueError(f"MySQLDestination '{self.name}': database cannot be empty")
    if not user or not user.strip():
      raise ValueError(f"MySQLDestination '{self.name}': user cannot be empty")
    if not password or not password.strip():
      raise ValueError(f"MySQLDestination '{self.name}': password cannot be empty")
    if not table or not table.strip():
      raise ValueError(f"MySQLDestination '{self.name}': table cannot be empty")
    if port <= 0:
      raise ValueError(f"MySQLDestination '{self.name}': port must be positive, got {port}")
    valid_if_exists = ['fail', 'replace', 'append']
    if if_exists not in valid_if_exists:
      raise ValueError(f"MySQLDestination '{self.name}': if_exists must be one of {valid_if_exists}")
    if before_query is not None and not before_query.strip():
      raise ValueError(f"MySQLDestination '{self.name}': before_query cannot be empty string")
    if after_query is not None and not after_query.strip():
      raise ValueError(f"MySQLDestination '{self.name}': after_query cannot be empty string")
    if timeout is not None and timeout <= 0:
      raise ValueError(f"MySQLDestination '{self.name}': timeout must be positive, got {timeout}")

  def _create_connection_string(self):
    connection_string = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    if self.timeout:
      connection_string += f"?connect_timeout={int(self.timeout)}"
    return connection_string

  def _initialize_engine(self):
    try:
      self.engine = create_engine(self._create_connection_string())
      logger.info("MySQLDestination '%s' connected to %s@%s:%s/%s",
                  self.name, self.user, self.host, self.port, self.database)
    except Exception as e:
      raise ValueError(f"MySQLDestination '{self.name}' failed to initialize engine: {str(e)}")

  def _execute_query(self, sql_query: str, description: str) -> None:
    try:
      logger.info("MySQLDestination '%s' executing %s", self.name, description)
      logger.debug("Query preview: %s", sql_query[:200])
      start_time = time.time()
      with self.engine.connect() as connection:
        result = connection.execute(text(sql_query))
        connection.commit()
        duration = time.time() - start_time
        logger.info("MySQLDestination '%s' %s completed in %.2fs%s",
                    self.name, description, duration,
                    f" ({result.rowcount:,} rows affected)" if result.rowcount >= 0 else "")
    except Exception as e:
      logger.error("MySQLDestination '%s' failed to execute %s: %s", self.name, description, e)
      raise

  def sink(self, data_package: DataPackage) -> None:
    logger.debug("MySQLDestination '%s' received data from pipe '%s'",
                 self.name, data_package.get_pipe_name())
    df = data_package.get_df()

    try:
      if self.engine is None:
        self._initialize_engine()

      if self.before_query:
        self._execute_query(self.before_query, "before_query")

      logger.info("MySQLDestination '%s' loading %d rows to %s.%s (if_exists=%s)",
                  self.name, len(df), self.database, self.table, self.if_exists)
      logger.debug("Columns: %s", list(df.columns))

      start_time = time.time()
      df.to_sql(
        name=self.table,
        con=self.engine,
        if_exists=self.if_exists,
        index=False,
        chunksize=1000
      )
      duration = time.time() - start_time
      logger.info("MySQLDestination '%s' load complete: %d rows in %.2fs", self.name, len(df), duration)

      if self.after_query:
        self._execute_query(self.after_query, "after_query")

    except Exception as e:
      error_msg = str(e).lower()
      if "can't connect" in error_msg or "connection refused" in error_msg:
        logger.error("MySQLDestination '%s' connection failed — is server running at %s:%s? %s",
                     self.name, self.host, self.port, e)
      elif "access denied" in error_msg or "authentication" in error_msg:
        logger.error("MySQLDestination '%s' authentication failed (user='%s'): %s", self.name, self.user, e)
      elif "unknown database" in error_msg:
        logger.error("MySQLDestination '%s' database '%s' not found: %s", self.name, self.database, e)
      elif "already exists" in error_msg:
        logger.error("MySQLDestination '%s' table already exists (use if_exists='replace'/'append'): %s",
                     self.name, e)
      elif "timeout" in error_msg or "timed out" in error_msg:
        logger.error("MySQLDestination '%s' query timed out (timeout=%ss): %s", self.name, self.timeout, e)
      else:
        logger.error("MySQLDestination '%s' failed: %s (table=%s, shape=%s)",
                     self.name, e, self.table, df.shape)
      raise

    finally:
      if self.engine is not None:
        self.engine.dispose()
        logger.debug("MySQLDestination '%s' connection closed", self.name)
