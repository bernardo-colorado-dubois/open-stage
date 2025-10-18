# src/mysql/common.py

from sqlalchemy import create_engine, text
import pandas as pd
from src.core.base import Origin, Pipe, Destination, DataPackage
from typing import Optional, Dict
import time


class MySQLOrigin(Origin):
  """
  MySQLOrigin - Enhanced MySQL Data Source
  =========================================
  Reads data from MySQL databases with advanced features including
  pre/post query execution.
  
  Connectivity: 0 → 1 (inherits from Origin)
  
  Parameters
  ----------
  name : str
    Component name
  host : str
    MySQL host
  port : int, default=3306
    MySQL port
  database : str
    Database name
  user : str
    Username
  password : str
    Password
  query : str, optional
    SQL query to execute (required if table is not provided)
  table : str, optional
    Table reference in format 'table' or 'database.table'
    (required if query is not provided)
  before_query : str, optional
    SQL query to execute BEFORE the main extraction query.
    Useful for: creating temp tables, calling procedures, setting session variables.
    Default: None
  after_query : str, optional
    SQL query to execute AFTER the main extraction query.
    Useful for: audit logging, marking records as processed, cleanup.
    Default: None
  max_results : int, optional
    Maximum number of rows to return (useful for testing)
  timeout : float, optional
    Query timeout in seconds (default: None = no timeout)
  query_parameters : dict, optional
    Dictionary of query parameters for parameterized queries
  
  Examples
  --------
  # Example 1: Simple query
  >>> origin = MySQLOrigin(
  ...     name="sales_data",
  ...     host="localhost",
  ...     database="warehouse",
  ...     user="root",
  ...     password="password",
  ...     query="SELECT * FROM sales WHERE date >= '2024-01-01'"
  ... )
  
  # Example 2: Direct table read with limit
  >>> origin = MySQLOrigin(
  ...     name="customers",
  ...     host="localhost",
  ...     database="crm",
  ...     user="root",
  ...     password="password",
  ...     table="customers",
  ...     max_results=1000
  ... )
  
  # Example 3: With before_query (create temp table)
  >>> origin = MySQLOrigin(
  ...     name="processed_orders",
  ...     host="localhost",
  ...     database="warehouse",
  ...     user="root",
  ...     password="password",
  ...     before_query='''
  ...         CREATE TEMPORARY TABLE temp_orders AS
  ...         SELECT * FROM raw_orders
  ...         WHERE status = 'completed';
  ...     ''',
  ...     query="SELECT * FROM temp_orders WHERE amount > 100"
  ... )
  
  # Example 4: Parameterized query
  >>> origin = MySQLOrigin(
  ...     name="filtered_sales",
  ...     host="localhost",
  ...     database="warehouse",
  ...     user="root",
  ...     password="password",
  ...     query="SELECT * FROM sales WHERE date >= :start_date AND amount > :min_amount",
  ...     query_parameters={
  ...         'start_date': '2024-01-01',
  ...         'min_amount': 100.0
  ...     }
  ... )
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
    
    # Validar que host no esté vacío
    if not host or not host.strip():
      raise ValueError(f"MySQLOrigin '{self.name}': host cannot be empty")
    
    # Validar que database no esté vacío
    if not database or not database.strip():
      raise ValueError(f"MySQLOrigin '{self.name}': database cannot be empty")
    
    # Validar que user no esté vacío
    if not user or not user.strip():
      raise ValueError(f"MySQLOrigin '{self.name}': user cannot be empty")
    
    # Validar que password no esté vacío
    if not password or not password.strip():
      raise ValueError(f"MySQLOrigin '{self.name}': password cannot be empty")
    
    # Validar que se proporcione query O table (pero no ambos)
    if query and table:
      raise ValueError(f"MySQLOrigin '{self.name}': cannot specify both 'query' and 'table'. Use one or the other.")
    
    if not query and not table:
      raise ValueError(f"MySQLOrigin '{self.name}': must specify either 'query' or 'table'")
    
    # Validar query si se proporciona
    if query and (not query.strip()):
      raise ValueError(f"MySQLOrigin '{self.name}': query cannot be empty")
    
    # Validar table si se proporciona
    if table and (not table.strip()):
      raise ValueError(f"MySQLOrigin '{self.name}': table cannot be empty")
    
    # Validar before_query si se proporciona
    if before_query is not None and (not before_query.strip()):
      raise ValueError(f"MySQLOrigin '{self.name}': before_query cannot be empty string")
    
    # Validar after_query si se proporciona
    if after_query is not None and (not after_query.strip()):
      raise ValueError(f"MySQLOrigin '{self.name}': after_query cannot be empty string")
    
    # Validar que port sea positivo
    if port <= 0:
      raise ValueError(f"MySQLOrigin '{self.name}': port must be positive, got {port}")
    
    # Validar max_results si se proporciona
    if max_results is not None and max_results <= 0:
      raise ValueError(f"MySQLOrigin '{self.name}': max_results must be positive, got {max_results}")
    
    # Validar timeout si se proporciona
    if timeout is not None and timeout <= 0:
      raise ValueError(f"MySQLOrigin '{self.name}': timeout must be positive, got {timeout}")
  
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
      raise ValueError(f"MySQLOrigin '{self.name}' can only have 1 output")
  
  def _create_connection_string(self):
    """Crea el connection string para MySQL"""
    connection_string = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    # Agregar timeout a la URL si se especificó
    if self.timeout:
      connection_string += f"?connect_timeout={int(self.timeout)}"
    
    return connection_string
  
  def _initialize_engine(self):
    """Inicializa el engine de SQLAlchemy"""
    try:
      connection_string = self._create_connection_string()
      self.engine = create_engine(connection_string)
      
      print(f"MySQLOrigin '{self.name}' engine initialized successfully")
      print(f"Connection: {self.user}@{self.host}:{self.port}/{self.database}")
      if self.timeout:
        print(f"  - Timeout: {self.timeout}s")
      
    except Exception as e:
      raise ValueError(f"MySQLOrigin '{self.name}' failed to initialize engine: {str(e)}")
  
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
      print(f"\nMySQLOrigin '{self.name}' executing {description}...")
      print(f"  Query preview: {sql_query[:200]}{'...' if len(sql_query) > 200 else ''}")
      
      start_time = time.time()
      
      # Ejecutar la query usando connection
      with self.engine.connect() as connection:
        # Ejecutar la query (puede ser múltiples statements)
        result = connection.execute(text(sql_query))
        connection.commit()
        
        duration = time.time() - start_time
        
        print(f"✅ MySQLOrigin '{self.name}' {description} executed successfully")
        
        # Mostrar filas afectadas si está disponible
        if result.rowcount >= 0:
          print(f"  - Rows affected: {result.rowcount:,}")
        
        print(f"  - Duration: {duration:.2f}s")
      
    except Exception as e:
      print(f"❌ Error: MySQLOrigin '{self.name}' failed to execute {description}: {str(e)}")
      print(f"Query: {sql_query[:500]}{'...' if len(sql_query) > 500 else ''}")
      raise
  
  def _build_query(self) -> str:
    """
    Construye la query final (desde tabla o query directa)
    """
    if self.table:
      # Construir query desde nombre de tabla
      # Soporta formatos: 'table' o 'database.table'
      parts = self.table.split('.')
      
      if len(parts) == 1:
        # Solo nombre de tabla
        table_ref = f'`{self.table}`'
      elif len(parts) == 2:
        # database.table
        db, table = parts
        table_ref = f'`{db}`.`{table}`'
      else:
        raise ValueError(f"MySQLOrigin '{self.name}': invalid table format '{self.table}'. Use 'table' or 'database.table'")
      
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
  
  def pump(self) -> None:
    """
    Execute query and pump data through output pipe
    
    Execution flow:
    1. Initialize MySQL engine
    2. Execute before_query (if provided)
    3. Execute main extraction query
    4. Execute after_query (if provided)
    5. Pump data through output pipe
    """
    try:
      # Inicializar engine si no existe
      if self.engine is None:
        self._initialize_engine()
      
      # ================================================================
      # STEP 1: BEFORE QUERY (Pre-processing)
      # ================================================================
      if self.before_query:
        self._execute_query(self.before_query, "before_query")
      
      # ================================================================
      # STEP 2: MAIN EXTRACTION QUERY
      # ================================================================
      
      # Construir query
      final_query = self._build_query()
      
      print(f"\n{'='*70}")
      print(f"MySQLOrigin '{self.name}' executing MAIN extraction query...")
      print(f"{'='*70}")
      print(f"  - Database: {self.database}")
      if self.table:
        print(f"  - Table: {self.table}")
      print(f"  - Query: {final_query[:150]}{'...' if len(final_query) > 150 else ''}")
      if self.max_results:
        print(f"  - Max results: {self.max_results:,}")
      if self.query_parameters:
        print(f"  - Parameters: {list(self.query_parameters.keys())}")
      
      # Ejecutar la query y obtener DataFrame
      start_time = time.time()
      
      if self.query_parameters:
        # Query con parámetros
        df = pd.read_sql(
          text(final_query),
          self.engine,
          params=self.query_parameters
        )
      else:
        # Query sin parámetros
        df = pd.read_sql(final_query, self.engine)
      
      duration = time.time() - start_time
      
      print(f"\n{'='*70}")
      print(f"MySQLOrigin '{self.name}' MAIN query results:")
      print(f"{'='*70}")
      print(f"  📊 Results:")
      print(f"     - Rows returned: {len(df):,}")
      print(f"     - Columns: {len(df.columns)}")
      print(f"     - Column names: {list(df.columns)}")
      
      print(f"  ⏱️  Query info:")
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
        print(f"✅ MySQLOrigin '{self.name}' pumped data through pipe '{output_pipe.get_name()}'")
        print(f"{'='*70}")
      else:
        print(f"\n{'='*70}")
        print(f"⚠️  Warning: MySQLOrigin '{self.name}' has no output pipe configured")
        print(f"{'='*70}")
        
    except Exception as e:
      error_msg = str(e).lower()
      
      print(f"\n{'='*70}")
      if "can't connect" in error_msg or "connection refused" in error_msg:
        print(f"❌ Error: MySQLOrigin '{self.name}' connection failed: {str(e)}")
        print(f"Check if MySQL server is running at {self.host}:{self.port}")
      elif "access denied" in error_msg or "authentication" in error_msg:
        print(f"❌ Error: MySQLOrigin '{self.name}' authentication failed: {str(e)}")
        print(f"Check username '{self.user}' and password")
      elif "unknown database" in error_msg:
        print(f"❌ Error: MySQLOrigin '{self.name}' database not found: {str(e)}")
        print(f"Check if database '{self.database}' exists")
      elif "syntax error" in error_msg or "table" in error_msg and "doesn't exist" in error_msg:
        print(f"❌ Error: MySQLOrigin '{self.name}' SQL error: {str(e)}")
        print(f"Query: {final_query}")
      elif "timeout" in error_msg or "timed out" in error_msg:
        print(f"❌ Error: MySQLOrigin '{self.name}' query timeout: {str(e)}")
        print(f"Timeout was: {self.timeout}s")
      else:
        print(f"❌ Error: MySQLOrigin '{self.name}' unexpected error: {str(e)}")
        print(f"Connection: {self.user}@{self.host}:{self.port}/{self.database}")
        if self.table:
          print(f"  - Table: {self.table}")
        else:
          print(f"  - Query: {self.query[:200]}{'...' if len(self.query) > 200 else ''}")
      print(f"{'='*70}")
      raise
    
    finally:
      # Cerrar el engine si existe
      if self.engine is not None:
        self.engine.dispose()
        print(f"MySQLOrigin '{self.name}' connection closed")


class MySQLDestination(Destination):
  """
  MySQLDestination - Enhanced MySQL Data Sink
  ============================================
  Writes data to MySQL databases with advanced features including
  pre/post query execution.
  
  Connectivity: 1 → 0 (inherits from Destination)
  
  Parameters
  ----------
  name : str
    Component name
  host : str
    MySQL host
  port : int, default=3306
    MySQL port
  database : str
    Database name
  user : str
    Username
  password : str
    Password
  table : str
    Table name
  if_exists : str, default='append'
    Write mode: 'fail', 'replace', or 'append'
  before_query : str, optional
    SQL query to execute BEFORE loading data.
    Useful for: creating backups, truncating tables, preparing staging.
    Default: None
  after_query : str, optional
    SQL query to execute AFTER loading data.
    Useful for: audit logging, post-processing, cleanup.
    Default: None
  timeout : float, optional
    Query timeout in seconds (default: None = no timeout)
  
  Examples
  --------
  # Example 1: Simple load
  >>> dest = MySQLDestination(
  ...     name="sales_loader",
  ...     host="localhost",
  ...     database="warehouse",
  ...     user="root",
  ...     password="password",
  ...     table="sales",
  ...     if_exists="append"
  ... )
  
  # Example 2: With before_query (truncate before load)
  >>> dest = MySQLDestination(
  ...     name="daily_load",
  ...     host="localhost",
  ...     database="warehouse",
  ...     user="root",
  ...     password="password",
  ...     table="daily_summary",
  ...     before_query="TRUNCATE TABLE daily_summary;",
  ...     if_exists="append"
  ... )
  
  # Example 3: With after_query (audit logging)
  >>> dest = MySQLDestination(
  ...     name="customer_loader",
  ...     host="localhost",
  ...     database="crm",
  ...     user="root",
  ...     password="password",
  ...     table="customers",
  ...     if_exists="append",
  ...     after_query='''
  ...         INSERT INTO audit.load_log (table_name, loaded_at, record_count)
  ...         VALUES ('customers', NOW(), (SELECT COUNT(*) FROM customers));
  ...     '''
  ... )
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
    
    # Validar que host no esté vacío
    if not host or not host.strip():
      raise ValueError(f"MySQLDestination '{self.name}': host cannot be empty")
    
    # Validar que database no esté vacío
    if not database or not database.strip():
      raise ValueError(f"MySQLDestination '{self.name}': database cannot be empty")
    
    # Validar que user no esté vacío
    if not user or not user.strip():
      raise ValueError(f"MySQLDestination '{self.name}': user cannot be empty")
    
    # Validar que password no esté vacío
    if not password or not password.strip():
      raise ValueError(f"MySQLDestination '{self.name}': password cannot be empty")
    
    # Validar que table no esté vacío
    if not table or not table.strip():
      raise ValueError(f"MySQLDestination '{self.name}': table cannot be empty")
    
    # Validar que port sea positivo
    if port <= 0:
      raise ValueError(f"MySQLDestination '{self.name}': port must be positive, got {port}")
    
    # Validar if_exists
    valid_if_exists = ['fail', 'replace', 'append']
    if if_exists not in valid_if_exists:
      raise ValueError(f"MySQLDestination '{self.name}': if_exists must be one of {valid_if_exists}, got '{if_exists}'")
    
    # Validar before_query si se proporciona
    if before_query is not None and (not before_query.strip()):
      raise ValueError(f"MySQLDestination '{self.name}': before_query cannot be empty string")
    
    # Validar after_query si se proporciona
    if after_query is not None and (not after_query.strip()):
      raise ValueError(f"MySQLDestination '{self.name}': after_query cannot be empty string")
    
    # Validar timeout si se proporciona
    if timeout is not None and timeout <= 0:
      raise ValueError(f"MySQLDestination '{self.name}': timeout must be positive, got {timeout}")
  
  def add_input_pipe(self, pipe: Pipe) -> None:
    # Solo permite 1 entrada
    if len(self.inputs.keys()) == 0:
      self.inputs[pipe.get_name()] = pipe
    else:
      raise ValueError(f"MySQLDestination '{self.name}' can only have 1 input")
  
  def _create_connection_string(self):
    """Crea el connection string para MySQL"""
    connection_string = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    # Agregar timeout a la URL si se especificó
    if self.timeout:
      connection_string += f"?connect_timeout={int(self.timeout)}"
    
    return connection_string
  
  def _initialize_engine(self):
    """Inicializa el engine de SQLAlchemy"""
    try:
      connection_string = self._create_connection_string()
      self.engine = create_engine(connection_string)
      
      print(f"MySQLDestination '{self.name}' engine initialized successfully")
      print(f"Connection: {self.user}@{self.host}:{self.port}/{self.database}")
      if self.timeout:
        print(f"  - Timeout: {self.timeout}s")
      
    except Exception as e:
      raise ValueError(f"MySQLDestination '{self.name}' failed to initialize engine: {str(e)}")
  
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
      print(f"\nMySQLDestination '{self.name}' executing {description}...")
      print(f"  Query preview: {sql_query[:200]}{'...' if len(sql_query) > 200 else ''}")
      
      start_time = time.time()
      
      # Ejecutar la query usando connection
      with self.engine.connect() as connection:
        # Ejecutar la query (puede ser múltiples statements)
        result = connection.execute(text(sql_query))
        connection.commit()
        
        duration = time.time() - start_time
        
        print(f"✅ MySQLDestination '{self.name}' {description} executed successfully")
        
        # Mostrar filas afectadas si está disponible
        if result.rowcount >= 0:
          print(f"  - Rows affected: {result.rowcount:,}")
        
        print(f"  - Duration: {duration:.2f}s")
      
    except Exception as e:
      print(f"❌ Error: MySQLDestination '{self.name}' failed to execute {description}: {str(e)}")
      print(f"Query: {sql_query[:500]}{'...' if len(sql_query) > 500 else ''}")
      raise
  
  def sink(self, data_package: DataPackage) -> None:
    """
    Load data to MySQL
    
    Execution flow:
    1. Initialize MySQL engine
    2. Execute before_query (if provided)
    3. Load DataFrame to MySQL
    4. Execute after_query (if provided)
    """
    print(f"MySQLDestination '{self.name}' received data from pipe: '{data_package.get_pipe_name()}'")
    df = data_package.get_df()
    
    try:
      # Inicializar engine si no existe
      if self.engine is None:
        self._initialize_engine()
      
      # ================================================================
      # STEP 1: BEFORE QUERY (Pre-processing)
      # ================================================================
      if self.before_query:
        self._execute_query(self.before_query, "before_query")
      
      # ================================================================
      # STEP 2: LOAD DATA TO MYSQL
      # ================================================================
      
      print(f"\n{'='*70}")
      print(f"MySQLDestination '{self.name}' loading data to MySQL...")
      print(f"{'='*70}")
      print(f"  - Database: {self.database}")
      print(f"  - Table: {self.table}")
      print(f"  - If exists: {self.if_exists}")
      print(f"  - DataFrame shape: {df.shape}")
      print(f"  - DataFrame columns: {list(df.columns)}")
      
      print(f"\nMySQLDestination '{self.name}' starting load operation...")
      start_time = time.time()
      
      # Escribir DataFrame a MySQL usando pandas
      df.to_sql(
        name=self.table,
        con=self.engine,
        if_exists=self.if_exists,
        index=False,
        chunksize=1000  # Procesar en chunks de 1000 filas para mejor performance
      )
      
      duration = time.time() - start_time
      
      print(f"\n{'='*70}")
      print(f"MySQLDestination '{self.name}' LOAD completed successfully:")
      print(f"{'='*70}")
      print(f"  📊 Load results:")
      print(f"     - Rows loaded: {len(df):,}")
      print(f"     - Columns loaded: {len(df.columns)}")
      print(f"     - Table: {self.table}")
      
      print(f"  ⏱️  Load info:")
      print(f"     - Duration: {duration:.2f}s")
      print(f"     - Write mode: {self.if_exists}")
      
      print(f"  📋 Data types:")
      for col, dtype in df.dtypes.items():
        print(f"     - {col}: {dtype}")
      
      # ================================================================
      # STEP 3: AFTER QUERY (Post-processing)
      # ================================================================
      if self.after_query:
        self._execute_query(self.after_query, "after_query")
      
      print(f"\n{'='*70}")
      print(f"✅ MySQLDestination '{self.name}' completed successfully")
      print(f"{'='*70}")
      
    except Exception as e:
      error_msg = str(e).lower()
      
      print(f"\n{'='*70}")
      if "can't connect" in error_msg or "connection refused" in error_msg:
        print(f"❌ Error: MySQLDestination '{self.name}' connection failed: {str(e)}")
        print(f"Check if MySQL server is running at {self.host}:{self.port}")
      elif "access denied" in error_msg or "authentication" in error_msg:
        print(f"❌ Error: MySQLDestination '{self.name}' authentication failed: {str(e)}")
        print(f"Check username '{self.user}' and password")
      elif "unknown database" in error_msg:
        print(f"❌ Error: MySQLDestination '{self.name}' database not found: {str(e)}")
        print(f"Check if database '{self.database}' exists")
      elif "already exists" in error_msg:
        print(f"❌ Error: MySQLDestination '{self.name}' table already exists: {str(e)}")
        print(f"Table: {self.table}")
        print(f"Use if_exists='replace' to overwrite or if_exists='append' to add rows")
      elif "doesn't exist" in error_msg and "table" in error_msg:
        print(f"❌ Error: MySQLDestination '{self.name}' table doesn't exist: {str(e)}")
        print(f"Table: {self.table}")
        print(f"The table will be created automatically with if_exists='append' or 'replace'")
      elif "timeout" in error_msg or "timed out" in error_msg:
        print(f"❌ Error: MySQLDestination '{self.name}' query timeout: {str(e)}")
        print(f"Timeout was: {self.timeout}s")
      else:
        print(f"❌ Error: MySQLDestination '{self.name}' unexpected error: {str(e)}")
        print(f"Connection: {self.user}@{self.host}:{self.port}/{self.database}")
        print(f"Table: {self.table}")
        print(f"DataFrame shape: {df.shape}")
      print(f"{'='*70}")
      raise
    
    finally:
      # Cerrar el engine si existe
      if self.engine is not None:
        self.engine.dispose()
        print(f"MySQLDestination '{self.name}' connection closed")