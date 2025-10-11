# src/postgres/common.py

from sqlalchemy import create_engine
import pandas as pd
from src.core.base import Origin, Pipe, Destination, DataPackage


class PostgresOrigin(Origin):
  def __init__(self, name: str, host: str, port: int = 5432, database: str = None, user: str = None, password: str = None, query: str = None):
    super().__init__()
    self.name = name
    self.host = host
    self.port = port
    self.database = database
    self.user = user
    self.password = password
    self.query = query
    self.engine = None
    
    # Validar que host no esté vacío
    if not host or not host.strip():
      raise ValueError(f"PostgresOrigin '{self.name}': host cannot be empty")
    
    # Validar que database no esté vacío
    if not database or not database.strip():
      raise ValueError(f"PostgresOrigin '{self.name}': database cannot be empty")
    
    # Validar que user no esté vacío
    if not user or not user.strip():
      raise ValueError(f"PostgresOrigin '{self.name}': user cannot be empty")
    
    # Validar que password no esté vacío
    if not password or not password.strip():
      raise ValueError(f"PostgresOrigin '{self.name}': password cannot be empty")
    
    # Validar que query no esté vacío
    if not query or not query.strip():
      raise ValueError(f"PostgresOrigin '{self.name}': query cannot be empty")
    
    # Validar que port sea positivo
    if port <= 0:
      raise ValueError(f"PostgresOrigin '{self.name}': port must be positive, got {port}")
  
  def add_output_pipe(self, pipe: Pipe) -> Pipe:
    # Solo permite 1 salida
    if len(self.outputs.keys()) == 0:
      self.outputs[pipe.get_name()] = pipe
      pipe.set_origin(self)
      return pipe
    else:
      raise ValueError(f"PostgresOrigin '{self.name}' can only have 1 output")
  
  def _create_connection_string(self):
    """Crea el connection string para PostgreSQL"""
    return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
  
  def _initialize_engine(self):
    """Inicializa el engine de SQLAlchemy"""
    try:
      connection_string = self._create_connection_string()
      self.engine = create_engine(connection_string)
      print(f"PostgresOrigin '{self.name}' engine initialized successfully")
      print(f"Connection: {self.user}@{self.host}:{self.port}/{self.database}")
    except Exception as e:
      raise ValueError(f"PostgresOrigin '{self.name}' failed to initialize engine: {str(e)}")
  
  def pump(self) -> None:
    try:
      # Inicializar engine si no existe
      if self.engine is None:
        self._initialize_engine()
      
      print(f"PostgresOrigin '{self.name}' executing query...")
      print(f"Database: {self.database}")
      print(f"Query: {self.query[:100]}{'...' if len(self.query) > 100 else ''}")
      
      # Ejecutar la query y obtener DataFrame usando pandas
      df = pd.read_sql(self.query, self.engine)
      
      print(f"PostgresOrigin '{self.name}' successfully executed query:")
      print(f"  - Rows returned: {len(df)}")
      print(f"  - Columns: {len(df.columns)}")
      print(f"  - Column names: {list(df.columns)}")
      print(f"  - Data types: {dict(df.dtypes)}")
      
      # Verificar que tenemos una salida configurada
      if len(self.outputs) > 0:
        output_pipe = list(self.outputs.values())[0]
        output_pipe.flow(df)
        print(f"PostgresOrigin '{self.name}' pumped data through pipe '{output_pipe.get_name()}'")
      else:
        print(f"Warning: PostgresOrigin '{self.name}' has no output pipe configured")
        
    except Exception as e:
      error_msg = str(e).lower()
      
      if "could not connect" in error_msg or "connection refused" in error_msg:
        print(f"Error: PostgresOrigin '{self.name}' connection failed: {str(e)}")
        print(f"Check if PostgreSQL server is running at {self.host}:{self.port}")
      elif "password authentication failed" in error_msg or "authentication" in error_msg:
        print(f"Error: PostgresOrigin '{self.name}' authentication failed: {str(e)}")
        print(f"Check username '{self.user}' and password")
      elif "database" in error_msg and "does not exist" in error_msg:
        print(f"Error: PostgresOrigin '{self.name}' database not found: {str(e)}")
        print(f"Check if database '{self.database}' exists")
      elif "syntax error" in error_msg or "relation" in error_msg:
        print(f"Error: PostgresOrigin '{self.name}' SQL error: {str(e)}")
        print(f"Query: {self.query}")
      else:
        print(f"Error: PostgresOrigin '{self.name}' unexpected error: {str(e)}")
        print(f"Connection: {self.user}@{self.host}:{self.port}/{self.database}")
        print(f"Query: {self.query[:200]}{'...' if len(self.query) > 200 else ''}")
    
    finally:
      # Cerrar el engine si existe
      if self.engine is not None:
        self.engine.dispose()
        print(f"PostgresOrigin '{self.name}' connection closed")


class PostgresDestination(Destination):
  def __init__(self, name: str, host: str, port: int = 5432, database: str = None, user: str = None, password: str = None, table: str = None, schema: str = 'public', if_exists: str = 'append'):
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
    self.engine = None
    
    # Validar que host no esté vacío
    if not host or not host.strip():
      raise ValueError(f"PostgresDestination '{self.name}': host cannot be empty")
    
    # Validar que database no esté vacío
    if not database or not database.strip():
      raise ValueError(f"PostgresDestination '{self.name}': database cannot be empty")
    
    # Validar que user no esté vacío
    if not user or not user.strip():
      raise ValueError(f"PostgresDestination '{self.name}': user cannot be empty")
    
    # Validar que password no esté vacío
    if not password or not password.strip():
      raise ValueError(f"PostgresDestination '{self.name}': password cannot be empty")
    
    # Validar que table no esté vacío
    if not table or not table.strip():
      raise ValueError(f"PostgresDestination '{self.name}': table cannot be empty")
    
    # Validar que schema no esté vacío
    if not schema or not schema.strip():
      raise ValueError(f"PostgresDestination '{self.name}': schema cannot be empty")
    
    # Validar que port sea positivo
    if port <= 0:
      raise ValueError(f"PostgresDestination '{self.name}': port must be positive, got {port}")
    
    # Validar if_exists
    valid_if_exists = ['fail', 'replace', 'append']
    if if_exists not in valid_if_exists:
      raise ValueError(f"PostgresDestination '{self.name}': if_exists must be one of {valid_if_exists}, got '{if_exists}'")
  
  def add_input_pipe(self, pipe: Pipe) -> None:
    # Solo permite 1 entrada
    if len(self.inputs.keys()) == 0:
      self.inputs[pipe.get_name()] = pipe
    else:
      raise ValueError(f"PostgresDestination '{self.name}' can only have 1 input")
  
  def _create_connection_string(self):
    """Crea el connection string para PostgreSQL"""
    return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
  
  def _initialize_engine(self):
    """Inicializa el engine de SQLAlchemy"""
    try:
      connection_string = self._create_connection_string()
      self.engine = create_engine(connection_string)
      print(f"PostgresDestination '{self.name}' engine initialized successfully")
      print(f"Connection: {self.user}@{self.host}:{self.port}/{self.database}")
    except Exception as e:
      raise ValueError(f"PostgresDestination '{self.name}' failed to initialize engine: {str(e)}")
  
  def sink(self, data_package: DataPackage) -> None:
    print(f"PostgresDestination '{self.name}' received data from pipe: '{data_package.get_pipe_name()}'")
    df = data_package.get_df()
    
    try:
      # Inicializar engine si no existe
      if self.engine is None:
        self._initialize_engine()
      
      print(f"PostgresDestination '{self.name}' writing data to PostgreSQL...")
      print(f"  - Database: {self.database}")
      print(f"  - Schema: {self.schema}")
      print(f"  - Table: {self.table}")
      print(f"  - If exists: {self.if_exists}")
      print(f"  - DataFrame shape: {df.shape}")
      print(f"  - DataFrame columns: {list(df.columns)}")
      
      # Escribir DataFrame a PostgreSQL usando pandas
      df.to_sql(
        name=self.table,
        con=self.engine,
        schema=self.schema,
        if_exists=self.if_exists,
        index=False,
        chunksize=1000,  # Procesar en chunks de 1000 filas para mejor performance
        method='multi'   # Usa INSERT con múltiples valores para mejor performance
      )
      
      print(f"PostgresDestination '{self.name}' successfully wrote data:")
      print(f"  - Rows written: {len(df)}")
      print(f"  - Columns written: {len(df.columns)}")
      print(f"  - Table: {self.schema}.{self.table}")
      
    except Exception as e:
      error_msg = str(e).lower()
      
      if "could not connect" in error_msg or "connection refused" in error_msg:
        print(f"Error: PostgresDestination '{self.name}' connection failed: {str(e)}")
        print(f"Check if PostgreSQL server is running at {self.host}:{self.port}")
      elif "password authentication failed" in error_msg or "authentication" in error_msg:
        print(f"Error: PostgresDestination '{self.name}' authentication failed: {str(e)}")
        print(f"Check username '{self.user}' and password")
      elif "database" in error_msg and "does not exist" in error_msg:
        print(f"Error: PostgresDestination '{self.name}' database not found: {str(e)}")
        print(f"Check if database '{self.database}' exists")
      elif "already exists" in error_msg:
        print(f"Error: PostgresDestination '{self.name}' table already exists: {str(e)}")
        print(f"Table: {self.schema}.{self.table}")
        print(f"Use if_exists='replace' to overwrite or if_exists='append' to add rows")
      elif "does not exist" in error_msg and ("schema" in error_msg or "relation" in error_msg):
        print(f"Error: PostgresDestination '{self.name}' schema or table doesn't exist: {str(e)}")
        print(f"Schema: {self.schema}")
        print(f"Table: {self.table}")
        print(f"The table will be created automatically with if_exists='append' or 'replace'")
      elif "permission denied" in error_msg:
        print(f"Error: PostgresDestination '{self.name}' permission denied: {str(e)}")
        print(f"Check if user '{self.user}' has write permissions on schema '{self.schema}'")
      else:
        print(f"Error: PostgresDestination '{self.name}' unexpected error: {str(e)}")
        print(f"Connection: {self.user}@{self.host}:{self.port}/{self.database}")
        print(f"Schema: {self.schema}")
        print(f"Table: {self.table}")
        print(f"DataFrame shape: {df.shape}")
    
    finally:
      # Cerrar el engine si existe
      if self.engine is not None:
        self.engine.dispose()
        print(f"PostgresDestination '{self.name}' connection closed")