from sqlalchemy import create_engine
import pandas as pd
from src.core.base import Origin, Pipe

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