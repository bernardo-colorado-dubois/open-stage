from src.core.base import DataPackage, Pipe, Origin, Destination
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd


class GCPBigQueryOrigin(Origin):
  def __init__(self, name: str, project_id: str, query: str, credentials_path: str = None):
    super().__init__()
    self.name = name
    self.project_id = project_id
    self.query = query
    self.credentials_path = credentials_path
    self.client = None
    
    # Validar que project_id no esté vacío
    if not project_id or not project_id.strip():
      raise ValueError(f"GCPBigQueryOrigin '{self.name}': project_id cannot be empty")
    
    # Validar que query no esté vacío
    if not query or not query.strip():
      raise ValueError(f"GCPBigQueryOrigin '{self.name}': query cannot be empty")
    
  def add_output_pipe(self, pipe: Pipe) -> Pipe:
    # Solo permite 1 salida (como otros Origins)
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
        self.client = bigquery.Client(project=self.project_id, credentials=credentials)
      else:
        # Usar default credentials (ambiente sin restricción)
        print(f"GCPBigQueryOrigin '{self.name}' using default credentials")
        self.client = bigquery.Client(project=self.project_id)
      
      print(f"GCPBigQueryOrigin '{self.name}' BigQuery client initialized successfully")
      
    except Exception as e:
      raise ValueError(f"GCPBigQueryOrigin '{self.name}' failed to initialize BigQuery client: {str(e)}")
  
  def pump(self) -> None:
    try:
      # Inicializar cliente si no existe
      if self.client is None:
        self._initialize_client()
      
      print(f"GCPBigQueryOrigin '{self.name}' executing query...")
      print(f"Project ID: {self.project_id}")
      print(f"Query: {self.query[:100]}{'...' if len(self.query) > 100 else ''}")
      
      # Ejecutar la query
      job = self.client.query(self.query)
      
      # Esperar a que termine y obtener resultados
      print(f"GCPBigQueryOrigin '{self.name}' waiting for query completion...")
      job_result = job.result()  # Esto espera a que termine la query
      
      # Convertir a DataFrame usando el método estándar de BigQuery
      df = job_result.to_dataframe()
      
      print(f"GCPBigQueryOrigin '{self.name}' successfully executed query:")
      print(f"  - Rows returned: {len(df)}")
      print(f"  - Columns: {len(df.columns)}")
      print(f"  - Column names: {list(df.columns)}")
      print(f"  - Data types: {dict(df.dtypes)}")
      
      # Verificar que tenemos una salida configurada
      if len(self.outputs) > 0:
        output_pipe = list(self.outputs.values())[0]
        output_pipe.flow(df)
        print(f"GCPBigQueryOrigin '{self.name}' pumped data through pipe '{output_pipe.get_name()}'")
      else:
        print(f"Warning: GCPBigQueryOrigin '{self.name}' has no output pipe configured")
        
    except bigquery.exceptions.BadRequest as e:
      print(f"Error: GCPBigQueryOrigin '{self.name}' invalid query: {str(e)}")
      print(f"Query: {self.query}")
    except bigquery.exceptions.NotFound as e:
      print(f"Error: GCPBigQueryOrigin '{self.name}' resource not found: {str(e)}")
      print(f"Check if project '{self.project_id}' exists and you have access")
    except bigquery.exceptions.Forbidden as e:
      print(f"Error: GCPBigQueryOrigin '{self.name}' permission denied: {str(e)}")
      print(f"Check if you have BigQuery permissions for project '{self.project_id}'")
    except bigquery.exceptions.GoogleCloudError as e:
      print(f"Error: GCPBigQueryOrigin '{self.name}' BigQuery error: {str(e)}")
    except Exception as e:
      print(f"Error: GCPBigQueryOrigin '{self.name}' unexpected error: {str(e)}")
      print(f"Project: {self.project_id}")
      print(f"Credentials path: {self.credentials_path}")
      print(f"Query: {self.query[:200]}{'...' if len(self.query) > 200 else ''}")
      
      
      
class GCPBigQueryDestination(Destination):
  def __init__(self, name: str, project_id: str, dataset: str, table: str, write_disposition: str, credentials_path: str = None):
    super().__init__()
    self.name = name
    self.project_id = project_id
    self.dataset = dataset
    self.table = table
    self.write_disposition = write_disposition
    self.credentials_path = credentials_path
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
    
  def add_input_pipe(self, pipe: Pipe) -> None:
    # Solo permite 1 entrada (como otros Destinations)
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
        self.client = bigquery.Client(project=self.project_id, credentials=credentials)
      else:
        # Usar default credentials (ambiente sin restricción)
        print(f"GCPBigQueryDestination '{self.name}' using default credentials")
        self.client = bigquery.Client(project=self.project_id)
      
      print(f"GCPBigQueryDestination '{self.name}' BigQuery client initialized successfully")
      
    except Exception as e:
      raise ValueError(f"GCPBigQueryDestination '{self.name}' failed to initialize BigQuery client: {str(e)}")
  
  def sink(self, data_package: DataPackage) -> None:
    print(f"GCPBigQueryDestination '{self.name}' received data from pipe: '{data_package.get_pipe_name()}'")
    df = data_package.get_df()
    
    try:
      # Inicializar cliente si no existe
      if self.client is None:
        self._initialize_client()
      
      # Construir la referencia completa de la tabla
      table_id = f"{self.project_id}.{self.dataset}.{self.table}"
      
      print(f"GCPBigQueryDestination '{self.name}' loading data to BigQuery...")
      print(f"  - Table: {table_id}")
      print(f"  - Write disposition: {self.write_disposition}")
      print(f"  - DataFrame shape: {df.shape}")
      print(f"  - DataFrame columns: {list(df.columns)}")
      
      # Configurar el job de carga
      job_config = bigquery.LoadJobConfig(
        write_disposition=self.write_disposition
      )
      
      # Cargar DataFrame a BigQuery
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
      
      print(f"GCPBigQueryDestination '{self.name}' successfully loaded data:")
      print(f"  - Rows loaded: {len(df)}")
      print(f"  - Total rows in table: {table.num_rows}")
      print(f"  - Table schema fields: {len(table.schema)}")
      
    except Exception as e:
      # Verificar tipos específicos de error por contenido del mensaje
      error_msg = str(e).lower()
      
      if "not found" in error_msg:
        print(f"Error: GCPBigQueryDestination '{self.name}' table or dataset not found: {str(e)}")
        print(f"Table: {self.project_id}.{self.dataset}.{self.table}")
        print(f"Check if dataset '{self.dataset}' exists or enable auto-create")
      elif "forbidden" in error_msg or "permission" in error_msg:
        print(f"Error: GCPBigQueryDestination '{self.name}' permission denied: {str(e)}")
        print(f"Check if you have write permissions for table '{self.project_id}.{self.dataset}.{self.table}'")
      elif "schema" in error_msg or "field" in error_msg:
        print(f"Error: GCPBigQueryDestination '{self.name}' schema mismatch: {str(e)}")
        print(f"DataFrame columns: {list(df.columns)}")
        print(f"DataFrame dtypes: {dict(df.dtypes)}")
      elif "already exists" in error_msg:
        print(f"Error: GCPBigQueryDestination '{self.name}' table already exists: {str(e)}")
        print(f"Write disposition was: {self.write_disposition}")
      else:
        print(f"Error: GCPBigQueryDestination '{self.name}' unexpected error: {str(e)}")
        print(f"Table: {self.project_id}.{self.dataset}.{self.table}")
        print(f"Credentials path: {self.credentials_path}")
        print(f"DataFrame shape: {df.shape}")
        
    