
import pandas as pd
import requests

from src.core.base import DataPackage, Pipe, Origin, Destination, Node

class Generator(Origin):
  def __init__(self, name: str, length: int):
    super().__init__()  # Inicializar la clase padre
    self.length = length
    self.name = name
    
  def add_output_pipe(self, pipe: Pipe) -> Pipe:
    if len(self.outputs.keys()) == 0:
      self.outputs[pipe.get_name()] = pipe
      pipe.set_origin(self)
      return pipe
    else:
      raise ValueError(f"Generator '{self.name}' can only have 1 input")
  
  def pump(self) -> None:
    numbers = [i for i in range(self.length)]
    df = pd.DataFrame(numbers, columns=["number"])
    self.outputs[list(self.outputs.keys())[0]].flow(df)
  

class Printer(Destination):
  def __init__(self, name: str) -> None:
    super().__init__()  # Inicializar la clase padre
    self.name = name 
  
  def add_input_pipe(self, pipe: Pipe) -> None:
    if len(self.inputs.keys()) == 0:
      self.inputs[pipe.get_name()] = pipe
    else:
      raise ValueError(f"Printer '{self.name}' can only have 1 input")
    
  def sink(self, data_package: DataPackage) -> None:
    print(f"Data received from pipe: {data_package.get_pipe_name()}")
    print(data_package.get_df())
    

class Funnel(Node):
  def __init__(self, name: str) -> None:
    super().__init__()  # Inicializar la clase padre
    self.name = name
    self.dfs = []
    self.received_inputs = 0
    self.expected_inputs = 0
    self.is_ready_to_pump = False
  
  def add_input_pipe(self, pipe: Pipe) -> None:
    self.inputs[pipe.get_name()] = pipe
    self.expected_inputs += 1
    
  def add_output_pipe(self, pipe: Pipe) -> Pipe:
    if len(self.outputs.keys()) == 0:
      self.outputs[pipe.get_name()] = pipe
      pipe.set_origin(self)
      return pipe
    else:
      raise ValueError(f"Funnel {self.name} can only have 1 output")
  
  def sink(self, data_package: DataPackage) -> None:
    print(f"Funnel '{self.name}' received data from pipe: '{data_package.get_pipe_name()}'")
    df = data_package.get_df()
    self.dfs.append(df)
    self.received_inputs += 1
    
    print(f"Funnel has received {self.received_inputs}/{self.expected_inputs} DataFrames")
    
    # Solo procesar cuando hayamos recibido todos los inputs esperados
    if self.received_inputs == self.expected_inputs:
      self.merge_and_pump()
  
  def merge_and_pump(self) -> None:
    if len(self.dfs) > 0:
      # Verificar que todos los DataFrames tienen la misma estructura
      base_columns = list(self.dfs[0].columns)
      for i, df in enumerate(self.dfs):
        if list(df.columns) != base_columns:
          print(f"Warning: DataFrame {i} has different columns")
          print(f"Expected: {base_columns}")
          print(f"Got: {list(df.columns)}")
      
      # Combinar todos los DataFrames (concatenar filas)
      self.combined_df = pd.concat(self.dfs, ignore_index=True)
      print(f"Funnel '{self.name}' merged {len(self.dfs)} DataFrames into shape: {self.combined_df.shape}")
      self.pump()
    else:
      print(f"Error: Funnel has no DataFrames to merge")
  
  def pump(self) -> None:
    if hasattr(self, 'combined_df') and len(self.outputs) > 0:
      output_pipe = list(self.outputs.values())[0]
      output_pipe.flow(self.combined_df)
      print(f"Funnel '{self.name}' pumped merged data through pipe '{output_pipe.get_name()}'")
    else:
      print(f"Warning: Funnel '{self.name}' has no combined data or no output pipe")
      
        
class Switcher(Node):
  def __init__(self, name: str, field: str, mapping: dict, fail_on_unmatch: bool = False):
    super().__init__()
    self.name = name
    self.field = field
    self.mapping = mapping
    self.fail_on_unmatch = fail_on_unmatch
    self.received_df = None  # Para almacenar el único DataFrame de entrada
    
    # Validar que el mapping contenga solo strings e integers como keys
    for key in mapping.keys():
      if not isinstance(key, (str, int)):
        raise ValueError(f"Switcher '{self.name}': mapping key '{key}' must be string or integer, got {type(key)}")
  
  def add_input_pipe(self, pipe: Pipe) -> None:
    # Solo permite 1 entrada
    if len(self.inputs.keys()) == 0:
      self.inputs[pipe.get_name()] = pipe
    else:
      raise ValueError(f"Switcher '{self.name}' can only have 1 input")
  
  def add_output_pipe(self, pipe: Pipe) -> Pipe:
    # Permite múltiples salidas
    self.outputs[pipe.get_name()] = pipe
    pipe.set_origin(self)
    return pipe
  
  def sink(self, data_package: DataPackage) -> None:
    print(f"Switcher '{self.name}' received data from pipe: '{data_package.get_pipe_name()}'")
    df = data_package.get_df()
    
    # Validar que el campo existe en el DataFrame
    if self.field not in df.columns:
      raise ValueError(f"Switcher '{self.name}': field '{self.field}' not found in DataFrame columns: {list(df.columns)}")
    
    # Validar que todos los valores del campo son string o int
    field_values = df[self.field]
    invalid_types = field_values.apply(lambda x: not isinstance(x, (str, int)) and pd.notna(x))
    if invalid_types.any():
      invalid_indices = field_values[invalid_types].index.tolist()
      raise ValueError(f"Switcher '{self.name}': field '{self.field}' contains non-string/non-integer values at indices: {invalid_indices}")
    
    # Almacenar el DataFrame para procesamiento en pump()
    self.received_df = df
    print(f"Switcher '{self.name}' stored DataFrame with {len(df)} rows")
    
    # Procesar inmediatamente
    self.pump()
  
  def pump(self) -> None:
    if self.received_df is None:
      print(f"Warning: Switcher '{self.name}' has no data to process")
      return
    
    df = self.received_df
    print(f"Switcher '{self.name}' processing DataFrame with {len(df)} rows")
    
    # Obtener valores únicos del campo de switch
    unique_values = df[self.field].dropna().unique()
    total_routed_rows = 0
    
    for value in unique_values:
      # Filtrar filas que corresponden a este valor
      filtered_df = df[df[self.field] == value]
      
      if value in self.mapping:
        # Encontrar el pipe de salida correspondiente
        target_pipe_name = self.mapping[value]
        if target_pipe_name in self.outputs:
          target_pipe = self.outputs[target_pipe_name]
          print(f"Switcher '{self.name}': routing {len(filtered_df)} rows with {self.field}='{value}' to pipe '{target_pipe_name}'")
          target_pipe.flow(filtered_df)
          total_routed_rows += len(filtered_df)
        else:
          error_msg = f"Switcher '{self.name}': target pipe '{target_pipe_name}' for value '{value}' not found in outputs: {list(self.outputs.keys())}"
          if self.fail_on_unmatch:
            raise ValueError(error_msg)
          else:
            print(f"Warning: {error_msg}")
      else:
        # Valor no mapeado
        error_msg = f"Switcher '{self.name}': no mapping found for value '{value}' in field '{self.field}'"
        if self.fail_on_unmatch:
          raise ValueError(error_msg)
        else:
          print(f"Warning: {error_msg} - ignoring {len(filtered_df)} rows")
    
    # Manejar valores NaN si existen
    nan_df = df[df[self.field].isna()]
    if not nan_df.empty:
      error_msg = f"Switcher '{self.name}': found {len(nan_df)} rows with NaN values in field '{self.field}'"
      if self.fail_on_unmatch:
        raise ValueError(error_msg)
      else:
        print(f"Warning: {error_msg} - ignoring {len(nan_df)} rows")
    
    print(f"Switcher '{self.name}' completed: routed {total_routed_rows}/{len(df)} rows")
    
    # Limpiar después del procesamiento
    self.received_df = None
    
    
class CSVOrigin(Origin):
  def __init__(self, name: str, **kwargs):
    super().__init__()
    self.name = name
    self.csv_kwargs = kwargs  # Almacenar todos los argumentos para read_csv
    
  def add_output_pipe(self, pipe: Pipe) -> Pipe:
    # Solo permite 1 salida (como Generator)
    if len(self.outputs.keys()) == 0:
      self.outputs[pipe.get_name()] = pipe
      pipe.set_origin(self)
      return pipe
    else:
      raise ValueError(f"CSVOrigin '{self.name}' can only have 1 output")
  
  def pump(self) -> None:
    try:
      # Leer CSV usando pandas con todos los kwargs proporcionados
      df = pd.read_csv(**self.csv_kwargs)
      print(f"CSVOrigin '{self.name}' successfully read CSV with shape: {df.shape}")
      
      # Verificar que tenemos una salida configurada
      if len(self.outputs) > 0:
        output_pipe = list(self.outputs.values())[0]
        output_pipe.flow(df)
        print(f"CSVOrigin '{self.name}' pumped data through pipe '{output_pipe.get_name()}'")
      else:
        print(f"Warning: CSVOrigin '{self.name}' has no output pipe configured")
        
    except Exception as e:
      print(f"Error: CSVOrigin '{self.name}' failed to read CSV: {str(e)}")
      print(f"CSV arguments used: {self.csv_kwargs}")
      # No lanzamos la excepción para que el pipeline no se rompa completamente
      
      
class CSVDestination(Destination):
  def __init__(self, name: str, **kwargs):
    super().__init__()
    self.name = name
    self.csv_kwargs = kwargs  # Almacenar todos los argumentos para to_csv
    
  def add_input_pipe(self, pipe: Pipe) -> None:
    # Solo permite 1 entrada (como Printer)
    if len(self.inputs.keys()) == 0:
      self.inputs[pipe.get_name()] = pipe
    else:
      raise ValueError(f"CSVDestination '{self.name}' can only have 1 input")
    
  def sink(self, data_package: DataPackage) -> None:
    print(f"CSVDestination '{self.name}' received data from pipe: '{data_package.get_pipe_name()}'")
    df = data_package.get_df()
    
    try:
      # Escribir CSV usando pandas con todos los kwargs proporcionados
      df.to_csv(**self.csv_kwargs)
      print(f"CSVDestination '{self.name}' successfully wrote CSV with {len(df)} rows")
      print(f"CSV arguments used: {self.csv_kwargs}")
      
    except Exception as e:
      print(f"Error: CSVDestination '{self.name}' failed to write CSV: {str(e)}")
      print(f"CSV arguments used: {self.csv_kwargs}")
      print(f"DataFrame shape: {df.shape}")
      # No lanzamos la excepción para que el pipeline no se rompa completamente
      
      
class Copy(Node):
  def __init__(self, name: str):
    super().__init__()
    self.name = name
    self.received_df = None  # Para almacenar el DataFrame de entrada
    
  def add_input_pipe(self, pipe: Pipe) -> None:
    # Solo permite 1 entrada
    if len(self.inputs.keys()) == 0:
      self.inputs[pipe.get_name()] = pipe
    else:
      raise ValueError(f"Copy '{self.name}' can only have 1 input")
  
  def add_output_pipe(self, pipe: Pipe) -> Pipe:
    # Permite múltiples salidas
    self.outputs[pipe.get_name()] = pipe
    pipe.set_origin(self)
    return pipe
  
  def sink(self, data_package: DataPackage) -> None:
    print(f"Copy '{self.name}' received data from pipe: '{data_package.get_pipe_name()}'")
    df = data_package.get_df()
    
    # Almacenar el DataFrame para procesamiento en pump()
    self.received_df = df
    print(f"Copy '{self.name}' stored DataFrame with {len(df)} rows")
    
    # Procesar inmediatamente
    self.pump()
  
  def pump(self) -> None:
    if self.received_df is None:
      print(f"Warning: Copy '{self.name}' has no data to process")
      return
    
    if len(self.outputs) == 0:
      print(f"Warning: Copy '{self.name}' has no output pipes configured")
      return
    
    df = self.received_df
    print(f"Copy '{self.name}' processing DataFrame with {len(df)} rows")
    print(f"Copy '{self.name}' will create {len(self.outputs)} copies")
    
    # Enviar una copia del DataFrame a cada pipe de salida
    for pipe_name, pipe in self.outputs.items():
      df_copy = df.copy()  # Crear una copia independiente del DataFrame
      print(f"Copy '{self.name}': sending copy to pipe '{pipe_name}'")
      pipe.flow(df_copy)
    
    print(f"Copy '{self.name}' completed: sent {len(self.outputs)} copies")
    
    # Limpiar después del procesamiento
    self.received_df = None
    
    
class Aggregator(Node):
  def __init__(self, name: str, key: str, agg_field_name: str, agg_type: str, field_to_agg: str = None):
    super().__init__()
    self.name = name
    self.key = key
    self.agg_field_name = agg_field_name
    self.agg_type = agg_type
    self.field_to_agg = field_to_agg  # Campo opcional sobre el cual agregar
    self.received_df = None  # Para almacenar el DataFrame de entrada
    
    # Validar que el tipo de agregación sea válido
    valid_agg_types = ['sum', 'count', 'mean', 'median', 'min', 'max', 'std', 'var', 
                       'nunique', 'first', 'last', 'size', 'sem', 'quantile']
    if self.agg_type not in valid_agg_types:
      print(f"Warning: Aggregator '{self.name}': aggregation type '{self.agg_type}' might not be supported by pandas")
    
    # Validar field_to_agg según el tipo de agregación
    if self.agg_type != 'count' and self.field_to_agg is None:
      raise ValueError(f"Aggregator '{self.name}': field_to_agg is required for aggregation type '{self.agg_type}' (only 'count' can have field_to_agg=None)")
    
  def add_input_pipe(self, pipe: Pipe) -> None:
    # Solo permite 1 entrada
    if len(self.inputs.keys()) == 0:
      self.inputs[pipe.get_name()] = pipe
    else:
      raise ValueError(f"Aggregator '{self.name}' can only have 1 input")
  
  def add_output_pipe(self, pipe: Pipe) -> Pipe:
    # Solo permite 1 salida
    if len(self.outputs.keys()) == 0:
      self.outputs[pipe.get_name()] = pipe
      pipe.set_origin(self)
      return pipe
    else:
      raise ValueError(f"Aggregator '{self.name}' can only have 1 output")
  
  def sink(self, data_package: DataPackage) -> None:
    print(f"Aggregator '{self.name}' received data from pipe: '{data_package.get_pipe_name()}'")
    df = data_package.get_df()
    
    # Validar que el campo key existe en el DataFrame
    if self.key not in df.columns:
      raise ValueError(f"Aggregator '{self.name}': key field '{self.key}' not found in DataFrame columns: {list(df.columns)}")
    
    # Validar que el campo a agregar existe (si se especificó)
    if self.field_to_agg is not None and self.field_to_agg not in df.columns:
      raise ValueError(f"Aggregator '{self.name}': field_to_agg '{self.field_to_agg}' not found in DataFrame columns: {list(df.columns)}")
    
    # Almacenar el DataFrame para procesamiento en pump()
    self.received_df = df
    print(f"Aggregator '{self.name}' stored DataFrame with {len(df)} rows")
    
    # Procesar inmediatamente
    self.pump()
  
  def pump(self) -> None:
    if self.received_df is None:
      print(f"Warning: Aggregator '{self.name}' has no data to process")
      return
    
    if len(self.outputs) == 0:
      print(f"Warning: Aggregator '{self.name}' has no output pipe configured")
      return
    
    df = self.received_df
    print(f"Aggregator '{self.name}' processing DataFrame with {len(df)} rows")
    
    try:
      # Realizar la agregación usando groupby
      grouped = df.groupby(self.key)
      
      # Aplicar la función de agregación especificada
      if self.agg_type == 'count':
        # Para count, usar size() que cuenta todas las filas (incluyendo NaN)
        agg_result = grouped.size()
      else:
        # Para otras agregaciones, field_to_agg es obligatorio
        if self.field_to_agg is None:
          raise ValueError(f"Aggregator '{self.name}': field_to_agg cannot be None for aggregation type '{self.agg_type}'")
        # Agregar sobre el campo específico
        agg_result = grouped[self.field_to_agg].agg(self.agg_type)
      
      # Crear el DataFrame de salida aplanado
      result_df = pd.DataFrame({
        self.key: agg_result.index,
        self.agg_field_name: agg_result.values
      }).reset_index(drop=True)
      
      print(f"Aggregator '{self.name}' completed aggregation:")
      print(f"  - Grouped by: '{self.key}'")
      print(f"  - Aggregation: '{self.agg_type}'")
      print(f"  - Field to aggregate: '{self.field_to_agg}'" if self.field_to_agg else "  - Field to aggregate: all numeric fields")
      print(f"  - Input rows: {len(df)}")
      print(f"  - Output rows: {len(result_df)}")
      print(f"  - Output columns: {list(result_df.columns)}")
      
      # Enviar el resultado al pipe de salida
      output_pipe = list(self.outputs.values())[0]
      output_pipe.flow(result_df)
      print(f"Aggregator '{self.name}' pumped aggregated data through pipe '{output_pipe.get_name()}'")
      
    except Exception as e:
      print(f"Error: Aggregator '{self.name}' failed to perform aggregation: {str(e)}")
      print(f"Aggregation parameters: key='{self.key}', agg_type='{self.agg_type}', field_to_agg='{self.field_to_agg}'")
    
    # Limpiar después del procesamiento
    self.received_df = None
    
    
class DeleteColumns(Node):
  def __init__(self, name: str, columns: list):
    super().__init__()
    self.name = name
    self.columns = columns
    self.received_df = None  # Para almacenar el DataFrame de entrada
    
    # Validar que columns sea una lista
    if not isinstance(columns, list):
      raise ValueError(f"DeleteColumns '{self.name}': columns must be a list, got {type(columns)}")
    
    # Validar que la lista no esté vacía
    if len(columns) == 0:
      raise ValueError(f"DeleteColumns '{self.name}': columns list cannot be empty")
    
  def add_input_pipe(self, pipe: Pipe) -> None:
    # Solo permite 1 entrada
    if len(self.inputs.keys()) == 0:
      self.inputs[pipe.get_name()] = pipe
    else:
      raise ValueError(f"DeleteColumns '{self.name}' can only have 1 input")
  
  def add_output_pipe(self, pipe: Pipe) -> Pipe:
    # Solo permite 1 salida
    if len(self.outputs.keys()) == 0:
      self.outputs[pipe.get_name()] = pipe
      pipe.set_origin(self)
      return pipe
    else:
      raise ValueError(f"DeleteColumns '{self.name}' can only have 1 output")
  
  def sink(self, data_package: DataPackage) -> None:
    print(f"DeleteColumns '{self.name}' received data from pipe: '{data_package.get_pipe_name()}'")
    df = data_package.get_df()
    
    # Validar que todas las columnas a eliminar existen en el DataFrame
    missing_columns = [col for col in self.columns if col not in df.columns]
    if missing_columns:
      raise ValueError(f"DeleteColumns '{self.name}': columns {missing_columns} not found in DataFrame. Available columns: {list(df.columns)}")
    
    # Almacenar el DataFrame para procesamiento en pump()
    self.received_df = df
    print(f"DeleteColumns '{self.name}' stored DataFrame with {len(df)} rows and {len(df.columns)} columns")
    
    # Procesar inmediatamente
    self.pump()
  
  def pump(self) -> None:
    if self.received_df is None:
      print(f"Warning: DeleteColumns '{self.name}' has no data to process")
      return
    
    if len(self.outputs) == 0:
      print(f"Warning: DeleteColumns '{self.name}' has no output pipe configured")
      return
    
    df = self.received_df
    print(f"DeleteColumns '{self.name}' processing DataFrame with {len(df)} rows")
    print(f"DeleteColumns '{self.name}' original columns: {list(df.columns)}")
    print(f"DeleteColumns '{self.name}' deleting columns: {self.columns}")
    
    try:
      # Eliminar las columnas especificadas
      result_df = df.drop(columns=self.columns)
      
      print(f"DeleteColumns '{self.name}' completed:")
      print(f"  - Original columns: {len(df.columns)}")
      print(f"  - Deleted columns: {len(self.columns)}")
      print(f"  - Remaining columns: {len(result_df.columns)}")
      print(f"  - Final columns: {list(result_df.columns)}")
      
      # Enviar el resultado al pipe de salida
      output_pipe = list(self.outputs.values())[0]
      output_pipe.flow(result_df)
      print(f"DeleteColumns '{self.name}' pumped processed data through pipe '{output_pipe.get_name()}'")
      
    except Exception as e:
      print(f"Error: DeleteColumns '{self.name}' failed to delete columns: {str(e)}")
      print(f"Columns to delete: {self.columns}")
    
    # Limpiar después del procesamiento
    self.received_df = None
    

class Filter(Node):
  def __init__(self, name: str, field: str, condition: str, value_or_values):
    super().__init__()
    self.name = name
    self.field = field
    self.condition = condition
    self.value_or_values = value_or_values
    self.received_df = None  # Para almacenar el DataFrame de entrada
    
    # Validar que la condición sea soportada
    valid_conditions = ['<', '>', '<=', '>=', '!=', '=', 'in', 'not in', 'between']
    if self.condition not in valid_conditions:
      raise ValueError(f"Filter '{self.name}': condition '{self.condition}' not supported. Valid conditions: {valid_conditions}")
    
    # Validar que para 'in' y 'not in' se proporcione una lista
    if self.condition in ['in', 'not in']:
      if not isinstance(self.value_or_values, list):
        raise ValueError(f"Filter '{self.name}': condition '{self.condition}' requires a list for value_or_values, got {type(self.value_or_values)}")
      if len(self.value_or_values) == 0:
        raise ValueError(f"Filter '{self.name}': value_or_values list cannot be empty for condition '{self.condition}'")
    
    # Validar que para 'between' se proporcione una lista con 2 elementos
    if self.condition == 'between':
      if not isinstance(self.value_or_values, list):
        raise ValueError(f"Filter '{self.name}': condition 'between' requires a list for value_or_values, got {type(self.value_or_values)}")
      if len(self.value_or_values) != 2:
        raise ValueError(f"Filter '{self.name}': condition 'between' requires exactly 2 values [lower, upper], got {len(self.value_or_values)} values")
    
  def add_input_pipe(self, pipe: Pipe) -> None:
    # Solo permite 1 entrada
    if len(self.inputs.keys()) == 0:
      self.inputs[pipe.get_name()] = pipe
    else:
      raise ValueError(f"Filter '{self.name}' can only have 1 input")
  
  def add_output_pipe(self, pipe: Pipe) -> Pipe:
    # Solo permite 1 salida
    if len(self.outputs.keys()) == 0:
      self.outputs[pipe.get_name()] = pipe
      pipe.set_origin(self)
      return pipe
    else:
      raise ValueError(f"Filter '{self.name}' can only have 1 output")
  
  def sink(self, data_package: DataPackage) -> None:
    print(f"Filter '{self.name}' received data from pipe: '{data_package.get_pipe_name()}'")
    df = data_package.get_df()
    
    # Validar que el campo existe en el DataFrame
    if self.field not in df.columns:
      raise ValueError(f"Filter '{self.name}': field '{self.field}' not found in DataFrame columns: {list(df.columns)}")
    
    # Almacenar el DataFrame para procesamiento en pump()
    self.received_df = df
    print(f"Filter '{self.name}' stored DataFrame with {len(df)} rows")
    
    # Procesar inmediatamente
    self.pump()
  
  def pump(self) -> None:
    if self.received_df is None:
      print(f"Warning: Filter '{self.name}' has no data to process")
      return
    
    if len(self.outputs) == 0:
      print(f"Warning: Filter '{self.name}' has no output pipe configured")
      return
    
    df = self.received_df
    print(f"Filter '{self.name}' processing DataFrame with {len(df)} rows")
    print(f"Filter '{self.name}' applying filter: {self.field} {self.condition} {self.value_or_values}")
    
    try:
      # Aplicar el filtro según la condición
      if self.condition == '<':
        mask = df[self.field] < self.value_or_values
      elif self.condition == '>':
        mask = df[self.field] > self.value_or_values
      elif self.condition == '<=':
        mask = df[self.field] <= self.value_or_values
      elif self.condition == '>=':
        mask = df[self.field] >= self.value_or_values
      elif self.condition == '!=':
        mask = df[self.field] != self.value_or_values
      elif self.condition == '=':
        mask = df[self.field] == self.value_or_values
      elif self.condition == 'in':
        mask = df[self.field].isin(self.value_or_values)
      elif self.condition == 'not in':
        mask = ~df[self.field].isin(self.value_or_values)
      elif self.condition == 'between':
        lower_bound = self.value_or_values[0]
        upper_bound = self.value_or_values[1]
        mask = (df[self.field] >= lower_bound) & (df[self.field] <= upper_bound)
      
      # Aplicar la máscara para filtrar el DataFrame
      filtered_df = df[mask]
      
      print(f"Filter '{self.name}' completed:")
      print(f"  - Input rows: {len(df)}")
      print(f"  - Filtered rows: {len(filtered_df)}")
      print(f"  - Rows removed: {len(df) - len(filtered_df)}")
      print(f"  - Filter condition: {self.field} {self.condition} {self.value_or_values}")
      
      # Enviar el resultado al pipe de salida
      output_pipe = list(self.outputs.values())[0]
      output_pipe.flow(filtered_df)
      print(f"Filter '{self.name}' pumped filtered data through pipe '{output_pipe.get_name()}'")
      
    except Exception as e:
      print(f"Error: Filter '{self.name}' failed to apply filter: {str(e)}")
      print(f"Filter parameters: field='{self.field}', condition='{self.condition}', value_or_values={self.value_or_values}")
    
    # Limpiar después del procesamiento
    self.received_df = None    

    
class Joiner(Node):
  def __init__(self, name: str, left: str, right: str, key: str, join_type: str):
    super().__init__()
    self.name = name
    self.left_pipe_name = left
    self.right_pipe_name = right
    self.key = key
    self.join_type = join_type
    self.received_dfs = {}  # Para almacenar los DataFrames de ambas entradas
    self.expected_inputs = 2
    self.received_inputs = 0
    
    # Validar que el tipo de join sea soportado
    valid_join_types = ['left', 'right', 'inner']
    if self.join_type not in valid_join_types:
      raise ValueError(f"Joiner '{self.name}': join_type '{self.join_type}' not supported. Valid types: {valid_join_types}")
    
    # Validar que los nombres de pipes sean diferentes
    if self.left_pipe_name == self.right_pipe_name:
      raise ValueError(f"Joiner '{self.name}': left and right pipe names must be different")
  
  def add_input_pipe(self, pipe: Pipe) -> None:
    # Solo permite exactamente 2 entradas
    if len(self.inputs.keys()) < 2:
      self.inputs[pipe.get_name()] = pipe
    else:
      raise ValueError(f"Joiner '{self.name}' can only have 2 inputs")
  
  def add_output_pipe(self, pipe: Pipe) -> Pipe:
    # Solo permite 1 salida
    if len(self.outputs.keys()) == 0:
      self.outputs[pipe.get_name()] = pipe
      pipe.set_origin(self)
      return pipe
    else:
      raise ValueError(f"Joiner '{self.name}' can only have 1 output")
  
  def sink(self, data_package: DataPackage) -> None:
    pipe_name = data_package.get_pipe_name()
    df = data_package.get_df()
    
    print(f"Joiner '{self.name}' received data from pipe: '{pipe_name}'")
    
    # Validar que el pipe es uno de los esperados
    if pipe_name not in [self.left_pipe_name, self.right_pipe_name]:
      raise ValueError(f"Joiner '{self.name}': unexpected pipe '{pipe_name}'. Expected pipes: '{self.left_pipe_name}', '{self.right_pipe_name}'")
    
    # Validar que el campo key existe en el DataFrame
    if self.key not in df.columns:
      raise ValueError(f"Joiner '{self.name}': key field '{self.key}' not found in DataFrame from pipe '{pipe_name}'. Available columns: {list(df.columns)}")
    
    # Almacenar el DataFrame
    self.received_dfs[pipe_name] = df
    self.received_inputs += 1
    
    print(f"Joiner '{self.name}' stored DataFrame from '{pipe_name}' with {len(df)} rows")
    print(f"Joiner '{self.name}' has received {self.received_inputs}/{self.expected_inputs} DataFrames")
    
    # Solo procesar cuando hayamos recibido ambos inputs
    if self.received_inputs == self.expected_inputs:
      self.pump()
  
  def pump(self) -> None:
    if len(self.received_dfs) != 2:
      print(f"Warning: Joiner '{self.name}' needs exactly 2 DataFrames to join, got {len(self.received_dfs)}")
      return
    
    if len(self.outputs) == 0:
      print(f"Warning: Joiner '{self.name}' has no output pipe configured")
      return
    
    # Obtener los DataFrames left y right
    if self.left_pipe_name not in self.received_dfs:
      raise ValueError(f"Joiner '{self.name}': left DataFrame from pipe '{self.left_pipe_name}' not received")
    
    if self.right_pipe_name not in self.received_dfs:
      raise ValueError(f"Joiner '{self.name}': right DataFrame from pipe '{self.right_pipe_name}' not received")
    
    left_df = self.received_dfs[self.left_pipe_name]
    right_df = self.received_dfs[self.right_pipe_name]
    
    print(f"Joiner '{self.name}' processing join:")
    print(f"  - Left DataFrame: {len(left_df)} rows from pipe '{self.left_pipe_name}'")
    print(f"  - Right DataFrame: {len(right_df)} rows from pipe '{self.right_pipe_name}'")
    print(f"  - Join key: '{self.key}'")
    print(f"  - Join type: '{self.join_type}'")
    
    try:
      # Realizar el join usando pandas merge
      result_df = pd.merge(
        left_df, 
        right_df, 
        on=self.key, 
        how=self.join_type
      )
      
      print(f"Joiner '{self.name}' completed join:")
      print(f"  - Left input rows: {len(left_df)}")
      print(f"  - Right input rows: {len(right_df)}")
      print(f"  - Result rows: {len(result_df)}")
      print(f"  - Result columns: {len(result_df.columns)}")
      print(f"  - Final columns: {list(result_df.columns)}")
      
      # Enviar el resultado al pipe de salida
      output_pipe = list(self.outputs.values())[0]
      output_pipe.flow(result_df)
      print(f"Joiner '{self.name}' pumped joined data through pipe '{output_pipe.get_name()}'")
      
    except Exception as e:
      print(f"Error: Joiner '{self.name}' failed to perform join: {str(e)}")
      print(f"Join parameters: left='{self.left_pipe_name}', right='{self.right_pipe_name}', key='{self.key}', join_type='{self.join_type}'")
    
    # Limpiar después del procesamiento
    self.received_dfs = {}
    self.received_inputs = 0
    
    
    
class Transformer(Node):
  def __init__(self, name: str, transformer_function):
    super().__init__()
    self.name = name
    self.transformer_function = transformer_function
    self.received_df = None  # Para almacenar el DataFrame de entrada
    
    # Validar que transformer_function sea callable
    if not callable(self.transformer_function):
      raise ValueError(f"Transformer '{self.name}': transformer_function must be callable, got {type(self.transformer_function)}")
  
  def add_input_pipe(self, pipe: Pipe) -> None:
    # Solo permite 1 entrada
    if len(self.inputs.keys()) == 0:
      self.inputs[pipe.get_name()] = pipe
    else:
      raise ValueError(f"Transformer '{self.name}' can only have 1 input")
  
  def add_output_pipe(self, pipe: Pipe) -> Pipe:
    # Solo permite 1 salida
    if len(self.outputs.keys()) == 0:
      self.outputs[pipe.get_name()] = pipe
      pipe.set_origin(self)
      return pipe
    else:
      raise ValueError(f"Transformer '{self.name}' can only have 1 output")
  
  def sink(self, data_package: DataPackage) -> None:
    print(f"Transformer '{self.name}' received data from pipe: '{data_package.get_pipe_name()}'")
    df = data_package.get_df()
    
    # Almacenar el DataFrame para procesamiento en pump()
    self.received_df = df
    print(f"Transformer '{self.name}' stored DataFrame with {len(df)} rows and {len(df.columns)} columns")
    
    # Procesar inmediatamente
    self.pump()
  
  def pump(self) -> None:
    if self.received_df is None:
      print(f"Warning: Transformer '{self.name}' has no data to process")
      return
    
    if len(self.outputs) == 0:
      print(f"Warning: Transformer '{self.name}' has no output pipe configured")
      return
    
    df = self.received_df
    print(f"Transformer '{self.name}' processing DataFrame with {len(df)} rows")
    print(f"Transformer '{self.name}' original columns: {list(df.columns)}")
    
    try:
      # Aplicar la función de transformación
      transformed_df = self.transformer_function(df)
      
      # Validar que la función retorne un DataFrame
      if not isinstance(transformed_df, pd.DataFrame):
        raise ValueError(f"Transformer '{self.name}': transformer_function must return a pandas DataFrame, got {type(transformed_df)}")
      
      print(f"Transformer '{self.name}' completed transformation:")
      print(f"  - Input rows: {len(df)}")
      print(f"  - Input columns: {len(df.columns)}")
      print(f"  - Output rows: {len(transformed_df)}")
      print(f"  - Output columns: {len(transformed_df.columns)}")
      print(f"  - Final columns: {list(transformed_df.columns)}")
      
      # Enviar el resultado al pipe de salida
      output_pipe = list(self.outputs.values())[0]
      output_pipe.flow(transformed_df)
      print(f"Transformer '{self.name}' pumped transformed data through pipe '{output_pipe.get_name()}'")
      
    except Exception as e:
      print(f"Error: Transformer '{self.name}' failed to apply transformation: {str(e)}")
      print(f"Transformer function: {self.transformer_function}")
      print(f"Input DataFrame shape: {df.shape}")
      print(f"Input DataFrame columns: {list(df.columns)}")
      # En caso de error, podríamos opcionalmente pasar el DataFrame original
      # o simplemente no enviar nada (comportamiento actual)
    
    # Limpiar después del procesamiento
    self.received_df = None
    
    
    
class APIRestOrigin(Origin):
  def __init__(self, name: str, path: str = '.', fields: list = None, **kwargs):
    super().__init__()
    self.name = name
    self.path = path
    self.fields = fields
    self.request_kwargs = kwargs  # Almacenar todos los argumentos para requests
    
    # Validar que fields sea una lista si se proporciona
    if self.fields is not None and not isinstance(self.fields, list):
      raise ValueError(f"APIRestOrigin '{self.name}': fields must be a list, got {type(self.fields)}")
    
    # Validar que fields no esté vacía si se proporciona
    if self.fields is not None and len(self.fields) == 0:
      raise ValueError(f"APIRestOrigin '{self.name}': fields list cannot be empty")
  
  def add_output_pipe(self, pipe: Pipe) -> Pipe:
    # Solo permite 1 salida (como CSVOrigin)
    if len(self.outputs.keys()) == 0:
      self.outputs[pipe.get_name()] = pipe
      pipe.set_origin(self)
      return pipe
    else:
      raise ValueError(f"APIRestOrigin '{self.name}' can only have 1 output")
  
  def pump(self) -> None:
    try:
      # Realizar la petición HTTP usando requests con todos los kwargs
      print(f"APIRestOrigin '{self.name}' making HTTP request...")
      print(f"Request parameters: {self.request_kwargs}")
      
      response = requests.request(**self.request_kwargs)
      
      # Verificar que la respuesta sea exitosa
      response.raise_for_status()
      
      # Obtener JSON de la respuesta
      json_data = response.json()
      print(f"APIRestOrigin '{self.name}' received JSON response")
      
      # Navegar por el path especificado
      data = self._navigate_path(json_data, self.path)
      
      # Convertir a DataFrame
      if isinstance(data, list):
        df = pd.DataFrame(data)
      elif isinstance(data, dict):
        # Si es un diccionario, convertir a lista de un elemento
        df = pd.DataFrame([data])
      else:
        raise ValueError(f"APIRestOrigin '{self.name}': data at path '{self.path}' must be a list or dict, got {type(data)}")
      
      # Filtrar por campos si se especificaron
      if self.fields is not None:
        # Validar que todos los campos existen
        missing_fields = [field for field in self.fields if field not in df.columns]
        if missing_fields:
          raise ValueError(f"APIRestOrigin '{self.name}': fields {missing_fields} not found in response data. Available fields: {list(df.columns)}")
        
        # Seleccionar solo los campos especificados
        df = df[self.fields]
        print(f"APIRestOrigin '{self.name}' filtered to fields: {self.fields}")
      
      print(f"APIRestOrigin '{self.name}' successfully created DataFrame with shape: {df.shape}")
      print(f"APIRestOrigin '{self.name}' columns: {list(df.columns)}")
      
      # Verificar que tenemos una salida configurada
      if len(self.outputs) > 0:
        output_pipe = list(self.outputs.values())[0]
        output_pipe.flow(df)
        print(f"APIRestOrigin '{self.name}' pumped data through pipe '{output_pipe.get_name()}'")
      else:
        print(f"Warning: APIRestOrigin '{self.name}' has no output pipe configured")
        
    except requests.exceptions.RequestException as e:
      print(f"Error: APIRestOrigin '{self.name}' HTTP request failed: {str(e)}")
      print(f"Request parameters: {self.request_kwargs}")
    except ValueError as e:
      print(f"Error: APIRestOrigin '{self.name}' data processing failed: {str(e)}")
    except Exception as e:
      print(f"Error: APIRestOrigin '{self.name}' unexpected error: {str(e)}")
  
  def _navigate_path(self, data, path):
    """
    Navega por el path especificado en la estructura JSON
    """
    if path == '.':
      return data
    
    try:
      # Dividir el path por puntos y navegar
      parts = path.split('.')
      current = data
      
      for part in parts:
        if isinstance(current, dict) and part in current:
          current = current[part]
        else:
          raise KeyError(f"Path part '{part}' not found")
      
      return current
      
    except Exception as e:
      raise ValueError(f"APIRestOrigin '{self.name}': failed to navigate path '{path}': {str(e)}")
    
    
    
        
class RemoveDuplicates(Node):
  def __init__(self, name: str, key: str, sort_by: str, orientation: str, retain: str):
    super().__init__()
    self.name = name
    self.key = key
    self.sort_by = sort_by
    self.orientation = orientation
    self.retain = retain
    self.received_df = None  # Para almacenar el DataFrame de entrada
    
    # Validar orientation
    valid_orientations = ['ASC', 'DESC']
    if self.orientation not in valid_orientations:
      raise ValueError(f"RemoveDuplicates '{self.name}': orientation must be one of {valid_orientations}, got '{self.orientation}'")
    
    # Validar retain
    valid_retains = ['FIRST', 'LAST']
    if self.retain not in valid_retains:
      raise ValueError(f"RemoveDuplicates '{self.name}': retain must be one of {valid_retains}, got '{self.retain}'")
  
  def add_input_pipe(self, pipe: Pipe) -> None:
    # Solo permite 1 entrada
    if len(self.inputs.keys()) == 0:
      self.inputs[pipe.get_name()] = pipe
    else:
      raise ValueError(f"RemoveDuplicates '{self.name}' can only have 1 input")
  
  def add_output_pipe(self, pipe: Pipe) -> Pipe:
    # Solo permite 1 salida
    if len(self.outputs.keys()) == 0:
      self.outputs[pipe.get_name()] = pipe
      pipe.set_origin(self)
      return pipe
    else:
      raise ValueError(f"RemoveDuplicates '{self.name}' can only have 1 output")
  
  def sink(self, data_package: DataPackage) -> None:
    print(f"RemoveDuplicates '{self.name}' received data from pipe: '{data_package.get_pipe_name()}'")
    df = data_package.get_df()
    
    # Validar que el campo key existe en el DataFrame
    if self.key not in df.columns:
      raise ValueError(f"RemoveDuplicates '{self.name}': key field '{self.key}' not found in DataFrame columns: {list(df.columns)}")
    
    # Validar que el campo sort_by existe en el DataFrame
    if self.sort_by not in df.columns:
      raise ValueError(f"RemoveDuplicates '{self.name}': sort_by field '{self.sort_by}' not found in DataFrame columns: {list(df.columns)}")
    
    # Almacenar el DataFrame para procesamiento en pump()
    self.received_df = df
    print(f"RemoveDuplicates '{self.name}' stored DataFrame with {len(df)} rows")
    
    # Procesar inmediatamente
    self.pump()
  
  def pump(self) -> None:
    if self.received_df is None:
      print(f"Warning: RemoveDuplicates '{self.name}' has no data to process")
      return
    
    if len(self.outputs) == 0:
      print(f"Warning: RemoveDuplicates '{self.name}' has no output pipe configured")
      return
    
    df = self.received_df
    print(f"RemoveDuplicates '{self.name}' processing DataFrame with {len(df)} rows")
    print(f"RemoveDuplicates '{self.name}' configuration:")
    print(f"  - Key field: '{self.key}'")
    print(f"  - Sort by: '{self.sort_by}'")
    print(f"  - Orientation: '{self.orientation}'")
    print(f"  - Retain: '{self.retain}'")
    
    try:
      # Determinar el orden ascendente/descendente
      ascending = True if self.orientation == 'ASC' else False
      
      # Ordenar el DataFrame por el campo sort_by
      sorted_df = df.sort_values(by=self.sort_by, ascending=ascending)
      print(f"RemoveDuplicates '{self.name}' sorted DataFrame by '{self.sort_by}' ({self.orientation})")
      
      # Determinar qué registro mantener (first o last después del ordenamiento)
      keep = 'first' if self.retain == 'FIRST' else 'last'
      
      # Remover duplicados basándose en el campo key
      result_df = sorted_df.drop_duplicates(subset=[self.key], keep=keep)
      
      # Contar duplicados removidos
      duplicates_removed = len(df) - len(result_df)
      
      print(f"RemoveDuplicates '{self.name}' completed:")
      print(f"  - Input rows: {len(df)}")
      print(f"  - Output rows: {len(result_df)}")
      print(f"  - Duplicates removed: {duplicates_removed}")
      print(f"  - Retention strategy: Keep {self.retain} occurrence after sorting by '{self.sort_by}' {self.orientation}")
      
      # Enviar el resultado al pipe de salida
      output_pipe = list(self.outputs.values())[0]
      output_pipe.flow(result_df)
      print(f"RemoveDuplicates '{self.name}' pumped deduplicated data through pipe '{output_pipe.get_name()}'")
      
    except Exception as e:
      print(f"Error: RemoveDuplicates '{self.name}' failed to remove duplicates: {str(e)}")
      print(f"Configuration: key='{self.key}', sort_by='{self.sort_by}', orientation='{self.orientation}', retain='{self.retain}'")
    
    # Limpiar después del procesamiento
    self.received_df = None
    


 
 