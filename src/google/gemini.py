
from src.core.base import DataPackage, Pipe, Node
import google.generativeai as genai
from google.generativeai import types
import pandas as pd
import re
from io import StringIO


class GeminiPromptTransformer(Node):
  def __init__(self, name: str, model: str, api_key: str, prompt: str, max_tokens: int = 16000):
    super().__init__()
    self.name = name
    self.model = model
    self.api_key = api_key
    self.prompt = prompt
    # Usamos max_output_tokens, el nombre correcto para la configuración de la API de Gemini
    self.max_output_tokens = max_tokens 
    self.received_df = None
    self.client = None
    
    # (Validaciones de constructor...)
    if not model or not model.strip():
      raise ValueError(f"GeminiPromptTransformer '{self.name}': model cannot be empty")
    if not api_key or not api_key.strip():
      raise ValueError(f"GeminiPromptTransformer '{self.name}': api_key cannot be empty")
    if not prompt or not prompt.strip():
      raise ValueError(f"GeminiPromptTransformer '{self.name}': prompt cannot be empty")
    if self.max_output_tokens <= 0:
      raise ValueError(f"GeminiPromptTransformer '{self.name}': max_tokens must be positive, got {self.max_output_tokens}")
  
  def add_input_pipe(self, pipe: Pipe) -> None:
    # Solo permite 1 entrada
    if len(self.inputs.keys()) == 0:
      self.inputs[pipe.get_name()] = pipe
    else:
      raise ValueError(f"GeminiPromptTransformer '{self.name}' can only have 1 input")
  
  def add_output_pipe(self, pipe: Pipe) -> Pipe:
    # Solo permite 1 salida
    if len(self.outputs.keys()) == 0:
      self.outputs[pipe.get_name()] = pipe
      pipe.set_origin(self)
      return pipe
    else:
      raise ValueError(f"GeminiPromptTransformer '{self.name}' can only have 1 output")
  
  def _initialize_client(self):
    """Inicializa el cliente de Gemini y el modelo."""
    try:
      genai.configure(api_key=self.api_key)
      self.client = genai.GenerativeModel(self.model)
      print(f"GeminiPromptTransformer '{self.name}' Gemini client initialized successfully")
    except Exception as e:
      raise ValueError(f"GeminiPromptTransformer '{self.name}' failed to initialize Gemini client: {str(e)}")
  
  def sink(self, data_package: DataPackage) -> None:
    print(f"GeminiPromptTransformer '{self.name}' received data from pipe: '{data_package.get_pipe_name()}'")
    df = data_package.get_df()
    
    self.received_df = df
    print(f"GeminiPromptTransformer '{self.name}' stored DataFrame with {len(df)} rows and {len(df.columns)} columns")
    
    self.pump()
  
  def pump(self) -> None:
    if self.received_df is None or len(self.outputs) == 0:
      print(f"Warning: GeminiPromptTransformer '{self.name}' has no data to process or no output pipe configured")
      return
    
    df = self.received_df
    print(f"GeminiPromptTransformer '{self.name}' processing DataFrame with {len(df)} rows")
    
    try:
      if self.client is None:
        self._initialize_client()
      
      # Convertir DataFrame a CSV
      data_csv = df.to_csv(index=False)
      
      print(f"GeminiPromptTransformer '{self.name}' sending request to Gemini API...")
      
      # *** CLAVE: system_instruction para replicar el system prompt de Anthropic ***
      system_instruction = """You are a data transformation assistant. You must ONLY transform the data provided in CSV format according to the user's explicit instructions.
CRITICAL OUTPUT RULES:
1. Return ONLY the transformed data in **raw CSV format** (no explanations, notes, or markdown formatting).
2. The first line MUST be the column headers (comma-separated).
3. Do NOT wrap the output in code blocks (```) or backticks."""
      
      user_message = f"""Here is the input data in CSV format:

{data_csv}

TRANSFORMATION TASK: {self.prompt}

Return the complete CSV only."""
      
      # *** CLAVE: Configuración de generación (Control de formato y temperatura baja) ***
      generation_config = types.GenerateContentConfig(
          system_instruction=system_instruction, 
          temperature=0.0, # Máxima adherencia a la instrucción
          max_output_tokens=self.max_output_tokens,
      )
      
      # Llamada a la API
      response = self.client.generate_content(
        contents=user_message,
        config=generation_config
      )
      
      response_text = response.text
      
      # (Lógica de logs de tokens...)
      try:
        usage_metadata = response.usage_metadata
        input_tokens = usage_metadata.prompt_token_count
        output_tokens = usage_metadata.candidates_token_count
        total_tokens = usage_metadata.total_token_count
        print(f"Token usage: Input={input_tokens}, Output={output_tokens}, Total={total_tokens}")
      except AttributeError:
        print(f"Token usage information not available")

      # Parsing Robusto
      response_text = response_text.strip()
      
      # Remover posibles code blocks de markdown (tu lógica es excelente)
      if response_text.startswith('```'):
        csv_match = re.search(r'```(?:csv)?\s*(.*?)\s*```', response_text, re.DOTALL)
        if csv_match:
          response_text = csv_match.group(1).strip()
          print(f"Removed markdown code block wrapper")
      
      result_df = pd.read_csv(StringIO(response_text))
      
      print(f"GeminiPromptTransformer '{self.name}' completed transformation successfully.")
      
      # Enviar el resultado al pipe de salida
      output_pipe = list(self.outputs.values())[0]
      output_pipe.flow(result_df)
      
    except Exception as e:
      print(f"Error: GeminiPromptTransformer '{self.name}' failed to process data: {type(e).__name__}: {str(e)}")
      # En caso de error de parsing, mostramos la respuesta para depurar
      if 'ParserError' in str(e) or 'EmptyDataError' in str(e):
          print(f"FATAL PARSING ERROR. Response preview: {response_text[:300]}...")
      raise
    
    finally:
      self.received_df = None
  def __init__(self, name: str, model: str, api_key: str, prompt: str, max_tokens: int = 16000):
    super().__init__()
    self.name = name
    self.model = model
    self.api_key = api_key
    self.prompt = prompt
    self.max_tokens = max_tokens
    self.received_df = None  # Para almacenar el DataFrame de entrada
    self.client = None
    
    # Validar que model no esté vacío
    if not model or not model.strip():
      raise ValueError(f"GeminiPromptTransformer '{self.name}': model cannot be empty")
    
    # Validar que api_key no esté vacía
    if not api_key or not api_key.strip():
      raise ValueError(f"GeminiPromptTransformer '{self.name}': api_key cannot be empty")
    
    # Validar que prompt no esté vacío
    if not prompt or not prompt.strip():
      raise ValueError(f"GeminiPromptTransformer '{self.name}': prompt cannot be empty")
    
    # Validar que max_tokens sea positivo
    if max_tokens <= 0:
      raise ValueError(f"GeminiPromptTransformer '{self.name}': max_tokens must be positive, got {max_tokens}")
  
  def add_input_pipe(self, pipe: Pipe) -> None:
    # Solo permite 1 entrada
    if len(self.inputs.keys()) == 0:
      self.inputs[pipe.get_name()] = pipe
    else:
      raise ValueError(f"GeminiPromptTransformer '{self.name}' can only have 1 input")
  
  def add_output_pipe(self, pipe: Pipe) -> Pipe:
    # Solo permite 1 salida
    if len(self.outputs.keys()) == 0:
      self.outputs[pipe.get_name()] = pipe
      pipe.set_origin(self)
      return pipe
    else:
      raise ValueError(f"GeminiPromptTransformer '{self.name}' can only have 1 output")
  
  def _initialize_client(self):
    """
    Inicializa el cliente de Gemini
    """
    try:
      genai.configure(api_key=self.api_key)
      self.client = genai.GenerativeModel(self.model)
      print(f"GeminiPromptTransformer '{self.name}' Gemini client initialized successfully")
    except Exception as e:
      raise ValueError(f"GeminiPromptTransformer '{self.name}' failed to initialize Gemini client: {str(e)}")
  
  def sink(self, data_package: DataPackage) -> None:
    print(f"GeminiPromptTransformer '{self.name}' received data from pipe: '{data_package.get_pipe_name()}'")
    df = data_package.get_df()
    
    # Almacenar el DataFrame para procesamiento en pump()
    self.received_df = df
    print(f"GeminiPromptTransformer '{self.name}' stored DataFrame with {len(df)} rows and {len(df.columns)} columns")
    
    # Procesar inmediatamente
    self.pump()
  
  def pump(self) -> None:
    if self.received_df is None:
      print(f"Warning: GeminiPromptTransformer '{self.name}' has no data to process")
      return
    
    if len(self.outputs) == 0:
      print(f"Warning: GeminiPromptTransformer '{self.name}' has no output pipe configured")
      return
    
    df = self.received_df
    print(f"GeminiPromptTransformer '{self.name}' processing DataFrame with {len(df)} rows")
    print(f"GeminiPromptTransformer '{self.name}' using model: {self.model}")
    
    try:
      # Inicializar cliente si no existe
      if self.client is None:
        self._initialize_client()
      
      # Convertir DataFrame a CSV
      data_csv = df.to_csv(index=False)
      
      print(f"GeminiPromptTransformer '{self.name}' sending request to Gemini API...")
      print(f"Input data size: {len(data_csv)} characters")
      
      # Construir el mensaje para Gemini - versión mejorada
      full_prompt = f"""You are a data transformation assistant. Transform the following CSV data according to these instructions.

INPUT DATA (CSV format):
{data_csv}

TRANSFORMATION INSTRUCTIONS:
{self.prompt}

CRITICAL OUTPUT REQUIREMENTS:
1. Return ONLY the transformed data in CSV format
2. First line MUST be the column headers (comma-separated)
3. Following lines MUST contain the data rows (comma-separated)
4. Include ALL matching rows, do not truncate or summarize
5. Do NOT include any explanations, notes, or markdown formatting
6. Do NOT wrap output in code blocks or backticks
7. Return the complete CSV with all data rows that match the criteria

OUTPUT (CSV format only):"""
      
      # Configurar generación con parámetros más permisivos
      generation_config = genai.types.GenerationConfig(
        max_output_tokens=self.max_tokens,
        temperature=0.2,  # Ligeramente aumentada para más creatividad
        top_p=0.95,
        top_k=40
      )
      
      # Llamar a la API de Gemini
      response = self.client.generate_content(
        full_prompt,
        generation_config=generation_config
      )
      
      # Extraer la respuesta
      response_text = response.text
      
      # Obtener información de uso de tokens (si está disponible)
      try:
        usage_metadata = response.usage_metadata
        input_tokens = usage_metadata.prompt_token_count
        output_tokens = usage_metadata.candidates_token_count
        total_tokens = usage_metadata.total_token_count
        
        print(f"GeminiPromptTransformer '{self.name}' received response from Gemini")
        print(f"Response size: {len(response_text)} characters")
        print(f"Token usage:")
        print(f"  - Input tokens: {input_tokens}")
        print(f"  - Output tokens: {output_tokens}")
        print(f"  - Total tokens: {total_tokens}")
      except AttributeError:
        print(f"GeminiPromptTransformer '{self.name}' received response from Gemini")
        print(f"Response size: {len(response_text)} characters")
        print(f"Token usage information not available")
      
      print(f"Response preview (first 300 chars): {response_text[:300]}")
      print(f"Response ending (last 300 chars): ...{response_text[-300:]}")
      
      # Parsear la respuesta CSV
      try:
        # Limpiar la respuesta
        response_text = response_text.strip()
        
        # Remover posibles code blocks de markdown
        if response_text.startswith('```'):
          import re
          csv_match = re.search(r'```(?:csv)?\s*(.*?)\s*```', response_text, re.DOTALL)
          if csv_match:
            response_text = csv_match.group(1).strip()
            print(f"Removed markdown code block wrapper")
        
        # Convertir CSV a DataFrame usando pandas
        result_df = pd.read_csv(StringIO(response_text))
        print(f"Successfully parsed CSV with {len(result_df)} records")
        
      except (pd.errors.ParserError, pd.errors.EmptyDataError) as e:
        print(f"Error: GeminiPromptTransformer '{self.name}' failed to parse CSV response: {str(e)}")
        print(f"Response preview (first 500 chars): {response_text[:500]}")
        print(f"Response ending (last 500 chars): ...{response_text[-500:]}")
        raise
      
      print(f"GeminiPromptTransformer '{self.name}' completed transformation:")
      print(f"  - Input rows: {len(df)}")
      print(f"  - Input columns: {len(df.columns)}")
      print(f"  - Output rows: {len(result_df)}")
      print(f"  - Output columns: {len(result_df.columns)}")
      print(f"  - Output column names: {list(result_df.columns)}")
      
      # Enviar el resultado al pipe de salida
      output_pipe = list(self.outputs.values())[0]
      output_pipe.flow(result_df)
      print(f"GeminiPromptTransformer '{self.name}' pumped transformed data through pipe '{output_pipe.get_name()}'")
      
    except Exception as e:
      print(f"Error: GeminiPromptTransformer '{self.name}' unexpected error: {str(e)}")
      print(f"Model: {self.model}")
      print(f"Input DataFrame shape: {df.shape}")
    
    # Limpiar después del procesamiento
    self.received_df = None
    
  