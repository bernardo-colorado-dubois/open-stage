# src/google/gemini.py

from src.core.base import DataPackage, Pipe, Node
import google.generativeai as genai
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
    self.max_tokens = max_tokens
    self.received_df = None
    self.client = None
    
    # Validaciones
    if not model or not model.strip():
      raise ValueError(f"GeminiPromptTransformer '{self.name}': model cannot be empty")
    
    if not api_key or not api_key.strip():
      raise ValueError(f"GeminiPromptTransformer '{self.name}': api_key cannot be empty")
    
    if not prompt or not prompt.strip():
      raise ValueError(f"GeminiPromptTransformer '{self.name}': prompt cannot be empty")
    
    if max_tokens <= 0:
      raise ValueError(f"GeminiPromptTransformer '{self.name}': max_tokens must be positive, got {max_tokens}")
  
  def add_input_pipe(self, pipe: Pipe) -> None:
    if len(self.inputs.keys()) == 0:
      self.inputs[pipe.get_name()] = pipe
    else:
      raise ValueError(f"GeminiPromptTransformer '{self.name}' can only have 1 input")
  
  def add_output_pipe(self, pipe: Pipe) -> Pipe:
    if len(self.outputs.keys()) == 0:
      self.outputs[pipe.get_name()] = pipe
      pipe.set_origin(self)
      return pipe
    else:
      raise ValueError(f"GeminiPromptTransformer '{self.name}' can only have 1 output")
  
  def _initialize_client(self):
    """
    Inicializa el cliente de Gemini con system_instruction
    """
    try:
      genai.configure(api_key=self.api_key)
      
      # System instruction se pasa al crear el modelo
      system_instruction = """You are a data transformation assistant. You will receive data in CSV format and transform it according to the user's instructions.
CRITICAL: You must ALWAYS return your response as valid CSV format:
- First line must be the header with column names
- Following lines contain the data rows
- Use comma as delimiter
- Properly escape values containing commas or quotes
- NEVER include explanations, markdown code blocks, or any text outside the CSV
- Do not wrap the CSV in any formatting like ```csv
- Return ONLY the raw CSV data"""
      
      self.client = genai.GenerativeModel(
        model_name=self.model,
        system_instruction=system_instruction
      )
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
      if self.client is None:
        self._initialize_client()
      
      # Convertir DataFrame a CSV
      data_csv = df.to_csv(index=False)
      
      print(f"GeminiPromptTransformer '{self.name}' sending request to Gemini API...")
      print(f"Input data size: {len(data_csv)} characters")
      
      user_message = f"""Here is the input data in CSV format:

{data_csv}

Task: {self.prompt}

Remember: Return ONLY raw CSV format, no explanations, no markdown, no code blocks."""
      
      # Configuración de generación
      generation_config = genai.types.GenerationConfig(
        max_output_tokens=self.max_tokens,
        temperature=0.0,  # Máxima adherencia a instrucciones
        top_p=0.95,
        top_k=40
      )
      
      # Llamar a la API de Gemini (sin system_instruction aquí)
      response = self.client.generate_content(
        contents=user_message,
        generation_config=generation_config
      )
      
      response_text = response.text
      
      # Obtener información de uso de tokens
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
        response_text = response_text.strip()
        
        # Remover posibles code blocks de markdown
        if response_text.startswith('```'):
          csv_match = re.search(r'```(?:csv)?\s*(.*?)\s*```', response_text, re.DOTALL)
          if csv_match:
            response_text = csv_match.group(1).strip()
            print(f"Removed markdown code block wrapper")
        
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
      raise
    
    finally:
      self.received_df = None