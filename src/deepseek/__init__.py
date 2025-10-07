# src/deepseek/deepseek.py

import pandas as pd
from openai import OpenAI
import re
from io import StringIO
from src.core.base import DataPackage, Pipe, Node


class DeepSeekTransformer(Node):
    def __init__(self, name: str, model: str, api_key: str, prompt: str, max_tokens: int = 16000, base_url: str = "https://api.deepseek.com"):
        super().__init__()
        self.name = name
        self.model = model
        self.api_key = api_key
        self.prompt = prompt
        self.max_tokens = max_tokens
        self.base_url = base_url
        self.received_df = None
        self.client = None
        
        # Validar que model no esté vacío
        if not model or not model.strip():
            raise ValueError(f"DeepSeekTransformer '{self.name}': model cannot be empty")
        
        # Validar que api_key no esté vacía
        if not api_key or not api_key.strip():
            raise ValueError(f"DeepSeekTransformer '{self.name}': api_key cannot be empty")
        
        # Validar que prompt no esté vacío
        if not prompt or not prompt.strip():
            raise ValueError(f"DeepSeekTransformer '{self.name}': prompt cannot be empty")
        
        # Validar que max_tokens sea positivo
        if max_tokens <= 0:
            raise ValueError(f"DeepSeekTransformer '{self.name}': max_tokens must be positive, got {max_tokens}")
    
    def add_input_pipe(self, pipe: Pipe) -> None:
        # Solo permite 1 entrada
        if len(self.inputs.keys()) == 0:
            self.inputs[pipe.get_name()] = pipe
        else:
            raise ValueError(f"DeepSeekTransformer '{self.name}' can only have 1 input")
    
    def add_output_pipe(self, pipe: Pipe) -> Pipe:
        # Solo permite 1 salida
        if len(self.outputs.keys()) == 0:
            self.outputs[pipe.get_name()] = pipe
            pipe.set_origin(self)
            return pipe
        else:
            raise ValueError(f"DeepSeekTransformer '{self.name}' can only have 1 output")
    
    def _initialize_client(self):
        """
        Inicializa el cliente de DeepSeek usando OpenAI SDK
        """
        try:
            self.client = OpenAI(
                api_key=self.api_key,
                base_url=self.base_url
            )
            print(f"DeepSeekTransformer '{self.name}' DeepSeek client initialized successfully")
            print(f"Base URL: {self.base_url}")
        except Exception as e:
            raise ValueError(f"DeepSeekTransformer '{self.name}' failed to initialize DeepSeek client: {str(e)}")
    
    def sink(self, data_package: DataPackage) -> None:
        print(f"DeepSeekTransformer '{self.name}' received data from pipe: '{data_package.get_pipe_name()}'")
        df = data_package.get_df()
        
        # Almacenar el DataFrame para procesamiento en pump()
        self.received_df = df
        print(f"DeepSeekTransformer '{self.name}' stored DataFrame with {len(df)} rows and {len(df.columns)} columns")
        
        # Procesar inmediatamente
        self.pump()
    
    def pump(self) -> None:
        if self.received_df is None:
            print(f"Warning: DeepSeekTransformer '{self.name}' has no data to process")
            return
        
        if len(self.outputs) == 0:
            print(f"Warning: DeepSeekTransformer '{self.name}' has no output pipe configured")
            return
        
        df = self.received_df
        print(f"DeepSeekTransformer '{self.name}' processing DataFrame with {len(df)} rows")
        print(f"DeepSeekTransformer '{self.name}' using model: {self.model}")
        
        try:
            # Inicializar cliente si no existe
            if self.client is None:
                self._initialize_client()
            
            # Convertir DataFrame a CSV
            data_csv = df.to_csv(index=False)
            
            print(f"DeepSeekTransformer '{self.name}' sending request to DeepSeek API...")
            print(f"Input data size: {len(data_csv)} characters")
            
            # Construir los mensajes para DeepSeek
            system_message = """You are a data transformation assistant. You will receive data in CSV format and transform it according to the user's instructions.
CRITICAL: You must ALWAYS return your response as valid CSV format:
- First line must be the header with column names
- Following lines contain the data rows
- Use comma as delimiter
- Properly escape values containing commas or quotes
- NEVER include explanations, markdown code blocks, or any text outside the CSV
- Do not wrap the CSV in any formatting like ```csv
- Return ONLY the raw CSV data"""
            
            user_message = f"""Here is the input data in CSV format:

{data_csv}

Task: {self.prompt}

Remember: Return ONLY raw CSV format, no explanations, no markdown, no code blocks."""
            
            # Llamar a la API de DeepSeek usando OpenAI SDK
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_message},
                    {"role": "user", "content": user_message}
                ],
                max_tokens=self.max_tokens,
                temperature=0.0,  # Máxima adherencia a instrucciones
                stream=False
            )
            
            # Extraer la respuesta
            response_text = response.choices[0].message.content
            
            # Verificar si la respuesta fue truncada
            finish_reason = response.choices[0].finish_reason
            
            # Obtener información de uso de tokens
            usage = response.usage
            input_tokens = usage.prompt_tokens
            output_tokens = usage.completion_tokens
            total_tokens = usage.total_tokens
            
            print(f"DeepSeekTransformer '{self.name}' received response from DeepSeek")
            print(f"Response size: {len(response_text)} characters")
            print(f"Finish reason: {finish_reason}")
            print(f"Token usage:")
            print(f"  - Input tokens: {input_tokens}")
            print(f"  - Output tokens: {output_tokens}")
            print(f"  - Total tokens: {total_tokens}")
            print(f"Response preview (first 300 chars): {response_text[:300]}")
            print(f"Response ending (last 300 chars): ...{response_text[-300:]}")
            
            if finish_reason == "length":
                print(f"WARNING: Response was truncated! Attempting to fix...")
            
            # Parsear la respuesta CSV
            try:
                # Limpiar la respuesta
                response_text = response_text.strip()
                
                # Remover posibles code blocks de markdown
                if response_text.startswith('```'):
                    # Buscar y remover bloques de código
                    csv_match = re.search(r'```(?:csv)?\s*(.*?)\s*```', response_text, re.DOTALL)
                    if csv_match:
                        response_text = csv_match.group(1).strip()
                        print(f"Removed markdown code block wrapper")
                
                # Si la respuesta fue truncada, intentar arreglarla
                if finish_reason == "length":
                    print(f"WARNING: Response was truncated! Attempting to fix...")
                    # Para CSV, buscar la última línea completa
                    last_newline = response_text.rfind('\n')
                    if last_newline != -1:
                        response_text = response_text[:last_newline]
                        print(f"Repaired CSV by removing incomplete last line")
                
                # Convertir CSV a DataFrame usando pandas
                result_df = pd.read_csv(StringIO(response_text))
                print(f"Successfully parsed CSV with {len(result_df)} records")
                
            except (pd.errors.ParserError, pd.errors.EmptyDataError) as e:
                print(f"Error: DeepSeekTransformer '{self.name}' failed to parse CSV response: {str(e)}")
                print(f"Response preview (first 500 chars): {response_text[:500]}")
                print(f"Response ending (last 500 chars): ...{response_text[-500:]}")
                raise
            
            print(f"DeepSeekTransformer '{self.name}' completed transformation:")
            print(f"  - Input rows: {len(df)}")
            print(f"  - Input columns: {len(df.columns)}")
            print(f"  - Output rows: {len(result_df)}")
            print(f"  - Output columns: {len(result_df.columns)}")
            print(f"  - Output column names: {list(result_df.columns)}")
            
            # Enviar el resultado al pipe de salida
            output_pipe = list(self.outputs.values())[0]
            output_pipe.flow(result_df)
            print(f"DeepSeekTransformer '{self.name}' pumped transformed data through pipe '{output_pipe.get_name()}'")
            
        except Exception as e:
            print(f"Error: DeepSeekTransformer '{self.name}' unexpected error: {str(e)}")
            print(f"Model: {self.model}")
            print(f"Base URL: {self.base_url}")
            print(f"Input DataFrame shape: {df.shape}")
            raise
        
        # Limpiar después del procesamiento
        self.received_df = None