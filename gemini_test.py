import os
from google import genai
from dotenv import load_dotenv

load_dotenv()  # Cargar variables de entorno desde un archivo .env si existe

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# --- Configuración de la Clave API ---
# Se recomienda encarecidamente establecer la variable de entorno 'GEMINI_API_KEY'
try:
    client = genai.Client(api_key=GEMINI_API_KEY)
except Exception as e:
    print("Error al inicializar el cliente de Gemini.")
    print("Asegúrate de haber establecido tu clave API en la variable de entorno 'GEMINI_API_KEY'.")
    exit()

# 2. Definir el prompt y el modelo
model_name = "gemini-2.5-flash" 
prompt = "¿Me puedes dar un sinónimo de la palabra 'espléndido'?"

print(f"Modelo a usar: {model_name}")
print(f"Prompt enviado: '{prompt}'\n")

# 3. Llamar a la API para generar contenido usando la sintaxis correcta: client.models.generate_content()
try:
    response = client.models.generate_content(
        model=model_name,
        contents=prompt
    )

    # 4. Imprimir la respuesta
    print("--- Respuesta de Gemini ---")
    print(response.text)
    print("--------------------------")

except Exception as e:
    print(f"Ocurrió un error al llamar a la API: {e}")