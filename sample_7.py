import os
from src.core import Pipe, Printer, GCPBigQueryOrigin,GeminiPromptTransformer
from dotenv import load_dotenv

load_dotenv()

GCP_CREDENTIALS_FILE_PATH = os.getenv('GCP_CREDENTIALS_FILE_PATH')
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')

bigquery_origin = GCPBigQueryOrigin(
  name="bigquery_origin",
  project_id=GCP_PROJECT_ID,
  query="SELECT * FROM `SAMPLE.IRIS`;",
  credentials_path=GCP_CREDENTIALS_FILE_PATH
)

bigquery_pipe = Pipe(name="bigquery_pipe")

gemini_prompt = """
  "1) Provide only the setosa species from the following data
   2) drop the sepal columns 
   3) show the species in upper case"
   4) change in species the '-' char to '_' char
"""

gemini_transformer = GeminiPromptTransformer(
  name="gemini_transformer",
  api_key=GEMINI_API_KEY,
  prompt=gemini_prompt,
  model="gemini-2.5-flash"
)

gemini_pipe = Pipe(name="gemini_pipe")

gemini_printer = Printer(name="gemini_printer")

bigquery_origin.add_output(bigquery_pipe).set_destination(gemini_transformer)

gemini_transformer.add_output(gemini_pipe).set_destination(gemini_printer)

bigquery_origin.pump()
