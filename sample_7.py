import os
from src.core.common import Printer, Pipe
from src.google.cloud import GCPBigQueryOrigin
from src.google.gemini import GeminiPromptTransformer
from dotenv import load_dotenv

load_dotenv()

GCP_CREDENTIALS_FILE_PATH = os.getenv('GCP_CREDENTIALS_FILE_PATH')
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')

bigquery_origin = GCPBigQueryOrigin(
  name="bigquery_origin",
  project_id=GCP_PROJECT_ID,
  query="SELECT * FROM `SAMPLES.IRIS`;",
  credentials_path=GCP_CREDENTIALS_FILE_PATH
)

bigquery_pipe = Pipe(name="bigquery_pipe")

gemini_prompt = """
  "1) Provide only the setosa flowers from the following data
   2) I don't need the sepal columns 
   3) show the species in upper"
   4) change in species the - char to space char
   5) pethal width no more to 0.2
   6) pethal no more than 1.4
"""

gemini_transformer = GeminiPromptTransformer(
  name="gemini_transformer",
  api_key=GEMINI_API_KEY,
  prompt=gemini_prompt,
  model="gemini-2.5-pro"
)

gemini_pipe = Pipe(name="gemini_pipe")

gemini_printer = Printer(name="gemini_printer")

bigquery_origin.add_output_pipe(bigquery_pipe).set_destination(gemini_transformer).add_output_pipe(gemini_pipe).set_destination(gemini_printer)

bigquery_origin.pump()
