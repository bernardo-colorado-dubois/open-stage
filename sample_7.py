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
  "1) Provide only the setosa  species
   2) with a petal length less or equal than 0.2 from the following data 
   3) drop the sepal columns 
   4) show the species in lower case"
   5) change in species the - to _
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
