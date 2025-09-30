import os
from src.core import Pipe, Printer, GCPBigQueryOrigin,AnthropicPromptTransformer
from dotenv import load_dotenv

load_dotenv()

GCP_CREDENTIALS_FILE_PATH = os.getenv('GCP_CREDENTIALS_FILE_PATH')
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY')
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')


bigquery_origin = GCPBigQueryOrigin(
  name="bigquery_origin",
  project_id=GCP_PROJECT_ID,
  query="SELECT * FROM `SAMPLE.IRIS`;",
  credentials_path=GCP_CREDENTIALS_FILE_PATH
)

bigquery_pipe = Pipe(name="bigquery_pipe")

anthropic_prompt = """
  "1) Provide only the setosa  species
   2) with a petal length less or equal than 0.2 from the following data 
   3) drop the sepal columns 
   4) show the species in lower case
   5) change in species the - to _
"""

anthropic_transformer = AnthropicPromptTransformer(
  name="anthropic_transformer",
  api_key=ANTHROPIC_API_KEY,
  prompt=anthropic_prompt,
  model="claude-sonnet-4-5-20250929"
)

anthropic_pipe = Pipe(name="anthropic_pipe")

anthropic_printer = Printer(name="anthropic_printer")

bigquery_origin.add_output(bigquery_pipe).set_destination(anthropic_transformer)

anthropic_transformer.add_output(anthropic_pipe).set_destination(anthropic_printer)

bigquery_origin.pump()
