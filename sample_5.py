import os
from src.core import Pipe, Printer, GCPBigQueryOrigin,AnthropicPromptTransformer
from dotenv import load_dotenv

load_dotenv()

GCP_CREDENTIALS_FILE_PATH = os.getenv('GCP_CREDENTIALS_FILE_PATH')
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY')

bigquery_origin = GCPBigQueryOrigin(
  name="bigquery_origin",
  project_id="qualitasfraude",
  query="SELECT * FROM `qualitasfraude.SAMPLE.IRIS`;",
  credentials_path=GCP_CREDENTIALS_FILE_PATH
)

bigquery_pipe = Pipe(name="bigquery_pipe")

anthropic_transformer = AnthropicPromptTransformer(
  name="anthropic_transformer",
  api_key=ANTHROPIC_API_KEY,
  prompt="Provide only the setosa  species, with a petal length less or equal than 0.2 from the following data, and drop the sepal columns also show the species in lower case",
  model="claude-sonnet-4-5-20250929"
)

anthropic_pipe = Pipe(name="anthropic_pipe")

anthropic_printer = Printer(name="anthropic_printer")

bigquery_origin.add_output(bigquery_pipe).set_destination(anthropic_transformer)

anthropic_transformer.add_output(anthropic_pipe).set_destination(anthropic_printer)

bigquery_origin.pump()
