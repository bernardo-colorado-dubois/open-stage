import os
from src.core import Pipe, Printer, GCPBigQueryOrigin
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

bigquery_printer = Printer(name="bigquery_printer")

bigquery_origin.add_output(bigquery_pipe).set_destination(bigquery_printer)

bigquery_origin.pump()
