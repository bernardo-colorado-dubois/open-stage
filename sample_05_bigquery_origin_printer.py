# Pipeline:
# [GCPBigQueryOrigin: SAMPLES.IRIS] --bigquery_pipe--> [Printer: bigquery_printer]

import os
from open_stage.core.common import Printer,Pipe
from open_stage.google.bigquery import GCPBigQueryOrigin
from dotenv import load_dotenv

load_dotenv()

GCP_CREDENTIALS_FILE_PATH = os.getenv('GCP_CREDENTIALS_FILE_PATH')
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY')
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')

bigquery_origin = GCPBigQueryOrigin(
  name="bigquery_origin",
  project_id=GCP_PROJECT_ID,
  #query="SELECT * FROM `SAMPLES.IRIS`;",
  table="SAMPLES.IRIS",
  credentials_path=GCP_CREDENTIALS_FILE_PATH
)

bigquery_pipe = Pipe(name="bigquery_pipe")

bigquery_printer = Printer(name="bigquery_printer")

bigquery_origin.add_output_pipe(bigquery_pipe).set_destination(bigquery_printer)

bigquery_origin.pump()
