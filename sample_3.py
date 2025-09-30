import os
from src.core import Pipe, Printer, GCPBigQueryOrigin

credentials_path = "/home/berni-boom/qualitasfraude-8be2cf5b3f4b.json"

bigquery_origin = GCPBigQueryOrigin(
  name="bigquery_origin",
  project_id="qualitasfraude",
  query="SELECT * FROM `qualitasfraude.DF_TEST.query_test` LIMIT 1000;",
  credentials_path=credentials_path
)
bigquery_pipe = Pipe(name="bigquery_origin")
printer = Printer(name="printer")
bigquery_origin.add_output(bigquery_pipe).set_destination(printer)
bigquery_origin.pump()
