import os
from src.core.common import Pipe, Printer
from dotenv import load_dotenv
from urllib.parse import quote_plus

from src.postgres.common import PostgresOrigin

load_dotenv()

PSQL_DB_HOST = os.getenv("PSQL_DB_HOST")
PSQL_DB_PORT = int(os.getenv("PSQL_DB_PORT"))
PSQL_DB_NAME = os.getenv("PSQL_DB_NAME")
PSQL_DB_USER = os.getenv("PSQL_DB_USER")
PSQL_DB_PASSWORD = quote_plus(os.getenv("PSQL_DB_PASSWORD"))

postgres_origin = PostgresOrigin(
  name="postgres_origin",
  host=PSQL_DB_HOST,
  port=PSQL_DB_PORT,
  database=PSQL_DB_NAME,
  user=PSQL_DB_USER,
  password=PSQL_DB_PASSWORD,
  query="select * FROM rocket.jira_issues;"
)

postgres_pipe = Pipe(name="postgres_pipe")

postgres_printer = Printer(name="postgres_printer")

postgres_origin.add_output_pipe(postgres_pipe).set_destination(postgres_printer)

postgres_origin.pump()

