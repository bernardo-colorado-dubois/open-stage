import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

from src.core.base import Pipe
from src.core.common import  Printer,DeleteColumns
from src.postgres.common import PostgresOrigin
from src.mysql.common import MySQLOrigin

load_dotenv()

PSQL_DB_HOST = os.getenv("PSQL_DB_HOST")
PSQL_DB_PORT = int(os.getenv("PSQL_DB_PORT"))
PSQL_DB_NAME = os.getenv("PSQL_DB_NAME")
PSQL_DB_USER = os.getenv("PSQL_DB_USER")
PSQL_DB_PASSWORD = quote_plus(os.getenv("PSQL_DB_PASSWORD"))

MYSQL_DB_HOST=os.getenv("MYSQL_DB_HOST")
MYSQL_DB_PORT=int(os.getenv("MYSQL_DB_PORT"))
MYSQL_DB_NAME=os.getenv("MYSQL_DB_NAME")
MYSQL_DB_USER=os.getenv("MYSQL_DB_USER")
MYSQL_DB_PASSWORD=os.getenv("MYSQL_DB_PASSWORD")

productos_origin = PostgresOrigin(
  name="postgres_origin",
  host=PSQL_DB_HOST,
  port=PSQL_DB_PORT,
  database=PSQL_DB_NAME,
  user=PSQL_DB_USER,
  password=PSQL_DB_PASSWORD,
  table="public.productos"
)

productos_deleter = DeleteColumns(
  name="productos_deleter",
  columns=["descripcion","imagen_url","proveedor_id","stock_actual","stock_minimo","fecha_caducidad","imagen_url","activo","created_at","updated_at"]
)

categorias_origin = MySQLOrigin(
  name="mysql_origin",
  host=MYSQL_DB_HOST,
  port=MYSQL_DB_PORT,
  database=MYSQL_DB_NAME,
  user=MYSQL_DB_USER,
  password=MYSQL_DB_PASSWORD,
  table="categorias"
)

categorias_deleter = DeleteColumns(
  name="categorias_deleter",
  columns=["descripcion","created_at","updated_at","activo"]
)

productos_origin.add_output_pipe(Pipe(name="productos")).set_destination(productos_deleter).add_output_pipe(Pipe(name="prouctos_reducidos")).set_destination(Printer(name="productos_printer"))

categorias_origin.add_output_pipe(Pipe(name="categorias")).set_destination(categorias_deleter).add_output_pipe(Pipe(name="categorias_reducidas")).set_destination(Printer(name="categorias_printer"))

productos_origin.pump()

categorias_origin.pump()
