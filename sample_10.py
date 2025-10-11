from src.mysql.common import MySQLOrigin
from src.core.base import Pipe
from src.core.common import Printer
from dotenv import load_dotenv
import os

load_dotenv()

MYSQL_DB_HOST = os.getenv("MYSQL_DB_HOST")
MYSQL_DB_NAME = os.getenv("MYSQL_DB_NAME")
MYSQL_DB_USER = os.getenv("MYSQL_DB_USER")
MYSQL_DB_PASSWORD = os.getenv("MYSQL_DB_PASSWORD")

mysql = MySQLOrigin(
  name="mysql_reader",
  host=MYSQL_DB_HOST,
  database=MYSQL_DB_NAME,
  user=MYSQL_DB_USER,
  password=MYSQL_DB_PASSWORD,
  query="""
    SELECT 
      id
      , nombre
      , apellido
      , telefono
      , email
      , fecha_nacimiento
      , puntos_fidelidad
      , activo
      , created_at
      , updated_at
    FROM dulceria.clientes;"""
)

pipe = Pipe("data_pipe")

printer = Printer(name="printer")

mysql.add_output_pipe(pipe).set_destination(printer)

mysql.pump()