from src.mysql.common import MySQLOrigin, MySQLDestination
from src.core.common import Filter
from src.core.base import Pipe
from dotenv import load_dotenv
import os

load_dotenv()

MYSQL_DB_HOST = os.getenv("MYSQL_DB_HOST")
MYSQL_DB_NAME = os.getenv("MYSQL_DB_NAME")
MYSQL_DB_USER = os.getenv("MYSQL_DB_USER")
MYSQL_DB_PASSWORD = os.getenv("MYSQL_DB_PASSWORD")

mysql_origin = MySQLOrigin(
  name="mysql_reader",
  host=MYSQL_DB_HOST,
  database=MYSQL_DB_NAME,
  user=MYSQL_DB_USER,
  password=MYSQL_DB_PASSWORD,
  query="""
  SELECT 
    id
    , codigo
    , nombre
    , descripcion
    , categoria_id
    , proveedor_id
    , precio_compra
    , precio_venta
    , stock_actual
    , stock_minimo
    , unidad_medida
    , peso_gramos
    , fecha_caducidad
    , imagen_url
    , activo
    , created_at
    , updated_at
  FROM dulceria.productos;"""
)

# Filtrar
filter_node = Filter(name="filtro_stock", field="stock_actual", condition=">", value_or_values=50)

# Escribir a MySQL (otra tabla)
mysql_dest = MySQLDestination(
  name="mysql_writer",
  host=MYSQL_DB_HOST,
  database=MYSQL_DB_NAME,
  user=MYSQL_DB_USER,
  password=MYSQL_DB_PASSWORD,
  table="productos_filtrados",  # Asegúrate de que esta tabla exista o se creará
  if_exists="replace"  # Agrega a la tabla existente
)

# Conectar pipeline
pipe1 = Pipe("extract")
pipe2 = Pipe("load")


mysql_origin.add_output_pipe(pipe1).set_destination(filter_node)
filter_node.add_output_pipe(pipe2).set_destination(mysql_dest)

mysql_origin.pump()