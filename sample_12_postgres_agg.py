# Pipeline:
# [PostgresOrigin: sales WHERE date >= '2024-01-01'] --extract--> [Aggregator: sum(amount)/category -> total_sales] --load--> [PostgresDest: analytics.sales_summary (replace)]

from open_stage.postgres.common import PostgresOrigin, PostgresDestination
from open_stage.core.common import Filter, Aggregator
from open_stage.core.base import Pipe

# Leer de PostgreSQL
pg_origin = PostgresOrigin(
  name="pg_reader",
  host="localhost",
  database="company_db",
  user="postgres",
  password="password",
  query="SELECT * FROM sales WHERE date >= '2024-01-01'"
)

# Agregar por categoría
aggregator = Aggregator("summary", "category", "total_sales", "sum", "amount")

# Escribir a PostgreSQL (schema y tabla diferentes)
pg_dest = PostgresDestination(
  name="pg_writer",
  host="localhost",
  database="company_db",
  user="postgres",
  password="password",
  table="sales_summary",
  schema="analytics",  # Schema diferente
  if_exists="replace"  # Reemplaza la tabla cada vez
)

# Conectar pipeline
pipe1 = Pipe("extract")
pipe2 = Pipe("load")

pg_origin.add_output_pipe(pipe1).set_destination(aggregator)
aggregator.add_output_pipe(pipe2).set_destination(pg_dest)

# Ejecutar
pg_origin.pump()