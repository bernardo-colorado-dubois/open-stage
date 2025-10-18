# PostgresOrigin - Guía de Uso

Componente para extraer datos de PostgreSQL con capacidades avanzadas.

---

## 🎯 Características

- ✅ Lectura directa de tablas o queries personalizadas
- ✅ Queries pre y post extracción (`before_query`, `after_query`)
- ✅ Límite de resultados para testing (`max_results`)
- ✅ Queries parametrizadas seguras
- ✅ Timeout configurable
- ✅ Logging detallado con estadísticas

---

## 📦 Instalación
```bash
pip install sqlalchemy psycopg2-binary pandas
```

---

## 🚀 Uso Básico

### Ejemplo 1: Query Simple
```python
from src.postgres.common import PostgresOrigin
from src.core.base import Pipe
from src.core.common import Printer

# Crear origen con query
origin = PostgresOrigin(
    name="sales_data",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    query="SELECT * FROM sales WHERE date >= '2024-01-01'"
)

# Conectar y ejecutar
pipe = Pipe("pipe1")
printer = Printer("output")

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

---

### Ejemplo 2: Lectura Directa de Tabla
```python
# Leer tabla completa sin escribir SELECT *
origin = PostgresOrigin(
    name="customers",
    host="localhost",
    database="crm",
    user="postgres",
    password="password",
    table="public.customers"  # ✨ Más simple!
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

---

### Ejemplo 3: Límite para Testing
```python
# Solo extraer 100 filas para pruebas
origin = PostgresOrigin(
    name="sales_sample",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    table="sales",
    max_results=100  # ✨ Rápido para desarrollo
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

---

## 🔧 Funcionalidades Avanzadas

### Ejemplo 4: Before Query (Preparar Datos)
```python
# Ejecutar query ANTES de la extracción
origin = PostgresOrigin(
    name="processed_orders",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    before_query="""
        -- Crear tabla temporal con datos filtrados
        CREATE TEMP TABLE temp_orders AS
        SELECT * FROM raw_orders
        WHERE status = 'completed'
        AND date >= '2024-01-01';
        
        -- Indexar para mejor performance
        CREATE INDEX idx_temp_orders_amount ON temp_orders(amount);
    """,
    query="SELECT * FROM temp_orders WHERE amount > 100"
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

**Casos de uso de `before_query`:**
- Crear tablas temporales
- Llamar stored procedures
- Preparar datos antes de extraer
- Limpiar staging areas
- SET variables de sesión
- Crear índices temporales

---

### Ejemplo 5: After Query (Auditoría)
```python
# Ejecutar query DESPUÉS de la extracción
origin = PostgresOrigin(
    name="customer_extract",
    host="localhost",
    database="crm",
    user="postgres",
    password="password",
    table="customers",
    after_query="""
        -- Registrar la extracción
        INSERT INTO audit.extraction_log (
            table_name,
            extracted_at,
            record_count
        ) VALUES (
            'customers',
            NOW(),
            (SELECT COUNT(*) FROM customers)
        );
        
        -- Marcar registros como procesados
        UPDATE customers
        SET last_extracted = NOW()
        WHERE last_extracted IS NULL;
    """
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

**Casos de uso de `after_query`:**
- Logging de auditoría
- Marcar registros como procesados
- Actualizar timestamps
- Limpiar tablas temporales
- Actualizar estadísticas de tablas
- Notificar completación

---

### Ejemplo 6: Workflow Completo (Before + After)
```python
# Pipeline completo con preparación y limpieza
origin = PostgresOrigin(
    name="daily_sales_etl",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    
    # ANTES: Preparar staging
    before_query="""
        -- Crear tabla staging
        DROP TABLE IF EXISTS staging.daily_sales;
        
        CREATE TABLE staging.daily_sales AS
        SELECT 
            DATE(order_timestamp) as sale_date,
            product_id,
            customer_id,
            amount,
            region
        FROM raw.orders
        WHERE DATE(order_timestamp) = CURRENT_DATE
        AND status = 'completed';
        
        -- Validar datos
        DO $$
        BEGIN
            IF (SELECT COUNT(*) FROM staging.daily_sales) = 0 THEN
                RAISE EXCEPTION 'No sales data for today';
            END IF;
        END $$;
        
        -- Crear índices
        CREATE INDEX idx_staging_sales_date ON staging.daily_sales(sale_date);
    """,
    
    # QUERY PRINCIPAL
    query="SELECT * FROM staging.daily_sales ORDER BY sale_date, customer_id",
    
    # DESPUÉS: Registrar y limpiar
    after_query="""
        -- Registrar ejecución
        INSERT INTO audit.etl_runs (
            pipeline_name,
            run_timestamp,
            records_processed,
            status
        ) VALUES (
            'daily_sales_etl',
            NOW(),
            (SELECT COUNT(*) FROM staging.daily_sales),
            'SUCCESS'
        );
        
        -- Actualizar metadata
        UPDATE control.table_metadata
        SET last_extraction = NOW()
        WHERE table_name = 'daily_sales';
        
        -- Limpiar tablas temporales viejas
        DROP TABLE IF EXISTS staging.temp_processing;
        
        -- Vacuum analyze para estadísticas
        ANALYZE staging.daily_sales;
    """
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

---

### Ejemplo 7: Queries Parametrizadas
```python
# Query segura con parámetros (evita SQL injection)
origin = PostgresOrigin(
    name="filtered_sales",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    query="""
        SELECT * FROM sales 
        WHERE date >= :start_date 
        AND amount > :min_amount
        AND region = :region
    """,
    query_parameters={
        'start_date': '2024-01-01',
        'min_amount': 100.0,
        'region': 'North'
    }
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

---

### Ejemplo 8: Con Timeout
```python
# Control de tiempo de ejecución para queries largas
origin = PostgresOrigin(
    name="large_extract",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    query="SELECT * FROM huge_table WHERE date >= '2024-01-01'",
    timeout=300  # 5 minutos máximo
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

---

### Ejemplo 9: Lectura con Schema Explícito
```python
# Especificar schema cuando no es 'public'
origin = PostgresOrigin(
    name="reporting_data",
    host="localhost",
    database="analytics",
    user="postgres",
    password="password",
    table="reports.monthly_summary",  # schema.table
    max_results=1000
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

---

### Ejemplo 10: Con Variables de Sesión
```python
# Configurar variables de sesión antes de extraer
origin = PostgresOrigin(
    name="custom_config",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    before_query="""
        -- Configurar variables de sesión
        SET work_mem = '256MB';
        SET statement_timeout = '300s';
        SET search_path = 'analytics, public';
        
        -- Crear tabla temporal
        CREATE TEMP TABLE filtered_data AS
        SELECT * FROM large_table WHERE category = 'A';
    """,
    query="SELECT * FROM filtered_data"
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

---

## 📊 Output Ejemplo
```
PostgresOrigin 'daily_sales_etl' engine initialized successfully
Connection: postgres@localhost:5432/warehouse
  - Timeout: 300.0s

PostgresOrigin 'daily_sales_etl' executing before_query...
  Query preview: -- Crear tabla staging
        DROP TABLE IF EXISTS staging.daily_sales;...
✅ PostgresOrigin 'daily_sales_etl' before_query executed successfully
  - Rows affected: 5,432
  - Duration: 2.34s

======================================================================
PostgresOrigin 'daily_sales_etl' executing MAIN extraction query...
======================================================================
  - Database: warehouse
  - Query: SELECT * FROM staging.daily_sales ORDER BY sale_date, customer_id
  - Parameters: ['start_date', 'region']

PostgresOrigin 'daily_sales_etl' waiting for query completion...

======================================================================
PostgresOrigin 'daily_sales_etl' MAIN query results:
======================================================================
  📊 Results:
     - Rows returned: 5,432
     - Columns: 5
     - Column names: ['sale_date', 'product_id', 'customer_id', 'amount', 'region']
  
  ⏱️  Query info:
     - Duration: 1.45s
  
  📋 Data types:
     - sale_date: object
     - product_id: int64
     - customer_id: int64
     - amount: float64
     - region: object

PostgresOrigin 'daily_sales_etl' executing after_query...
  Query preview: -- Registrar ejecución
        INSERT INTO audit.etl_runs (...
✅ PostgresOrigin 'daily_sales_etl' after_query executed successfully
  - Rows affected: 1
  - Duration: 0.45s

======================================================================
✅ PostgresOrigin 'daily_sales_etl' pumped data through pipe 'pipe1'
======================================================================
PostgresOrigin 'daily_sales_etl' connection closed
```

---

## 📋 Parámetros Completos

| Parámetro | Tipo | Requerido | Default | Descripción |
|-----------|------|-----------|---------|-------------|
| `name` | str | ✅ | - | Nombre del componente |
| `host` | str | ✅ | - | Host de PostgreSQL |
| `port` | int | ❌ | 5432 | Puerto de PostgreSQL |
| `database` | str | ✅ | - | Nombre de la base de datos |
| `user` | str | ✅ | - | Usuario de PostgreSQL |
| `password` | str | ✅ | - | Contraseña |
| `query` | str | * | None | Query SQL a ejecutar |
| `table` | str | * | None | Tabla en formato `table` o `schema.table` |
| `before_query` | str | ❌ | None | Query a ejecutar ANTES |
| `after_query` | str | ❌ | None | Query a ejecutar DESPUÉS |
| `max_results` | int | ❌ | None | Límite de filas a retornar |
| `timeout` | float | ❌ | None | Timeout en segundos |
| `query_parameters` | dict | ❌ | {} | Parámetros para queries |

\* **Nota**: Debes proporcionar `query` O `table`, pero no ambos.

---

## 🔐 Formatos de Tabla Soportados

```python
# Formato 1: Solo nombre de tabla (usa schema por defecto)
table="customers"
# Genera: SELECT * FROM "customers"

# Formato 2: Schema explícito
table="public.customers"
# Genera: SELECT * FROM "public"."customers"

# Formato 3: Schema no-public
table="analytics.sales_summary"
# Genera: SELECT * FROM "analytics"."sales_summary"
```

---

## ✅ Buenas Prácticas

1. **Usa `table`** cuando solo necesites `SELECT *` (más simple)
2. **Usa `max_results`** en desarrollo para pruebas rápidas
3. **Usa `before_query`** para preparar datos y staging
4. **Usa `after_query`** para auditoría y cleanup
5. **Usa `query_parameters`** en lugar de concatenar strings (seguridad)
6. **Especifica `timeout`** para queries largas
7. **Usa tablas temporales** en `before_query` para transformaciones complejas
8. **Indexa tablas temporales** si vas a filtrar/ordenar sobre ellas
9. **Limpia recursos** en `after_query` (DROP TEMP TABLES)
10. **Usa transacciones** cuando sea necesario en before/after queries

---

## ⚠️ Consideraciones Importantes

### Tablas Temporales
- Las tablas TEMP se eliminan automáticamente al cerrar la conexión
- Usa `CREATE TEMP TABLE` para datos intermedios
- Son visibles solo para la sesión actual

### Parámetros de Queries
- Usa `:param_name` en la query
- Proporciona valores en `query_parameters` como diccionario
- Evita SQL injection usando parámetros

### Timeout
- Se aplica tanto a la conexión inicial como a queries
- Útil para queries largas o para evitar bloqueos
- Si una query supera el timeout, lanza excepción

### Performance
- `ANALYZE` tablas después de cargas grandes en `after_query`
- Usa `EXPLAIN ANALYZE` en desarrollo para optimizar
- Considera índices temporales en staging

### Schemas
- El schema por defecto es `public`
- Especifica schema explícitamente: `schema.table`
- Usa `SET search_path` en `before_query` si necesario

---

## 🔗 Ver También

- [PostgresDestination](./PostgresDestination.md) - Para escribir a PostgreSQL
- [Open-Stage Documentation](../README.md) - Documentación completa

---

**Open-Stage v2.4** - Enterprise ETL Framework