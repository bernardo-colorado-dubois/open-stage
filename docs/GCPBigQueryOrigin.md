# GCPBigQueryOrigin - Guía de Uso

Componente para extraer datos de Google BigQuery con capacidades avanzadas.

---

## 🎯 Características

- ✅ Lectura directa de tablas o queries personalizadas
- ✅ Queries pre y post extracción (`before_query`, `after_query`)
- ✅ Límite de resultados para testing (`max_results`)
- ✅ Validación de queries sin ejecutar (`dry_run`)
- ✅ Queries parametrizadas
- ✅ Estimación de costos automática
- ✅ Logging detallado

---

## 📦 Instalación
```bash
pip install google-cloud-bigquery google-auth db-dtypes
```

---

## 🚀 Uso Básico

### Ejemplo 1: Query Simple
```python
from src.google.cloud import GCPBigQueryOrigin
from src.core.base import Pipe
from src.core.common import Printer

# Crear origen con query
origin = GCPBigQueryOrigin(
    name="sales_data",
    project_id="my-project",
    query="SELECT * FROM dataset.sales WHERE date >= '2024-01-01'"
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
origin = GCPBigQueryOrigin(
    name="customers",
    project_id="my-project",
    table="dataset.customers"  # ✨ Más simple!
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

---

### Ejemplo 3: Límite para Testing
```python
# Solo extraer 100 filas para pruebas
origin = GCPBigQueryOrigin(
    name="sales_sample",
    project_id="my-project",
    table="dataset.sales",
    max_results=100  # ✨ Rápido para desarrollo
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

---

## 🔧 Funcionalidades Avanzadas

### Ejemplo 4: Dry Run (Validar y Estimar Costos)
```python
# Validar query SIN ejecutar
origin = GCPBigQueryOrigin(
    name="cost_check",
    project_id="my-project",
    query="SELECT * FROM `bigquery-public-data.usa_names.usa_1910_current`",
    dry_run=True  # ✨ Solo valida y estima costo
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()

# Output:
# ✅ Query is valid
# 📊 Estimated bytes processed: 6,432,432 bytes (6.13 MB)
# 💰 Estimated cost: $0.000038 USD
```

---

### Ejemplo 5: Before Query (Preparar Datos)
```python
# Ejecutar query ANTES de la extracción
origin = GCPBigQueryOrigin(
    name="processed_sales",
    project_id="my-project",
    before_query="""
        -- Crear tabla temporal con datos filtrados
        CREATE TEMP TABLE temp_sales AS
        SELECT * FROM dataset.raw_sales
        WHERE status = 'completed'
        AND date >= '2024-01-01';
    """,
    query="SELECT * FROM temp_sales WHERE amount > 100"
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

**Casos de uso de `before_query`:**
- Crear tablas temporales
- Llamar stored procedures
- Preparar datos antes de extraer
- Limpiar staging areas

---

### Ejemplo 6: After Query (Auditoría)
```python
# Ejecutar query DESPUÉS de la extracción
origin = GCPBigQueryOrigin(
    name="customer_extract",
    project_id="my-project",
    table="crm.customers",
    after_query="""
        -- Registrar la extracción
        INSERT INTO `my-project.audit.extraction_log` (
            table_name,
            extracted_at,
            record_count
        ) VALUES (
            'customers',
            CURRENT_TIMESTAMP(),
            (SELECT COUNT(*) FROM `my-project.crm.customers`)
        );
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

---

### Ejemplo 7: Workflow Completo (Before + After)
```python
# Pipeline completo con preparación y limpieza
origin = GCPBigQueryOrigin(
    name="daily_sales_etl",
    project_id="my-project",
    
    # ANTES: Preparar staging
    before_query="""
        -- Crear tabla staging
        CREATE OR REPLACE TABLE `my-project.staging.daily_sales` AS
        SELECT 
            DATE(order_timestamp) as sale_date,
            product_id,
            customer_id,
            amount,
            region
        FROM `my-project.raw.orders`
        WHERE DATE(order_timestamp) = CURRENT_DATE()
        AND status = 'completed';
        
        -- Validar datos
        CALL `my-project.procedures.validate_sales`();
    """,
    
    # QUERY PRINCIPAL
    query="SELECT * FROM `my-project.staging.daily_sales`",
    
    # DESPUÉS: Registrar y limpiar
    after_query="""
        -- Registrar ejecución
        INSERT INTO `my-project.audit.etl_runs` (
            pipeline_name,
            run_timestamp,
            records_processed
        ) VALUES (
            'daily_sales_etl',
            CURRENT_TIMESTAMP(),
            (SELECT COUNT(*) FROM `my-project.staging.daily_sales`)
        );
        
        -- Limpiar tablas temporales
        DROP TABLE IF EXISTS `my-project.staging.temp_processing`;
    """
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

---

### Ejemplo 8: Queries Parametrizadas
```python
from google.cloud import bigquery

# Query segura con parámetros
origin = GCPBigQueryOrigin(
    name="filtered_sales",
    project_id="my-project",
    query="""
        SELECT * FROM dataset.sales 
        WHERE date >= @start_date 
        AND amount > @min_amount
    """,
    query_parameters=[
        bigquery.ScalarQueryParameter("start_date", "DATE", "2024-01-01"),
        bigquery.ScalarQueryParameter("min_amount", "FLOAT64", 100.0)
    ]
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

---

### Ejemplo 9: Con Credenciales y Location
```python
# Usar service account y región específica
origin = GCPBigQueryOrigin(
    name="secure_extract",
    project_id="my-project",
    table="dataset.sensitive_data",
    credentials_path="/path/to/service-account.json",
    location="US",  # Región de BigQuery
    timeout=300,    # Timeout de 5 minutos
    job_labels={
        "team": "data-engineering",
        "env": "production"
    }
)

origin.add_output_pipe(pipe).set_destination(printer)
origin.pump()
```

---

## 📊 Output Ejemplo
```
GCPBigQueryOrigin 'daily_sales_etl' using default credentials
GCPBigQueryOrigin 'daily_sales_etl' BigQuery client initialized successfully

GCPBigQueryOrigin 'daily_sales_etl' executing before_query...
  Query preview: CREATE OR REPLACE TABLE `my-project.staging.daily_sales` AS...
✅ GCPBigQueryOrigin 'daily_sales_etl' before_query executed successfully
  - Bytes processed: 1,234,567 bytes (1.18 MB)
  - Rows affected: 5,432
  - Duration: 2.34s

======================================================================
GCPBigQueryOrigin 'daily_sales_etl' executing MAIN extraction query...
======================================================================
  - Project ID: my-project
  - Query: SELECT * FROM `my-project.staging.daily_sales`

GCPBigQueryOrigin 'daily_sales_etl' waiting for query completion...

======================================================================
GCPBigQueryOrigin 'daily_sales_etl' MAIN query results:
======================================================================
  📊 Results:
     - Rows returned: 5,432
     - Columns: 6
     - Column names: ['sale_date', 'product_id', 'customer_id', 'amount', ...]
  
  💾 Data processed:
     - Bytes processed: 234,567 bytes (0.22 GB)
     - Bytes billed: 234,567 bytes (0.22 GB)
  
  💰 Estimated cost: $0.001380 USD
  
  ⏱️  Job info:
     - Job ID: job_xyz123abc
     - Duration: 1.45s

GCPBigQueryOrigin 'daily_sales_etl' executing after_query...
  Query preview: INSERT INTO `my-project.audit.etl_runs` (...
✅ GCPBigQueryOrigin 'daily_sales_etl' after_query executed successfully
  - Bytes processed: 12,345 bytes (0.01 MB)
  - Rows affected: 1
  - Duration: 0.89s

======================================================================
✅ GCPBigQueryOrigin 'daily_sales_etl' pumped data through pipe 'pipe1'
======================================================================
```

---

## 📋 Parámetros Completos

| Parámetro | Tipo | Requerido | Default | Descripción |
|-----------|------|-----------|---------|-------------|
| `name` | str | ✅ | - | Nombre del componente |
| `project_id` | str | ✅ | - | ID del proyecto GCP |
| `query` | str | * | None | Query SQL a ejecutar |
| `table` | str | * | None | Tabla en formato `dataset.table` |
| `credentials_path` | str | ❌ | None | Ruta al JSON de service account |
| `before_query` | str | ❌ | None | Query a ejecutar ANTES |
| `after_query` | str | ❌ | None | Query a ejecutar DESPUÉS |
| `max_results` | int | ❌ | None | Límite de filas a retornar |
| `use_legacy_sql` | bool | ❌ | False | Usar SQL legacy |
| `query_parameters` | list | ❌ | [] | Parámetros para queries |
| `location` | str | ❌ | None | Región de BigQuery |
| `job_labels` | dict | ❌ | {} | Labels del job |
| `timeout` | float | ❌ | None | Timeout en segundos |
| `use_query_cache` | bool | ❌ | True | Usar cache de BigQuery |
| `dry_run` | bool | ❌ | False | Solo validar sin ejecutar |

\* **Nota**: Debes proporcionar `query` O `table`, pero no ambos.

---

## ✅ Buenas Prácticas

1. **Usa `dry_run`** antes de ejecutar queries grandes para estimar costos
2. **Usa `max_results`** en desarrollo para pruebas rápidas
3. **Usa `before_query`** para preparar datos y staging
4. **Usa `after_query`** para auditoría y cleanup
5. **Usa `query_parameters`** en lugar de concatenar strings (seguridad)
6. **Usa `table`** cuando solo necesites `SELECT *` (más simple)
7. **Especifica `location`** si trabajas con datos en regiones específicas

---

## 🔗 Ver También

- [GCPBigQueryDestination](./GCPBigQueryDestination.md) - Para escribir a BigQuery
- [Open-Stage Documentation](../README.md) - Documentación completa

---

**Open-Stage v2.3** - Enterprise ETL Framework