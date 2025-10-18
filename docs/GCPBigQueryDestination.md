# GCPBigQueryDestination - Guía de Uso

Componente para cargar datos a Google BigQuery con capacidades avanzadas.

---

## 🎯 Características

- ✅ Carga de DataFrames a BigQuery
- ✅ Queries pre y post carga (`before_query`, `after_query`)
- ✅ Particionamiento temporal (por día, hora, mes, año)
- ✅ Clustering para optimizar queries
- ✅ Actualización automática de schema
- ✅ Control de disposición de escritura (TRUNCATE, APPEND, EMPTY)
- ✅ Tolerancia a errores configurable
- ✅ Logging detallado con estadísticas

---

## 📦 Instalación
```bash
pip install google-cloud-bigquery google-auth db-dtypes pandas
```

---

## 🚀 Uso Básico

### Ejemplo 1: Carga Simple (APPEND)
```python
from src.google.cloud import GCPBigQueryDestination
from src.core.base import Pipe
from src.core.common import CSVOrigin

# Leer datos
origin = CSVOrigin("reader", filepath_or_buffer="sales.csv")

# Destino BigQuery
destination = GCPBigQueryDestination(
    name="sales_loader",
    project_id="my-project",
    dataset="analytics",
    table="sales",
    write_disposition="WRITE_APPEND"  # Agregar datos
)

# Conectar y ejecutar
pipe = Pipe("pipe1")
origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Ejemplo 2: Reemplazar Tabla (TRUNCATE)
```python
# Reemplazar todos los datos de la tabla
destination = GCPBigQueryDestination(
    name="daily_report",
    project_id="my-project",
    dataset="reports",
    table="daily_summary",
    write_disposition="WRITE_TRUNCATE"  # ✨ Reemplazar tabla completa
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Ejemplo 3: Solo si Tabla Está Vacía (EMPTY)
```python
# Solo cargar si la tabla no tiene datos
destination = GCPBigQueryDestination(
    name="initial_load",
    project_id="my-project",
    dataset="warehouse",
    table="customers",
    write_disposition="WRITE_EMPTY",  # ✨ Solo si tabla vacía
    create_disposition="CREATE_IF_NEEDED"
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

## 🔧 Funcionalidades Avanzadas

### Ejemplo 4: Before Query (Preparar Antes de Cargar)
```python
# Ejecutar query ANTES de cargar datos
destination = GCPBigQueryDestination(
    name="staged_load",
    project_id="my-project",
    dataset="warehouse",
    table="orders",
    before_query="""
        -- Crear backup antes de cargar
        CREATE OR REPLACE TABLE `my-project.warehouse.orders_backup` AS
        SELECT * FROM `my-project.warehouse.orders`;
        
        -- Truncar tabla staging
        TRUNCATE TABLE `my-project.staging.temp_orders`;
    """,
    write_disposition="WRITE_TRUNCATE"
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

**Casos de uso de `before_query`:**
- Crear backups antes de cargar
- Truncar tablas específicas
- Preparar staging areas
- Validar pre-condiciones
- Limpiar datos antiguos

---

### Ejemplo 5: After Query (Auditoría y Post-procesamiento)
```python
# Ejecutar query DESPUÉS de cargar datos
destination = GCPBigQueryDestination(
    name="customer_loader",
    project_id="my-project",
    dataset="crm",
    table="customers",
    write_disposition="WRITE_APPEND",
    after_query="""
        -- Registrar en log de auditoría
        INSERT INTO `my-project.audit.load_log` (
            table_name,
            loaded_at,
            record_count,
            loaded_by
        ) VALUES (
            'customers',
            CURRENT_TIMESTAMP(),
            (SELECT COUNT(*) FROM `my-project.crm.customers`),
            'open-stage-pipeline'
        );
        
        -- Actualizar metadata
        UPDATE `my-project.crm.table_metadata`
        SET last_updated = CURRENT_TIMESTAMP()
        WHERE table_name = 'customers';
    """
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

**Casos de uso de `after_query`:**
- Logging de auditoría
- Actualizar tablas de metadata
- Ejecutar validaciones post-carga
- Llamar stored procedures
- Actualizar vistas materializadas

---

### Ejemplo 6: Workflow Completo (Before + After)
```python
# Pipeline completo con preparación y post-procesamiento
destination = GCPBigQueryDestination(
    name="sales_etl",
    project_id="my-project",
    dataset="warehouse",
    table="sales_fact",
    
    # ANTES: Preparar
    before_query="""
        -- Backup incremental
        CREATE OR REPLACE TABLE `my-project.warehouse.sales_fact_backup_{DATE}` AS
        SELECT * FROM `my-project.warehouse.sales_fact`;
        
        -- Preparar staging
        TRUNCATE TABLE `my-project.staging.sales_staging`;
        
        -- Marcar inicio de carga
        UPDATE `my-project.control.etl_status`
        SET status = 'LOADING', start_time = CURRENT_TIMESTAMP()
        WHERE table_name = 'sales_fact';
    """,
    
    write_disposition="WRITE_TRUNCATE",
    
    # DESPUÉS: Validar y registrar
    after_query="""
        -- Validar datos cargados
        CALL `my-project.procedures.validate_sales_data`();
        
        -- Actualizar dimensiones
        CALL `my-project.procedures.refresh_sales_aggregates`();
        
        -- Registrar éxito
        INSERT INTO `my-project.audit.etl_runs` (
            pipeline_name,
            table_name,
            run_timestamp,
            records_loaded,
            status
        ) VALUES (
            'sales_etl',
            'sales_fact',
            CURRENT_TIMESTAMP(),
            (SELECT COUNT(*) FROM `my-project.warehouse.sales_fact`),
            'SUCCESS'
        );
        
        -- Actualizar control
        UPDATE `my-project.control.etl_status`
        SET status = 'COMPLETED', end_time = CURRENT_TIMESTAMP()
        WHERE table_name = 'sales_fact';
    """
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Ejemplo 7: Tabla Particionada por Tiempo
```python
# Crear tabla particionada por fecha para mejor performance
destination = GCPBigQueryDestination(
    name="events_loader",
    project_id="my-project",
    dataset="analytics",
    table="events",
    write_disposition="WRITE_APPEND",
    time_partitioning={
        'type': 'DAY',           # Partición por día
        'field': 'event_date'    # Campo de fecha
    }
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

**Tipos de particionamiento:**
- `'DAY'` - Por día (recomendado)
- `'HOUR'` - Por hora
- `'MONTH'` - Por mes
- `'YEAR'` - Por año

---

### Ejemplo 8: Tabla con Clustering
```python
# Optimizar queries con clustering
destination = GCPBigQueryDestination(
    name="orders_loader",
    project_id="my-project",
    dataset="warehouse",
    table="orders",
    write_disposition="WRITE_APPEND",
    clustering_fields=['customer_id', 'product_id', 'region']  # Max 4 campos
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

**Beneficios del clustering:**
- Mejora performance de queries filtradas
- Reduce costos (solo escanea bloques necesarios)
- Ideal para campos frecuentemente filtrados

---

### Ejemplo 9: Particionamiento + Clustering
```python
# Combinar particionamiento y clustering para máxima optimización
destination = GCPBigQueryDestination(
    name="optimized_sales",
    project_id="my-project",
    dataset="warehouse",
    table="sales",
    write_disposition="WRITE_APPEND",
    time_partitioning={
        'type': 'DAY',
        'field': 'sale_date'
    },
    clustering_fields=['region', 'product_category', 'store_id']
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Ejemplo 10: Schema Personalizado
```python
from google.cloud import bigquery

# Definir schema manualmente
custom_schema = [
    bigquery.SchemaField("customer_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("total_purchases", "FLOAT", mode="NULLABLE"),
]

destination = GCPBigQueryDestination(
    name="customers_loader",
    project_id="my-project",
    dataset="crm",
    table="customers",
    write_disposition="WRITE_APPEND",
    schema=custom_schema,  # Schema explícito
    autodetect=False       # Desactivar auto-detección
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Ejemplo 11: Actualización Automática de Schema
```python
# Permitir agregar columnas automáticamente
destination = GCPBigQueryDestination(
    name="flexible_loader",
    project_id="my-project",
    dataset="warehouse",
    table="evolving_table",
    write_disposition="WRITE_APPEND",
    schema_update_options=[
        'ALLOW_FIELD_ADDITION',      # Permitir nuevas columnas
        'ALLOW_FIELD_RELAXATION'     # Relajar restricciones (REQUIRED → NULLABLE)
    ]
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Ejemplo 12: Con Credenciales y Labels
```python
# Configuración completa con autenticación y organización
destination = GCPBigQueryDestination(
    name="secure_loader",
    project_id="my-project",
    dataset="sensitive_data",
    table="pii_customers",
    write_disposition="WRITE_APPEND",
    credentials_path="/path/to/service-account.json",
    location="EU",  # Datos en Europa
    job_labels={
        "team": "data-engineering",
        "env": "production",
        "pipeline": "customer-etl"
    }
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Ejemplo 13: Tolerancia a Errores
```python
# Permitir algunos registros con errores
destination = GCPBigQueryDestination(
    name="lenient_loader",
    project_id="my-project",
    dataset="raw",
    table="web_logs",
    write_disposition="WRITE_APPEND",
    max_bad_records=100  # Tolerar hasta 100 registros con errores
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

## 📊 Output Ejemplo
```
GCPBigQueryDestination 'sales_etl' received data from pipe: 'pipe1'
GCPBigQueryDestination 'sales_etl' using default credentials
GCPBigQueryDestination 'sales_etl' BigQuery client initialized successfully

GCPBigQueryDestination 'sales_etl' executing before_query...
  Query preview: CREATE OR REPLACE TABLE `my-project.warehouse.sales_fact_backup` AS...
✅ GCPBigQueryDestination 'sales_etl' before_query executed successfully
  - Bytes processed: 2,345,678 bytes (2.24 MB)
  - Rows affected: 15,432
  - Duration: 3.12s

======================================================================
GCPBigQueryDestination 'sales_etl' loading data to BigQuery...
======================================================================
  - Table: my-project.warehouse.sales_fact
  - Write disposition: WRITE_TRUNCATE
  - Create disposition: CREATE_IF_NEEDED
  - DataFrame shape: (15432, 12)
  - DataFrame columns: ['sale_id', 'customer_id', 'product_id', 'amount', ...]
  - Clustering fields: ['region', 'product_category']
  - Time partitioning: {'type': 'DAY', 'field': 'sale_date'}

GCPBigQueryDestination 'sales_etl' starting load job...
GCPBigQueryDestination 'sales_etl' waiting for load job completion...

======================================================================
GCPBigQueryDestination 'sales_etl' LOAD completed successfully:
======================================================================
  📊 Load results:
     - Rows loaded: 15,432
     - Total rows in table: 15,432
     - Table schema fields: 12
  
  ⏱️  Job info:
     - Job ID: job_abc123xyz
     - Duration: 4.56s
     - Output rows: 15,432
  
  📋 Table info:
     - Created: 2024-01-15 10:30:00
     - Modified: 2025-01-18 14:25:33
     - Clustering: ['region', 'product_category']
     - Partitioning: DAY

GCPBigQueryDestination 'sales_etl' executing after_query...
  Query preview: CALL `my-project.procedures.validate_sales_data`();...
✅ GCPBigQueryDestination 'sales_etl' after_query executed successfully
  - Bytes processed: 45,678 bytes (0.04 MB)
  - Rows affected: 3
  - Duration: 1.89s

======================================================================
✅ GCPBigQueryDestination 'sales_etl' completed successfully
======================================================================
```

---

## 📋 Parámetros Completos

| Parámetro | Tipo | Requerido | Default | Descripción |
|-----------|------|-----------|---------|-------------|
| `name` | str | ✅ | - | Nombre del componente |
| `project_id` | str | ✅ | - | ID del proyecto GCP |
| `dataset` | str | ✅ | - | Nombre del dataset |
| `table` | str | ✅ | - | Nombre de la tabla |
| `write_disposition` | str | ✅ | - | Modo de escritura |
| `credentials_path` | str | ❌ | None | Ruta al JSON de service account |
| `before_query` | str | ❌ | None | Query a ejecutar ANTES |
| `after_query` | str | ❌ | None | Query a ejecutar DESPUÉS |
| `schema` | list | ❌ | None | Schema personalizado |
| `create_disposition` | str | ❌ | 'CREATE_IF_NEEDED' | Crear tabla si no existe |
| `schema_update_options` | list | ❌ | [] | Opciones de actualización |
| `clustering_fields` | list | ❌ | None | Campos para clustering (max 4) |
| `time_partitioning` | dict | ❌ | None | Configuración de particionamiento |
| `location` | str | ❌ | None | Región de BigQuery |
| `job_labels` | dict | ❌ | {} | Labels del job |
| `max_bad_records` | int | ❌ | 0 | Máximo de registros erróneos |
| `autodetect` | bool | ❌ | True | Auto-detectar schema |

---

## 📝 Valores de write_disposition

| Valor | Comportamiento |
|-------|----------------|
| `WRITE_TRUNCATE` | Reemplaza toda la tabla (borra y recrea) |
| `WRITE_APPEND` | Agrega datos a la tabla existente |
| `WRITE_EMPTY` | Solo escribe si la tabla está vacía (falla si tiene datos) |

---

## 📝 Valores de create_disposition

| Valor | Comportamiento |
|-------|----------------|
| `CREATE_IF_NEEDED` | Crea la tabla si no existe (default) |
| `CREATE_NEVER` | Falla si la tabla no existe |

---

## 📝 Schema Update Options

| Opción | Descripción |
|--------|-------------|
| `ALLOW_FIELD_ADDITION` | Permite agregar nuevas columnas al schema |
| `ALLOW_FIELD_RELAXATION` | Permite cambiar campos REQUIRED a NULLABLE |

---

## ✅ Buenas Prácticas

1. **Usa `WRITE_TRUNCATE`** para reemplazos completos diarios
2. **Usa `WRITE_APPEND`** para cargas incrementales
3. **Usa `before_query`** para crear backups antes de cargar
4. **Usa `after_query`** para validaciones y auditoría
5. **Particiona tablas grandes** por fecha para mejor performance
6. **Usa clustering** en campos frecuentemente filtrados
7. **Define schema explícito** para producción (evita auto-detect)
8. **Usa `schema_update_options`** con precaución en producción
9. **Especifica `location`** para cumplir con regulaciones de datos
10. **Agrega `job_labels`** para organización y tracking

---

## ⚠️ Consideraciones Importantes

### Particionamiento
- Solo se puede particionar por **UN** campo de fecha/timestamp
- No se puede cambiar el particionamiento de una tabla existente
- Considera el costo de escaneo vs. beneficio de performance

### Clustering
- Máximo **4 campos** para clustering
- El orden de los campos importa (más selectivo primero)
- Solo tiene efecto en tablas grandes (>1GB)

### Schema Updates
- `ALLOW_FIELD_ADDITION` es seguro
- `ALLOW_FIELD_RELAXATION` puede causar problemas si hay queries que asumen NOT NULL
- No se pueden eliminar columnas con schema updates

### Costos
- Las operaciones de carga son **gratuitas**
- Se cobra por almacenamiento y queries
- Particionamiento reduce costos de queries

---

## 🔗 Ver También

- [GCPBigQueryOrigin](./GCPBigQueryOrigin_Guide.md) - Para extraer de BigQuery
- [Open-Stage Documentation](../README.md) - Documentación completa

---

**Open-Stage v2.3** - Enterprise ETL Framework