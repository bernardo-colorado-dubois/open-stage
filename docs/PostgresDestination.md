# PostgresDestination - Guía de Uso

Componente para cargar datos a PostgreSQL con capacidades avanzadas.

---

## 🎯 Características

- ✅ Carga de DataFrames a PostgreSQL
- ✅ Queries pre y post carga (`before_query`, `after_query`)
- ✅ Control de disposición de escritura (FAIL, REPLACE, APPEND)
- ✅ Timeout configurable
- ✅ Carga optimizada con chunks y multi-insert
- ✅ Logging detallado con estadísticas

---

## 📦 Instalación
```bash
pip install sqlalchemy psycopg2-binary pandas
```

---

## 🚀 Uso Básico

### Ejemplo 1: Carga Simple (APPEND)
```python
from src.postgres.common import PostgresDestination
from src.core.base import Pipe
from src.core.common import CSVOrigin

# Leer datos
origin = CSVOrigin("reader", filepath_or_buffer="sales.csv")

# Destino PostgreSQL
destination = PostgresDestination(
    name="sales_loader",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    table="sales",
    schema="public",
    if_exists="append"  # Agregar datos
)

# Conectar y ejecutar
pipe = Pipe("pipe1")
origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Ejemplo 2: Reemplazar Tabla (REPLACE)
```python
# Reemplazar todos los datos de la tabla
destination = PostgresDestination(
    name="daily_report",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    table="daily_summary",
    schema="reports",
    if_exists="replace"  # ✨ Reemplazar tabla completa
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Ejemplo 3: Solo si Tabla No Existe (FAIL)
```python
# Fallar si la tabla ya existe (carga inicial)
destination = PostgresDestination(
    name="initial_load",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    table="customers",
    schema="public",
    if_exists="fail"  # ✨ Error si tabla existe
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

## 🔧 Funcionalidades Avanzadas

### Ejemplo 4: Before Query (Preparar Antes de Cargar)
```python
# Ejecutar query ANTES de cargar datos
destination = PostgresDestination(
    name="staged_load",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    table="orders",
    schema="public",
    before_query="""
        -- Crear backup antes de cargar
        DROP TABLE IF EXISTS public.orders_backup;
        CREATE TABLE public.orders_backup AS
        SELECT * FROM public.orders;
        
        -- Truncar tabla staging
        TRUNCATE TABLE staging.temp_orders;
        
        -- Preparar secuencias
        SELECT setval('orders_id_seq', COALESCE(MAX(id), 1))
        FROM public.orders;
    """,
    if_exists="replace"
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
- Resetear secuencias
- Deshabilitar triggers temporalmente

---

### Ejemplo 5: After Query (Auditoría y Post-procesamiento)
```python
# Ejecutar query DESPUÉS de cargar datos
destination = PostgresDestination(
    name="customer_loader",
    host="localhost",
    database="crm",
    user="postgres",
    password="password",
    table="customers",
    schema="public",
    if_exists="append",
    after_query="""
        -- Registrar en log de auditoría
        INSERT INTO audit.load_log (
            table_name,
            loaded_at,
            record_count,
            loaded_by
        ) VALUES (
            'customers',
            NOW(),
            (SELECT COUNT(*) FROM public.customers),
            'open-stage-pipeline'
        );
        
        -- Actualizar metadata
        UPDATE control.table_metadata
        SET 
            last_updated = NOW(),
            row_count = (SELECT COUNT(*) FROM public.customers)
        WHERE table_name = 'customers';
        
        -- Refresh materialized view
        REFRESH MATERIALIZED VIEW reports.customer_summary;
        
        -- Analyze para estadísticas
        ANALYZE public.customers;
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
- ANALYZE para actualizar estadísticas
- Crear índices adicionales
- Habilitar triggers nuevamente

---

### Ejemplo 6: Workflow Completo (Before + After)
```python
# Pipeline completo con preparación y post-procesamiento
destination = PostgresDestination(
    name="sales_etl",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    table="sales_fact",
    schema="public",
    
    # ANTES: Preparar
    before_query="""
        -- Backup incremental con timestamp
        DROP TABLE IF EXISTS public.sales_fact_backup;
        CREATE TABLE public.sales_fact_backup AS
        SELECT *, NOW() as backup_timestamp
        FROM public.sales_fact;
        
        -- Preparar staging
        TRUNCATE TABLE staging.sales_staging;
        
        -- Marcar inicio de carga en control
        INSERT INTO control.etl_status (
            table_name, status, start_time
        ) VALUES (
            'sales_fact', 'LOADING', NOW()
        )
        ON CONFLICT (table_name) DO UPDATE
        SET status = 'LOADING', start_time = NOW();
        
        -- Deshabilitar triggers para mejor performance
        ALTER TABLE public.sales_fact DISABLE TRIGGER ALL;
    """,
    
    if_exists="replace",
    timeout=600,  # 10 minutos
    
    # DESPUÉS: Validar y registrar
    after_query="""
        -- Habilitar triggers nuevamente
        ALTER TABLE public.sales_fact ENABLE TRIGGER ALL;
        
        -- Validar datos cargados
        DO $$
        DECLARE
            invalid_count INTEGER;
        BEGIN
            SELECT COUNT(*) INTO invalid_count
            FROM public.sales_fact
            WHERE amount < 0 OR customer_id IS NULL;
            
            IF invalid_count > 0 THEN
                RAISE EXCEPTION 'Found % invalid records', invalid_count;
            END IF;
        END $$;
        
        -- Actualizar agregados
        REFRESH MATERIALIZED VIEW CONCURRENTLY reports.sales_by_region;
        REFRESH MATERIALIZED VIEW CONCURRENTLY reports.sales_by_product;
        
        -- Registrar éxito
        INSERT INTO audit.etl_runs (
            pipeline_name,
            table_name,
            run_timestamp,
            records_loaded,
            status,
            duration_seconds
        ) VALUES (
            'sales_etl',
            'sales_fact',
            NOW(),
            (SELECT COUNT(*) FROM public.sales_fact),
            'SUCCESS',
            EXTRACT(EPOCH FROM (NOW() - (
                SELECT start_time FROM control.etl_status 
                WHERE table_name = 'sales_fact'
            )))
        );
        
        -- Actualizar control
        UPDATE control.etl_status
        SET 
            status = 'COMPLETED',
            end_time = NOW(),
            record_count = (SELECT COUNT(*) FROM public.sales_fact)
        WHERE table_name = 'sales_fact';
        
        -- VACUUM ANALYZE para optimizar
        VACUUM ANALYZE public.sales_fact;
    """
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Ejemplo 7: Con Timeout
```python
# Control de tiempo para cargas grandes
destination = PostgresDestination(
    name="large_load",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    table="big_table",
    schema="public",
    if_exists="append",
    timeout=1800  # 30 minutos máximo
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Ejemplo 8: Carga en Schema No-Public
```python
# Cargar datos en schema específico
destination = PostgresDestination(
    name="analytics_load",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    table="monthly_summary",
    schema="analytics",  # Schema específico
    if_exists="append"
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Ejemplo 9: Con Validaciones Pre y Post
```python
# Validar antes y después de cargar
destination = PostgresDestination(
    name="validated_load",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    table="products",
    schema="public",
    
    before_query="""
        -- Validar espacio disponible
        DO $$
        DECLARE
            free_space BIGINT;
        BEGIN
            SELECT pg_database_size(current_database()) INTO free_space;
            -- Validaciones personalizadas aquí
        END $$;
        
        -- Verificar que la tabla destino existe
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'products'
            ) THEN
                RAISE EXCEPTION 'Table public.products does not exist';
            END IF;
        END $$;
    """,
    
    if_exists="append",
    
    after_query="""
        -- Validar integridad referencial
        DO $$
        DECLARE
            orphan_count INTEGER;
        BEGIN
            SELECT COUNT(*) INTO orphan_count
            FROM public.products p
            WHERE NOT EXISTS (
                SELECT 1 FROM public.categories c 
                WHERE c.id = p.category_id
            );
            
            IF orphan_count > 0 THEN
                RAISE WARNING 'Found % products with invalid category', orphan_count;
            END IF;
        END $$;
        
        -- Verificar duplicados
        DO $$
        DECLARE
            dup_count INTEGER;
        BEGIN
            SELECT COUNT(*) - COUNT(DISTINCT sku) INTO dup_count
            FROM public.products;
            
            IF dup_count > 0 THEN
                RAISE EXCEPTION 'Found % duplicate SKUs', dup_count;
            END IF;
        END $$;
    """
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

### Ejemplo 10: Carga con Índices Optimizados
```python
# Optimizar carga removiendo/recreando índices
destination = PostgresDestination(
    name="optimized_load",
    host="localhost",
    database="warehouse",
    user="postgres",
    password="password",
    table="transactions",
    schema="public",
    
    before_query="""
        -- Guardar definiciones de índices
        CREATE TEMP TABLE temp_indexes AS
        SELECT indexdef
        FROM pg_indexes
        WHERE schemaname = 'public' AND tablename = 'transactions';
        
        -- Eliminar índices para carga más rápida
        DROP INDEX IF EXISTS idx_transactions_date;
        DROP INDEX IF EXISTS idx_transactions_customer;
        DROP INDEX IF EXISTS idx_transactions_amount;
    """,
    
    if_exists="append",
    
    after_query="""
        -- Recrear índices
        CREATE INDEX idx_transactions_date 
            ON public.transactions(transaction_date);
        CREATE INDEX idx_transactions_customer 
            ON public.transactions(customer_id);
        CREATE INDEX idx_transactions_amount 
            ON public.transactions(amount) WHERE amount > 1000;
        
        -- ANALYZE después de crear índices
        ANALYZE public.transactions;
    """
)

origin.add_output_pipe(pipe).set_destination(destination)
origin.pump()
```

---

## 📊 Output Ejemplo
```
PostgresDestination 'sales_etl' received data from pipe: 'pipe1'
PostgresDestination 'sales_etl' engine initialized successfully
Connection: postgres@localhost:5432/warehouse
  - Timeout: 600.0s

PostgresDestination 'sales_etl' executing before_query...
  Query preview: -- Backup incremental con timestamp
        DROP TABLE IF EXISTS public.sales_fact_backup;...
✅ PostgresDestination 'sales_etl' before_query executed successfully
  - Rows affected: 15,432
  - Duration: 3.12s

======================================================================
PostgresDestination 'sales_etl' loading data to PostgreSQL...
======================================================================
  - Database: warehouse
  - Schema: public
  - Table: sales_fact
  - Full table reference: public.sales_fact
  - If exists: replace
  - DataFrame shape: (15432, 12)
  - DataFrame columns: ['sale_id', 'customer_id', 'product_id', 'amount', ...]

PostgresDestination 'sales_etl' starting load operation...

======================================================================
PostgresDestination 'sales_etl' LOAD completed successfully:
======================================================================
  📊 Load results:
     - Rows loaded: 15,432
     - Columns loaded: 12
     - Table: public.sales_fact
  
  ⏱️  Load info:
     - Duration: 4.56s
     - Write mode: replace
  
  📋 Data types:
     - sale_id: int64
     - customer_id: int64
     - product_id: int64
     - amount: float64
     - sale_date: object
     - region: object
     - ...

PostgresDestination 'sales_etl' executing after_query...
  Query preview: -- Habilitar triggers nuevamente
        ALTER TABLE public.sales_fact ENABLE TRIGGER ALL;...
✅ PostgresDestination 'sales_etl' after_query executed successfully
  - Rows affected: 3
  - Duration: 2.34s

======================================================================
✅ PostgresDestination 'sales_etl' completed successfully
======================================================================
PostgresDestination 'sales_etl' connection closed
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
| `table` | str | ✅ | - | Nombre de la tabla |
| `schema` | str | ❌ | 'public' | Schema de PostgreSQL |
| `if_exists` | str | ❌ | 'append' | Modo de escritura |
| `before_query` | str | ❌ | None | Query a ejecutar ANTES |
| `after_query` | str | ❌ | None | Query a ejecutar DESPUÉS |
| `timeout` | float | ❌ | None | Timeout en segundos |

---

## 🔧 Valores de if_exists

| Valor | Comportamiento |
|-------|----------------|
| `append` | Agrega datos a la tabla existente (default) |
| `replace` | Elimina y recrea la tabla con los nuevos datos |
| `fail` | Error si la tabla ya existe |

---

## ✅ Buenas Prácticas

1. **Usa `append`** para cargas incrementales
2. **Usa `replace`** para reemplazos completos diarios
3. **Usa `before_query`** para crear backups antes de cargar
4. **Usa `after_query`** para validaciones y auditoría
5. **Deshabilita triggers** en `before_query` para mejor performance
6. **Habilita triggers** en `after_query` después de cargar
7. **Ejecuta `ANALYZE`** en `after_query` para actualizar estadísticas
8. **Usa `VACUUM`** después de cargas grandes
9. **Especifica `timeout`** para cargas grandes
10. **Valida datos** en `after_query` antes de confirmar éxito
11. **Elimina índices** antes de cargas grandes y recréalos después
12. **Usa transacciones** en before/after queries cuando sea necesario

---

## ⚠️ Consideraciones Importantes

### Performance
- La carga usa `chunksize=1000` para procesar en lotes
- Usa `method='multi'` para INSERTs optimizados
- Considera deshabilitar triggers en cargas grandes
- Elimina índices antes de cargar y recréalos después
- Ejecuta `VACUUM ANALYZE` después de cargas grandes

### Schemas
- El schema por defecto es `public`
- Especifica schema explícitamente si usas otros schemas
- El usuario debe tener permisos en el schema destino

### Modos de Escritura
- `append`: Más rápido, agrega datos
- `replace`: Elimina tabla completa y recrea (DROP + CREATE)
- `fail`: Útil para cargas iniciales (previene sobrescritura)

### Timeout
- Se aplica tanto a la conexión inicial como a queries
- Útil para cargas muy grandes
- Si una operación supera el timeout, lanza excepción

### Transacciones
- Cada operación (before, load, after) usa su propia transacción
- Si `after_query` falla, los datos YA están cargados
- Usa transacciones explícitas en before/after si necesitas rollback

### Permisos
- El usuario necesita permisos CREATE/INSERT/UPDATE/DELETE
- Para `replace` necesita permisos DROP TABLE
- Para schemas no-public, necesita permisos en ese schema

---

## 🔍 Troubleshooting

### Error: "Table already exists"
```python
# Solución 1: Usar append
if_exists="append"

# Solución 2: Usar replace
if_exists="replace"

# Solución 3: Eliminar tabla en before_query
before_query="DROP TABLE IF EXISTS schema.table;"
```

### Error: "Permission denied for schema"
```sql
-- Verificar permisos
SELECT has_schema_privilege('username', 'schema_name', 'CREATE');

-- Otorgar permisos
GRANT CREATE, USAGE ON SCHEMA schema_name TO username;
```

### Error: "Timeout exceeded"
```python
# Aumentar timeout
timeout=1800  # 30 minutos

# O dividir carga en chunks más pequeños
```

### Carga muy lenta
```python
# Optimizar con before_query y after_query
before_query="""
    -- Eliminar índices
    DROP INDEX IF EXISTS idx_table_field;
    
    -- Deshabilitar triggers
    ALTER TABLE schema.table DISABLE TRIGGER ALL;
"""

after_query="""
    -- Recrear índices
    CREATE INDEX idx_table_field ON schema.table(field);
    
    -- Habilitar triggers
    ALTER TABLE schema.table ENABLE TRIGGER ALL;
    
    -- Actualizar estadísticas
    ANALYZE schema.table;
"""
```

---

## 🔗 Ver También

- [PostgresOrigin](./PostgresOrigin.md) - Para extraer de PostgreSQL
- [Open-Stage Documentation](../README.md) - Documentación completa

---

**Open-Stage v2.4** - Enterprise ETL Framework