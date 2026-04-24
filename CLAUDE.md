# CLAUDE.md

Guía de referencia para Claude Code al trabajar en este repositorio.

---

## Descripción general

Open-Stage es un framework ETL para Python, inspirado en IBM DataStage. Implementa una arquitectura **Pipes and Filters**: los datos fluyen a través de pipelines compuestos por Origins → Pipes → Nodes → Destinations.

Es un **paquete instalable** (`pip install -e ".[all]"`), diseñado para usarse como dependencia en otros proyectos.

---

## Setup

```bash
# Instalar con todas las dependencias opcionales (Python 3.8+)
pip install -e ".[all]"

# O solo lo necesario
pip install -e ".[postgres,anthropic]"

# Ejecutar un pipeline de ejemplo
venv/bin/python sample_1_csv_print.py

# Limpiar cache de Python
./delete_python_cache.sh
```

**Importante:** usar siempre `venv/bin/python` y `venv/bin/pip`. El Python del sistema no tiene las dependencias del proyecto.

---

## Tests

```bash
venv/bin/python -m pytest tests/ -v
```

El suite cubre todos los componentes core sin depender de servicios externos (sin DB, sin API keys). Los `sample_*.py` sirven como ejemplos de integración end-to-end y sí requieren credenciales reales.

---

## Arquitectura

### Abstracciones base (`open_stage/core/base.py`)

| Clase | Rol | Conectividad |
|-------|-----|-------------|
| `Origin` | Fuente de datos | 0 entradas → 1 salida |
| `Destination` | Destino de datos | 1 entrada → 0 salidas |
| `Node` | Transformador (hereda de ambos) | M entradas → N salidas |
| `Pipe` | Conecta componentes, transporta `DataPackage` | — |
| `DataPackage` | Envuelve un DataFrame + nombre del pipe | — |

### Mixins de cardinalidad (`open_stage/core/base.py`)

Todos los componentes usan mixins para declarar cuántos pipes aceptan. **No implementar `add_input_pipe`/`add_output_pipe` manualmente** — usar los mixins.

| Mixin | Comportamiento |
|-------|---------------|
| `SingleInputMixin` | Exactamente 1 entrada; lanza `ValueError` si se agrega una segunda |
| `SingleOutputMixin` | Exactamente 1 salida; lanza `ValueError` si se agrega una segunda |
| `MultiOutputMixin` | Salidas ilimitadas |

Ejemplo de declaración de un componente:
```python
class Filter(SingleInputMixin, SingleOutputMixin, Node):
    ...
```

### Flujo de datos

```
origin.pump()
  → pipe.flow(df)
    → node.sink(data_package)
      → node.pump()
        → pipe.flow(df)
          → destination.sink(data_package)
```

El pipeline se dispara siempre llamando `origin.pump()` al final. Nunca `destination.sink()`.

Construcción con method chaining:
```python
origin.add_output_pipe(Pipe("p1")).set_destination(node)
node.add_output_pipe(Pipe("p2")).set_destination(destination)
origin.pump()
```

`add_output_pipe(pipe)` devuelve el `pipe`. `pipe.set_destination(dest)` devuelve `dest` si es un `Node`, `None` si es un `Destination`.

---

## Módulos y componentes

### `open_stage/core/base.py`
`DataPackage`, `Pipe`, `Origin`, `Destination`, `Node`, `SingleInputMixin`, `SingleOutputMixin`, `MultiOutputMixin`

### `open_stage/core/base_ai.py`
`BasePromptTransformer` — base abstracta para todos los AI Transformers. Implementa el ciclo completo CSV → LLM → CSV. Las subclases solo implementan `_initialize_client()` y `_call_api()`.

### `open_stage/core/common.py`
| Componente | Tipo | Conectividad |
|------------|------|-------------|
| `CSVOrigin` | Origin | 0 → 1 |
| `OpenOrigin` | Origin | 0 → 1 |
| `APIRestOrigin` | Origin | 0 → 1 |
| `Printer` | Destination | 1 → 0 |
| `CSVDestination` | Destination | 1 → 0 |
| `Filter` | Node | 1 → 1 |
| `Aggregator` | Node | 1 → 1 |
| `DeleteColumns` | Node | 1 → 1 |
| `RemoveDuplicates` | Node | 1 → 1 |
| `Transformer` | Node | 1 → 1 |
| `Joiner` | Node | 2 → 1 |
| `Funnel` | Node | N → 1 |
| `Switcher` | Node | 1 → N |
| `Copy` | Node | 1 → N |

### `open_stage/postgres/common.py`
`PostgresOrigin`, `PostgresDestination`

### `open_stage/mysql/common.py`
`MySQLOrigin`, `MySQLDestination`

### `open_stage/google/bigquery.py`
`GCPBigQueryOrigin`, `GCPBigQueryDestination`

### `open_stage/google/gemini.py`
`GeminiPromptTransformer`

### `open_stage/anthropic/claude.py`
`AnthropicPromptTransformer`

### `open_stage/open_ai/transformer.py`
`OpenAIPromptTransformer`

### `open_stage/deepseek/transformer.py`
`DeepSeekPromptTransformer`

---

## AI Transformers

Todas las clases extienden `BasePromptTransformer`. La base maneja el ciclo completo; las subclases solo implementan dos métodos:

```python
def _initialize_client(self) -> None:
    # Instanciar self.client con self.api_key
    ...

def _call_api(self, system_message: str, user_message: str) -> dict:
    # Llamar al LLM y devolver:
    return {
        "response_text": str,   # respuesta del modelo
        "truncated": bool,      # True si se cortó por límite de tokens
        "input_tokens": int,
        "output_tokens": int,
    }
```

El cliente se inicializa de forma lazy (primera llamada a `pump()`).

`DeepSeekPromptTransformer` tiene un límite fijo: `max_tokens` no puede superar 8192.

---

## Bases de datos — opciones avanzadas

`PostgresOrigin`, `MySQLOrigin` y `GCPBigQueryOrigin` comparten estas opciones:

| Parámetro | Descripción |
|-----------|-------------|
| `query` | SQL principal a ejecutar |
| `table` | Lee una tabla completa sin escribir `SELECT *` |
| `before_query` | SQL ejecutado antes de la consulta (temp tables, auditoría) |
| `after_query` | SQL ejecutado después de la consulta (limpieza, logs) |
| `query_parameters` | Dict con parámetros nombrados: sintaxis `:nombre` (SQLAlchemy-safe) |
| `max_results` | Limita las filas devueltas (útil en pruebas) |
| `timeout` | Tiempo máximo de ejecución en segundos |

`PostgresDestination` y `MySQLDestination` soportan `before_query`, `after_query` y `timeout`.

`GCPBigQueryDestination` soporta además `time_partitioning`, `clustering_fields`, `write_disposition` y `schema_update_options`.

---

## Patrones de diseño

- **Lazy initialization**: los clientes de DB y API se conectan en el primer uso, no al construir
- **Validación en construcción**: configuraciones inválidas lanzan `ValueError` inmediatamente
- **Los errores siempre propagan**: todos los bloques `except` hacen `raise` después de loggear — los pipelines nunca fallan silenciosamente
- **Limpieza de recursos**: los DataFrames se limpian en bloques `finally` tras el procesamiento
- **`**kwargs` forwarding**: `CSVOrigin` y `CSVDestination` pasan kwargs adicionales a pandas

---

## Logging

Todos los módulos usan `logging.getLogger(__name__)`. **No hay `print()` en el código del framework** (excepto `Printer.sink()`, que es salida intencional).

```python
import logging

logging.basicConfig(level=logging.WARNING)   # producción
logging.basicConfig(level=logging.INFO)      # desarrollo
logging.basicConfig(level=logging.DEBUG)     # depuración

# Control por módulo
logging.getLogger("open_stage.google.bigquery").setLevel(logging.WARNING)
logging.getLogger("open_stage.core").setLevel(logging.DEBUG)
```

---

## Packaging (`pyproject.toml`)

```
[project.optional-dependencies]
postgres  = ["sqlalchemy>=1.4", "psycopg2-binary>=2.9"]
mysql     = ["sqlalchemy>=1.4", "pymysql>=1.0"]
bigquery  = ["google-cloud-bigquery>=3.0", "db-dtypes>=1.0", "google-auth>=2.0"]
anthropic = ["anthropic>=0.20"]
openai    = ["openai>=1.0"]
deepseek  = ["openai>=1.0"]
gemini    = ["google-genai>=1.0", "google-generativeai>=0.4"]
all       = [todos los anteriores]
```

---

## Variables de entorno (`.env`)

| Variable | Usada por |
|----------|-----------|
| `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` | PostgresOrigin/Destination |
| `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_DB`, `MYSQL_USER`, `MYSQL_PASSWORD` | MySQLOrigin/Destination |
| `GCP_PROJECT`, `GOOGLE_APPLICATION_CREDENTIALS` | BigQuery, Gemini |
| `ANTHROPIC_API_KEY` | AnthropicPromptTransformer |
| `OPENAI_API_KEY` | OpenAIPromptTransformer |
| `DEEPSEEK_API_KEY` | DeepSeekPromptTransformer |

---

## Convenciones de código

- Indentación: 2 espacios (estilo original del proyecto)
- Sin emojis en logs ni en código del framework
- Sin comentarios que expliquen el "qué" — solo el "por qué" si es no obvio
- Sin manejo de errores para escenarios imposibles — los errores deben propagarse
- Imports en archivos de ejemplo/sample: `from open_stage.X.Y import Z` (nunca `from src.`)

---

## Estructura de archivos

```
open_stage/
├── core/
│   ├── base.py
│   ├── base_ai.py
│   └── common.py
├── postgres/
│   └── common.py
├── mysql/
│   └── common.py
├── google/
│   ├── bigquery.py
│   └── gemini.py
├── anthropic/
│   └── claude.py
├── open_ai/
│   └── transformer.py
└── deepseek/
    └── transformer.py

tests/
├── conftest.py       # CaptureDest helper, fixture sample_df
├── test_base.py      # Tests de DataPackage, Pipe y Mixins
└── test_common.py    # Tests de todos los componentes core

sample_*.py           # Ejemplos de integración (requieren credenciales reales)
pyproject.toml
requirements.txt
```

---

## Documentación

Los archivos en `docs/` son anteriores a la refactorización actual y están desactualizados. La fuente de verdad es este archivo y el `README.md`.
