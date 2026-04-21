# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Open-Stage is an ETL (Extract, Transform, Load) framework inspired by IBM DataStage, built on a **Pipes and Filters** architecture. Data flows through pipelines composed of Origins → Pipes → Nodes/Destinations.

## Setup & Running

```bash
# Install dependencies (requires Python 3.8+)
pip install -r requirements.txt

# Create .env with credentials (copy from .env.example if present)
# Run a sample pipeline
python sample_1_csv_print.py

# Clean Python cache
./delete_python_cache.sh
```

No formal test suite — the `sample_*.py` scripts serve as integration tests/examples. Each sample requires valid `.env` credentials (DB or API keys) and running external services where applicable.

## Architecture

### Core Abstractions (`src/core/base.py`)

| Class | Role | Pipes |
|-------|------|-------|
| `Origin` | Data source | 0 inputs → 1 output |
| `Destination` | Data sink | 1 input → 0 outputs |
| `Node` | Transformer (inherits both) | M inputs → N outputs |

**`DataPackage`**: The unit of data transport. Wraps a pandas `DataFrame` plus metadata (`pipe_name`).

**`Pipe`**: Connects components. Call `.set_destination(component)` to link and `.flow()` to push data.

### Data Flow

```
Origin.pump() → Pipe → Destination.sink()
                          └── calls upstream pump() automatically
```

Pipelines are built with **method chaining**:
```python
origin.add_output_pipe().set_destination(node).add_output_pipe().set_destination(destination)
destination.sink()  # triggers the entire chain
```

### Component Modules

- `src/core/base.py` — `DataPackage`, `Pipe`, `Origin`, `Destination`, `Node`
- `src/core/common.py` — `CSVOrigin`, `Generator`, `APIRestOrigin`, `Printer`, `CSVDestination`, `Filter`, `Aggregator`, `DeleteColumns`, `RemoveDuplicates`, `Joiner`, `Transformer`, `Funnel`, `Switcher`, `Copy`
- `src/postgres/` — `PostgresOrigin`, `PostgresDestination`
- `src/mysql/` — `MySQLOrigin`, `MySQLDestination`
- `src/google/` — `GCPBigQueryOrigin`, `GCPBigQueryDestination`, `GeminiPromptTransformer`
- `src/anthropic/` — `AnthropicPromptTransformer`
- `src/open_ai/` — `OpenAIPromptTransformer`
- `src/deepseek/` — `DeepSeekPromptTransformer`

### Routers

- **`Funnel`** (N→1): Merges multiple input pipes into one output
- **`Switcher`** (1→N): Routes rows to different outputs based on a condition
- **`Copy`** (1→N): Duplicates data to multiple outputs

### AI Transformers

All AI transformers (`AnthropicPromptTransformer`, `OpenAIPromptTransformer`, etc.) convert the incoming DataFrame to CSV, send it to the LLM with a user-defined prompt, and parse the CSV response back into a DataFrame.

### Database Origins — Advanced Features

PostgreSQL and MySQL Origins support:
- `before_query` / `after_query`: SQL executed before/after extraction (temp tables, audit logging)
- `query_parameters`: Dict of named parameters using `:param_name` syntax (SQLAlchemy-safe)
- `table`: Read a full table without writing `SELECT *`
- `max_results`: Limit rows (useful for testing)
- `timeout`: Connection/query timeout in seconds

### Key Design Patterns

- **Lazy initialization**: DB/API clients connect on first use, not at construction
- **Validation at construction**: Invalid pipe configurations raise errors early (e.g., adding two output pipes to an Origin)
- **Resource cleanup**: DataFrames are cleared after downstream processing
- **`**kwargs` forwarding**: Origins/Destinations accept extra kwargs passed to pandas or the underlying driver

## Environment Variables (`.env`)

Required keys depending on which components are used:

| Variable | Used by |
|----------|---------|
| `POSTGRES_*` | PostgresOrigin/Destination |
| `MYSQL_*` | MySQLOrigin/Destination |
| `GCP_PROJECT`, `GOOGLE_APPLICATION_CREDENTIALS` | BigQuery, Gemini |
| `ANTHROPIC_API_KEY` | AnthropicPromptTransformer |
| `OPENAI_API_KEY` | OpenAIPromptTransformer |
| `DEEPSEEK_API_KEY` | DeepSeekPromptTransformer |

## Documentation

Detailed component guides live in `docs/`:
- `docs/StartHere.md` — quick start
- `docs/OPEN_STAGE_KNOWLEDGE_BASE.md` — complete reference
- `docs/ClaudeQuickReference.md` — AI transformer usage
- `docs/complex_pipelines.md` — advanced pipeline patterns
- `docs/Transformer.md` — custom transformation functions
