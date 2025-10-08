# Open-Stage

> A modern, AI-powered ETL framework for enterprise data workflows

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Framework](https://img.shields.io/badge/framework-ETL-green.svg)]()

Open-Stage is an enterprise-grade ETL (Extract, Transform, Load) platform built in Python, inspired by IBM DataStage. It implements a pipes and filters architecture that enables the creation of modular, scalable data processing pipelines with multi-model generative AI capabilities.

## ✨ Key Features

- 🧩 **24 Modular Components** (5 base + 19 specialized)
- 🔌 **Multiple Data Sources**: CSV, PostgreSQL, BigQuery, REST APIs
- 🤖 **AI-Powered Transformations**: Claude (Anthropic), Gemini (Google), DeepSeek
- ✅ **Robust Validations** and intelligent error handling
- ⛓️ **Method Chaining** for fluent syntax
- 🔧 **Extensible Architecture** by provider and component type
- 📜 **Open Source** under MIT License

## Core Components

### Class Diagram

```mermaid
classDiagram
    class DataPackage {
        -string pipe_name
        -DataFrame df
        +get_pipe_name() string
        +get_df() DataFrame
    }
    
    class Pipe {
        -string name
        -Origin origin
        -Destination destination
        +get_name() string
        +set_origin(origin)
        +set_destination(destination)
        +flow(df DataFrame)
    }
    
    class Origin {
        <<abstract>>
        -dict outputs
        +add_output_pipe(pipe)*
        +pump()*
    }
    
    class Destination {
        <<abstract>>
        -dict inputs
        +add_input_pipe(pipe)*
        +sink(data_package)*
    }
    
    class Node {
        -dict inputs
        -dict outputs
        +add_input_pipe(pipe)*
        +add_output_pipe(pipe)*
        +sink(data_package)*
        +pump()*
    }
    
    Origin <|-- Node
    Destination <|-- Node
    
    Pipe --> Origin : origin
    Pipe --> Destination : destination
    Pipe ..> DataPackage : creates
    
    Origin --> Pipe : outputs
    Destination --> Pipe : inputs
```

## 🚀 Quick Start

### Installation

```bash
pip install -r requirements.txt
```

### Simple Pipeline Example

```python
from src.core.common import CSVOrigin, Filter, CSVDestination
from src.core.base import Pipe

# Create components
csv_origin = CSVOrigin("reader", filepath_or_buffer="data.csv")
filter_node = Filter("adults", "age", ">=", 18)
csv_dest = CSVDestination("writer", path_or_buf="output.csv", index=False)

# Create pipes
pipe1 = Pipe("pipe1")
pipe2 = Pipe("pipe2")

# Connect pipeline with method chaining
csv_origin.add_output_pipe(pipe1).set_destination(filter_node)
filter_node.add_output_pipe(pipe2).set_destination(csv_dest)

# Execute
csv_origin.pump()
```

## 📦 Project Structure

```
project/
├── LICENSE                    # MIT License
├── README.md
├── requirements.txt
├── src/
│   ├── core/
│   │   ├── __init__.py
│   │   ├── base.py           # Base classes (5)
│   │   └── common.py         # Generic components (14)
│   ├── postgres/
│   │   ├── __init__.py
│   │   └── common.py         # PostgreSQL connector (1)
│   ├── google/
│   │   ├── __init__.py
│   │   ├── cloud.py          # BigQuery services (2)
│   │   └── gemini.py         # Gemini AI (1)
│   ├── anthropic/
│   │   ├── __init__.py
│   │   └── claude.py         # Claude AI (1)
│   └── deepseek/
│       ├── __init__.py
│       └── deepseek.py       # DeepSeek AI (1)
```

## 🏗️ Architecture

### Base Classes

Open-Stage is built on 5 fundamental classes:

1. **DataPackage**: Encapsulates data and metadata
2. **Pipe**: Connects components and transports data
3. **Origin** (0→1): Abstract class for data sources
4. **Destination** (1→0): Abstract class for data sinks
5. **Node**: Abstract class for transformers (inherits from Origin and Destination)

### Component Categories

#### 🔵 Origins (Data Sources) - 0→1

| Component | Description |
|-----------|-------------|
| `Generator` | Generates sequential numeric data |
| `CSVOrigin` | Reads CSV files |
| `APIRestOrigin` | Consumes REST APIs |
| `PostgresOrigin` | Queries PostgreSQL databases |
| `GCPBigQueryOrigin` | Queries Google BigQuery |

#### 🟢 Destinations (Data Sinks) - 1→0

| Component | Description |
|-----------|-------------|
| `Printer` | Displays data to console |
| `CSVDestination` | Writes CSV files |
| `GCPBigQueryDestination` | Loads data to BigQuery |

#### 🟡 Routers - N↔M

| Component | Connectivity | Description |
|-----------|--------------|-------------|
| `Funnel` | N→1 | Combines multiple streams |
| `Switcher` | 1→N | Routes data conditionally |
| `Copy` | 1→N | Duplicates data streams |

#### 🔴 Transformers - 1→1

| Component | Description |
|-----------|-------------|
| `Filter` | Filters rows (9 operators: <, >, <=, >=, !=, =, in, not in, between) |
| `Aggregator` | Aggregates data (sum, count, mean, etc.) |
| `DeleteColumns` | Removes specified columns |
| `RemoveDuplicates` | Deduplicates based on key field |
| `Joiner` | Joins two DataFrames (2→1) |
| `Transformer` | Applies custom functions |

#### 🤖 AI Transformers - 1→1

| Component | Provider | Model Examples |
|-----------|----------|----------------|
| `AnthropicPromptTransformer` | Anthropic | claude-sonnet-4-5-20250929 |
| `GeminiPromptTransformer` | Google | gemini-2.5-flash |
| `DeepSeekPromptTransformer` | DeepSeek | deepseek-chat, deepseek-coder |

## 💡 Usage Examples

### Example 1: Filter and Aggregate

```python
from src.core.common import CSVOrigin, Filter, Aggregator, CSVDestination
from src.core.base import Pipe

# Read CSV
csv_origin = CSVOrigin("reader", filepath_or_buffer="sales.csv")

# Filter high-value sales
filter_node = Filter("high_value", "amount", ">", 1000)

# Aggregate by category
aggregator = Aggregator("total_sales", "category", "total", "sum", "amount")

# Write results
csv_dest = CSVDestination("writer", path_or_buf="summary.csv", index=False)

# Connect pipeline
pipe1, pipe2, pipe3 = Pipe("p1"), Pipe("p2"), Pipe("p3")

csv_origin.add_output_pipe(pipe1).set_destination(filter_node)
filter_node.add_output_pipe(pipe2).set_destination(aggregator)
aggregator.add_output_pipe(pipe3).set_destination(csv_dest)

# Execute
csv_origin.pump()
```

### Example 2: AI-Powered Transformation

```python
from src.core.common import CSVOrigin, CSVDestination
from src.anthropic.claude import AnthropicPromptTransformer
from src.core.base import Pipe

# Read reviews
csv_origin = CSVOrigin("reader", filepath_or_buffer="reviews.csv")

# AI sentiment analysis with Claude
claude = AnthropicPromptTransformer(
    name="sentiment_analyzer",
    model="claude-sonnet-4-5-20250929",
    api_key="your-api-key",
    prompt="Add a sentiment_score column (positive, negative, neutral) based on the review text",
    max_tokens=16000
)

# Write enriched data
csv_dest = CSVDestination("writer", path_or_buf="reviews_analyzed.csv", index=False)

# Connect pipeline
pipe1, pipe2 = Pipe("input"), Pipe("output")

csv_origin.add_output_pipe(pipe1).set_destination(claude)
claude.add_output_pipe(pipe2).set_destination(csv_dest)

# Execute
csv_origin.pump()
```

### Example 3: Split and Rejoin with BigQuery

```python
from src.google.cloud import GCPBigQueryOrigin, GCPBigQueryDestination
from src.core.common import Switcher, Funnel, Copy, Aggregator
from src.core.base import Pipe

# Read from BigQuery
bq_origin = GCPBigQueryOrigin(
    name="reader",
    project_id="my-project",
    query="SELECT * FROM dataset.table"
)

# Split by category
switcher = Switcher(
    "router",
    field="category",
    mapping={"A": "pipe_a", "B": "pipe_b", "C": "pipe_c"}
)

# Rejoin streams
funnel = Funnel("combiner")

# Duplicate for parallel processing
copy = Copy("splitter")

# Aggregate
aggregator = Aggregator("counter", "category", "count", "count")

# Write to BigQuery
bq_dest = GCPBigQueryDestination(
    name="writer",
    project_id="my-project",
    dataset="output_dataset",
    table="results",
    write_disposition="WRITE_TRUNCATE"
)

# Connect complex pipeline
# ... (pipe connections)

bq_origin.pump()
```

## 🔧 Configuration

### Dependencies

```txt
pandas>=1.3.0
requests>=2.25.0
sqlalchemy>=1.4.0
psycopg2-binary>=2.9.0
google-cloud-bigquery>=3.0.0
google-auth>=2.0.0
db-dtypes>=1.0.0
anthropic>=0.18.0
google-generativeai>=0.3.0
openai>=1.0.0
```


## 🎯 Design Principles

1. **Pipes and Filters Architecture**: Modular, reusable components with clear separation of concerns
2. **Robust Validations**: Validation at construction time, data type validation, connectivity validation
3. **Error Handling**: Try-catch in critical operations, detailed error logging, graceful recovery
4. **Default Connectivity**: Origin (0→1), Destination (1→0), Node (flexible via override)
5. **Method Chaining**: Fluent syntax for pipeline construction
6. **Lazy Initialization**: External clients initialized on demand
7. **Immediate Processing**: `sink()` automatically calls `pump()`
8. **Resource Cleanup**: DataFrames cleaned post-processing, connections closed appropriately

## 📊 Connectivity Rules

| Component Type | Inputs | Outputs | Override |
|----------------|--------|---------|----------|
| **Origins** | 0 | 1 | No |
| **Destinations** | 1 | 0 | No |
| **Routers** (Funnel) | N | 1 | Yes (inputs) |
| **Routers** (Switcher, Copy) | 1 | N | Yes (outputs) |
| **Transformers** | 1 | 1 | No |
| **Transformers** (Joiner) | 2 | 1 | Yes (inputs) |
| **AI Transformers** | 1 | 1 | No |

## 🤝 Contributing

Contributions are welcome! To contribute:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

### Contribution Guidelines

- Follow existing code style (2-space indentation)
- Add tests for new functionality
- Update documentation
- Follow the framework's design principles

## 🗺️ Roadmap

### Pending AI Providers
- [ ] OpenAI Transformer (GPT-4, GPT-4 Turbo, GPT-4o)
- [ ] Mistral AI Transformer
- [ ] Cohere Transformer
- [ ] Llama Transformer (via Ollama/local)

### Potential Components

#### Origins
- MySQL/MariaDB, MongoDB, Kafka Consumer
- S3 (AWS), Azure Blob Storage, Snowflake
- Excel, Parquet, JSON, XML, SFTP

#### Destinations
- MySQL/MariaDB, MongoDB, Kafka Producer
- S3 (AWS), Azure Blob Storage, Snowflake
- Excel, Parquet, JSON, XML, SFTP

#### Transformers
- Sort, Pivot, Unpivot, Window Functions
- Lookup, Merge, Split, Sample
- Normalize, Encode

#### Validators
- Schema Validator, Data Quality Validator
- Business Rules Validator

#### Utilities
- Logger, Profiler, Cache, Checkpoint

## 📄 License

This project is licensed under the MIT License.

```
MIT License

Copyright (c) 2025 Bernardo Colorado Dubois and Saul Hernandez Cordova

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

## 👥 Authors

- **Bernardo Colorado Dubois**
- **Saul Hernandez Cordova**

## 🙏 Acknowledgments

Inspired by IBM DataStage and Unix pipes and filters architecture. Thanks to the Python open-source community and the libraries that make this project possible.

---

**Open-Stage** - A modern, AI-powered ETL framework for enterprise data workflows.