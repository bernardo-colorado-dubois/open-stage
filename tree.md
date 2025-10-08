project/
├── LICENSE                    # MIT License
├── README.md
├── requirements.txt
├── src/
│   ├── core/
│   │   ├── __init__.py
│   │   ├── base.py                    # Clases base fundamentales (5 clases)
│   │   │   ├── DataPackage           # Empaqueta datos + metadatos
│   │   │   ├── Pipe                  # Conecta componentes
│   │   │   ├── Origin                # Clase abstracta para fuentes (0→1)
│   │   │   ├── Destination           # Clase abstracta para destinos (1→0)
│   │   │   └── Node                  # Clase abstracta para transformadores
│   │   └── common.py                  # Componentes genéricos (14 clases)
│   │       ├── Generator             # Origin: Datos secuenciales
│   │       ├── CSVOrigin             # Origin: Archivos CSV
│   │       ├── APIRestOrigin         # Origin: APIs REST
│   │       ├── Printer               # Destination: Visualización
│   │       ├── CSVDestination        # Destination: Archivos CSV
│   │       ├── Funnel                # Node/Router: N→1 combinación
│   │       ├── Switcher              # Node/Router: 1→N condicional
│   │       ├── Copy                  # Node/Router: 1→N duplicación
│   │       ├── Filter                # Node/Transformer: Filtrado (9 ops)
│   │       ├── Aggregator            # Node/Transformer: Agregaciones
│   │       ├── DeleteColumns         # Node/Transformer: Elimina columnas
│   │       ├── RemoveDuplicates      # Node/Transformer: Deduplicación
│   │       ├── Joiner                # Node/Transformer: Joins (2→1)
│   │       └── Transformer           # Node/Transformer: Funciones custom
│   ├── postgres/
│   │   ├── __init__.py
│   │   └── common.py                  # Conectores PostgreSQL (1 clase)
│   │       └── PostgresOrigin        # Origin: PostgreSQL queries
│   ├── google/
│   │   ├── __init__.py
│   │   ├── cloud.py                   # Servicios GCP BigQuery (2 clases)
│   │   │   ├── GCPBigQueryOrigin     # Origin: BigQuery queries
│   │   │   └── GCPBigQueryDestination # Destination: BigQuery load
│   │   └── gemini.py                  # IA Generativa Google (1 clase)
│   │       └── GeminiPromptTransformer # Node: Gemini transformations
│   ├── anthropic/
│   │   ├── __init__.py
│   │   └── claude.py                  # IA Generativa Anthropic (1 clase)
│   │       └── AnthropicPromptTransformer # Node: Claude transformations
│   └── deepseek/
│       ├── __init__.py
│       └── deepseek.py                # IA Generativa DeepSeek (1 clase)
│           └── DeepSeekPromptTransformer # Node: DeepSeek transformations