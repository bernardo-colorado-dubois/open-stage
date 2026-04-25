# OpenOrigin - Usage Guide

Component for injecting an existing pandas DataFrame into a pipeline.

---

## Features

- Accepts any pandas DataFrame directly as data source
- Validates that the DataFrame is not None, not empty, and is actually a DataFrame
- Ideal for testing, in-memory processing, and bridging external data into a pipeline

---

## Installation

```bash
pip install pandas
```

---

## Basic Usage

### Example 1: Simple in-memory data

```python
import pandas as pd
from open_stage.core.common import OpenOrigin, Printer
from open_stage.core.base import Pipe

df = pd.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "amount": [100.0, 200.0, 300.0]
})

origin = OpenOrigin("data", df=df)
printer = Printer("output")

origin.add_output_pipe(Pipe("p1")).set_destination(printer)
origin.pump()
```

---

### Example 2: Bridge data from an external source

```python
import pandas as pd
from open_stage.core.common import OpenOrigin, Filter, CSVDestination
from open_stage.core.base import Pipe

# Data obtained outside of open-stage (e.g. from a custom API client)
df = fetch_from_custom_api()

origin = OpenOrigin("api_data", df=df)
filtro = Filter("active_only", filter_function=lambda df: df[df["active"] == True])
dest = CSVDestination("output", path_or_buf="active.csv", index=False)

origin.add_output_pipe(Pipe("p1")).set_destination(filtro)
filtro.add_output_pipe(Pipe("p2")).set_destination(dest)
origin.pump()
```

---

### Example 3: Testing a pipeline with fixed data

```python
import pandas as pd
from open_stage.core.common import OpenOrigin, Transformer, Printer
from open_stage.core.base import Pipe

# Fixed DataFrame to test a transformation without touching real files
df = pd.DataFrame({
    "price": [10.0, 20.0, 30.0],
    "quantity": [2, 5, 1]
})

def add_total(df):
    df["total"] = df["price"] * df["quantity"]
    return df

origin = OpenOrigin("test_data", df=df)
transformer = Transformer("calc_total", transformer_function=add_total)
printer = Printer("output")

origin.add_output_pipe(Pipe("p1")).set_destination(transformer)
transformer.add_output_pipe(Pipe("p2")).set_destination(printer)
origin.pump()
```

---

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | str | Yes | Component name |
| `df` | pd.DataFrame | Yes | DataFrame to inject into the pipeline |

---

## Validation

`OpenOrigin` validates the DataFrame at construction time and raises `ValueError` if:

- `df` is `None`
- `df` is not a pandas DataFrame
- `df` is empty (`df.empty == True`)

---

## Best Practices

1. Use `OpenOrigin` for unit tests — avoids dependencies on files or databases
2. Use it to bridge data from external libraries or custom clients into a pipeline
3. If your DataFrame comes from `pd.read_csv()` or a database query, prefer `CSVOrigin` or the corresponding DB origin instead
4. The DataFrame is not copied — mutations inside the pipeline will affect the original object if components modify in place

---

## See Also

- [CSVOrigin](./CSVOrigin.md) - For reading from CSV files
- [Open-Stage Documentation](../README.md) - Complete documentation
