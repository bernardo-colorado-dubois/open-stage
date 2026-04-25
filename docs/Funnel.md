# Funnel - Usage Guide

Node that waits for all its input pipes to deliver data, then merges them into a single DataFrame and sends it downstream.

---

## Features

- Accepts any number of input pipes
- Waits until **all** inputs have arrived before merging
- Merges with `pd.concat(..., ignore_index=True)`
- Logs a warning if incoming DataFrames have different columns

---

## Connectivity

```
N inputs → 1 output
```

---

## Basic Usage

### Example 1: Merge two CSV files with the same schema

```python
from open_stage.core.common import CSVOrigin, Funnel, Printer
from open_stage.core.base import Pipe

origin_a = CSVOrigin("sales_jan", filepath_or_buffer="sales_jan.csv")
origin_b = CSVOrigin("sales_feb", filepath_or_buffer="sales_feb.csv")
funnel = Funnel("merge")
printer = Printer("output")

pipe_a = Pipe("p_jan")
pipe_b = Pipe("p_feb")
pipe_out = Pipe("p_out")

origin_a.add_output_pipe(pipe_a).set_destination(funnel)
origin_b.add_output_pipe(pipe_b).set_destination(funnel)
funnel.add_output_pipe(pipe_out).set_destination(printer)

origin_a.pump()
origin_b.pump()
```

---

### Example 2: Merge three database sources

```python
from open_stage.core.common import Funnel, CSVDestination
from open_stage.postgres.common import PostgresOrigin
from open_stage.core.base import Pipe

origin_us = PostgresOrigin("us", host="db-us", database="sales", user="u", password="p", table="orders")
origin_eu = PostgresOrigin("eu", host="db-eu", database="sales", user="u", password="p", table="orders")
origin_ap = PostgresOrigin("ap", host="db-ap", database="sales", user="u", password="p", table="orders")

funnel = Funnel("all_regions")
dest = CSVDestination("global", path_or_buf="global_orders.csv", index=False)

pipe_us = Pipe("p_us")
pipe_eu = Pipe("p_eu")
pipe_ap = Pipe("p_ap")

origin_us.add_output_pipe(pipe_us).set_destination(funnel)
origin_eu.add_output_pipe(pipe_eu).set_destination(funnel)
origin_ap.add_output_pipe(pipe_ap).set_destination(funnel)
funnel.add_output_pipe(Pipe("p_out")).set_destination(dest)

origin_us.pump()
origin_eu.pump()
origin_ap.pump()
```

---

### Example 3: Merge after independent transformations

```python
from open_stage.core.common import CSVOrigin, Transformer, Funnel, CSVDestination
from open_stage.core.base import Pipe

def tag_source(source_name):
    def fn(df):
        df["source"] = source_name
        return df
    return fn

origin_a = CSVOrigin("a", filepath_or_buffer="region_a.csv")
origin_b = CSVOrigin("b", filepath_or_buffer="region_b.csv")

tag_a = Transformer("tag_a", transformer_function=tag_source("region_a"))
tag_b = Transformer("tag_b", transformer_function=tag_source("region_b"))

funnel = Funnel("merge")
dest = CSVDestination("out", path_or_buf="combined.csv", index=False)

origin_a.add_output_pipe(Pipe("p1")).set_destination(tag_a)
origin_b.add_output_pipe(Pipe("p2")).set_destination(tag_b)
tag_a.add_output_pipe(Pipe("p3")).set_destination(funnel)
tag_b.add_output_pipe(Pipe("p4")).set_destination(funnel)
funnel.add_output_pipe(Pipe("p5")).set_destination(dest)

origin_a.pump()
origin_b.pump()
```

---

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | str | Yes | Component name |

---

## How it works

`Funnel` counts how many input pipes have been added via `add_output_pipe(...).set_destination(funnel)`. Each time a pipe delivers data, it increments a counter. When the counter matches the expected number of inputs, it concatenates all received DataFrames with `pd.concat(..., ignore_index=True)` and pumps the result downstream.

---

## Considerations

- **Column mismatch**: if incoming DataFrames have different columns, `Funnel` logs a warning and concatenates anyway — missing columns become `NaN`. Make sure all sources share the same schema, or transform them before the funnel.
- **Ordering**: the merge order depends on the order in which `origin.pump()` is called.
- **Each pipeline run resets state**: received DataFrames accumulate per run. Do not reuse a `Funnel` instance across multiple pipeline executions.

---

## See Also

- [Copy](./Copy.md) - Inverse operation: one input to many outputs
- [Switcher](./Switcher.md) - Route rows to different outputs based on a field value
- [Open-Stage Documentation](../README.md) - Complete documentation
