# Copy - Usage Guide

Node that receives one DataFrame and sends an independent copy to each output pipe.

---

## Features

- Fans out a single input to any number of outputs
- Each downstream component receives its own `.copy()` — mutations in one branch don't affect others
- No configuration required beyond the component name

---

## Connectivity

```
1 input → N outputs
```

---

## Basic Usage

### Example 1: Send data to two destinations simultaneously

```python
from open_stage.core.common import CSVOrigin, Copy, CSVDestination, Printer
from open_stage.core.base import Pipe

origin = CSVOrigin("sales", filepath_or_buffer="sales.csv")
copy = Copy("fan_out")
dest_csv = CSVDestination("to_file", path_or_buf="output.csv", index=False)
printer = Printer("debug")

origin.add_output_pipe(Pipe("p1")).set_destination(copy)
copy.add_output_pipe(Pipe("to_csv")).set_destination(dest_csv)
copy.add_output_pipe(Pipe("to_printer")).set_destination(printer)
origin.pump()
```

---

### Example 2: Apply different transformations to the same data

```python
from open_stage.core.common import CSVOrigin, Copy, Filter, CSVDestination
from open_stage.core.base import Pipe

origin = CSVOrigin("orders", filepath_or_buffer="orders.csv")
copy = Copy("split")

active = Filter("active", filter_function=lambda df: df[df["status"] == "active"])
cancelled = Filter("cancelled", filter_function=lambda df: df[df["status"] == "cancelled"])

dest_active = CSVDestination("out_active", path_or_buf="active.csv", index=False)
dest_cancelled = CSVDestination("out_cancelled", path_or_buf="cancelled.csv", index=False)

origin.add_output_pipe(Pipe("p1")).set_destination(copy)
copy.add_output_pipe(Pipe("to_active")).set_destination(active)
copy.add_output_pipe(Pipe("to_cancelled")).set_destination(cancelled)
active.add_output_pipe(Pipe("p_active")).set_destination(dest_active)
cancelled.add_output_pipe(Pipe("p_cancelled")).set_destination(dest_cancelled)
origin.pump()
```

---

### Example 3: Debug mid-pipeline without breaking the flow

```python
from open_stage.core.common import CSVOrigin, Transformer, Copy, Printer, CSVDestination
from open_stage.core.base import Pipe

def enrich(df):
    df["total"] = df["price"] * df["quantity"]
    return df

origin = CSVOrigin("products", filepath_or_buffer="products.csv")
transformer = Transformer("enrich", transformer_function=enrich)
copy = Copy("inspect")
printer = Printer("debug")
dest = CSVDestination("out", path_or_buf="enriched.csv", index=False)

origin.add_output_pipe(Pipe("p1")).set_destination(transformer)
transformer.add_output_pipe(Pipe("p2")).set_destination(copy)
copy.add_output_pipe(Pipe("to_printer")).set_destination(printer)
copy.add_output_pipe(Pipe("to_file")).set_destination(dest)
origin.pump()
```

---

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | str | Yes | Component name |

---

## Considerations

- Each output receives `df.copy()` — deep enough to protect column mutations, but not nested objects inside cells
- Output pipes are iterated in insertion order
- If no output pipes are configured, the component logs a warning and does nothing

---

## See Also

- [Switcher](./Switcher.md) - For routing rows to different outputs based on a field value
- [Funnel](./Funnel.md) - For merging multiple inputs into one output
- [Printer](./Printer.md) - Useful as a debug branch alongside Copy
- [Open-Stage Documentation](../README.md) - Complete documentation
