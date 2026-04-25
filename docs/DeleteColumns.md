# DeleteColumns - Usage Guide

Node that removes a list of columns from a DataFrame.

---

## Connectivity

```
1 input → 1 output
```

---

## Basic Usage

### Example 1: Remove a single column

```python
from open_stage.core.common import CSVOrigin, DeleteColumns, Printer
from open_stage.core.base import Pipe

origin = CSVOrigin("data", filepath_or_buffer="data.csv")
drop = DeleteColumns("clean", columns=["internal_id"])
printer = Printer("output")

origin.add_output_pipe(Pipe("p1")).set_destination(drop)
drop.add_output_pipe(Pipe("p2")).set_destination(printer)
origin.pump()
```

---

### Example 2: Remove multiple columns

```python
drop = DeleteColumns(
    "remove_sensitive",
    columns=["password_hash", "salt", "recovery_token"]
)
```

---

### Example 3: Clean up temporary columns before writing to destination

```python
from open_stage.core.common import CSVOrigin, Transformer, DeleteColumns, CSVDestination
from open_stage.core.base import Pipe

def add_totals(df):
    df["_unit_total"] = df["price"] * df["quantity"]
    df["_tax"] = df["_unit_total"] * 0.21
    df["grand_total"] = df["_unit_total"] + df["_tax"]
    return df

origin = CSVOrigin("orders", filepath_or_buffer="orders.csv")
transformer = Transformer("calc", transformer_function=add_totals)
drop = DeleteColumns("cleanup", columns=["_unit_total", "_tax"])
dest = CSVDestination("out", path_or_buf="output.csv", index=False)

origin.add_output_pipe(Pipe("p1")).set_destination(transformer)
transformer.add_output_pipe(Pipe("p2")).set_destination(drop)
drop.add_output_pipe(Pipe("p3")).set_destination(dest)
origin.pump()
```

---

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | str | Yes | Component name |
| `columns` | list | Yes | List of column names to remove |

---

## Considerations

- `columns` must be a non-empty list — passing an empty list or a non-list raises `ValueError` at construction time
- All columns in the list must exist in the DataFrame — if any are missing, a `ValueError` is raised at runtime listing which ones were not found
- Column order in the list does not matter

---

## See Also

- [Transformer](./Transformer.md) - For renaming columns or more complex column manipulation
- [Open-Stage Documentation](../README.md) - Complete documentation
