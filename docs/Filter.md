# Filter - Usage Guide

Node that filters rows from a DataFrame based on a field, a condition, and a value.

---

## Features

- Supports comparison, membership, and range conditions
- Validates condition type and value format at construction time
- Validates that the field exists in the DataFrame at runtime

---

## Connectivity

```
1 input → 1 output
```

---

## Basic Usage

### Example 1: Equality filter

```python
from open_stage.core.common import CSVOrigin, Filter, Printer
from open_stage.core.base import Pipe

origin = CSVOrigin("orders", filepath_or_buffer="orders.csv")
filtro = Filter("completed", field="status", condition="=", value_or_values="completed")
printer = Printer("output")

origin.add_output_pipe(Pipe("p1")).set_destination(filtro)
filtro.add_output_pipe(Pipe("p2")).set_destination(printer)
origin.pump()
```

---

### Example 2: Numeric comparison

```python
# Keep only orders above 500
filtro = Filter("high_value", field="amount", condition=">", value_or_values=500)
```

---

### Example 3: Membership filter (`in`)

```python
# Keep only rows from specific regions
filtro = Filter(
    "selected_regions",
    field="region",
    condition="in",
    value_or_values=["North", "South", "East"]
)
```

---

### Example 4: Exclusion filter (`not in`)

```python
# Exclude cancelled and refunded orders
filtro = Filter(
    "exclude_bad",
    field="status",
    condition="not in",
    value_or_values=["cancelled", "refunded"]
)
```

---

### Example 5: Range filter (`between`)

```python
# Keep orders from Q1 2024 (inclusive on both ends)
filtro = Filter(
    "q1",
    field="amount",
    condition="between",
    value_or_values=[100, 500]
)
```

---

### Example 6: Chain multiple filters

```python
from open_stage.core.common import CSVOrigin, Filter, CSVDestination
from open_stage.core.base import Pipe

origin = CSVOrigin("sales", filepath_or_buffer="sales.csv")
f1 = Filter("active", field="status", condition="=", value_or_values="active")
f2 = Filter("high_value", field="amount", condition=">=", value_or_values=1000)
f3 = Filter("allowed_regions", field="region", condition="in", value_or_values=["US", "EU"])
dest = CSVDestination("out", path_or_buf="filtered.csv", index=False)

origin.add_output_pipe(Pipe("p1")).set_destination(f1)
f1.add_output_pipe(Pipe("p2")).set_destination(f2)
f2.add_output_pipe(Pipe("p3")).set_destination(f3)
f3.add_output_pipe(Pipe("p4")).set_destination(dest)
origin.pump()
```

---

### Example 7: Split data with Switcher instead of multiple filters

If you need to route different values to different destinations, prefer `Switcher` over chaining multiple `Filter` nodes — it avoids scanning the DataFrame multiple times.

---

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | str | Yes | Component name |
| `field` | str | Yes | Column to filter on |
| `condition` | str | Yes | Condition to apply (see table below) |
| `value_or_values` | any | Yes | Value or list of values depending on the condition |

---

## Supported conditions

| `condition` | `value_or_values` type | Behavior |
|-------------|----------------------|----------|
| `=` | scalar | Rows where field equals value |
| `!=` | scalar | Rows where field does not equal value |
| `>` | scalar | Rows where field is greater than value |
| `>=` | scalar | Rows where field is greater than or equal to value |
| `<` | scalar | Rows where field is less than value |
| `<=` | scalar | Rows where field is less than or equal to value |
| `in` | non-empty list | Rows where field value is in the list |
| `not in` | non-empty list | Rows where field value is not in the list |
| `between` | list of exactly 2 values | Rows where field is between `[lower, upper]` — inclusive on both ends |

---

## Considerations

- **`between` is inclusive**: `[100, 500]` keeps rows where `100 <= field <= 500`
- **`in` / `not in`** require a non-empty list — passing a scalar raises `ValueError`
- **`between`** requires exactly 2 values — more or fewer raises `ValueError`
- If the result is an empty DataFrame, it is passed downstream as-is — no error is raised
- For complex conditions (multiple fields, string contains, regex), use `Transformer` with a custom function instead

---

## See Also

- [Switcher](./Switcher.md) - For routing rows to different outputs based on field value
- [Transformer](./Transformer.md) - For complex filtering logic that goes beyond single-field conditions
- [Open-Stage Documentation](../README.md) - Complete documentation
