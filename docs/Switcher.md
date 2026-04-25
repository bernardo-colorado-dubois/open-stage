# Switcher - Usage Guide

Node that routes rows to different output pipes based on the value of a field.

---

## Features

- Splits a DataFrame into subsets by field value and sends each to a different pipe
- Mapping keys can be strings or integers
- Unmatched rows and NaN values are silently dropped by default, or raise `ValueError` if `fail_on_unmatch=True`

---

## Connectivity

```
1 input → N outputs
```

---

## Basic Usage

### Example 1: Route orders by status

```python
from open_stage.core.common import CSVOrigin, Switcher, CSVDestination
from open_stage.core.base import Pipe

origin = CSVOrigin("orders", filepath_or_buffer="orders.csv")

switcher = Switcher(
    name="by_status",
    field="status",
    mapping={
        "completed":  "pipe_completed",
        "pending":    "pipe_pending",
        "cancelled":  "pipe_cancelled"
    }
)

dest_completed = CSVDestination("out_completed", path_or_buf="completed.csv", index=False)
dest_pending   = CSVDestination("out_pending",   path_or_buf="pending.csv",   index=False)
dest_cancelled = CSVDestination("out_cancelled", path_or_buf="cancelled.csv", index=False)

origin.add_output_pipe(Pipe("p1")).set_destination(switcher)
switcher.add_output_pipe(Pipe("pipe_completed")).set_destination(dest_completed)
switcher.add_output_pipe(Pipe("pipe_pending")).set_destination(dest_pending)
switcher.add_output_pipe(Pipe("pipe_cancelled")).set_destination(dest_cancelled)

origin.pump()
```

---

### Example 2: Route by integer code

```python
switcher = Switcher(
    name="by_priority",
    field="priority",
    mapping={
        1: "pipe_high",
        2: "pipe_medium",
        3: "pipe_low"
    }
)
```

---

### Example 3: Fail on unmatched values

```python
# Raises ValueError if any row has a value not present in mapping,
# or if the field contains NaN
switcher = Switcher(
    name="strict_routing",
    field="region",
    mapping={"US": "pipe_us", "EU": "pipe_eu"},
    fail_on_unmatch=True
)
```

---

### Example 4: Route a subset, ignore the rest

```python
# Rows with status other than "vip" are silently dropped
switcher = Switcher(
    name="vip_only",
    field="tier",
    mapping={"vip": "pipe_vip"}
)
```

---

### Example 5: Switcher followed by independent pipelines

```python
from open_stage.core.common import CSVOrigin, Switcher, Transformer, CSVDestination
from open_stage.core.base import Pipe

def apply_discount(df):
    df["final_price"] = df["price"] * 0.9
    return df

def apply_surcharge(df):
    df["final_price"] = df["price"] * 1.15
    return df

origin = CSVOrigin("products", filepath_or_buffer="products.csv")
switcher = Switcher(
    name="by_category",
    field="category",
    mapping={"sale": "pipe_sale", "premium": "pipe_premium"}
)
t_sale    = Transformer("discount",  transformer_function=apply_discount)
t_premium = Transformer("surcharge", transformer_function=apply_surcharge)
dest_sale    = CSVDestination("out_sale",    path_or_buf="sale.csv",    index=False)
dest_premium = CSVDestination("out_premium", path_or_buf="premium.csv", index=False)

origin.add_output_pipe(Pipe("p1")).set_destination(switcher)
switcher.add_output_pipe(Pipe("pipe_sale")).set_destination(t_sale)
switcher.add_output_pipe(Pipe("pipe_premium")).set_destination(t_premium)
t_sale.add_output_pipe(Pipe("p_sale")).set_destination(dest_sale)
t_premium.add_output_pipe(Pipe("p_premium")).set_destination(dest_premium)

origin.pump()
```

---

## Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | str | Yes | — | Component name |
| `field` | str | Yes | — | Column whose value determines the routing |
| `mapping` | dict | Yes | — | Maps field values to output pipe names |
| `fail_on_unmatch` | bool | No | `False` | Raise `ValueError` on unmatched values or NaN |

---

## How pipe names work

The values in `mapping` must exactly match the pipe names used in `add_output_pipe()`:

```python
switcher = Switcher(name="s", field="status", mapping={"active": "pipe_active"})
switcher.add_output_pipe(Pipe("pipe_active")).set_destination(dest)  # name must match
```

If `mapping` references a pipe name that was never added as an output, that route is skipped and a warning is logged.

---

## Unmatched rows and NaN

| Situation | `fail_on_unmatch=False` | `fail_on_unmatch=True` |
|-----------|------------------------|------------------------|
| Value not in mapping | Warning, rows dropped | `ValueError` |
| NaN in field | Warning, rows dropped | `ValueError` |

---

## Considerations

- Mapping keys must be `str` or `int` — other types raise `ValueError` at construction time
- Each unique value in the field is processed independently — one subset per value
- Prefer `Switcher` over chaining multiple `Filter` nodes when routing by the same field, as it scans the DataFrame only once
- For complex routing logic (multiple fields, computed conditions), use `Copy` + `Filter` instead

---

## See Also

- [Filter](./Filter.md) - For keeping/dropping rows based on a condition on a single pipeline branch
- [Copy](./Copy.md) - For sending the full DataFrame to multiple branches unchanged
- [Open-Stage Documentation](../README.md) - Complete documentation
