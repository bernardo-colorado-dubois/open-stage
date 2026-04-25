# Aggregator - Usage Guide

Node that groups a DataFrame by a key column and applies an aggregation function, producing a two-column result.

---

## Features

- Groups by any column and applies a standard pandas aggregation
- Supports: `sum`, `count`, `mean`, `median`, `min`, `max`, `std`, `var`, `nunique`, `first`, `last`, `size`, `sem`, `quantile`
- `count` does not require a `field_to_agg` — counts rows per group
- Output always has exactly two columns: the key and the aggregated value

---

## Connectivity

```
1 input → 1 output
```

---

## Basic Usage

### Example 1: Sum sales by region

```python
from open_stage.core.common import CSVOrigin, Aggregator, Printer
from open_stage.core.base import Pipe

# Input: region, product, amount
origin = CSVOrigin("sales", filepath_or_buffer="sales.csv")
agg = Aggregator(
    name="sum_by_region",
    key="region",
    agg_field_name="total_amount",
    agg_type="sum",
    field_to_agg="amount"
)
printer = Printer("output")

origin.add_output_pipe(Pipe("p1")).set_destination(agg)
agg.add_output_pipe(Pipe("p2")).set_destination(printer)
origin.pump()

# Output:
#    region  total_amount
# 0    East        5400.0
# 1   North        3200.0
# 2   South        1800.0
# 3    West        4100.0
```

---

### Example 2: Count orders per customer

```python
# count does not need field_to_agg
agg = Aggregator(
    name="orders_per_customer",
    key="customer_id",
    agg_field_name="order_count",
    agg_type="count"
)
```

---

### Example 3: Average ticket per product category

```python
agg = Aggregator(
    name="avg_ticket",
    key="category",
    agg_field_name="avg_amount",
    agg_type="mean",
    field_to_agg="amount"
)
```

---

### Example 4: Most recent date per user

```python
agg = Aggregator(
    name="last_activity",
    key="user_id",
    agg_field_name="last_login",
    agg_type="max",
    field_to_agg="login_date"
)
```

---

### Example 5: Count distinct values per group

```python
agg = Aggregator(
    name="unique_products",
    key="category",
    agg_field_name="distinct_products",
    agg_type="nunique",
    field_to_agg="product_id"
)
```

---

### Example 6: Chain with other nodes

```python
from open_stage.core.common import CSVOrigin, Filter, Aggregator, CSVDestination
from open_stage.core.base import Pipe

origin = CSVOrigin("sales", filepath_or_buffer="sales.csv")
filtro = Filter("completed", filter_function=lambda df: df[df["status"] == "completed"])
agg = Aggregator("total", key="region", agg_field_name="revenue", agg_type="sum", field_to_agg="amount")
dest = CSVDestination("out", path_or_buf="revenue_by_region.csv", index=False)

origin.add_output_pipe(Pipe("p1")).set_destination(filtro)
filtro.add_output_pipe(Pipe("p2")).set_destination(agg)
agg.add_output_pipe(Pipe("p3")).set_destination(dest)
origin.pump()
```

---

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | str | Yes | Component name |
| `key` | str | Yes | Column to group by |
| `agg_field_name` | str | Yes | Name of the output column with aggregated values |
| `agg_type` | str | Yes | Aggregation function to apply |
| `field_to_agg` | str | No* | Column to aggregate — required for all types except `count` |

\* Required for every `agg_type` other than `count`.

---

## Supported aggregation types

| `agg_type` | Description | `field_to_agg` required |
|------------|-------------|------------------------|
| `sum` | Sum of values | Yes |
| `count` | Number of rows per group | No |
| `mean` | Average | Yes |
| `median` | Median | Yes |
| `min` | Minimum value | Yes |
| `max` | Maximum value | Yes |
| `std` | Standard deviation | Yes |
| `var` | Variance | Yes |
| `nunique` | Count of distinct values | Yes |
| `first` | First value in group | Yes |
| `last` | Last value in group | Yes |
| `size` | Group size (same as count) | No |
| `sem` | Standard error of the mean | Yes |
| `quantile` | 50th percentile by default | Yes |

---

## Output shape

The result DataFrame always has exactly two columns regardless of the input shape:

```
key_column | agg_field_name
```

If you need to keep other columns, join the result back to the original DataFrame using `Joiner`.

---

## Considerations

- **Unknown agg_type**: if `agg_type` is not in the supported list, a warning is logged but execution continues — pandas may still accept it if it's a valid aggregation string
- **Column validation**: `key` and `field_to_agg` are validated against the actual DataFrame at runtime, raising `ValueError` if not found
- For `count`, pandas' `groupby.size()` is used, which counts all rows including those with `NaN` values

---

## See Also

- [Joiner](./Joiner.md) - For joining the aggregated result back to the original data
- [Filter](./Filter.md) - For filtering rows before aggregating
- [Open-Stage Documentation](../README.md) - Complete documentation
