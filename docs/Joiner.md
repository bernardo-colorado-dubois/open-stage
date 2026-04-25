# Joiner - Usage Guide

Node that waits for exactly two input pipes and joins them into a single DataFrame using a common key column.

---

## Features

- Performs `left`, `right`, or `inner` joins
- Waits for both inputs before executing — order of arrival does not matter
- Identifies left vs right DataFrame by pipe name
- Validates that the key column exists in both DataFrames

---

## Connectivity

```
2 inputs → 1 output
```

---

## Basic Usage

### Example 1: Left join — enrich orders with customer data

```python
from open_stage.core.common import CSVOrigin, Joiner, Printer
from open_stage.core.base import Pipe

orders = CSVOrigin("orders", filepath_or_buffer="orders.csv")       # order_id, customer_id, amount
customers = CSVOrigin("customers", filepath_or_buffer="customers.csv") # customer_id, name, region

joiner = Joiner(
    name="enrich",
    left="pipe_orders",
    right="pipe_customers",
    key="customer_id",
    join_type="left"
)
printer = Printer("output")

orders.add_output_pipe(Pipe("pipe_orders")).set_destination(joiner)
customers.add_output_pipe(Pipe("pipe_customers")).set_destination(joiner)
joiner.add_output_pipe(Pipe("p_out")).set_destination(printer)

orders.pump()
customers.pump()
```

---

### Example 2: Inner join — only matching rows

```python
joiner = Joiner(
    name="matched_only",
    left="pipe_a",
    right="pipe_b",
    key="product_id",
    join_type="inner"
)
```

---

### Example 3: Join aggregated result back to original data

```python
from open_stage.core.common import CSVOrigin, Copy, Aggregator, Joiner, CSVDestination
from open_stage.core.base import Pipe

origin = CSVOrigin("sales", filepath_or_buffer="sales.csv")
copy = Copy("split")
agg = Aggregator("total", key="customer_id", agg_field_name="total_spent", agg_type="sum", field_to_agg="amount")
joiner = Joiner(name="enrich", left="pipe_raw", right="pipe_agg", key="customer_id", join_type="left")
dest = CSVDestination("out", path_or_buf="enriched.csv", index=False)

origin.add_output_pipe(Pipe("p1")).set_destination(copy)
copy.add_output_pipe(Pipe("pipe_raw")).set_destination(joiner)
copy.add_output_pipe(Pipe("p_to_agg")).set_destination(agg)
agg.add_output_pipe(Pipe("pipe_agg")).set_destination(joiner)
joiner.add_output_pipe(Pipe("p_out")).set_destination(dest)

origin.pump()
```

---

### Example 4: Full pipeline — orders enriched with product and customer info

```python
from open_stage.core.common import CSVOrigin, Joiner, CSVDestination
from open_stage.core.base import Pipe

orders = CSVOrigin("orders", filepath_or_buffer="orders.csv")
products = CSVOrigin("products", filepath_or_buffer="products.csv")
customers = CSVOrigin("customers", filepath_or_buffer="customers.csv")

join_products = Joiner(name="add_product", left="p_orders", right="p_products", key="product_id", join_type="left")
join_customers = Joiner(name="add_customer", left="p_with_product", right="p_customers", key="customer_id", join_type="left")
dest = CSVDestination("out", path_or_buf="full_orders.csv", index=False)

orders.add_output_pipe(Pipe("p_orders")).set_destination(join_products)
products.add_output_pipe(Pipe("p_products")).set_destination(join_products)
join_products.add_output_pipe(Pipe("p_with_product")).set_destination(join_customers)
customers.add_output_pipe(Pipe("p_customers")).set_destination(join_customers)
join_customers.add_output_pipe(Pipe("p_out")).set_destination(dest)

orders.pump()
products.pump()
customers.pump()
```

---

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | str | Yes | Component name |
| `left` | str | Yes | Name of the pipe that carries the left DataFrame |
| `right` | str | Yes | Name of the pipe that carries the right DataFrame |
| `key` | str | Yes | Column name to join on (must exist in both DataFrames) |
| `join_type` | str | Yes | Type of join: `'left'`, `'right'`, or `'inner'` |

---

## Join types

| `join_type` | Behavior |
|-------------|----------|
| `left` | All rows from left, matching rows from right — unmatched right rows become `NaN` |
| `right` | All rows from right, matching rows from left — unmatched left rows become `NaN` |
| `inner` | Only rows with a matching key in both DataFrames |

---

## How pipe names work

`Joiner` identifies which DataFrame is left and which is right by the **pipe name**, not by the order of arrival. The values passed to `left` and `right` must exactly match the names used when creating the pipes:

```python
joiner = Joiner(name="j", left="pipe_orders", right="pipe_customers", ...)

orders.add_output_pipe(Pipe("pipe_orders")).set_destination(joiner)   # matches left
customers.add_output_pipe(Pipe("pipe_customers")).set_destination(joiner)  # matches right
```

If a pipe delivers data with a name that doesn't match either `left` or `right`, a `ValueError` is raised.

---

## Considerations

- Only `left`, `right`, and `inner` joins are supported — `outer` is not
- The key column must have the same name in both DataFrames
- If both DataFrames have columns with the same name (other than the key), pandas will suffix them with `_x` and `_y`
- `Joiner` resets its state after each join, so it can receive a new pair of DataFrames in a subsequent pipeline run

---

## See Also

- [Aggregator](./Aggregator.md) - Often used with Joiner to enrich data with aggregated metrics
- [Copy](./Copy.md) - For sending the same DataFrame to both inputs of a Joiner
- [Funnel](./Funnel.md) - For merging N inputs with the same schema via concatenation
- [Open-Stage Documentation](../README.md) - Complete documentation
