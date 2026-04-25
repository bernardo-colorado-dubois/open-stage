# RemoveDuplicates - Usage Guide

Node that removes duplicate rows based on a key column, keeping the row determined by sorting a secondary column.

---

## Connectivity

```
1 input → 1 output
```

---

## How it works

1. Sorts the DataFrame by `sort_by` in the direction given by `orientation`
2. Drops duplicates on `key`, keeping the row indicated by `retain`

The combination of `orientation` + `retain` determines which duplicate survives:

| `orientation` | `retain` | Row kept |
|---------------|----------|----------|
| `'DESC'` | `'FIRST'` | Row with the **highest** `sort_by` value |
| `'ASC'` | `'LAST'` | Row with the **highest** `sort_by` value |
| `'ASC'` | `'FIRST'` | Row with the **lowest** `sort_by` value |
| `'DESC'` | `'LAST'` | Row with the **lowest** `sort_by` value |

The most common pattern is `DESC` + `FIRST` — sort newest first, keep first occurrence.

---

## Basic Usage

### Example 1: Keep the most recent record per customer

```python
from open_stage.core.common import CSVOrigin, RemoveDuplicates, CSVDestination
from open_stage.core.base import Pipe

# Input: customer_id, order_date, amount  (may have multiple rows per customer)
origin = CSVOrigin("orders", filepath_or_buffer="orders.csv")

dedup = RemoveDuplicates(
    name="latest_per_customer",
    key="customer_id",
    sort_by="order_date",
    orientation="DESC",   # newest first
    retain="FIRST"        # keep first after sorting = most recent
)

dest = CSVDestination("out", path_or_buf="latest_orders.csv", index=False)

origin.add_output_pipe(Pipe("p1")).set_destination(dedup)
dedup.add_output_pipe(Pipe("p2")).set_destination(dest)
origin.pump()
```

---

### Example 2: Keep the oldest record per user (first registration)

```python
dedup = RemoveDuplicates(
    name="first_registration",
    key="user_id",
    sort_by="created_at",
    orientation="ASC",    # oldest first
    retain="FIRST"        # keep first after sorting = oldest
)
```

---

### Example 3: Keep the highest-value transaction per account

```python
dedup = RemoveDuplicates(
    name="max_transaction",
    key="account_id",
    sort_by="amount",
    orientation="DESC",   # highest first
    retain="FIRST"        # keep first after sorting = highest amount
)
```

---

### Example 4: Chain after a Funnel to deduplicate merged data

```python
from open_stage.core.common import CSVOrigin, Funnel, RemoveDuplicates, CSVDestination
from open_stage.core.base import Pipe

origin_a = CSVOrigin("a", filepath_or_buffer="data_jan.csv")
origin_b = CSVOrigin("b", filepath_or_buffer="data_feb.csv")
funnel = Funnel("merge")
dedup = RemoveDuplicates(
    name="dedup",
    key="transaction_id",
    sort_by="updated_at",
    orientation="DESC",
    retain="FIRST"
)
dest = CSVDestination("out", path_or_buf="clean.csv", index=False)

origin_a.add_output_pipe(Pipe("p_a")).set_destination(funnel)
origin_b.add_output_pipe(Pipe("p_b")).set_destination(funnel)
funnel.add_output_pipe(Pipe("p_merged")).set_destination(dedup)
dedup.add_output_pipe(Pipe("p_out")).set_destination(dest)

origin_a.pump()
origin_b.pump()
```

---

## Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | str | Yes | Component name |
| `key` | str | Yes | Column used to identify duplicates |
| `sort_by` | str | Yes | Column used to decide which duplicate to keep |
| `orientation` | str | Yes | Sort direction: `'ASC'` or `'DESC'` |
| `retain` | str | Yes | Which row to keep after sorting: `'FIRST'` or `'LAST'` |

---

## Considerations

- Both `key` and `sort_by` must exist in the DataFrame — missing columns raise `ValueError` at runtime
- `orientation` and `retain` only accept their exact uppercase values — anything else raises `ValueError` at construction time
- Rows not involved in any duplicate are always kept
- The output preserves the sorted order from the deduplication step — add a `Transformer` downstream if you need a different order

---

## See Also

- [Funnel](./Funnel.md) - Often used before RemoveDuplicates when merging sources that may overlap
- [Filter](./Filter.md) - For removing rows based on a condition rather than deduplication
- [Open-Stage Documentation](../README.md) - Complete documentation
