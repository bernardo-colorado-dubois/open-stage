# CSVOrigin - Usage Guide

Component for reading CSV files and passing them into the pipeline.

---

## Features

- Reads any CSV file supported by pandas
- Forwards all kwargs directly to `pd.read_csv()` — full pandas flexibility
- Supports local paths, URLs, and file-like objects
- Row limiting for fast development testing

---

## Installation

```bash
pip install pandas
```

---

## Basic Usage

### Example 1: Simple file read

```python
from open_stage.core.common import CSVOrigin, Printer
from open_stage.core.base import Pipe

origin = CSVOrigin("sales", filepath_or_buffer="sales.csv")
printer = Printer("output")

origin.add_output_pipe(Pipe("p1")).set_destination(printer)
origin.pump()
```

---

### Example 2: Custom separator

```python
# Semicolon-separated file (common in European exports)
origin = CSVOrigin("data", filepath_or_buffer="data.csv", sep=";")
```

---

### Example 3: Limit rows for testing

```python
# Read only the first 100 rows
origin = CSVOrigin("sample", filepath_or_buffer="large_file.csv", nrows=100)
```

---

### Example 4: Read from URL

```python
origin = CSVOrigin(
    "remote",
    filepath_or_buffer="https://example.com/data.csv"
)
```

---

## Advanced Features

### Example 5: Select specific columns

```python
origin = CSVOrigin(
    "filtered_cols",
    filepath_or_buffer="orders.csv",
    usecols=["order_id", "customer_id", "amount", "date"]
)
```

---

### Example 6: Enforce column data types

```python
origin = CSVOrigin(
    "typed",
    filepath_or_buffer="products.csv",
    dtype={
        "product_id": str,
        "price": float,
        "stock": int
    }
)
```

---

### Example 7: Handle encoding

```python
# Windows-1252 encoding (common in Latin America exports)
origin = CSVOrigin(
    "latin",
    filepath_or_buffer="report.csv",
    encoding="latin-1"
)
```

---

### Example 8: Skip header rows and rename columns

```python
# File has 2 comment lines at top, then data with no header
origin = CSVOrigin(
    "raw",
    filepath_or_buffer="raw_export.csv",
    skiprows=2,
    header=None,
    names=["id", "name", "value", "date"]
)
```

---

### Example 9: Handle missing values and decimal format

```python
origin = CSVOrigin(
    "european",
    filepath_or_buffer="data.csv",
    sep=";",
    decimal=",",         # European decimal separator
    thousands=".",       # European thousands separator
    na_values=["N/A", "-", ""]
)
```

---

### Example 10: Parse dates

```python
origin = CSVOrigin(
    "timeseries",
    filepath_or_buffer="events.csv",
    parse_dates=["event_date", "created_at"]
)
```

---

### Example 11: Read from file-like object

```python
import io

csv_content = "id,name\n1,Alice\n2,Bob"
buffer = io.StringIO(csv_content)

origin = CSVOrigin("buffer", filepath_or_buffer=buffer)
```

---

## Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | str | Yes | — | Component name |
| `filepath_or_buffer` | str / URL / buffer | Yes | — | Path to file, URL, or file-like object |
| `sep` | str | No | `','` | Field delimiter |
| `header` | int / list / None | No | `'infer'` | Row(s) to use as column names |
| `names` | list | No | None | Custom column names (disables auto-detection) |
| `index_col` | int / str / list / False | No | None | Column(s) to set as index |
| `usecols` | list / callable | No | None | Subset of columns to read |
| `dtype` | dict / type | No | None | Data type per column or for all columns |
| `encoding` | str | No | `'utf-8'` | File encoding |
| `skiprows` | int / list | No | None | Rows to skip at the start |
| `nrows` | int | No | None | Maximum number of rows to read |
| `na_values` | str / list / dict | No | None | Additional values to treat as NA |
| `parse_dates` | bool / list | No | False | Columns to parse as dates |
| `decimal` | str | No | `'.'` | Decimal separator |
| `thousands` | str | No | None | Thousands separator |

All other `pd.read_csv()` parameters are accepted and forwarded as-is.

---

## Best Practices

1. Always pass `filepath_or_buffer` as a keyword argument (positional is not supported)
2. Use `nrows` during development to speed up iteration
3. Use `usecols` to avoid loading columns you won't use — saves memory
4. Specify `dtype` explicitly for ID columns that look like numbers (`dtype={"id": str}`)
5. Use `encoding="latin-1"` for files from Windows systems that fail with default UTF-8

---

## See Also

- [CSVDestination](./CSVDestination.md) - For writing CSV files
- [Open-Stage Documentation](../README.md) - Complete documentation
