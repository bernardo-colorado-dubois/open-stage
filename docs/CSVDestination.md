# CSVDestination - Usage Guide

Destination that writes the received DataFrame to a CSV file.

---

## Features

- Forwards all kwargs directly to `df.to_csv()` — full pandas flexibility
- Supports local paths and file-like objects
- No fixed parameter list beyond `name` — anything pandas accepts works here

---

## Installation

```bash
pip install pandas
```

---

## Basic Usage

### Example 1: Write to a file

```python
from open_stage.core.common import CSVOrigin, CSVDestination
from open_stage.core.base import Pipe

origin = CSVOrigin("input", filepath_or_buffer="input.csv")
dest = CSVDestination("output", path_or_buf="output.csv", index=False)

origin.add_output_pipe(Pipe("p1")).set_destination(dest)
origin.pump()
```

---

### Example 2: Custom separator

```python
dest = CSVDestination("output", path_or_buf="data.csv", sep=";", index=False)
```

---

### Example 3: Write specific columns only

```python
dest = CSVDestination(
    "output",
    path_or_buf="report.csv",
    columns=["id", "name", "amount"],
    index=False
)
```

---

### Example 4: Control encoding and decimal separator

```python
dest = CSVDestination(
    "european",
    path_or_buf="report.csv",
    sep=";",
    decimal=",",
    encoding="latin-1",
    index=False
)
```

---

### Example 5: Append to an existing file

```python
dest = CSVDestination(
    "append",
    path_or_buf="log.csv",
    mode="a",
    header=False,   # don't write header on subsequent runs
    index=False
)
```

---

### Example 6: Write to a file-like object

```python
import io

buffer = io.StringIO()
dest = CSVDestination("buffer", path_or_buf=buffer, index=False)
```

---

## Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | str | Yes | — | Component name |
| `path_or_buf` | str / path / buffer | Yes | — | Output file path or file-like object |
| `sep` | str | No | `','` | Field delimiter |
| `index` | bool | No | `True` | Write row index — usually set to `False` |
| `columns` | list | No | None | Subset of columns to write |
| `header` | bool / list | No | `True` | Write column names as header |
| `mode` | str | No | `'w'` | Write mode: `'w'` to overwrite, `'a'` to append |
| `encoding` | str | No | `'utf-8'` | File encoding |
| `decimal` | str | No | `'.'` | Decimal separator |
| `date_format` | str | No | None | Format for datetime columns |
| `float_format` | str | No | None | Format string for floats (e.g. `'%.2f'`) |
| `na_rep` | str | No | `''` | String to represent missing values |

All other `df.to_csv()` parameters are accepted and forwarded as-is.

---

## Best Practices

1. Always set `index=False` unless the index carries meaningful data
2. Use `mode="a"` with `header=False` when appending to an existing file
3. Match `sep` and `decimal` to the expected consumer of the file (e.g. Excel in some locales expects `sep=";"` and `decimal=","`)
4. Use `float_format="%.2f"` to avoid floating point noise in numeric columns

---

## See Also

- [CSVOrigin](./CSVOrigin.md) - For reading CSV files
- [Printer](./Printer.md) - For printing output to stdout during development
- [Open-Stage Documentation](../README.md) - Complete documentation
