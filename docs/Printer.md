# Printer - Usage Guide

Destination that prints the received DataFrame to stdout. Intended for debugging and development.

---

## Features

- Prints the pipe name and the full DataFrame to stdout
- No configuration required beyond the component name
- Useful for inspecting data at any point in a pipeline

---

## Basic Usage

### Example 1: Print pipeline output

```python
from open_stage.core.common import CSVOrigin, Printer
from open_stage.core.base import Pipe

origin = CSVOrigin("sales", filepath_or_buffer="sales.csv")
printer = Printer("output")

origin.add_output_pipe(Pipe("p1")).set_destination(printer)
origin.pump()
```

Output:
```
Data received from pipe: p1
   id     name  amount
0   1    Alice   100.0
1   2      Bob   200.0
2   3  Charlie   300.0
```

---

### Example 2: Inspect intermediate data

Use `Copy` to fan out and print mid-pipeline without interrupting the flow.

```python
from open_stage.core.common import CSVOrigin, Filter, Copy, Printer, CSVDestination
from open_stage.core.base import Pipe

origin = CSVOrigin("sales", filepath_or_buffer="sales.csv")
filtro = Filter("active", filter_function=lambda df: df[df["active"] == True])
copy = Copy("inspect")
printer = Printer("debug")
dest = CSVDestination("out", path_or_buf="output.csv", index=False)

origin.add_output_pipe(Pipe("p1")).set_destination(filtro)
filtro.add_output_pipe(Pipe("p2")).set_destination(copy)
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

- Output uses pandas' default `__repr__`, which truncates wide or long DataFrames
- Not intended for production pipelines — use `CSVDestination` or a DB destination instead
- `Printer` is the only component in the framework that intentionally uses `print()`

---

## See Also

- [CSVDestination](./CSVDestination.md) - For writing output to a file
- [Copy](./Copy.md) - For branching a pipeline without consuming the data
- [Open-Stage Documentation](../README.md) - Complete documentation
