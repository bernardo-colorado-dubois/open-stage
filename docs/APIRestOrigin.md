# APIRestOrigin - Usage Guide

Origin that fetches data from a REST API endpoint and converts the JSON response into a DataFrame.

---

## Features

- Forwards all kwargs directly to `requests.request()` — full requests library flexibility
- Navigates nested JSON responses using dot-notation paths
- Optionally selects a subset of fields from the response
- Raises on HTTP errors (`4xx`, `5xx`) via `response.raise_for_status()`

---

## Installation

```bash
pip install requests pandas
```

---

## Basic Usage

### Example 1: GET request, root-level list response

```python
from open_stage.core.common import APIRestOrigin, Printer
from open_stage.core.base import Pipe

# Response: [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
origin = APIRestOrigin(
    "users",
    method="GET",
    url="https://api.example.com/users"
)
printer = Printer("output")

origin.add_output_pipe(Pipe("p1")).set_destination(printer)
origin.pump()
```

---

### Example 2: Response nested inside a key

```python
# Response: {"status": "ok", "data": [{"id": 1, ...}, {"id": 2, ...}]}
origin = APIRestOrigin(
    "users",
    path="data",          # navigate to response["data"]
    method="GET",
    url="https://api.example.com/users"
)
```

---

### Example 3: Deeply nested path

```python
# Response: {"result": {"items": [{"id": 1, ...}, ...]}}
origin = APIRestOrigin(
    "items",
    path="result.items",  # navigate to response["result"]["items"]
    method="GET",
    url="https://api.example.com/items"
)
```

---

### Example 4: Select specific fields

```python
origin = APIRestOrigin(
    "users",
    path="data",
    fields=["id", "name", "email"],   # discard other fields
    method="GET",
    url="https://api.example.com/users"
)
```

---

### Example 5: POST request with JSON body

```python
origin = APIRestOrigin(
    "report",
    path="data.rows",
    method="POST",
    url="https://api.example.com/reports",
    json={"date_from": "2024-01-01", "date_to": "2024-12-31", "format": "json"},
    headers={"Content-Type": "application/json"}
)
```

---

### Example 6: Authentication with headers

```python
origin = APIRestOrigin(
    "secure_data",
    path="records",
    method="GET",
    url="https://api.example.com/records",
    headers={"Authorization": "Bearer MY_TOKEN"}
)
```

---

### Example 7: Query parameters and timeout

```python
origin = APIRestOrigin(
    "filtered",
    path="data",
    method="GET",
    url="https://api.example.com/orders",
    params={"status": "completed", "limit": 1000},
    timeout=30
)
```

---

### Example 8: Basic auth

```python
origin = APIRestOrigin(
    "protected",
    method="GET",
    url="https://api.example.com/data",
    auth=("username", "password")
)
```

---

## Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | str | Yes | — | Component name |
| `path` | str | No | `'.'` | Dot-separated path to the data inside the JSON response |
| `fields` | list | No | None | Subset of fields to keep — must be non-empty if provided |
| `method` | str | Yes* | — | HTTP method: `'GET'`, `'POST'`, etc. |
| `url` | str | Yes* | — | Request URL |
| `headers` | dict | No | None | HTTP headers |
| `params` | dict | No | None | URL query parameters |
| `json` | dict | No | None | JSON request body |
| `data` | dict / str | No | None | Form-encoded or raw request body |
| `auth` | tuple | No | None | Basic auth as `(username, password)` |
| `timeout` | float | No | None | Request timeout in seconds |

\* `method` and `url` are required by `requests.request()`. All other `requests` parameters are accepted and forwarded as-is.

---

## How `path` works

`path` is a dot-separated string that navigates into the parsed JSON:

| Response shape | `path` | Data used |
|---------------|--------|-----------|
| `[{...}, {...}]` | `'.'` | Root list |
| `{"data": [{...}]}` | `"data"` | `response["data"]` |
| `{"result": {"items": [{...}]}}` | `"result.items"` | `response["result"]["items"]` |

The data at the resolved path must be a **list of dicts** (→ multiple rows) or a **single dict** (→ one row). Any other type raises `ValueError`.

---

## Considerations

- HTTP errors (`4xx`, `5xx`) raise immediately via `raise_for_status()` — the pipeline does not continue
- If `fields` contains a name not present in the response, a `ValueError` is raised listing available fields
- For paginated APIs, call `origin.pump()` multiple times with different `params` and funnel the results with `Funnel`
- For APIs that require session handling or OAuth flows, build the token outside the pipeline and pass it via `headers`

---

## See Also

- [Funnel](./Funnel.md) - For combining results from multiple API calls
- [OpenOrigin](./OpenOrigin.md) - For injecting data already fetched outside the pipeline
- [Open-Stage Documentation](../README.md) - Complete documentation
