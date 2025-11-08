# Transformer - Usage Guide

Component for applying custom transformation functions to DataFrames with support for additional arguments.

---

## 🎯 Features

- ✅ Custom transformation functions
- ✅ Support for additional arguments (`transformer_kwargs`)
- ✅ Function signature validation
- ✅ Detailed logging with before/after statistics
- ✅ Support for lambda functions
- ✅ Chainable with other transformers

---

## 📦 Installation
```bash
pip install pandas
```

---

## 🚀 Basic Usage

### Example 1: Simple Transformation (No Extra Arguments)
```python
from src.core.common import Transformer, OpenOrigin, Printer
from src.core.base import Pipe
import pandas as pd

# Define transformation function
def uppercase_names(df):
    df['name'] = df['name'].str.upper()
    return df

# Create sample data
data = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['alice', 'bob', 'charlie'],
    'age': [25, 30, 35]
})

# Create pipeline
origin = OpenOrigin("data", df=data)
transformer = Transformer(
    name="uppercase",
    transformer_function=uppercase_names
)
printer = Printer("output")

# Connect and execute
pipe1 = Pipe("pipe1")
pipe2 = Pipe("pipe2")
origin.add_output_pipe(pipe1).set_destination(transformer)
transformer.add_output_pipe(pipe2).set_destination(printer)
origin.pump()
```

---

### Example 2: Transformation with Arguments
```python
# Define function with additional parameters
def multiply_and_add(df, column, multiplier, add_value):
    df[f'{column}_new'] = df[column] * multiplier + add_value
    return df

# Create sample data
data = pd.DataFrame({
    'product': ['A', 'B', 'C'],
    'price': [100, 200, 300]
})

# Create transformer with kwargs
origin = OpenOrigin("data", df=data)
transformer = Transformer(
    name="price_calculator",
    transformer_function=multiply_and_add,
    transformer_kwargs={
        'column': 'price',
        'multiplier': 1.16,  # Add 16% tax
        'add_value': 50      # Add shipping
    }
)
printer = Printer("output")

# Connect and execute
pipe1 = Pipe("pipe1")
pipe2 = Pipe("pipe2")
origin.add_output_pipe(pipe1).set_destination(transformer)
transformer.add_output_pipe(pipe2).set_destination(printer)
origin.pump()
```

---

## 🔧 Advanced Features

### Example 3: Filter and Enrich
```python
# Complex transformation with multiple parameters
def filter_and_enrich(df, min_value, max_value, filter_column, category, new_column):
    # Filter
    df_filtered = df[
        (df[filter_column] >= min_value) & 
        (df[filter_column] <= max_value)
    ].copy()
    
    # Enrich
    df_filtered[new_column] = category
    
    return df_filtered

# Create transformer
transformer = Transformer(
    name="filter_enrich",
    transformer_function=filter_and_enrich,
    transformer_kwargs={
        'min_value': 100,
        'max_value': 300,
        'filter_column': 'price',
        'category': 'mid-range',
        'new_column': 'segment'
    }
)
```

---

### Example 4: Using Lambda Functions
```python
# Simple lambda with kwargs
transformer = Transformer(
    name="add_tax",
    transformer_function=lambda df, tax_rate: df.assign(
        tax_amount=df['price'] * tax_rate
    ),
    transformer_kwargs={'tax_rate': 0.16}
)
```

---

### Example 5: Chaining Multiple Transformers
```python
# Define functions
def clean_data(df, columns_to_drop):
    return df.drop(columns=columns_to_drop, errors='ignore')

def calculate_metrics(df, revenue_col, cost_col, margin_col):
    df[margin_col] = df[revenue_col] - df[cost_col]
    return df

# Create pipeline
origin = OpenOrigin("data", df=data)

cleaner = Transformer(
    name="cleaner",
    transformer_function=clean_data,
    transformer_kwargs={'columns_to_drop': ['temp_id', 'debug_flag']}
)

calculator = Transformer(
    name="calculator",
    transformer_function=calculate_metrics,
    transformer_kwargs={
        'revenue_col': 'revenue',
        'cost_col': 'cost',
        'margin_col': 'profit'
    }
)

printer = Printer("output")

# Connect
pipe1 = Pipe("clean")
pipe2 = Pipe("calculate")
pipe3 = Pipe("output")

origin.add_output_pipe(pipe1).set_destination(cleaner)
cleaner.add_output_pipe(pipe2).set_destination(calculator)
calculator.add_output_pipe(pipe3).set_destination(printer)

# Execute
origin.pump()
```

---

## 📊 Example Output
```
Transformer 'price_calculator' initialized successfully
  - Function: multiply_and_add
  - Function parameters: ['df', 'column', 'multiplier', 'add_value']
  - Provided kwargs: ['column', 'multiplier', 'add_value']
  - Kwargs values: {'column': 'price', 'multiplier': 1.16, 'add_value': 50}

Transformer 'price_calculator' received data from pipe: 'pipe1'
Transformer 'price_calculator' stored DataFrame with 3 rows and 2 columns

======================================================================
Transformer 'price_calculator' processing DataFrame...
======================================================================
  - Input rows: 3
  - Input columns: 2
  - Input column names: ['product', 'price']
  - Function: multiply_and_add
  - Applying with kwargs:
    • column: price
    • multiplier: 1.16
    • add_value: 50

Transformer 'price_calculator' executing transformation function...

======================================================================
Transformer 'price_calculator' transformation completed:
======================================================================
  📊 Results:
     - Input rows: 3
     - Output rows: 3
     - Rows changed: +0
     - Input columns: 2
     - Output columns: 3
     - Columns changed: +1
     - Columns added: ['price_new']
  📋 Output column names: ['product', 'price', 'price_new']
  📋 Output data types:
     - product: object
     - price: int64
     - price_new: float64

======================================================================
✅ Transformer 'price_calculator' pumped transformed data through pipe 'pipe2'
======================================================================
```

---

## 📋 Complete Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `name` | str | ✅ | - | Component name |
| `transformer_function` | callable | ✅ | - | Function to apply to DataFrame |
| `transformer_kwargs` | dict | ❌ | {} | Additional arguments for the function |

---

## 📝 Function Requirements

Your `transformer_function` must:
1. Accept a DataFrame as the **first parameter**
2. Return a pandas DataFrame
3. (Optional) Accept additional parameters that match keys in `transformer_kwargs`

**Valid function signatures:**
```python
# No extra parameters
def my_function(df):
    return df

# With positional parameters
def my_function(df, param1, param2):
    return df

# With keyword parameters
def my_function(df, param1=default1, param2=default2):
    return df

# With **kwargs
def my_function(df, **kwargs):
    return df
```

---

## ✅ Best Practices

1. **Use descriptive function names** for better logging
2. **Validate DataFrame inside function** before applying transformations
3. **Return a copy** if modifying the original DataFrame: `df = df.copy()`
4. **Use `transformer_kwargs`** instead of hardcoding values (more flexible)
5. **Document your functions** with docstrings
6. **Chain transformers** for complex pipelines (separation of concerns)
7. **Test functions independently** before using in pipeline
8. **Handle edge cases** (empty DataFrames, missing columns)

---

## ⚠️ Important Considerations

### Function Signature Validation
- The component validates that `transformer_kwargs` keys match function parameters
- Validation occurs during initialization (fail fast)
- Use `**kwargs` in function if you want flexible parameters

### DataFrame Modification
- Modifications in-place may affect the original DataFrame
- Use `.copy()` if you need to preserve the original
- Return the modified DataFrame

### Error Handling
- Errors show detailed information: function name, kwargs, DataFrame shape
- TypeError indicates parameter mismatch between kwargs and function
- ValueError indicates the function didn't return a DataFrame

### Performance
- Transformers execute immediately upon receiving data
- For large DataFrames, consider using chunking inside your function
- Chain multiple simple transformers instead of one complex one

---

## 💡 Common Use Cases

**Data Cleaning:**
```python
def clean(df, columns_to_drop):
    return df.drop(columns=columns_to_drop, errors='ignore')
```

**Feature Engineering:**
```python
def add_features(df, col1, col2, new_col):
    df[new_col] = df[col1] + df[col2]
    return df
```

**Filtering:**
```python
def filter_rows(df, column, threshold):
    return df[df[column] > threshold]
```

**Type Conversion:**
```python
def convert_types(df, columns, target_type):
    for col in columns:
        df[col] = df[col].astype(target_type)
    return df
```

**Aggregation:**
```python
def aggregate(df, group_by, agg_col, agg_func):
    return df.groupby(group_by)[agg_col].agg(agg_func).reset_index()
```

---

## 🔗 See Also

- [Filter](./Filter.md) - For row filtering
- [Aggregator](./Aggregator.md) - For aggregations
- [DeleteColumns](./DeleteColumns.md) - For column removal
- [Open-Stage Documentation](../README.md) - Complete documentation

---

**Open-Stage v2.4** - Enterprise ETL Framework