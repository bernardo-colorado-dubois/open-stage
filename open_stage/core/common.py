
import logging
import pandas as pd
import requests
from open_stage.core.base import DataPackage, Pipe, Origin, Destination, Node, SingleInputMixin, SingleOutputMixin, MultiOutputMixin
from typing import Callable, Optional, Dict, Any
import inspect

logger = logging.getLogger(__name__)


class Printer(SingleInputMixin, Destination):
  def __init__(self, name: str) -> None:
    super().__init__()
    self.name = name

  def sink(self, data_package: DataPackage) -> None:
    print(f"Data received from pipe: {data_package.get_pipe_name()}")
    print(data_package.get_df())


class Funnel(SingleOutputMixin, Node):
  def __init__(self, name: str) -> None:
    super().__init__()
    self.name = name
    self.dfs = []
    self.received_inputs = 0
    self.expected_inputs = 0

  def add_input_pipe(self, pipe: Pipe) -> None:
    self.inputs[pipe.get_name()] = pipe
    self.expected_inputs += 1

  def sink(self, data_package: DataPackage) -> None:
    logger.debug("Funnel '%s' received data from pipe '%s'", self.name, data_package.get_pipe_name())
    self.dfs.append(data_package.get_df())
    self.received_inputs += 1
    logger.debug("Funnel '%s' has received %d/%d DataFrames", self.name, self.received_inputs, self.expected_inputs)
    if self.received_inputs == self.expected_inputs:
      self.merge_and_pump()

  def merge_and_pump(self) -> None:
    if not self.dfs:
      logger.error("Funnel '%s' has no DataFrames to merge", self.name)
      return
    base_columns = list(self.dfs[0].columns)
    for i, df in enumerate(self.dfs):
      if list(df.columns) != base_columns:
        logger.warning("Funnel '%s': DataFrame %d has different columns. Expected %s, got %s",
                       self.name, i, base_columns, list(df.columns))
    self.combined_df = pd.concat(self.dfs, ignore_index=True)
    logger.info("Funnel '%s' merged %d DataFrames into shape %s", self.name, len(self.dfs), self.combined_df.shape)
    self.pump()

  def pump(self) -> None:
    if not hasattr(self, 'combined_df') or len(self.outputs) == 0:
      logger.warning("Funnel '%s' has no combined data or no output pipe", self.name)
      return
    output_pipe = list(self.outputs.values())[0]
    output_pipe.flow(self.combined_df)
    logger.info("Funnel '%s' pumped merged data through pipe '%s'", self.name, output_pipe.get_name())


class Switcher(SingleInputMixin, MultiOutputMixin, Node):
  def __init__(self, name: str, field: str, mapping: dict, fail_on_unmatch: bool = False):
    super().__init__()
    self.name = name
    self.field = field
    self.mapping = mapping
    self.fail_on_unmatch = fail_on_unmatch
    self.received_df = None

    for key in mapping.keys():
      if not isinstance(key, (str, int)):
        raise ValueError(f"Switcher '{self.name}': mapping key '{key}' must be string or integer, got {type(key)}")

  def sink(self, data_package: DataPackage) -> None:
    logger.debug("Switcher '%s' received data from pipe '%s'", self.name, data_package.get_pipe_name())
    df = data_package.get_df()
    if self.field not in df.columns:
      raise ValueError(f"Switcher '{self.name}': field '{self.field}' not found in DataFrame columns: {list(df.columns)}")
    field_values = df[self.field]
    invalid_types = field_values.apply(lambda x: not isinstance(x, (str, int)) and pd.notna(x))
    if invalid_types.any():
      invalid_indices = field_values[invalid_types].index.tolist()
      raise ValueError(f"Switcher '{self.name}': field '{self.field}' contains non-string/non-integer values at indices: {invalid_indices}")
    self.received_df = df
    self.pump()

  def pump(self) -> None:
    if self.received_df is None:
      logger.warning("Switcher '%s' has no data to process", self.name)
      return
    df = self.received_df
    logger.debug("Switcher '%s' processing %d rows", self.name, len(df))
    unique_values = df[self.field].dropna().unique()
    total_routed_rows = 0
    for value in unique_values:
      filtered_df = df[df[self.field] == value]
      if value in self.mapping:
        target_pipe_name = self.mapping[value]
        if target_pipe_name in self.outputs:
          logger.debug("Switcher '%s': routing %d rows with %s='%s' to pipe '%s'",
                       self.name, len(filtered_df), self.field, value, target_pipe_name)
          self.outputs[target_pipe_name].flow(filtered_df)
          total_routed_rows += len(filtered_df)
        else:
          msg = f"Switcher '{self.name}': target pipe '{target_pipe_name}' for value '{value}' not found in outputs: {list(self.outputs.keys())}"
          if self.fail_on_unmatch:
            raise ValueError(msg)
          logger.warning(msg)
      else:
        msg = f"Switcher '{self.name}': no mapping found for value '{value}' in field '{self.field}'"
        if self.fail_on_unmatch:
          raise ValueError(msg)
        logger.warning("%s — ignoring %d rows", msg, len(filtered_df))
    nan_df = df[df[self.field].isna()]
    if not nan_df.empty:
      msg = f"Switcher '{self.name}': found {len(nan_df)} rows with NaN in field '{self.field}'"
      if self.fail_on_unmatch:
        raise ValueError(msg)
      logger.warning("%s — ignoring", msg)
    logger.info("Switcher '%s' completed: routed %d/%d rows", self.name, total_routed_rows, len(df))
    self.received_df = None


class CSVOrigin(SingleOutputMixin, Origin):
  def __init__(self, name: str, **kwargs):
    super().__init__()
    self.name = name
    self.csv_kwargs = kwargs

  def pump(self) -> None:
    try:
      df = pd.read_csv(**self.csv_kwargs)
      logger.info("CSVOrigin '%s' read CSV with shape %s", self.name, df.shape)
      if len(self.outputs) > 0:
        output_pipe = list(self.outputs.values())[0]
        output_pipe.flow(df)
        logger.debug("CSVOrigin '%s' pumped data through pipe '%s'", self.name, output_pipe.get_name())
      else:
        logger.warning("CSVOrigin '%s' has no output pipe configured", self.name)
    except Exception as e:
      logger.error("CSVOrigin '%s' failed to read CSV: %s", self.name, e)
      logger.debug("CSV arguments: %s", self.csv_kwargs)
      raise


class CSVDestination(SingleInputMixin, Destination):
  def __init__(self, name: str, **kwargs):
    super().__init__()
    self.name = name
    self.csv_kwargs = kwargs

  def sink(self, data_package: DataPackage) -> None:
    logger.debug("CSVDestination '%s' received data from pipe '%s'", self.name, data_package.get_pipe_name())
    df = data_package.get_df()
    try:
      df.to_csv(**self.csv_kwargs)
      logger.info("CSVDestination '%s' wrote CSV with %d rows", self.name, len(df))
      logger.debug("CSV arguments: %s", self.csv_kwargs)
    except Exception as e:
      logger.error("CSVDestination '%s' failed to write CSV: %s", self.name, e)
      logger.debug("CSV arguments: %s | DataFrame shape: %s", self.csv_kwargs, df.shape)
      raise


class Copy(SingleInputMixin, MultiOutputMixin, Node):
  def __init__(self, name: str):
    super().__init__()
    self.name = name
    self.received_df = None

  def sink(self, data_package: DataPackage) -> None:
    logger.debug("Copy '%s' received data from pipe '%s'", self.name, data_package.get_pipe_name())
    self.received_df = data_package.get_df()
    self.pump()

  def pump(self) -> None:
    if self.received_df is None:
      logger.warning("Copy '%s' has no data to process", self.name)
      return
    if len(self.outputs) == 0:
      logger.warning("Copy '%s' has no output pipes configured", self.name)
      return
    df = self.received_df
    logger.debug("Copy '%s' sending %d copies of %d rows", self.name, len(self.outputs), len(df))
    for pipe_name, pipe in self.outputs.items():
      pipe.flow(df.copy())
    logger.info("Copy '%s' sent %d copies", self.name, len(self.outputs))
    self.received_df = None


class Aggregator(SingleInputMixin, SingleOutputMixin, Node):
  def __init__(self, name: str, key: str, agg_field_name: str, agg_type: str, field_to_agg: str = None):
    super().__init__()
    self.name = name
    self.key = key
    self.agg_field_name = agg_field_name
    self.agg_type = agg_type
    self.field_to_agg = field_to_agg
    self.received_df = None

    valid_agg_types = ['sum', 'count', 'mean', 'median', 'min', 'max', 'std', 'var',
                       'nunique', 'first', 'last', 'size', 'sem', 'quantile']
    if self.agg_type not in valid_agg_types:
      logger.warning("Aggregator '%s': aggregation type '%s' might not be supported by pandas", self.name, self.agg_type)

    if self.agg_type != 'count' and self.field_to_agg is None:
      raise ValueError(f"Aggregator '{self.name}': field_to_agg is required for aggregation type '{self.agg_type}'")

  def sink(self, data_package: DataPackage) -> None:
    logger.debug("Aggregator '%s' received data from pipe '%s'", self.name, data_package.get_pipe_name())
    df = data_package.get_df()
    if self.key not in df.columns:
      raise ValueError(f"Aggregator '{self.name}': key field '{self.key}' not found in DataFrame columns: {list(df.columns)}")
    if self.field_to_agg is not None and self.field_to_agg not in df.columns:
      raise ValueError(f"Aggregator '{self.name}': field_to_agg '{self.field_to_agg}' not found in DataFrame columns: {list(df.columns)}")
    self.received_df = df
    self.pump()

  def pump(self) -> None:
    if self.received_df is None:
      logger.warning("Aggregator '%s' has no data to process", self.name)
      return
    if len(self.outputs) == 0:
      logger.warning("Aggregator '%s' has no output pipe configured", self.name)
      return
    df = self.received_df
    try:
      grouped = df.groupby(self.key)
      if self.agg_type == 'count':
        agg_result = grouped.size()
      else:
        if self.field_to_agg is None:
          raise ValueError(f"Aggregator '{self.name}': field_to_agg cannot be None for aggregation type '{self.agg_type}'")
        agg_result = grouped[self.field_to_agg].agg(self.agg_type)
      result_df = pd.DataFrame({
        self.key: agg_result.index,
        self.agg_field_name: agg_result.values
      }).reset_index(drop=True)
      logger.info("Aggregator '%s' completed: %d rows → %d groups", self.name, len(df), len(result_df))
      output_pipe = list(self.outputs.values())[0]
      output_pipe.flow(result_df)
      logger.debug("Aggregator '%s' pumped data through pipe '%s'", self.name, output_pipe.get_name())
    except Exception as e:
      logger.error("Aggregator '%s' failed: %s (key='%s', agg_type='%s', field='%s')",
                   self.name, e, self.key, self.agg_type, self.field_to_agg)
      raise
    finally:
      self.received_df = None


class DeleteColumns(SingleInputMixin, SingleOutputMixin, Node):
  def __init__(self, name: str, columns: list):
    super().__init__()
    self.name = name
    self.columns = columns
    self.received_df = None

    if not isinstance(columns, list):
      raise ValueError(f"DeleteColumns '{self.name}': columns must be a list, got {type(columns)}")
    if len(columns) == 0:
      raise ValueError(f"DeleteColumns '{self.name}': columns list cannot be empty")

  def sink(self, data_package: DataPackage) -> None:
    logger.debug("DeleteColumns '%s' received data from pipe '%s'", self.name, data_package.get_pipe_name())
    df = data_package.get_df()
    missing_columns = [col for col in self.columns if col not in df.columns]
    if missing_columns:
      raise ValueError(f"DeleteColumns '{self.name}': columns {missing_columns} not found in DataFrame. Available columns: {list(df.columns)}")
    self.received_df = df
    self.pump()

  def pump(self) -> None:
    if self.received_df is None:
      logger.warning("DeleteColumns '%s' has no data to process", self.name)
      return
    if len(self.outputs) == 0:
      logger.warning("DeleteColumns '%s' has no output pipe configured", self.name)
      return
    df = self.received_df
    try:
      result_df = df.drop(columns=self.columns)
      logger.info("DeleteColumns '%s' dropped %d columns: %s", self.name, len(self.columns), self.columns)
      output_pipe = list(self.outputs.values())[0]
      output_pipe.flow(result_df)
      logger.debug("DeleteColumns '%s' pumped data through pipe '%s'", self.name, output_pipe.get_name())
    except Exception as e:
      logger.error("DeleteColumns '%s' failed: %s (columns=%s)", self.name, e, self.columns)
      raise
    finally:
      self.received_df = None


class Filter(SingleInputMixin, SingleOutputMixin, Node):
  def __init__(self, name: str, field: str, condition: str, value_or_values):
    super().__init__()
    self.name = name
    self.field = field
    self.condition = condition
    self.value_or_values = value_or_values
    self.received_df = None

    valid_conditions = ['<', '>', '<=', '>=', '!=', '=', 'in', 'not in', 'between']
    if self.condition not in valid_conditions:
      raise ValueError(f"Filter '{self.name}': condition '{self.condition}' not supported. Valid conditions: {valid_conditions}")
    if self.condition in ['in', 'not in']:
      if not isinstance(self.value_or_values, list):
        raise ValueError(f"Filter '{self.name}': condition '{self.condition}' requires a list for value_or_values, got {type(self.value_or_values)}")
      if len(self.value_or_values) == 0:
        raise ValueError(f"Filter '{self.name}': value_or_values list cannot be empty for condition '{self.condition}'")
    if self.condition == 'between':
      if not isinstance(self.value_or_values, list):
        raise ValueError(f"Filter '{self.name}': condition 'between' requires a list for value_or_values, got {type(self.value_or_values)}")
      if len(self.value_or_values) != 2:
        raise ValueError(f"Filter '{self.name}': condition 'between' requires exactly 2 values [lower, upper], got {len(self.value_or_values)} values")

  def sink(self, data_package: DataPackage) -> None:
    logger.debug("Filter '%s' received data from pipe '%s'", self.name, data_package.get_pipe_name())
    df = data_package.get_df()
    if self.field not in df.columns:
      raise ValueError(f"Filter '{self.name}': field '{self.field}' not found in DataFrame columns: {list(df.columns)}")
    self.received_df = df
    self.pump()

  def pump(self) -> None:
    if self.received_df is None:
      logger.warning("Filter '%s' has no data to process", self.name)
      return
    if len(self.outputs) == 0:
      logger.warning("Filter '%s' has no output pipe configured", self.name)
      return
    df = self.received_df
    try:
      if self.condition == '<':
        mask = df[self.field] < self.value_or_values
      elif self.condition == '>':
        mask = df[self.field] > self.value_or_values
      elif self.condition == '<=':
        mask = df[self.field] <= self.value_or_values
      elif self.condition == '>=':
        mask = df[self.field] >= self.value_or_values
      elif self.condition == '!=':
        mask = df[self.field] != self.value_or_values
      elif self.condition == '=':
        mask = df[self.field] == self.value_or_values
      elif self.condition == 'in':
        mask = df[self.field].isin(self.value_or_values)
      elif self.condition == 'not in':
        mask = ~df[self.field].isin(self.value_or_values)
      elif self.condition == 'between':
        lower_bound, upper_bound = self.value_or_values
        mask = (df[self.field] >= lower_bound) & (df[self.field] <= upper_bound)
      filtered_df = df[mask]
      logger.info("Filter '%s' passed %d/%d rows (%s %s %s)",
                  self.name, len(filtered_df), len(df), self.field, self.condition, self.value_or_values)
      output_pipe = list(self.outputs.values())[0]
      output_pipe.flow(filtered_df)
      logger.debug("Filter '%s' pumped data through pipe '%s'", self.name, output_pipe.get_name())
    except Exception as e:
      logger.error("Filter '%s' failed: %s (field='%s', condition='%s', value=%s)",
                   self.name, e, self.field, self.condition, self.value_or_values)
      raise
    finally:
      self.received_df = None


class Joiner(SingleOutputMixin, Node):
  def __init__(self, name: str, left: str, right: str, key: str, join_type: str):
    super().__init__()
    self.name = name
    self.left_pipe_name = left
    self.right_pipe_name = right
    self.key = key
    self.join_type = join_type
    self.received_dfs = {}
    self.expected_inputs = 2
    self.received_inputs = 0

    valid_join_types = ['left', 'right', 'inner']
    if self.join_type not in valid_join_types:
      raise ValueError(f"Joiner '{self.name}': join_type '{self.join_type}' not supported. Valid types: {valid_join_types}")
    if self.left_pipe_name == self.right_pipe_name:
      raise ValueError(f"Joiner '{self.name}': left and right pipe names must be different")

  def add_input_pipe(self, pipe: Pipe) -> None:
    if len(self.inputs.keys()) < 2:
      self.inputs[pipe.get_name()] = pipe
    else:
      raise ValueError(f"Joiner '{self.name}' can only have 2 inputs")

  def sink(self, data_package: DataPackage) -> None:
    pipe_name = data_package.get_pipe_name()
    df = data_package.get_df()
    logger.debug("Joiner '%s' received data from pipe '%s'", self.name, pipe_name)
    if pipe_name not in [self.left_pipe_name, self.right_pipe_name]:
      raise ValueError(f"Joiner '{self.name}': unexpected pipe '{pipe_name}'. Expected: '{self.left_pipe_name}', '{self.right_pipe_name}'")
    if self.key not in df.columns:
      raise ValueError(f"Joiner '{self.name}': key field '{self.key}' not found in DataFrame from pipe '{pipe_name}'. Available columns: {list(df.columns)}")
    self.received_dfs[pipe_name] = df
    self.received_inputs += 1
    logger.debug("Joiner '%s' has received %d/%d DataFrames", self.name, self.received_inputs, self.expected_inputs)
    if self.received_inputs == self.expected_inputs:
      self.pump()

  def pump(self) -> None:
    if len(self.received_dfs) != 2:
      logger.warning("Joiner '%s' needs exactly 2 DataFrames, got %d", self.name, len(self.received_dfs))
      return
    if len(self.outputs) == 0:
      logger.warning("Joiner '%s' has no output pipe configured", self.name)
      return
    if self.left_pipe_name not in self.received_dfs:
      raise ValueError(f"Joiner '{self.name}': left DataFrame from pipe '{self.left_pipe_name}' not received")
    if self.right_pipe_name not in self.received_dfs:
      raise ValueError(f"Joiner '{self.name}': right DataFrame from pipe '{self.right_pipe_name}' not received")
    left_df = self.received_dfs[self.left_pipe_name]
    right_df = self.received_dfs[self.right_pipe_name]
    try:
      result_df = pd.merge(left_df, right_df, on=self.key, how=self.join_type)
      logger.info("Joiner '%s' completed %s join on '%s': %d + %d rows → %d rows, %d columns",
                  self.name, self.join_type, self.key, len(left_df), len(right_df),
                  len(result_df), len(result_df.columns))
      output_pipe = list(self.outputs.values())[0]
      output_pipe.flow(result_df)
      logger.debug("Joiner '%s' pumped data through pipe '%s'", self.name, output_pipe.get_name())
    except Exception as e:
      logger.error("Joiner '%s' failed: %s (left='%s', right='%s', key='%s', join_type='%s')",
                   self.name, e, self.left_pipe_name, self.right_pipe_name, self.key, self.join_type)
      raise
    finally:
      self.received_dfs = {}
      self.received_inputs = 0


class Transformer(SingleInputMixin, SingleOutputMixin, Node):
  """
  Transformer - Custom Function Transformer
  =========================================
  Applies a custom transformation function to a DataFrame with support
  for additional arguments.

  Connectivity: 1 → 1 (inherits from Node)

  Parameters
  ----------
  name : str
    Component name
  transformer_function : callable
    Function that transforms the DataFrame. Must accept DataFrame as first
    parameter and return a transformed DataFrame.
    Signature: func(df: pd.DataFrame, **kwargs) -> pd.DataFrame
  transformer_kwargs : dict, optional
    Dictionary of additional keyword arguments to pass to the transformer
    function. Similar to op_kwargs in Airflow's PythonOperator.
    Default: {}

  Examples
  --------
  # Example 1: Simple transformation without extra arguments
  >>> def uppercase_names(df):
  ...     df['name'] = df['name'].str.upper()
  ...     return df
  >>>
  >>> transformer = Transformer(
  ...     name="uppercase",
  ...     transformer_function=uppercase_names
  ... )

  # Example 2: Transformation with extra arguments
  >>> def multiply_column(df, column, multiplier, add_value=0):
  ...     df[column] = df[column] * multiplier + add_value
  ...     return df
  >>>
  >>> transformer = Transformer(
  ...     name="price_calculator",
  ...     transformer_function=multiply_column,
  ...     transformer_kwargs={
  ...         'column': 'price',
  ...         'multiplier': 1.15,
  ...         'add_value': 10
  ...     }
  ... )
  """

  def __init__(
    self,
    name: str,
    transformer_function: Callable,
    transformer_kwargs: Optional[Dict[str, Any]] = None
  ):
    super().__init__()
    self.name = name
    self.transformer_function = transformer_function
    self.transformer_kwargs = transformer_kwargs or {}
    self.received_df = None

    if transformer_function is None:
      raise ValueError(f"Transformer '{self.name}': transformer_function cannot be None")
    if not callable(transformer_function):
      raise ValueError(
        f"Transformer '{self.name}': transformer_function must be callable (function or lambda), "
        f"got {type(transformer_function)}"
      )
    if not isinstance(self.transformer_kwargs, dict):
      raise ValueError(
        f"Transformer '{self.name}': transformer_kwargs must be a dictionary, "
        f"got {type(self.transformer_kwargs)}"
      )

    try:
      sig = inspect.signature(transformer_function)
      params = list(sig.parameters.keys())
      if len(params) == 0:
        raise ValueError(
          f"Transformer '{self.name}': transformer_function must accept at least one parameter (DataFrame)"
        )
      if self.transformer_kwargs:
        func_params = set(params[1:])
        has_var_keyword = any(
          p.kind == inspect.Parameter.VAR_KEYWORD
          for p in sig.parameters.values()
        )
        if not has_var_keyword:
          provided_kwargs = set(self.transformer_kwargs.keys())
          invalid_kwargs = provided_kwargs - func_params
          if invalid_kwargs:
            raise ValueError(
              f"Transformer '{self.name}': transformer_kwargs contains parameters "
              f"not defined in function: {invalid_kwargs}. "
              f"Function parameters: {func_params}"
            )
      logger.debug("Transformer '%s' initialized: function='%s', kwargs=%s",
                   self.name, transformer_function.__name__, list(self.transformer_kwargs.keys()))
    except ValueError:
      raise
    except Exception as e:
      logger.warning("Transformer '%s': could not validate function signature: %s", self.name, e)

  def sink(self, data_package: DataPackage) -> None:
    logger.debug("Transformer '%s' received data from pipe '%s': %d rows, %d columns",
                 self.name, data_package.get_pipe_name(),
                 len(data_package.get_df()), len(data_package.get_df().columns))
    self.received_df = data_package.get_df()
    self.pump()

  def pump(self) -> None:
    if self.received_df is None:
      logger.warning("Transformer '%s' has no data to process", self.name)
      return
    if len(self.outputs) == 0:
      logger.warning("Transformer '%s' has no output pipe configured", self.name)
      return
    df = self.received_df
    try:
      result_df = self.transformer_function(df, **self.transformer_kwargs)
      if not isinstance(result_df, pd.DataFrame):
        raise ValueError(
          f"Transformer '{self.name}': transformer_function must return a pandas DataFrame, "
          f"got {type(result_df)}"
        )
      if result_df.empty:
        logger.warning("Transformer '%s': transformation resulted in empty DataFrame", self.name)
      logger.info("Transformer '%s' completed: %d rows → %d rows, %d → %d columns",
                  self.name, len(df), len(result_df), len(df.columns), len(result_df.columns))
      output_pipe = list(self.outputs.values())[0]
      output_pipe.flow(result_df)
      logger.debug("Transformer '%s' pumped data through pipe '%s'", self.name, output_pipe.get_name())
    except TypeError as e:
      error_msg = str(e)
      if "unexpected keyword argument" in error_msg or "missing" in error_msg:
        logger.error("Transformer '%s': function signature mismatch — %s", self.name, error_msg)
        raise ValueError(
          f"Transformer '{self.name}': Function parameter mismatch. "
          f"Check that transformer_kwargs keys match function parameters. "
          f"Error: {error_msg}"
        )
      raise
    except Exception as e:
      logger.error("Transformer '%s' failed: %s", self.name, e)
      raise
    finally:
      self.received_df = None


class APIRestOrigin(SingleOutputMixin, Origin):
  def __init__(self, name: str, path: str = '.', fields: list = None, **kwargs):
    super().__init__()
    self.name = name
    self.path = path
    self.fields = fields
    self.request_kwargs = kwargs

    if self.fields is not None and not isinstance(self.fields, list):
      raise ValueError(f"APIRestOrigin '{self.name}': fields must be a list, got {type(self.fields)}")
    if self.fields is not None and len(self.fields) == 0:
      raise ValueError(f"APIRestOrigin '{self.name}': fields list cannot be empty")

  def pump(self) -> None:
    try:
      logger.debug("APIRestOrigin '%s' making HTTP request: %s", self.name, self.request_kwargs)
      response = requests.request(**self.request_kwargs)
      response.raise_for_status()
      json_data = response.json()
      data = self._navigate_path(json_data, self.path)
      if isinstance(data, list):
        df = pd.DataFrame(data)
      elif isinstance(data, dict):
        df = pd.DataFrame([data])
      else:
        raise ValueError(f"APIRestOrigin '{self.name}': data at path '{self.path}' must be a list or dict, got {type(data)}")
      if self.fields is not None:
        missing_fields = [field for field in self.fields if field not in df.columns]
        if missing_fields:
          raise ValueError(f"APIRestOrigin '{self.name}': fields {missing_fields} not found in response data. Available fields: {list(df.columns)}")
        df = df[self.fields]
      logger.info("APIRestOrigin '%s' fetched DataFrame with shape %s", self.name, df.shape)
      if len(self.outputs) > 0:
        output_pipe = list(self.outputs.values())[0]
        output_pipe.flow(df)
        logger.debug("APIRestOrigin '%s' pumped data through pipe '%s'", self.name, output_pipe.get_name())
      else:
        logger.warning("APIRestOrigin '%s' has no output pipe configured", self.name)
    except requests.exceptions.RequestException as e:
      logger.error("APIRestOrigin '%s' HTTP request failed: %s", self.name, e)
      logger.debug("Request parameters: %s", self.request_kwargs)
      raise
    except Exception as e:
      logger.error("APIRestOrigin '%s' failed: %s", self.name, e)
      raise

  def _navigate_path(self, data, path):
    if path == '.':
      return data
    try:
      parts = path.split('.')
      current = data
      for part in parts:
        if isinstance(current, dict) and part in current:
          current = current[part]
        else:
          raise KeyError(f"Path part '{part}' not found")
      return current
    except Exception as e:
      raise ValueError(f"APIRestOrigin '{self.name}': failed to navigate path '{path}': {str(e)}")


class RemoveDuplicates(SingleInputMixin, SingleOutputMixin, Node):
  def __init__(self, name: str, key: str, sort_by: str, orientation: str, retain: str):
    super().__init__()
    self.name = name
    self.key = key
    self.sort_by = sort_by
    self.orientation = orientation
    self.retain = retain
    self.received_df = None

    valid_orientations = ['ASC', 'DESC']
    if self.orientation not in valid_orientations:
      raise ValueError(f"RemoveDuplicates '{self.name}': orientation must be one of {valid_orientations}, got '{self.orientation}'")
    valid_retains = ['FIRST', 'LAST']
    if self.retain not in valid_retains:
      raise ValueError(f"RemoveDuplicates '{self.name}': retain must be one of {valid_retains}, got '{self.retain}'")

  def sink(self, data_package: DataPackage) -> None:
    logger.debug("RemoveDuplicates '%s' received data from pipe '%s'", self.name, data_package.get_pipe_name())
    df = data_package.get_df()
    if self.key not in df.columns:
      raise ValueError(f"RemoveDuplicates '{self.name}': key field '{self.key}' not found in DataFrame columns: {list(df.columns)}")
    if self.sort_by not in df.columns:
      raise ValueError(f"RemoveDuplicates '{self.name}': sort_by field '{self.sort_by}' not found in DataFrame columns: {list(df.columns)}")
    self.received_df = df
    self.pump()

  def pump(self) -> None:
    if self.received_df is None:
      logger.warning("RemoveDuplicates '%s' has no data to process", self.name)
      return
    if len(self.outputs) == 0:
      logger.warning("RemoveDuplicates '%s' has no output pipe configured", self.name)
      return
    df = self.received_df
    try:
      ascending = self.orientation == 'ASC'
      sorted_df = df.sort_values(by=self.sort_by, ascending=ascending)
      keep = 'first' if self.retain == 'FIRST' else 'last'
      result_df = sorted_df.drop_duplicates(subset=[self.key], keep=keep)
      duplicates_removed = len(df) - len(result_df)
      logger.info("RemoveDuplicates '%s' removed %d duplicates: %d → %d rows (key='%s', sort='%s' %s, retain=%s)",
                  self.name, duplicates_removed, len(df), len(result_df),
                  self.key, self.sort_by, self.orientation, self.retain)
      output_pipe = list(self.outputs.values())[0]
      output_pipe.flow(result_df)
      logger.debug("RemoveDuplicates '%s' pumped data through pipe '%s'", self.name, output_pipe.get_name())
    except Exception as e:
      logger.error("RemoveDuplicates '%s' failed: %s (key='%s', sort_by='%s', orientation='%s', retain='%s')",
                   self.name, e, self.key, self.sort_by, self.orientation, self.retain)
      raise
    finally:
      self.received_df = None


class OpenOrigin(SingleOutputMixin, Origin):
  """
  OpenOrigin - Simple DataFrame Origin
  ====================================
  A simple origin that accepts a pandas DataFrame directly.
  Useful for testing, in-memory data processing, or starting pipelines
  with data already loaded in memory.

  Connectivity: 0 → 1 (inherits from Origin)

  Parameters
  ----------
  name : str
    Component name
  df : pd.DataFrame
    Pandas DataFrame to be used as data source

  Example
  -------
  >>> import pandas as pd
  >>> from open_stage.core.common import OpenOrigin, Printer
  >>> from open_stage.core.base import Pipe
  >>>
  >>> df = pd.DataFrame({'id': [1, 2, 3], 'name': ['Alice', 'Bob', 'Charlie']})
  >>> origin = OpenOrigin(name="my_data", df=df)
  >>> pipe = Pipe("pipe1")
  >>> printer = Printer("output")
  >>> origin.add_output_pipe(pipe).set_destination(printer)
  >>> origin.pump()
  """

  def __init__(self, name: str, df: pd.DataFrame):
    super().__init__()
    self.name = name
    self.df = df

    if df is None:
      raise ValueError(f"OpenOrigin '{self.name}': df cannot be None")
    if not isinstance(df, pd.DataFrame):
      raise ValueError(f"OpenOrigin '{self.name}': df must be a pandas DataFrame, got {type(df)}")
    if df.empty:
      raise ValueError(f"OpenOrigin '{self.name}': df cannot be empty")

  def pump(self) -> None:
    try:
      logger.info("OpenOrigin '%s' pumping DataFrame with shape %s", self.name, self.df.shape)
      if len(self.outputs) > 0:
        output_pipe = list(self.outputs.values())[0]
        output_pipe.flow(self.df)
        logger.debug("OpenOrigin '%s' pumped data through pipe '%s'", self.name, output_pipe.get_name())
      else:
        logger.warning("OpenOrigin '%s' has no output pipe configured", self.name)
    except Exception as e:
      logger.error("OpenOrigin '%s' failed to pump data: %s", self.name, e)
      raise
