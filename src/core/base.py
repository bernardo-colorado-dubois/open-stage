# src/core/base.py

from abc import abstractmethod
import pandas as pd


class DataPackage:
  def __init__(self, pipe_name: str, df: pd.DataFrame) -> None:
    self.pipe_name = pipe_name
    self.df = df
  
  def get_pipe_name(self) -> str:
    return self.pipe_name
  
  def get_df(self) -> pd.DataFrame:
    return self.df


class Pipe:
  def __init__(self, name: str) -> None:
    self.name = name
    self.origin = None
    self.destination = None
    
  def get_name(self) -> str:
    return self.name

  def set_origin(self, origin) -> None:
    self.origin = origin
  
  def set_destination(self, destination):
    self.destination = destination
    self.destination.add_input_pipe(self)
    if isinstance(destination, Node):
      return destination
  
  def flow(self, df: pd.DataFrame) -> None:
    data_package = DataPackage(self.name, df)
    self.destination.sink(data_package)


# 0 -> 1
class Origin:
  def __init__(self):
    self.outputs = {}  # Initialize as instance variable
  
  @abstractmethod
  def add_output_pipe(self, pipe: Pipe) -> Pipe:
    if len(self.outputs.keys()) == 0:
      self.outputs[pipe.get_name()] = pipe
      pipe.set_origin(self)
      return pipe
  
  @abstractmethod
  def pump(self):
    pass


# 1 -> 0
class Destination:
  def __init__(self):
    self.inputs = {}  # Initialize as instance variable
  
  @abstractmethod
  def add_input_pipe(self, pipe: Pipe) -> None:
    if len(self.inputs.keys()) == 0:
      self.inputs[pipe.get_name()] = pipe
    
  @abstractmethod
  def sink(self, data_package: DataPackage) -> None:
    pass

# M -> N (depends on component implementation)
class Node(Origin, Destination):
  def __init__(self):
    Origin.__init__(self)
    Destination.__init__(self)