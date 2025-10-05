from src.core.common import Pipe, CSVOrigin, Printer, Filter


csv_iris = CSVOrigin(
  name='csv_iris',filepath_or_buffer='./csv/iris.csv',
  skiprows=1,
  header=None,
  names=[
    'sepal_length', 
    'sepal_width', 
    'petal_length', 
    'petal_width', 
    'species'
  ],
  dtype={
    'sepal_length': 'float64', 
    'sepal_width': 'float64', 
    'petal_length': 'float64', 
    'petal_width': 'float64', 
    'species': 'string'
  }
)

iris_pipe = Pipe(name='iris_pipe')

iris_filter = Filter(
  name='iris_filter',
  field='petal_length',
  condition='between',
  value_or_values=[1.0, 4.0]
)

iris_pipe_filtered = Pipe(name='iris_pipe')

iris_printer = Printer(name='iris_printer')

csv_iris.add_output_pipe(iris_pipe).set_destination(iris_filter)

iris_filter.add_output_pipe(iris_pipe_filtered).set_destination(iris_printer)

csv_iris.pump()