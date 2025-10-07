from src.core.base import Pipe
from src.core.common import CSVOrigin, Printer
from src.deepseek.deepseek import DeepSeekPromptTransformer
from dotenv import load_dotenv
import os

load_dotenv()

DEEPSEEK_API_KEY = os.getenv('DEEPSEEK_API_KEY')

csv_iris = CSVOrigin(
  name='csv_iris',
  filepath_or_buffer='./csv/iris.csv',
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

deepseek_prompt = """
  "1) Provide only the virginica  species
   2) drop the sepal columns 
   3) show the species in lower case
   4) change in species the - to space
   5) sort by petal_length descending
"""

deepseek_transformer = DeepSeekPromptTransformer(
  name="deepseek_transformer",
  prompt=deepseek_prompt,
  api_key=DEEPSEEK_API_KEY,
  model="deepseek-chat"
)

iris_output_pipe = Pipe(name='iris_output_pipe')

iris_printer = Printer(name='iris_printer')

csv_iris.add_output_pipe(iris_pipe).set_destination(deepseek_transformer)

deepseek_transformer.add_output_pipe(iris_output_pipe).set_destination(iris_printer)

csv_iris.pump()
