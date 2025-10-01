from src.core import CSVOrigin, Pipe, Printer, GCPBigQueryDestination
from dotenv import load_dotenv
import os


load_dotenv()

GCP_CREDENTIALS_FILE_PATH = os.getenv('GCP_CREDENTIALS_FILE_PATH')
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')

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

iris_pipe_a = Pipe(name='iris_pipe')

iris_bigquery = GCPBigQueryDestination(
    name="iris_bq_destination",
    project_id=GCP_PROJECT_ID,
    dataset="SAMPLES",
    table="IRIS",
    write_disposition="WRITE_TRUNCATE",
    credentials_path=GCP_CREDENTIALS_FILE_PATH
)

csv_iris.add_output(iris_pipe_a).set_destination(iris_bigquery)

csv_iris.pump()