import pandas as pd

df = pd.read_csv(filepath_or_buffer='./csv/iris.csv')

print(df.head())

df.to_csv(path_or_buf='./csv/iris_output.csv', index=False)

df = pd.read_csv(filepath_or_buffer='./csv/iris_output.csv',skiprows=1)