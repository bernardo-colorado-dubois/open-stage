from src.core.base import Pipe
from src.core.common import CSVOrigin, Printer

coffee_sales_csv_origin = CSVOrigin(
  name='coffee_sales_csv_origin',
  filepath_or_buffer='./csv/coffee_sales.csv',
  names =[
    'hour_of_day'
    ,'cash_type'
    ,'money'
    ,'coffee_name'
    ,'time_of_day'
    ,'day_of_week'
    ,'month_name'
    ,'week_number'
    ,'month_number'
    ,'date'
    ,'time'
  ],
  dtype={
    'hour_of_day': 'int64'
    ,'cash_type': 'string'
    ,'money': 'float64'
    ,'coffee_name': 'string'
    ,'time_of_day': 'string'
    ,'day_of_week': 'string'
    ,'month_name': 'string'
    ,'week_number': 'int64'
    ,'month_number': 'int64'
    ,'date': 'string'
    ,'time': 'string'
  },
  skiprows=1,
  header=None,
)

coffee_sales_csv_origin.add_output_pipe(Pipe(name='coffee_sales_pipe')).set_destination(Printer(name='coffee_sales_printer'))

coffee_sales_csv_origin.pump()