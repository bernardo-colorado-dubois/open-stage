from src.core.base import Pipe
from src.core.common import CSVOrigin,CSVDestination, Printer, Filter, DeleteColumns, Switcher,Aggregator

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

coffee_sales_pipe_1 = Pipe(name='coffee_sales_pipe_1')

coffee_sales_filter = Filter(
  name='coffee_sales_filter',
  field="time_of_day",
  condition="in",
  value_or_values=["Afternoon", "Morning"]
)

coffee_sales_pipe_2 = Pipe(name='coffee_sales_pipe_2')

coffee_sales_delete_columns = DeleteColumns(
  name='coffee_sales_delete_columns',
  columns=['cash_type','hour_of_day']
)

coffee_sales_pipe_3 = Pipe(name='coffee_sales_pipe_3')

coffee_sales_switcher = Switcher(
  name='coffee_sales_switcher',
  field='time_of_day',
  mapping={
    'Afternoon': 'coffe_sales_pipe_afternoon',
    'Morning': 'coffe_sales_pipe_morning',
  }
)

coffe_sales_pipe_morning = Pipe(name='coffe_sales_pipe_morning')
coffe_sales_pipe_afternoon = Pipe(name='coffe_sales_pipe_afternoon')

coffee_sales_morning_agg = Aggregator(
  name='coffee_sales_morning_agg',
  key='day_of_week',
  agg_field_name='sales_count',
  agg_type='count',
  field_to_agg=None
)

coffee_sales_afternoon_agg = Aggregator(
  name='coffee_sales_morning_agg',
  key='day_of_week',
  agg_field_name='average_price',
  agg_type='mean',
  field_to_agg='money'
)

coffee_sales_morning_count_pipe = Pipe(name='coffee_sales_morning_count_pipe')
coffee_sales_afternoon_mean_pipe = Pipe(name='coffee_sales_afternoon_mean_pipe')

coffee_sales_morning_agg_csv_destination = CSVDestination(
  name='coffee_sales_morning_agg_csv_destination',
  path_or_buf='./csv/coffee_sales_morning_agg.csv',
  index=False
)

coffee_sales_afternoon_agg_csv_destination = CSVDestination(
  name='coffee_sales_morning_agg_csv_destination',
  path_or_buf='./csv/coffee_sales_afternoon_agg.csv',
  index=False
)

coffee_sales_csv_origin.add_output_pipe(coffee_sales_pipe_1).set_destination(coffee_sales_filter)

coffee_sales_filter.add_output_pipe(coffee_sales_pipe_2).set_destination(coffee_sales_delete_columns)

coffee_sales_delete_columns.add_output_pipe(coffee_sales_pipe_3).set_destination(coffee_sales_switcher)

coffee_sales_switcher.add_output_pipe(coffe_sales_pipe_morning).set_destination(coffee_sales_morning_agg)

coffee_sales_switcher.add_output_pipe(coffe_sales_pipe_afternoon).set_destination(coffee_sales_afternoon_agg)

coffee_sales_morning_agg.add_output_pipe(coffee_sales_morning_count_pipe).set_destination(coffee_sales_morning_agg_csv_destination)

coffee_sales_afternoon_agg.add_output_pipe(coffee_sales_afternoon_mean_pipe).set_destination(coffee_sales_afternoon_agg_csv_destination)

coffee_sales_csv_origin.pump()