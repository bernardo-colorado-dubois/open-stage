from src.core.common import CSVOrigin, Pipe, Funnel, Switcher, CSVDestination, Copy, Aggregator

csv_origin = CSVOrigin(name='iris',filepath_or_buffer='./csv/iris.csv')

iris_pipe_a = Pipe(name='iris_pipe')

iris_switcher = Switcher(
    name='iris_switcher',
    field='Species',
    mapping={
        'Iris-setosa': 'setosa_pipe',
        'Iris-versicolor': 'versicolor_pipe',
        'Iris-virginica': 'virginica_pipe'
    },
    fail_on_unmatch=True
)

setosa_pipe = Pipe(name='setosa_pipe')
versicolor_pipe = Pipe(name='versicolor_pipe')
virginica_pipe = Pipe(name='virginica_pipe')

iris_funnel = Funnel(name='iris_funnel')

iris_pipe_b = Pipe(name='iris_pipe_b')

iris_copy = Copy(name='iris_copy')

iris_pipe_to_csv = Pipe(name='iris_pipe_to_csv')
iris_csv_destination = CSVDestination(name='iris_csv_destination',path_or_buf='./csv/iris_output.csv',index=False)
iris_pipe_to_agg = Pipe(name='iris_pipe_to_agg')
iris_counter = Aggregator(name='iris_counter',key='Species',agg_field_name='FlowerCount',field_to_agg=None,agg_type='count')
iris_pipe_to_csv_count = Pipe(name='iris_pipe_to_csv_count')
iris_count_to_csv = CSVDestination(name='iris_count_to_csv',path_or_buf='./csv/iris_count_output.csv',index=False)

csv_origin.add_output_pipe(iris_pipe_b).set_destination(iris_switcher)
iris_switcher.add_output_pipe(setosa_pipe).set_destination(iris_funnel)
iris_switcher.add_output_pipe(versicolor_pipe).set_destination(iris_funnel)
iris_switcher.add_output_pipe(virginica_pipe).set_destination(iris_funnel)
iris_funnel.add_output_pipe(iris_pipe_b).set_destination(iris_copy)
iris_copy.add_output_pipe(iris_pipe_to_csv).set_destination(iris_csv_destination)
iris_copy.add_output_pipe(iris_pipe_to_agg).set_destination(iris_counter)
iris_counter.add_output_pipe(iris_pipe_to_csv_count).set_destination(iris_count_to_csv)

csv_origin.pump()