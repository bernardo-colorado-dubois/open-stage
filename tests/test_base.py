import pandas as pd
import pytest
from open_stage.core.base import (
    DataPackage, Pipe, Origin, Destination, Node,
    SingleInputMixin, SingleOutputMixin, MultiOutputMixin,
)
from tests.conftest import CaptureDest


class _SimpleOrigin(SingleOutputMixin, Origin):
    def __init__(self, name):
        super().__init__()
        self.name = name

    def pump(self):
        pass


class _SimpleNode(SingleInputMixin, SingleOutputMixin, Node):
    def __init__(self, name):
        super().__init__()
        self.name = name

    def sink(self, data_package):
        pass

    def pump(self):
        pass


class _MultiOutNode(SingleInputMixin, MultiOutputMixin, Node):
    def __init__(self, name):
        super().__init__()
        self.name = name

    def sink(self, data_package):
        pass

    def pump(self):
        pass


class TestDataPackage:
    def test_getters(self):
        df = pd.DataFrame({'x': [1, 2]})
        pkg = DataPackage("my_pipe", df)
        assert pkg.get_pipe_name() == "my_pipe"
        assert pkg.get_df().equals(df)


class TestPipe:
    def test_get_name(self):
        pipe = Pipe("p1")
        assert pipe.get_name() == "p1"

    def test_set_destination_links_input(self):
        origin = _SimpleOrigin("orig")
        pipe = Pipe("p1")
        dest = CaptureDest()
        origin.add_output_pipe(pipe).set_destination(dest)
        assert "p1" in dest.inputs

    def test_flow_delivers_datapackage(self):
        pipe = Pipe("p1")
        dest = CaptureDest()
        pipe.set_destination(dest)
        df = pd.DataFrame({'a': [1]})
        pipe.flow(df)
        assert dest.last_df.equals(df)
        assert dest.received[0].get_pipe_name() == "p1"


class TestSingleOutputMixin:
    def test_allows_one_output(self):
        origin = _SimpleOrigin("orig")
        pipe = Pipe("p1")
        result = origin.add_output_pipe(pipe)
        assert result is pipe

    def test_blocks_second_output(self):
        origin = _SimpleOrigin("orig")
        origin.add_output_pipe(Pipe("p1"))
        with pytest.raises(ValueError, match="can only have 1 output"):
            origin.add_output_pipe(Pipe("p2"))


class TestSingleInputMixin:
    def test_allows_one_input(self):
        node = _SimpleNode("n")
        pipe = Pipe("p1")
        node.add_input_pipe(pipe)
        assert "p1" in node.inputs

    def test_blocks_second_input(self):
        node = _SimpleNode("n")
        node.add_input_pipe(Pipe("p1"))
        with pytest.raises(ValueError, match="can only have 1 input"):
            node.add_input_pipe(Pipe("p2"))


class TestMultiOutputMixin:
    def test_allows_multiple_outputs(self):
        origin = _MultiOutNode("m")
        # add_input_pipe once to satisfy SingleInputMixin
        origin.add_input_pipe(Pipe("in"))
        for i in range(5):
            origin.add_output_pipe(Pipe(f"p{i}"))
        assert len(origin.outputs) == 5
