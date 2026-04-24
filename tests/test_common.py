import io
import os
import tempfile
import pandas as pd
import pytest
from open_stage.core.base import Pipe
from open_stage.core.common import (
    OpenOrigin, Printer,
    Filter, DeleteColumns, Aggregator,
    RemoveDuplicates, Copy, Funnel, Switcher, Joiner, Transformer,
    CSVOrigin, CSVDestination,
)
from tests.conftest import CaptureDest


def run_pipeline(origin, dest):
    """Wire origin → single pipe → dest and pump."""
    pipe = Pipe("test_pipe")
    origin.add_output_pipe(pipe).set_destination(dest)
    origin.pump()
    return dest.last_df


class TestOpenOrigin:
    def test_passthrough(self, sample_df):
        origin = OpenOrigin("o", sample_df)
        dest = CaptureDest()
        result = run_pipeline(origin, dest)
        assert result.equals(sample_df)

    def test_rejects_none(self):
        with pytest.raises(ValueError):
            OpenOrigin("o", None)

    def test_rejects_empty_df(self):
        with pytest.raises(ValueError):
            OpenOrigin("o", pd.DataFrame())

    def test_rejects_non_dataframe(self):
        with pytest.raises(ValueError):
            OpenOrigin("o", [1, 2, 3])


class TestFilter:
    def test_equal(self, sample_df):
        f = Filter("f", field="category", condition="=", value_or_values="A")
        origin = OpenOrigin("o", sample_df)
        dest = CaptureDest()
        run_pipeline(origin, dest)  # prime dest — we need to rewire through Filter
        # Proper pipeline: origin → filter → dest
        dest2 = CaptureDest()
        pipe1 = Pipe("p1")
        pipe2 = Pipe("p2")
        origin2 = OpenOrigin("o2", sample_df)
        origin2.add_output_pipe(pipe1).set_destination(f)
        f.add_output_pipe(pipe2).set_destination(dest2)
        origin2.pump()
        result = dest2.last_df
        assert all(result["category"] == "A")
        assert len(result) == 3

    def test_greater_than(self, sample_df):
        f = Filter("f", field="score", condition=">", value_or_values=85)
        dest = CaptureDest()
        pipe1, pipe2 = Pipe("p1"), Pipe("p2")
        origin = OpenOrigin("o", sample_df)
        origin.add_output_pipe(pipe1).set_destination(f)
        f.add_output_pipe(pipe2).set_destination(dest)
        origin.pump()
        assert all(dest.last_df["score"] > 85)

    def test_in_condition(self, sample_df):
        f = Filter("f", field="name", condition="in", value_or_values=["Alice", "Charlie"])
        dest = CaptureDest()
        pipe1, pipe2 = Pipe("p1"), Pipe("p2")
        origin = OpenOrigin("o", sample_df)
        origin.add_output_pipe(pipe1).set_destination(f)
        f.add_output_pipe(pipe2).set_destination(dest)
        origin.pump()
        assert set(dest.last_df["name"].tolist()) <= {"Alice", "Charlie"}

    def test_between(self, sample_df):
        f = Filter("f", field="score", condition="between", value_or_values=[80, 90])
        dest = CaptureDest()
        pipe1, pipe2 = Pipe("p1"), Pipe("p2")
        origin = OpenOrigin("o", sample_df)
        origin.add_output_pipe(pipe1).set_destination(f)
        f.add_output_pipe(pipe2).set_destination(dest)
        origin.pump()
        result = dest.last_df
        assert all(result["score"] >= 80)
        assert all(result["score"] <= 90)

    def test_missing_field_raises(self, sample_df):
        f = Filter("f", field="nonexistent", condition="=", value_or_values=1)
        dest = CaptureDest()
        pipe1, pipe2 = Pipe("p1"), Pipe("p2")
        origin = OpenOrigin("o", sample_df)
        origin.add_output_pipe(pipe1).set_destination(f)
        f.add_output_pipe(pipe2).set_destination(dest)
        with pytest.raises(ValueError, match="not found in DataFrame"):
            origin.pump()

    def test_invalid_condition_raises(self):
        with pytest.raises(ValueError, match="not supported"):
            Filter("f", field="x", condition="LIKE", value_or_values="a")

    def test_in_requires_list(self):
        with pytest.raises(ValueError, match="requires a list"):
            Filter("f", field="x", condition="in", value_or_values="a")

    def test_between_requires_two_values(self):
        with pytest.raises(ValueError, match="exactly 2 values"):
            Filter("f", field="x", condition="between", value_or_values=[1, 2, 3])


class TestDeleteColumns:
    def test_removes_columns(self, sample_df):
        dc = DeleteColumns("dc", columns=["id", "category"])
        dest = CaptureDest()
        pipe1, pipe2 = Pipe("p1"), Pipe("p2")
        origin = OpenOrigin("o", sample_df)
        origin.add_output_pipe(pipe1).set_destination(dc)
        dc.add_output_pipe(pipe2).set_destination(dest)
        origin.pump()
        result = dest.last_df
        assert "id" not in result.columns
        assert "category" not in result.columns
        assert "name" in result.columns

    def test_missing_column_raises(self, sample_df):
        dc = DeleteColumns("dc", columns=["no_such_col"])
        dest = CaptureDest()
        pipe1, pipe2 = Pipe("p1"), Pipe("p2")
        origin = OpenOrigin("o", sample_df)
        origin.add_output_pipe(pipe1).set_destination(dc)
        dc.add_output_pipe(pipe2).set_destination(dest)
        with pytest.raises(ValueError, match="not found in DataFrame"):
            origin.pump()

    def test_empty_list_raises(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            DeleteColumns("dc", columns=[])

    def test_non_list_raises(self):
        with pytest.raises(ValueError, match="must be a list"):
            DeleteColumns("dc", columns="id")


class TestAggregator:
    def test_count(self, sample_df):
        agg = Aggregator("agg", key="category", agg_field_name="total", agg_type="count")
        dest = CaptureDest()
        pipe1, pipe2 = Pipe("p1"), Pipe("p2")
        origin = OpenOrigin("o", sample_df)
        origin.add_output_pipe(pipe1).set_destination(agg)
        agg.add_output_pipe(pipe2).set_destination(dest)
        origin.pump()
        result = dest.last_df.set_index("category")
        assert result.loc["A", "total"] == 3
        assert result.loc["B", "total"] == 2

    def test_sum(self, sample_df):
        agg = Aggregator("agg", key="category", agg_field_name="total_score",
                         agg_type="sum", field_to_agg="score")
        dest = CaptureDest()
        pipe1, pipe2 = Pipe("p1"), Pipe("p2")
        origin = OpenOrigin("o", sample_df)
        origin.add_output_pipe(pipe1).set_destination(agg)
        agg.add_output_pipe(pipe2).set_destination(dest)
        origin.pump()
        result = dest.last_df.set_index("category")
        assert result.loc["A", "total_score"] == 90 + 70 + 95
        assert result.loc["B", "total_score"] == 80 + 85

    def test_missing_key_raises(self, sample_df):
        agg = Aggregator("agg", key="no_col", agg_field_name="n", agg_type="count")
        dest = CaptureDest()
        pipe1, pipe2 = Pipe("p1"), Pipe("p2")
        origin = OpenOrigin("o", sample_df)
        origin.add_output_pipe(pipe1).set_destination(agg)
        agg.add_output_pipe(pipe2).set_destination(dest)
        with pytest.raises(ValueError, match="key field"):
            origin.pump()

    def test_non_count_without_field_raises(self):
        with pytest.raises(ValueError, match="field_to_agg is required"):
            Aggregator("agg", key="x", agg_field_name="n", agg_type="sum")


class TestRemoveDuplicates:
    def test_keeps_first_by_id(self, sample_df):
        rd = RemoveDuplicates("rd", key="name", sort_by="score",
                              orientation="DESC", retain="FIRST")
        dest = CaptureDest()
        pipe1, pipe2 = Pipe("p1"), Pipe("p2")
        origin = OpenOrigin("o", sample_df)
        origin.add_output_pipe(pipe1).set_destination(rd)
        rd.add_output_pipe(pipe2).set_destination(dest)
        origin.pump()
        result = dest.last_df
        assert result["name"].nunique() == result["name"].count()
        # Alice has scores 90 and 95 — DESC+FIRST keeps 95
        alice_score = result[result["name"] == "Alice"]["score"].values[0]
        assert alice_score == 95

    def test_invalid_orientation_raises(self):
        with pytest.raises(ValueError, match="orientation"):
            RemoveDuplicates("rd", key="x", sort_by="y", orientation="RANDOM", retain="FIRST")

    def test_invalid_retain_raises(self):
        with pytest.raises(ValueError, match="retain"):
            RemoveDuplicates("rd", key="x", sort_by="y", orientation="ASC", retain="MIDDLE")


class TestCopy:
    def test_sends_to_all_outputs(self, sample_df):
        copy = Copy("copy")
        dest1, dest2, dest3 = CaptureDest(), CaptureDest(), CaptureDest()
        # wire: must override dest name for SingleInputMixin
        dest2.name = "capture2"
        dest3.name = "capture3"
        origin = OpenOrigin("o", sample_df)
        pipe_in = Pipe("in")
        origin.add_output_pipe(pipe_in).set_destination(copy)
        copy.add_output_pipe(Pipe("out1")).set_destination(dest1)
        copy.add_output_pipe(Pipe("out2")).set_destination(dest2)
        copy.add_output_pipe(Pipe("out3")).set_destination(dest3)
        origin.pump()
        assert dest1.last_df.equals(sample_df)
        assert dest2.last_df.equals(sample_df)
        assert dest3.last_df.equals(sample_df)

    def test_modifying_copy_doesnt_affect_others(self, sample_df):
        copy = Copy("copy")
        dest1, dest2 = CaptureDest(), CaptureDest()
        dest2.name = "capture2"
        origin = OpenOrigin("o", sample_df)
        origin.add_output_pipe(Pipe("in")).set_destination(copy)
        copy.add_output_pipe(Pipe("out1")).set_destination(dest1)
        copy.add_output_pipe(Pipe("out2")).set_destination(dest2)
        origin.pump()
        dest1.last_df["score"] = 0  # mutate copy 1
        assert dest2.last_df["score"].tolist() == sample_df["score"].tolist()


class TestFunnel:
    def test_merges_two_inputs(self):
        df_a = pd.DataFrame({'x': [1, 2]})
        df_b = pd.DataFrame({'x': [3, 4]})
        funnel = Funnel("funnel")
        dest = CaptureDest()
        origin_a = OpenOrigin("oa", df_a)
        origin_b = OpenOrigin("ob", df_b)
        pipe_a, pipe_b, pipe_out = Pipe("pa"), Pipe("pb"), Pipe("pout")
        origin_a.add_output_pipe(pipe_a).set_destination(funnel)
        origin_b.add_output_pipe(pipe_b).set_destination(funnel)
        funnel.add_output_pipe(pipe_out).set_destination(dest)
        origin_a.pump()
        origin_b.pump()
        result = dest.last_df
        assert len(result) == 4
        assert set(result["x"].tolist()) == {1, 2, 3, 4}


class TestSwitcher:
    def test_routes_by_value(self, sample_df):
        sw = Switcher("sw", field="category",
                      mapping={"A": "pipe_a", "B": "pipe_b"})
        dest_a, dest_b = CaptureDest(), CaptureDest()
        dest_b.name = "capture_b"
        origin = OpenOrigin("o", sample_df)
        pipe_in = Pipe("in")
        origin.add_output_pipe(pipe_in).set_destination(sw)
        sw.add_output_pipe(Pipe("pipe_a")).set_destination(dest_a)
        sw.add_output_pipe(Pipe("pipe_b")).set_destination(dest_b)
        origin.pump()
        assert all(dest_a.last_df["category"] == "A")
        assert all(dest_b.last_df["category"] == "B")

    def test_fail_on_unmatch_raises(self):
        df = pd.DataFrame({'cat': ['X', 'Y']})
        sw = Switcher("sw", field="cat", mapping={"A": "pipe_a"}, fail_on_unmatch=True)
        dest = CaptureDest()
        origin = OpenOrigin("o", df)
        origin.add_output_pipe(Pipe("in")).set_destination(sw)
        sw.add_output_pipe(Pipe("pipe_a")).set_destination(dest)
        with pytest.raises(ValueError, match="no mapping found"):
            origin.pump()

    def test_missing_field_raises(self, sample_df):
        sw = Switcher("sw", field="no_col", mapping={"A": "p"})
        dest = CaptureDest()
        origin = OpenOrigin("o", sample_df)
        origin.add_output_pipe(Pipe("in")).set_destination(sw)
        sw.add_output_pipe(Pipe("p")).set_destination(dest)
        with pytest.raises(ValueError, match="not found in DataFrame"):
            origin.pump()


class TestJoiner:
    def test_inner_join(self):
        left = pd.DataFrame({'id': [1, 2, 3], 'name': ['A', 'B', 'C']})
        right = pd.DataFrame({'id': [2, 3, 4], 'value': [20, 30, 40]})
        joiner = Joiner("j", left="left_pipe", right="right_pipe", key="id", join_type="inner")
        dest = CaptureDest()
        origin_l = OpenOrigin("ol", left)
        origin_r = OpenOrigin("or", right)
        pipe_l, pipe_r, pipe_out = Pipe("left_pipe"), Pipe("right_pipe"), Pipe("out")
        origin_l.add_output_pipe(pipe_l).set_destination(joiner)
        origin_r.add_output_pipe(pipe_r).set_destination(joiner)
        joiner.add_output_pipe(pipe_out).set_destination(dest)
        origin_l.pump()
        origin_r.pump()
        result = dest.last_df
        assert set(result["id"].tolist()) == {2, 3}
        assert "name" in result.columns
        assert "value" in result.columns

    def test_left_join(self):
        left = pd.DataFrame({'id': [1, 2, 3], 'name': ['A', 'B', 'C']})
        right = pd.DataFrame({'id': [2, 3], 'value': [20, 30]})
        joiner = Joiner("j", left="left_pipe", right="right_pipe", key="id", join_type="left")
        dest = CaptureDest()
        origin_l = OpenOrigin("ol", left)
        origin_r = OpenOrigin("or", right)
        pipe_l, pipe_r, pipe_out = Pipe("left_pipe"), Pipe("right_pipe"), Pipe("out")
        origin_l.add_output_pipe(pipe_l).set_destination(joiner)
        origin_r.add_output_pipe(pipe_r).set_destination(joiner)
        joiner.add_output_pipe(pipe_out).set_destination(dest)
        origin_l.pump()
        origin_r.pump()
        result = dest.last_df
        assert len(result) == 3
        assert result[result["id"] == 1]["value"].isna().all()

    def test_invalid_join_type_raises(self):
        with pytest.raises(ValueError, match="not supported"):
            Joiner("j", left="l", right="r", key="id", join_type="outer")

    def test_same_pipe_names_raises(self):
        with pytest.raises(ValueError, match="must be different"):
            Joiner("j", left="same", right="same", key="id", join_type="inner")

    def test_third_input_raises(self):
        joiner = Joiner("j", left="l", right="r", key="id", join_type="inner")
        joiner.add_input_pipe(Pipe("l"))
        joiner.add_input_pipe(Pipe("r"))
        with pytest.raises(ValueError, match="can only have 2 inputs"):
            joiner.add_input_pipe(Pipe("extra"))


class TestTransformer:
    def test_applies_function(self, sample_df):
        def double_score(df):
            df = df.copy()
            df["score"] = df["score"] * 2
            return df

        tr = Transformer("tr", transformer_function=double_score)
        dest = CaptureDest()
        origin = OpenOrigin("o", sample_df)
        pipe1, pipe2 = Pipe("p1"), Pipe("p2")
        origin.add_output_pipe(pipe1).set_destination(tr)
        tr.add_output_pipe(pipe2).set_destination(dest)
        origin.pump()
        result = dest.last_df
        assert result["score"].tolist() == [s * 2 for s in sample_df["score"].tolist()]

    def test_applies_function_with_kwargs(self, sample_df):
        def add_col(df, col_name, value):
            df = df.copy()
            df[col_name] = value
            return df

        tr = Transformer("tr", transformer_function=add_col,
                         transformer_kwargs={"col_name": "new_col", "value": 42})
        dest = CaptureDest()
        origin = OpenOrigin("o", sample_df)
        pipe1, pipe2 = Pipe("p1"), Pipe("p2")
        origin.add_output_pipe(pipe1).set_destination(tr)
        tr.add_output_pipe(pipe2).set_destination(dest)
        origin.pump()
        assert "new_col" in dest.last_df.columns
        assert all(dest.last_df["new_col"] == 42)

    def test_non_dataframe_return_raises(self, sample_df):
        def bad_func(df):
            return [1, 2, 3]

        tr = Transformer("tr", transformer_function=bad_func)
        dest = CaptureDest()
        origin = OpenOrigin("o", sample_df)
        pipe1, pipe2 = Pipe("p1"), Pipe("p2")
        origin.add_output_pipe(pipe1).set_destination(tr)
        tr.add_output_pipe(pipe2).set_destination(dest)
        with pytest.raises(ValueError, match="must return a pandas DataFrame"):
            origin.pump()

    def test_none_function_raises(self):
        with pytest.raises(ValueError, match="cannot be None"):
            Transformer("tr", transformer_function=None)

    def test_non_callable_raises(self):
        with pytest.raises(ValueError, match="must be callable"):
            Transformer("tr", transformer_function="not_a_function")

    def test_invalid_kwarg_raises(self):
        def func(df):
            return df

        with pytest.raises(ValueError, match="not defined in function"):
            Transformer("tr", transformer_function=func,
                        transformer_kwargs={"nonexistent_param": 1})


class TestCSVRoundtrip:
    def test_write_and_read(self, sample_df, tmp_path):
        csv_path = str(tmp_path / "test.csv")
        dest = CSVDestination("csv_out", path_or_buf=csv_path, index=False)
        origin = OpenOrigin("o", sample_df)
        pipe = Pipe("p1")
        origin.add_output_pipe(pipe).set_destination(dest)
        origin.pump()

        assert os.path.exists(csv_path)
        result = pd.read_csv(csv_path)
        assert list(result.columns) == list(sample_df.columns)
        assert len(result) == len(sample_df)
