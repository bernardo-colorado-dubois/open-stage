import pandas as pd
import pytest
from open_stage.core.base import DataPackage, Destination, SingleInputMixin


class CaptureDest(SingleInputMixin, Destination):
    """Test helper — records all DataPackages received via sink()."""
    def __init__(self):
        super().__init__()
        self.name = "capture"
        self.received: list = []

    def sink(self, data_package: DataPackage) -> None:
        self.received.append(data_package)

    @property
    def last_df(self) -> pd.DataFrame:
        return self.received[-1].get_df()


@pytest.fixture
def sample_df():
    return pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'Alice', 'Bob'],
        'score': [90, 80, 70, 95, 85],
        'category': ['A', 'B', 'A', 'A', 'B'],
    })
