# pylint: disable=redefined-outer-name
import pytest

from earthquake_data_layer import Fetcher


@pytest.fixture()
def mock_start_date():
    return "2021-03-01"


@pytest.fixture()
def mock_end_date():
    return "2021-03-31"


@pytest.fixture(scope="function")
def mock_fetcher(mock_start_date, mock_end_date):
    return Fetcher(start_date=mock_start_date, end_date=mock_end_date)
