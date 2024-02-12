# pylint: disable=redefined-outer-name
import pytest

from earthquake_data_layer import Fetcher
from earthquake_data_layer.definitions import MAX_RESULTS_PER_REQUEST_


@pytest.fixture()
def mock_start_date():
    return "2021-03-01"


@pytest.fixture()
def mock_end_date():
    return "2021-03-31"


@pytest.fixture(scope="function")
def mock_fetcher(mock_start_date, mock_end_date):
    return Fetcher(start_date=mock_start_date, end_date=mock_end_date)


@pytest.fixture
def mock_response_data():
    return {"col1": "val1", "col2": "val2"}


@pytest.fixture
def data_point_1(mock_response_data):
    return {"id": 2, "properties": mock_response_data}


@pytest.fixture
def data_point_2(mock_response_data):
    return {
        "id": 1,
        "properties": {val: key for key, val in mock_response_data.items()},
    }


@pytest.fixture
def last_response_content(data_point_2):
    return {
        "metadata": {"status": 200, "count": MAX_RESULTS_PER_REQUEST_ // 2},
        "features": [data_point_2],
    }


@pytest.fixture
def first_response_content(data_point_1):
    return {
        "metadata": {"status": 200, "count": MAX_RESULTS_PER_REQUEST_},
        "features": [data_point_1],
    }


@pytest.fixture
def expected_key():
    return "data/raw_data/2021/2021_03_raw_data.parquet"


@pytest.fixture
def expected_data(mock_response_data):
    return [
        {"id": 2, **mock_response_data},
        {"id": 1, **{val: key for key, val in mock_response_data.items()}},
    ]


@pytest.fixture
def expected_count(first_response_content, last_response_content):
    return (
        first_response_content["metadata"]["count"]
        + last_response_content["metadata"]["count"]
    )
