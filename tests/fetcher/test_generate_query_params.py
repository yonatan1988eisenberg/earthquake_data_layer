# pylint: disable=redefined-outer-name
import pytest

from earthquake_data_layer import Fetcher, definitions


@pytest.fixture()
def mock_start_date():
    return "2021-03-01"


@pytest.fixture()
def mock_end_date():
    return "2021-03-31"


@pytest.fixture(scope="function")
def expected_params(mock_start_date, mock_end_date):
    return {
        "start_date": mock_start_date,
        "end_date": mock_end_date,
        "limit": definitions.MAX_RESULTS_PER_REQUEST_,
        "offset": 1,
        "format": "geojson",
    }


@pytest.fixture(scope="function")
def mock_fetcher(mock_start_date, mock_end_date):
    return Fetcher(start_date=mock_start_date, end_date=mock_end_date)


def test_no_overwrite(mock_fetcher, expected_params):
    assert mock_fetcher.generate_query_params() == expected_params


def test_overwrite(mock_fetcher, expected_params):
    new_mock_end_date = "2021-04-01"
    expected_params["end_date"] = new_mock_end_date

    assert (
        mock_fetcher.generate_query_params({"end_date": new_mock_end_date})
        == expected_params
    )
