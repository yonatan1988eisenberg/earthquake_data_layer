# pylint: disable=redefined-outer-name
import pytest

from earthquake_data_layer import definitions


@pytest.fixture(scope="function")
def expected_params(mock_start_date, mock_end_date):
    return {
        "startdate": mock_start_date,
        "enddate": mock_end_date,
        "limit": definitions.MAX_RESULTS_PER_REQUEST,
        "offset": 1,
        "format": "geojson",
    }


def test_no_overwrite(mock_fetcher, expected_params):
    assert mock_fetcher.generate_query_params() == expected_params


def test_overwrite(mock_fetcher, expected_params):
    new_mock_end_date = "2021-04-01"
    expected_params["end_date"] = new_mock_end_date

    assert (
        mock_fetcher.generate_query_params({"end_date": new_mock_end_date})
        == expected_params
    )
