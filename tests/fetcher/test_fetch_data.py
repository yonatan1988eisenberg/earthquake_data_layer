# pylint: disable=redefined-outer-name
from unittest.mock import patch

import pytest
import requests

from earthquake_data_layer import definitions
from tests.utils import MockApiResponse


@pytest.fixture
def expected_metadata(mock_start_date, mock_end_date, expected_count):
    return {
        "start_date": mock_start_date,
        "end_date": mock_end_date,
        "count": expected_count,
        "execution_date": definitions.TODAY,
        "status": definitions.STATUS_UPLOAD_DATA_SUCCESS,
    }


def test_success(
    mock_fetcher, first_response_content, last_response_content, expected_metadata
):
    expected_responses = [
        MockApiResponse(content=first_response_content),
        MockApiResponse(content=last_response_content),
    ]

    with patch("earthquake_data_layer.fetcher.requests.get") as mock_response:
        with patch(
            "earthquake_data_layer.helpers.add_rows_to_parquet", return_value=True
        ):
            mock_response.side_effect = expected_responses
            result = mock_fetcher.fetch_data()

            assert result == expected_metadata


def test_error_api(mock_fetcher, expected_metadata):
    expected_error = requests.RequestException()
    expected_metadata.update(
        {"status": definitions.STATUS_QUERY_API_FAILED, "error": expected_error}
    )
    expected_metadata.pop("count")

    with patch("earthquake_data_layer.fetcher.requests.get") as mock_response:
        mock_response.side_effect = expected_error
        result = mock_fetcher.fetch_data()

        assert result == expected_metadata
