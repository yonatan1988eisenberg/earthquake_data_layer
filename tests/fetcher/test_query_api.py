from unittest.mock import patch

import requests

from earthquake_data_layer import definitions
from tests.utils import MockApiResponse


def test_vanilla(mock_fetcher, last_response_content):
    with patch(
        "earthquake_data_layer.fetcher.requests.get",
        return_value=MockApiResponse(content=last_response_content),
    ):
        result = mock_fetcher.query_api(query_params={})

        assert not result.get("error")
        assert result.get("status") == definitions.STATUS_QUERY_API_SUCCESS


def test_error(mock_fetcher):
    expected_error = requests.RequestException()

    with patch("earthquake_data_layer.fetcher.requests.get") as mock_response:
        mock_response.side_effect = expected_error
        result = mock_fetcher.query_api(query_params={})

        assert result.get("error") == expected_error
        assert result.get("status") == definitions.STATUS_QUERY_API_FAIL


def test_multiple_responses(
    mock_fetcher, first_response_content, last_response_content
):
    expected_responses = [
        MockApiResponse(content=first_response_content),
        MockApiResponse(content=last_response_content),
    ]

    with patch("earthquake_data_layer.fetcher.requests.get") as mock_response:
        mock_response.side_effect = expected_responses
        result = mock_fetcher.query_api(query_params={})

        assert not result.get("error")
        assert result.get("status") == definitions.STATUS_QUERY_API_SUCCESS
        assert len(mock_fetcher.responses) == len(expected_responses)
