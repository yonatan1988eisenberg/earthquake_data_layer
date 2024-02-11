# pylint: disable=redefined-outer-name
from unittest.mock import patch

import pytest
import requests

from earthquake_data_layer.definitions import MAX_RESULTS_PER_REQUEST_
from tests.utils import MockApiResponse


@pytest.fixture
def last_response_content():
    return {"metadata": {"status": 200, "count": MAX_RESULTS_PER_REQUEST_ // 2}}


@pytest.fixture
def first_response_content():
    return {"metadata": {"status": 200, "count": MAX_RESULTS_PER_REQUEST_}}


def test_vanilla(mock_fetcher, last_response_content):
    with patch(
        "earthquake_data_layer.fetcher.requests.get",
        return_value=MockApiResponse(content=last_response_content),
    ):
        result = mock_fetcher.query_api(query_params={})

        assert not result.get("error")
        assert result.get("status")


def test_error(mock_fetcher):
    with patch("earthquake_data_layer.fetcher.requests.get") as mock_response:
        mock_response.side_effect = requests.RequestException
        result = mock_fetcher.query_api(query_params={})

        assert result.get("error")
        assert not result.get("status")


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
        assert result.get("status")
        assert len(mock_fetcher.responses) == len(expected_responses)
