# pylint: disable=redefined-outer-name,unused-argument
from unittest.mock import patch

import pytest

from earthquake_data_layer import Downloader, MetadataManager


@pytest.fixture
def mock_remaining_requests():
    return 150


@pytest.fixture
def mock_data_get_remaining_requests(mock_remaining_requests):
    with patch.object(
        MetadataManager, "key_remaining_requests", return_value=mock_remaining_requests
    ):
        yield


def test_get_num_results_max(mock_data_get_remaining_requests):
    result = Downloader.get_num_results("max")
    assert (
        result == 0
    )  # Assuming 150 remaining requests and MAX_RESULTS_PER_REQUEST is 1000


def test_get_num_results_integer(mock_data_get_remaining_requests):
    result = Downloader.get_num_results(50000)
    assert result == 0  # If 50000 is within the available API requests limit


def test_get_num_results_integer_zero(mock_data_get_remaining_requests):
    with pytest.raises(
        ValueError, match="num_results must be an integer greater than 0"
    ):
        Downloader.get_num_results(0)


def test_get_num_results_invalid_input(mock_data_get_remaining_requests):
    with pytest.raises(
        ValueError, match="num_results must be an integer greater than 0"
    ):
        Downloader.get_num_results("invalid_input")
