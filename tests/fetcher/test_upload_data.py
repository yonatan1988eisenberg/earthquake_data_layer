# pylint: disable=redefined-outer-name
from unittest.mock import patch

import pytest

from earthquake_data_layer.helpers import generate_raw_data_key_from_date


@pytest.fixture
def expected_key():
    return "data/raw_data/2021/2021_03_raw_data.parquet"


@pytest.fixture
def expected_data(mock_response_data):
    return [
        {"id": 2, **mock_response_data},
        {"id": 1, **{val: key for key, val in mock_response_data.items()}},
    ]


def test_success(expected_key, expected_data, mock_fetcher):
    # if failing here check the feature start_date that is used in the feature mock_fetcher
    assert expected_key == generate_raw_data_key_from_date(
        mock_fetcher.year, mock_fetcher.month
    )

    mock_fetcher.data = expected_data

    with patch(
        "earthquake_data_layer.helpers.add_rows_to_parquet", return_value=True
    ) as mock_upload:
        result = mock_fetcher.upload_data()

        assert not result.get("error")
        assert result.get("status")

        mock_upload.assert_called_once_with(expected_data, expected_key)


def test_failed(expected_key, expected_data, mock_fetcher):
    expected_key = "data/raw_data/2021/2021_03_raw_data.parquet"

    # if failing here check the feature start_date that is used in the feature mock_fetcher
    assert expected_key == generate_raw_data_key_from_date(
        mock_fetcher.year, mock_fetcher.month
    )

    mock_fetcher.data = expected_data

    with patch(
        "earthquake_data_layer.helpers.add_rows_to_parquet", return_value=False
    ) as mock_upload:
        result = mock_fetcher.upload_data()

        assert result.get("error")
        assert not result.get("status")

        mock_upload.assert_called_once_with(expected_data, expected_key)
