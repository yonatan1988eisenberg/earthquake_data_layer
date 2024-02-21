from unittest.mock import patch

from earthquake_data_layer import definitions
from earthquake_data_layer.helpers import generate_raw_data_key_from_date


def test_success(expected_key, expected_data, mock_fetcher):
    # if failing here check the feature start_date that is used in the feature mock_fetcher
    assert expected_key == generate_raw_data_key_from_date(
        mock_fetcher.year, mock_fetcher.month
    )

    mock_fetcher.data = expected_data

    with patch(
        "earthquake_data_layer.fetcher.add_rows_to_parquet", return_value=True
    ) as mock_upload:
        result = mock_fetcher.upload_data()

        assert not result.get("error")
        assert result.get("status") == definitions.STATUS_UPLOAD_DATA_SUCCESS

        mock_upload.assert_called_once_with(expected_data, expected_key)


def test_failed(expected_key, expected_data, mock_fetcher):
    expected_key = "data/raw_data/2021/2021_03_raw_data.parquet"

    # if failing here check the feature start_date that is used in the feature mock_fetcher
    assert expected_key == generate_raw_data_key_from_date(
        mock_fetcher.year, mock_fetcher.month
    )

    mock_fetcher.data = expected_data

    with patch(
        "earthquake_data_layer.fetcher.add_rows_to_parquet", return_value=False
    ) as mock_upload:
        result = mock_fetcher.upload_data()

        assert result.get("error") is True
        assert result.get("status") == definitions.STATUS_UPLOAD_DATA_FAIL

        mock_upload.assert_called_once_with(expected_data, expected_key)
