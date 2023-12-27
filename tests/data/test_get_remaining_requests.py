from unittest.mock import mock_open, patch

from earthquake_data_layer import Data, definitions


def test_get_remaining_requests_no_key():
    result = Data.get_remaining_requests("nonexistent_key")

    assert result == definitions.MAX_REQUESTS_PER_DAY


def test_get_remaining_requests_key_not_used_today():
    mock_metadata_file_content = '{"keys": {"api_key": {"2023-01-01": 100}}, "latest_update": {"date": "2023-01-01", "offset": 1}}'
    with patch("builtins.open", mock_open(read_data=mock_metadata_file_content)):
        result = Data.get_remaining_requests("api_key")

    assert result == definitions.MAX_REQUESTS_PER_DAY


def test_get_remaining_requests_key_used_today():
    mock_metadata_file_content = '{"keys": {"api_key": {"2023-12-27": 50}}, "latest_update": {"date": "2023-12-27", "offset": 1}}'
    with patch("builtins.open", mock_open(read_data=mock_metadata_file_content)):
        result = Data.get_remaining_requests("api_key")

    assert result == 50
