# pylint: disable=unused-argument
from unittest.mock import mock_open, patch

from earthquake_data_layer import Data


def test_get_latest_update_existing_date(mock_open_function):
    result = Data.get_latest_update()
    assert result == ("2023-01-01", 1)


def test_get_latest_update_default_date(mock_open_function):
    mock_metadata_file_content = '{"latest_update": {}}'
    with patch("builtins.open", mock_open(read_data=mock_metadata_file_content)):
        result = Data.get_latest_update()
    assert result == ("1638-01-01", 1)


def test_update_latest_update_success(mock_open_function):
    result = Data.update_latest_update("2023-01-01", 2)
    assert result is True


def test_update_latest_update_failure(mock_open_function):
    mock_open_instance = mock_open_function
    mock_open_instance().write.side_effect = Exception("File write error")

    result = Data.update_latest_update("2023-01-01", 2)
    assert result is False
