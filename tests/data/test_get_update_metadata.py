from unittest.mock import patch

from earthquake_data_layer import Data, definitions


def test_update_metadata_success(mock_open_function):
    mock_open_instance = mock_open_function
    mock_metadata = {"keys": {"api_key": {}}}

    result = Data.update_metadata(mock_metadata)

    assert result is True
    mock_open_instance.assert_called_once_with(
        definitions.METADATA_LOCATION, "w", encoding="utf-8"
    )


def test_update_metadata_failure(mock_open_function):
    mock_open_instance = mock_open_function
    mock_metadata = {"keys": {"api_key": {}}}

    with patch("json.dump", side_effect=Exception("File write error")):
        result = Data.update_metadata(mock_metadata)

    assert result is False
    mock_open_instance.assert_called_once_with(
        definitions.METADATA_LOCATION, "w", encoding="utf-8"
    )


# pylint: disable=unused-argument
def test_get_metadate(mock_open_function):
    result = Data.get_metadate()

    assert result == {"keys": {}, "latest_update": {"date": "2023-01-01", "offset": 1}}
