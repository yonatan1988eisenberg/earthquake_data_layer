import json
from unittest.mock import mock_open, patch

from earthquake_data_layer import Metadata, definitions


def test_update_metadata_success(get_blank_metadata):
    with patch("builtins.open", mock_open()) as mock_file:
        result = Metadata._update_metadata(get_blank_metadata)

    assert result is True
    mock_file.assert_called_once_with(
        definitions.METADATA_LOCATION, "w", encoding="utf-8"
    )
    mock_file.return_value.write.assert_called_once_with(json.dumps(get_blank_metadata))


def test_update_metadata_failure(get_blank_metadata):
    with patch("builtins.open", side_effect=Exception("File write error")):
        result = Metadata._update_metadata(get_blank_metadata)

    assert result is False


# pylint: disable=unused-argument
def test_get_metadate(get_blank_metadata):
    result = Metadata.get_metadate()

    assert result == {
        "keys": {},
        "collection_dates": {
            "start_date": False,
            "end_date": False,
            "offset": 1,
            "collection_start_date": False,
        },
    }
