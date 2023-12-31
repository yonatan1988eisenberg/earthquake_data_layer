from unittest.mock import mock_open, patch

from freezegun import freeze_time

from earthquake_data_layer import Metadata, definitions


# pylint: disable=unused-argument
def test_get_remaining_requests_no_key(get_blank_metadata):
    result = Metadata.get_remaining_requests("nonexistent_key")

    assert result == definitions.MAX_REQUESTS_PER_DAY


def test_get_remaining_requests_key_not_used_today():
    mock_metadata_file_content = '{"keys": {"api_key": {"2023-01-01": 100}}}'
    with patch("builtins.open", mock_open(read_data=mock_metadata_file_content)):
        result = Metadata.get_remaining_requests("api_key")

    assert result == definitions.MAX_REQUESTS_PER_DAY


@freeze_time("2023-12-27")
def test_get_remaining_requests_key_used_today():
    mock_metadata_file_content = '{"keys": {"api_key": {"2023-12-27": 50}}}'
    with patch("builtins.open", mock_open(read_data=mock_metadata_file_content)):
        result = Metadata.get_remaining_requests("api_key")

    assert result == 50


@freeze_time("2023-12-27")
def test_update_remaining_requests():
    mock_metadata = {"keys": {"api_key": {"2023-12-27": 100}}}

    with patch(
        "earthquake_data_layer.Metadata.get_metadate", return_value=mock_metadata
    ), patch(
        "earthquake_data_layer.Metadata._update_metadata", return_value=True
    ) as _update_metadata, patch(
        "builtins.open", mock_open()
    ):

        result = Metadata.update_remaining_requests("api_key", 50)

    expected_metadata = mock_metadata
    expected_metadata["keys"]["api_key"] = {"2023-12-27": 50}

    assert result is True
    _update_metadata.assert_called_with(expected_metadata)
