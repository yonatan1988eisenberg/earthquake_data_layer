import json
from unittest.mock import mock_open, patch

from earthquake_data_layer import Metadata, definitions


def test_update_metadata_local_success(get_blank_metadata):
    with patch("builtins.open", mock_open()) as mock_file:
        result = Metadata._update_metadata(get_blank_metadata, local=True)

    assert result is True
    mock_file.assert_called_once_with(
        definitions.METADATA_LOCATION, "w", encoding="utf-8"
    )
    mock_file.return_value.write.assert_called_once_with(json.dumps(get_blank_metadata))


def test_update_metadata_local_failure(get_blank_metadata):
    with patch("builtins.open", side_effect=Exception("File write error")):
        result = Metadata._update_metadata(get_blank_metadata, local=True)

    assert result is False


# pylint: disable=unused-argument
def test_get_metadate_local_success(get_blank_metadata, blank_metadata):
    result = Metadata.get_metadate(local=True)

    assert result == blank_metadata


def test_get_metadata_local_failure():
    with patch("builtins.open", side_effect=FileNotFoundError("File read error")):
        result = Metadata.get_metadate(local=True)

    assert result == {}


def test_get_metadate_remote_no_such_file(storage, test_bucket):
    with patch("earthquake_data_layer.storage.Storage", return_value=storage):
        metadata = Metadata.get_metadate(local=False, bucket=test_bucket)
        assert metadata == {}


def test_get_metadate_remote_success(storage, test_bucket, blank_metadata):
    content = json.dumps(blank_metadata, indent=2).encode("utf-8")
    storage.save_object(content, definitions.METADATA_KEY, test_bucket)

    with patch("earthquake_data_layer.storage.Storage", return_value=storage):
        metadata = Metadata.get_metadate(local=False, bucket=test_bucket)
        assert metadata == blank_metadata


def test_update_metadata_remote_success(storage, test_bucket, blank_metadata):
    with patch("earthquake_data_layer.storage.Storage", return_value=storage):
        result = Metadata._update_metadata(
            blank_metadata, local=False, bucket=test_bucket
        )

    assert result is True


def test_update_metadata_remote_failure(storage, test_bucket, blank_metadata):
    with patch("earthquake_data_layer.storage.Storage", return_value=storage):
        result = Metadata._update_metadata(
            blank_metadata, local=False, bucket="non_exiting_bucket"
        )

    assert result is False
