import json
from unittest.mock import mock_open, patch

from earthquake_data_layer import MetadataManager, definitions


def test_update_metadata_local_success(blank_metadata):
    metadata_manager = MetadataManager(blank_metadata, local=True)
    with patch("builtins.open", mock_open()) as mock_file:
        result = metadata_manager._save_metadata()

    assert result is True
    mock_file.assert_called_once_with(
        definitions.METADATA_LOCATION, "w", encoding="utf-8"
    )
    mock_file.return_value.write.assert_called()


def test_update_metadata_local_failure(blank_metadata):
    metadata_manager = MetadataManager(blank_metadata, local=True)
    with patch("builtins.open", side_effect=Exception("File write error")):
        result = metadata_manager._save_metadata()

    assert not result


# pylint: disable=unused-argument
def test_get_metadate_local_success(blank_metadata):

    with patch("builtins.open", mock_open(read_data=json.dumps(blank_metadata))):
        metadata_manager = MetadataManager(local=True)

    assert metadata_manager.metadata == blank_metadata


def test_get_metadata_local_failure():

    with patch("builtins.open", side_effect=FileNotFoundError("File read error")):
        metadata_manager = MetadataManager(local=True)

    assert metadata_manager.metadata == {}


def test_get_metadate_remote_no_such_file(storage, test_bucket):

    with patch("earthquake_data_layer.storage.Storage", return_value=storage):
        metadata_manager = MetadataManager(local=False, bucket=test_bucket)
    assert metadata_manager.metadata == {}


def test_get_metadate_remote_success(storage, test_bucket, blank_metadata):
    content = json.dumps(blank_metadata, indent=2).encode("utf-8")
    storage.save_object(content, definitions.METADATA_KEY, test_bucket)

    with patch("earthquake_data_layer.storage.Storage", return_value=storage):
        metadata_manager = MetadataManager(local=False, bucket=test_bucket)
        assert metadata_manager.metadata == blank_metadata


def test_update_metadata_remote_success(storage, test_bucket, blank_metadata):
    with patch("earthquake_data_layer.storage.Storage", return_value=storage):
        metadata_manager = MetadataManager(
            blank_metadata, local=False, bucket=test_bucket
        )
        result = metadata_manager._save_metadata()

    assert result is True


def test_update_metadata_remote_failure(storage, test_bucket, blank_metadata):
    with patch("earthquake_data_layer.storage.Storage", return_value=storage):
        metadata_manager = MetadataManager(
            blank_metadata, local=False, bucket="non_exiting_bucket"
        )
        result = metadata_manager._save_metadata()

    assert result is False
