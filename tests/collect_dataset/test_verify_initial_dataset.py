# pylint: disable=redefined-outer-name
import json
from unittest.mock import patch

import pytest

from collect_dataset import verify_initial_dataset
from earthquake_data_layer import definitions


@pytest.fixture
def successful_run():
    return {"status": definitions.STATUS_COLLECTION_METADATA_COMPLETE}


@pytest.fixture
def unsuccessful_run():
    return {"status": definitions.STATUS_COLLECTION_METADATA_INCOMPLETE}


def test_no_metadata_file_complete(storage, successful_run):
    with patch("collect_dataset.Storage", return_value=storage):
        with patch.object(storage, "list_objects", return_value=[]):
            with patch(
                "earthquake_data_layer.helpers.fetch_months_data",
                return_value=successful_run,
            ):
                result = verify_initial_dataset()

                assert result


def test_no_metadata_file_incomplete(storage, unsuccessful_run):
    with patch("collect_dataset.Storage", return_value=storage):
        with patch.object(storage, "list_objects", return_value=[]):
            with patch(
                "earthquake_data_layer.helpers.fetch_months_data",
                return_value=unsuccessful_run,
            ):
                result = verify_initial_dataset()

                assert not result


def test_complete(storage, successful_run):
    storage.client.put_object(
        Bucket=storage.bucket_name,
        Key=definitions.COLLECTION_METADATA_KEY,
        Body=bytes(json.dumps(successful_run, indent=2).encode("utf-8")),
    )
    with patch("collect_dataset.Storage", return_value=storage):
        result = verify_initial_dataset()
        assert result


def test_incomplete(storage, unsuccessful_run):
    storage.client.put_object(
        Bucket=storage.bucket_name,
        Key=definitions.COLLECTION_METADATA_KEY,
        Body=bytes(json.dumps(unsuccessful_run, indent=2).encode("utf-8")),
    )
    with patch("collect_dataset.Storage", return_value=storage):
        with patch(
            "earthquake_data_layer.helpers.fetch_months_data",
            return_value=unsuccessful_run,
        ):
            result = verify_initial_dataset()

            assert not result
