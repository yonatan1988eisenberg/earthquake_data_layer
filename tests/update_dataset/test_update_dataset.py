# pylint: disable=redefined-outer-name
from unittest.mock import patch

import pytest

from earthquake_data_layer import definitions
from update_dataset import update_dataset


@pytest.fixture
def successful_run():
    return {"status": definitions.STATUS_COLLECTION_METADATA_COMPLETE}


@pytest.fixture
def unsuccessful_run():
    return {"status": definitions.STATUS_COLLECTION_METADATA_INCOMPLETE}


def test_complete(successful_run):
    with patch(
        "earthquake_data_layer.helpers.fetch_months_data",
        return_value=successful_run,
    ):
        result = update_dataset(2020, 1)

        assert result.get("status") == definitions.STATUS_COLLECTION_METADATA_COMPLETE


def test_incomplete(unsuccessful_run):

    with patch(
        "earthquake_data_layer.helpers.fetch_months_data",
        return_value=unsuccessful_run,
    ):
        result = update_dataset(2020, 1)

        assert result.get("status") == definitions.STATUS_COLLECTION_METADATA_INCOMPLETE
