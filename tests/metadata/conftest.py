# pylint: disable=redefined-outer-name
from unittest.mock import patch

import pytest


@pytest.fixture
def blank_metadata():
    return {
        "collection_dates": {
            "start_date": False,
            "end_date": False,
            "offset": 1,
            "collection_start_date": False,
        },
        "keys": {},
    }


@pytest.fixture
def get_blank_metadata(blank_metadata):
    with patch(
        "earthquake_data_layer.Metadata.get_metadate", return_value=blank_metadata
    ):
        yield
