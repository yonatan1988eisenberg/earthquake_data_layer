import datetime
from copy import deepcopy

import pytest

from earthquake_data_layer import Downloader, MetadataManager, definitions, settings


def test_get_base_query_params_update_mode(blank_metadata):
    expected_start_date = datetime.date.today() - datetime.timedelta(
        days=settings.UPDATE_TIME_DELTA_DAYS
    )
    expected_start_date = expected_start_date.strftime(definitions.DATE_FORMAT)
    expected_end_date = datetime.date.today().strftime(definitions.DATE_FORMAT)

    downloader = Downloader(MetadataManager(blank_metadata), mode="update")
    base_query_params = downloader.get_base_query_params()

    assert base_query_params == {
        "startDate": expected_start_date,
        "endDate": expected_end_date,
        "type": settings.DATA_TYPE_TO_FETCH,
    }


def test_get_base_query_params_collection_mode(blank_metadata):
    expected_start = "2023-01-01"
    expected_end = "2023-02-01"

    mock_metadata = deepcopy(blank_metadata)
    mock_metadata["collection_dates"]["start_date"] = expected_start
    mock_metadata["collection_dates"]["end_date"] = expected_end

    downloader = Downloader(MetadataManager(mock_metadata))
    base_query_params = downloader.get_base_query_params()

    assert base_query_params == {
        "type": settings.DATA_TYPE_TO_FETCH,
        "endDate": expected_end,
        "startDate": expected_start,
    }


def test_get_base_query_params_invalid_start_date(blank_metadata):
    expected_start = "invalid_date_format"

    mock_metadata = deepcopy(blank_metadata)
    mock_metadata["collection_dates"]["start_date"] = expected_start

    downloader = Downloader(MetadataManager(mock_metadata))
    with pytest.raises(
        ValueError,
        match=r".* needs to be a datetime.date object or string in YYYY-MM-DD format",
    ):
        downloader.get_base_query_params()
