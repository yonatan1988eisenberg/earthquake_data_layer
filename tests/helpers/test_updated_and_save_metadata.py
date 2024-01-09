import datetime
from unittest.mock import MagicMock

from earthquake_data_layer import MetadataManager, definitions, helpers, settings


def test_updated_and_save_metadata_first_run(mock_run_metadata):
    expected_start_date = settings.EARLIEST_EARTHQUAKE_DATE
    expected_end_date = mock_run_metadata["next_run_dates"]["earliest_date"]
    expected_offset = mock_run_metadata["next_run_dates"]["offset"]
    expected_collection_start_date = definitions.TODAY.strftime(definitions.DATE_FORMAT)

    metadata_manager = MetadataManager(metadata={})
    metadata_manager.update_collection_dates = MagicMock(return_value=True)
    result = helpers.updated_and_save_metadata(metadata_manager, mock_run_metadata)

    assert result is True
    assert metadata_manager.update_collection_dates.call_args.kwargs == {
        "start_date": expected_start_date,
        "end_date": expected_end_date,
        "offset": expected_offset,
        "collection_start_date": expected_collection_start_date,
        "save": True,
    }


def test_updated_and_save_metadata_last_run(mock_run_metadata, blank_metadata):
    # mock metadata - a new cycle began today, collected from yesterday to yesterday
    blank_metadata["collection_dates"]["start_date"] = (
        definitions.YESTERDAY - datetime.timedelta(days=1)
    ).strftime(definitions.DATE_FORMAT)

    mock_run_metadata["next_run_dates"]["earliest_date"] = blank_metadata[
        "collection_dates"
    ]["start_date"]

    expected_call_kwargs = {
        "start_date": settings.EARLIEST_EARTHQUAKE_DATE,
        "end_date": definitions.YESTERDAY.strftime(definitions.DATE_FORMAT),
        "offset": False,
        "collection_start_date": False,
        "save": True,
    }
    metadata_manager = MetadataManager(metadata=blank_metadata)

    metadata_manager.update_collection_dates = MagicMock(return_value=True)
    result = helpers.updated_and_save_metadata(metadata_manager, mock_run_metadata)

    assert result is True
    assert metadata_manager.update_collection_dates.call_args.kwargs == {
        **expected_call_kwargs
    }


def test_updated_and_save_metadata_end_of_cycle(mock_run_metadata, blank_metadata):
    # collecting the whole 2022 year, started at 2023-01-01 and needed 2 runs, next cycle will collect from 2022-12-31
    expected_start_date = "2022-12-31"
    expected_end_date = False
    expected_offset = False
    expected_collection_start_date = False

    blank_metadata["collection_dates"]["start_date"] = "2022-01-01"
    blank_metadata["collection_dates"]["end_date"] = "2022-06-01"
    blank_metadata["collection_dates"]["collection_start_date"] = "2023-01-01"
    mock_run_metadata["next_run_dates"]["earliest_date"] = "2022-01-01"

    metadata_manager = MetadataManager(metadata=blank_metadata)
    metadata_manager.update_collection_dates = MagicMock(return_value=True)
    result = helpers.updated_and_save_metadata(metadata_manager, mock_run_metadata)

    assert result is True
    assert metadata_manager.update_collection_dates.call_args.kwargs == {
        "start_date": expected_start_date,
        "end_date": expected_end_date,
        "offset": expected_offset,
        "collection_start_date": expected_collection_start_date,
        "save": True,
    }


def test_updated_and_save_metadata_done_collecting(mock_run_metadata, blank_metadata):
    # we started a new cycle today and was able to collect as far as start_date
    expected_start_date = settings.EARLIEST_EARTHQUAKE_DATE
    expected_end_date = definitions.YESTERDAY.strftime(definitions.DATE_FORMAT)
    expected_offset = False
    expected_collection_start_date = False

    blank_metadata["collection_dates"]["start_date"] = expected_start_date

    mock_run_metadata["next_run_dates"]["earliest_date"] = expected_start_date

    metadata_manager = MetadataManager(metadata=blank_metadata)
    metadata_manager.update_collection_dates = MagicMock(return_value=True)

    result = helpers.updated_and_save_metadata(metadata_manager, mock_run_metadata)

    assert result is True
    assert metadata_manager.update_collection_dates.call_args.kwargs == {
        "start_date": expected_start_date,
        "end_date": expected_end_date,
        "offset": expected_offset,
        "collection_start_date": expected_collection_start_date,
        "save": True,
    }
    assert blank_metadata["done_collecting"] is True
