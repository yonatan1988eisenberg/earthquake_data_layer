from copy import deepcopy
from unittest.mock import patch

from earthquake_data_layer import MetadataManager, definitions, settings


def test_get_collection_dates_default():
    metadata_manager = MetadataManager({"an_empty_dict": True})

    (
        start_date,
        end_date,
        offset,
        collection_start_date,
    ) = metadata_manager.collection_dates

    assert start_date == settings.EARLIEST_EARTHQUAKE_DATE
    assert end_date == definitions.YESTERDAY.strftime(definitions.DATE_FORMAT)
    assert offset == 1
    assert collection_start_date is False


def test_update_collection_dates_no_save(blank_metadata):
    updated_start_date = "2023-02-01"
    updated_end_date = "2023-02-15"
    updated_offset = 2
    updated_collection_start_date = "2023-02-01"

    expected_metadata = deepcopy(blank_metadata)
    expected_metadata["collection_dates"]["start_date"] = updated_start_date
    expected_metadata["collection_dates"]["end_date"] = updated_end_date
    expected_metadata["collection_dates"]["offset"] = updated_offset
    expected_metadata["collection_dates"][
        "collection_start_date"
    ] = updated_collection_start_date

    metadata_manager = MetadataManager(blank_metadata)

    result = metadata_manager.update_collection_dates(
        start_date=updated_start_date,
        end_date=updated_end_date,
        offset=updated_offset,
        collection_start_date=updated_collection_start_date,
    )
    assert result is True


def test_update_collection_dates_save(blank_metadata):
    updated_start_date = "2023-02-01"
    updated_end_date = "2023-02-15"
    updated_offset = 2
    updated_collection_start_date = "2023-02-01"

    expected_metadata = blank_metadata
    expected_metadata["collection_dates"]["start_date"] = updated_start_date
    expected_metadata["collection_dates"]["end_date"] = updated_end_date
    expected_metadata["collection_dates"]["offset"] = updated_offset
    expected_metadata["collection_dates"][
        "collection_start_date"
    ] = updated_collection_start_date

    with patch(
        "earthquake_data_layer.MetadataManager._save_metadata", return_value=True
    ):
        metadata_manager = MetadataManager(blank_metadata)
        result = metadata_manager.update_collection_dates(
            save=True,
            start_date=updated_start_date,
            end_date=updated_end_date,
            offset=updated_offset,
            collection_start_date=updated_collection_start_date,
        )
        assert result is True
