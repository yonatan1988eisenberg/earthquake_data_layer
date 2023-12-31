from unittest.mock import patch

from earthquake_data_layer import Metadata


# pylint: disable=unused-argument
def test_get_collection_dates_default(get_blank_metadata):
    (
        start_date,
        end_date,
        offset,
        collection_start_date,
    ) = Metadata.get_collection_dates()

    assert start_date is False
    assert end_date is False
    assert offset == 1
    assert collection_start_date is False


# pylint: disable=unused-argument
def test_update_metadata(get_blank_metadata, blank_metadata):
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

    with patch("earthquake_data_layer.Metadata._update_metadata") as updating_metadata:
        assert Metadata.update_collection_dates(
            start_date=updated_start_date,
            end_date=updated_end_date,
            offset=updated_offset,
            collection_start_date=updated_collection_start_date,
        )
        updating_metadata.assert_called_once_with(expected_metadata)
