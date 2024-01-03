from unittest.mock import patch

from earthquake_data_layer import Metadata


# pylint: disable=unused-argument,redefined-outer-name
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


def test_update_collection_dates_no_upload(blank_metadata):
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

    result = Metadata.update_collection_dates(
        blank_metadata,
        start_date=updated_start_date,
        end_date=updated_end_date,
        offset=updated_offset,
        collection_start_date=updated_collection_start_date,
    )
    assert result == expected_metadata


def test_update_collection_dates_upload(blank_metadata):
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
        "earthquake_data_layer.Metadata._update_metadata", return_value=True
    ) as mock_upload:
        result = Metadata.update_collection_dates(
            blank_metadata,
            upload=True,
            start_date=updated_start_date,
            end_date=updated_end_date,
            offset=updated_offset,
            collection_start_date=updated_collection_start_date,
        )
    assert result is True
    mock_upload.assert_called_once_with(expected_metadata)
