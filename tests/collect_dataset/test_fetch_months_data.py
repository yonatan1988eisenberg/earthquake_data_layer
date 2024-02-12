# pylint: disable=redefined-outer-name
from unittest.mock import patch

import pytest

from collect_dataset import fetch_months_data
from earthquake_data_layer import definitions, settings


@pytest.fixture
def mock_metadata():
    return {"status": definitions.STATUS_COLLECTION_METADATA_INCOMPLETE, "details": {}}


def test_success(storage, mock_metadata):
    min_year = 2020
    max_year = 2023

    dates = [
        (year, month)
        for year in range(min_year, max_year + 1)
        for month in range(1, 13)
    ]

    # two uploads for every batch and an extra at the end
    expected_num_saves = ((len(dates) // settings.COLLECTION_BATCH_SIZE) * 2) + 1
    with patch("collect_dataset.Storage", return_value=storage):
        with patch.object(storage, "save_object", return_value=True) as mock_save:
            with patch(
                "earthquake_data_layer.fetcher.Fetcher.fetch_data",
                return_value={"status": definitions.STATUS_UPLOAD_DATA_SUCCESS},
            ):
                result_metadata = fetch_months_data(dates, mock_metadata)
                assert (
                    result_metadata.get("status")
                    == definitions.STATUS_COLLECTION_METADATA_COMPLETE
                )
        dates_results = [
            status == definitions.STATUS_PIPELINE_SUCCESS
            for year, months in result_metadata["details"].items()
            for month, status in months.items()
        ]
        assert all(dates_results)
        assert mock_save.call_count == expected_num_saves


def test_success_multiple_batches(storage, mock_metadata):
    min_year = 2010
    max_year = 2023

    dates = [
        (year, month)
        for year in range(min_year, max_year + 1)
        for month in range(1, 13)
    ]

    # two uploads for every batch and an extra at the end
    expected_num_saves = ((len(dates) // settings.COLLECTION_BATCH_SIZE) * 2) + 1
    with patch("collect_dataset.Storage", return_value=storage):
        with patch.object(storage, "save_object", return_value=True) as mock_save:
            with patch(
                "earthquake_data_layer.fetcher.Fetcher.fetch_data",
                return_value={"status": definitions.STATUS_UPLOAD_DATA_SUCCESS},
            ):
                result_metadata = fetch_months_data(dates, mock_metadata)
                assert (
                    result_metadata.get("status")
                    == definitions.STATUS_COLLECTION_METADATA_COMPLETE
                )
        dates_results = [
            status == definitions.STATUS_PIPELINE_SUCCESS
            for year, months in result_metadata["details"].items()
            for month, status in months.items()
        ]
        assert all(dates_results)
        assert mock_save.call_count == expected_num_saves


def test_fail(storage, mock_metadata):
    min_year = 2020
    max_year = 2023

    dates = [
        (year, month)
        for year in range(min_year, max_year + 1)
        for month in range(1, 13)
    ]

    # two uploads for every batch and an extra at the end
    expected_num_saves = ((len(dates) // settings.COLLECTION_BATCH_SIZE) * 2) + 1
    with patch("collect_dataset.Storage", return_value=storage):
        with patch.object(storage, "save_object", return_value=True) as mock_save:
            with patch(
                "earthquake_data_layer.fetcher.Fetcher.fetch_data",
                return_value={"status": definitions.STATUS_UPLOAD_DATA_FAIL},
            ):
                result_metadata = fetch_months_data(dates, mock_metadata)
                assert (
                    result_metadata.get("status")
                    == definitions.STATUS_COLLECTION_METADATA_INCOMPLETE
                )
        dates_results = [
            status == definitions.STATUS_PIPELINE_FAIL
            for year, months in result_metadata["details"].items()
            for month, status in months.items()
        ]
        assert all(dates_results)
        assert mock_save.call_count == expected_num_saves
