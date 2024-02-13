# pylint: disable=redefined-outer-name
from unittest.mock import patch

import pytest

from earthquake_data_layer import definitions, settings
from earthquake_data_layer.helpers import fetch_months_data


@pytest.fixture
def mock_metadata():
    return {"status": definitions.STATUS_COLLECTION_METADATA_INCOMPLETE, "details": {}}


@pytest.fixture
def fetch_data_return_value():
    return {"status": definitions.STATUS_UPLOAD_DATA_SUCCESS}


def test_success(storage, mock_metadata, fetch_data_return_value):
    min_year = 2020
    max_year = 2023

    dates = [
        (year, month)
        for year in range(min_year, max_year + 1)
        for month in range(1, 13)
    ]

    # two uploads for every batch and an extra at the end
    expected_num_saves = (len(dates) // settings.COLLECTION_BATCH_SIZE) + 1
    expected_rows = [fetch_data_return_value] * len(dates)
    with patch("collect_dataset.Storage", return_value=storage):
        with patch(
            "earthquake_data_layer.helpers.add_rows_to_parquet", return_value=True
        ) as mock_save_rows:
            with patch(
                "earthquake_data_layer.helpers.Fetcher.fetch_data",
                return_value=fetch_data_return_value,
            ):
                result_metadata = fetch_months_data(dates, mock_metadata, storage)
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
        assert mock_save_rows.call_count == expected_num_saves
        mock_save_rows.assert_called_once_with(
            expected_rows, definitions.COLLECTION_RUNS_KEY, storage=storage
        )


def test_success_multiple_batches(storage, mock_metadata, fetch_data_return_value):
    min_year = 2010
    max_year = 2023

    dates = [
        (year, month)
        for year in range(min_year, max_year + 1)
        for month in range(1, 13)
    ]

    # an upload for every batch and an extra at the end
    expected_num_saves = (len(dates) // settings.COLLECTION_BATCH_SIZE) + 1
    expected_rows = [fetch_data_return_value] * len(dates)
    with patch("collect_dataset.Storage", return_value=storage):
        with patch(
            "collect_dataset.helpers.add_rows_to_parquet", return_value=True
        ) as mock_save_rows:
            with patch(
                "earthquake_data_layer.fetcher.Fetcher.fetch_data",
                return_value=fetch_data_return_value,
            ):
                result_metadata = fetch_months_data(dates, mock_metadata, storage)
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
        assert mock_save_rows.call_count == expected_num_saves
        mock_save_rows.assert_called_with(
            expected_rows, definitions.COLLECTION_RUNS_KEY, storage=storage
        )


def test_fail(storage, mock_metadata):
    min_year = 2015
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
                result_metadata = fetch_months_data(dates, mock_metadata, storage)
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
