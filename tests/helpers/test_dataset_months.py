import datetime

import pytest

from earthquake_data_layer import definitions, helpers


def test_dataset_months_valid_input():
    start_date = "2022-01-01"
    end_date = "2022-03-01"
    dataset_months = helpers.DatasetMonths(first_date=start_date, last_date=end_date)

    assert dataset_months.first_date == datetime.datetime.strptime(
        start_date, definitions.DATE_FORMAT
    )
    assert dataset_months.last_date == datetime.datetime.strptime(
        end_date, definitions.DATE_FORMAT
    )


def test_dataset_months_invalid_input():
    invalid_date = "invalid_date"
    with pytest.raises(ValueError, match="Invalid input"):
        helpers.DatasetMonths(first_date=invalid_date)


def test_dataset_months_iteration():
    start_date = "2021-11-01"
    end_date = "2022-01-03"
    dataset_months = helpers.DatasetMonths(first_date=start_date, last_date=end_date)

    expected_results = [(2021, 11), (2021, 12), (2022, 1), (2022, 2), (2022, 3)]
    for expected_result, result in zip(expected_results, dataset_months):
        assert expected_result == result
