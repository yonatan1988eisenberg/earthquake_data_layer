import datetime

import pytest
from freezegun import freeze_time

from earthquake_data_layer import Downloader

freeze_time("2023-01-01")


def test_get_base_query_params_defaults():
    expected_start_date = datetime.date.today() - datetime.timedelta(days=7)
    expected_start_date = expected_start_date.strftime("%Y-%m-%d")
    expected_end_date = datetime.date.today().strftime("%Y-%m-%d")

    base_query_params = Downloader.get_base_query_params()

    assert base_query_params == {
        "startDate": expected_start_date,
        "endDate": expected_end_date,
        "type": "earthquake",
    }


def test_get_base_query_params_custom_start_date():
    expected_end_date = datetime.date.today().strftime("%Y-%m-%d")
    base_query_params = Downloader.get_base_query_params(start_date="2023-01-01")
    assert base_query_params == {
        "type": "earthquake",
        "endDate": expected_end_date,
        "startDate": "2023-01-01",
    }


def test_get_base_query_params_invalid_start_date():
    with pytest.raises(
        ValueError,
        match=r".* needs to be a datetime.date object or string in YYYY-MM-DD format",
    ):
        Downloader.get_base_query_params(start_date="invalid-date-format")


def test_get_base_query_params_datetime_start_date():
    expected_end_date = datetime.date.today().strftime("%Y-%m-%d")
    start_date = datetime.date(2023, 1, 1)
    base_query_params = Downloader.get_base_query_params(start_date=start_date)
    assert base_query_params == {
        "type": "earthquake",
        "startDate": "2023-01-01",
        "endDate": expected_end_date,
    }
