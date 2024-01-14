import datetime

from earthquake_data_layer.helpers import is_valid_date


def test_datetime():
    date_obj = datetime.date(2023, 1, 1)
    expected_result = "2023-01-01"
    assert is_valid_date(date_obj) == expected_result


def test_valid_str():
    date_obj = "2023-01-01"
    assert is_valid_date(date_obj) == date_obj


def test_unvalid_str():
    date_obj = "2023/01/01"
    assert not is_valid_date(date_obj)
