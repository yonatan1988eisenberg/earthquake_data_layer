from unittest.mock import patch

from earthquake_data_layer import settings
from earthquake_data_layer.helpers import key_api2name


def test_api_key2name_single_match():
    expected_key_name = "key1"
    api_key = "api_key1"

    with patch.object(
        settings, "API_KEYs", {expected_key_name: api_key, "key2": "api_key2"}
    ):
        actual_key_name = key_api2name(api_key)
        assert actual_key_name == expected_key_name


def test_key_api2name_no_match():
    with patch.object(settings, "API_KEYs", {"key1": "api_key1", "key2": "api_key2"}):
        assert key_api2name("nonexistent_key") is False


def test_key_api2name_multiple_matches():
    expected_key_name = "key1"
    api_key = "api_key1"

    with patch.object(
        settings, "API_KEYs", {expected_key_name: api_key, "key2": "api_key1"}
    ):
        with patch(
            "earthquake_data_layer.helpers.choice", return_value=expected_key_name
        ):
            with patch("logging.info") as mock_logging_info:
                assert key_api2name(api_key) == expected_key_name
                mock_logging_info.assert_called_once_with(
                    f"more than one API name matches the key, returning a random choice - {expected_key_name}"
                )
