from unittest.mock import patch

from earthquake_data_layer import Downloader, MetadataManager, settings


def test_api_key2name_single_match():
    expected_key_name = "key1"
    api_key = "api_key1"

    downloader = Downloader(
        metadata_manager=MetadataManager(
            {
                "keys": {
                    expected_key_name: {"some_date": 10},
                    "key2": {"another_date": 15},
                }
            }
        )
    )
    with patch.object(
        settings, "API_KEYs", {expected_key_name: api_key, "key2": "api_key2"}
    ):
        actual_key_name = downloader.key_api2name(api_key)
        assert actual_key_name == expected_key_name


def test_key_api2name_no_match():
    downloader = Downloader(
        metadata_manager=MetadataManager(
            {"keys": {"key1": {"some_date": 10}, "key2": {"another_date": 15}}}
        )
    )
    with patch.object(settings, "API_KEYs", {"key1": "api_key1", "key2": "api_key2"}):
        assert downloader.key_api2name("nonexistent_key") is False


def test_key_api2name_multiple_matches():
    expected_key_name = "key1"
    api_key = "api_key1"

    mock_metadata = {
        "keys": {expected_key_name: {"some_date": 10}, "key2": {"another_date": 15}}
    }
    metadata_manager = MetadataManager(mock_metadata)
    downloader = Downloader(metadata_manager)

    with patch.object(
        settings, "API_KEYs", {expected_key_name: api_key, "key2": "api_key1"}
    ):
        with patch(
            "earthquake_data_layer.downloader.choice", return_value=expected_key_name
        ):
            with patch("logging.info") as mock_logging_info:
                assert downloader.key_api2name(api_key) == expected_key_name
                mock_logging_info.assert_called_once_with(
                    f"more than one API name matches the key, returning a random choice - {expected_key_name}"
                )
