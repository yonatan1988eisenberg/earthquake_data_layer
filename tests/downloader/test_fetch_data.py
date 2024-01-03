# todo: make it work
from unittest.mock import patch

from earthquake_data_layer import Downloader, MetadataManager


def test_fetch_data(blank_metadata):
    downloader = Downloader(
        metadata_manager=MetadataManager(blank_metadata), mode="collection"
    )
    with patch.object(downloader, "generate_requests_params", return_value=([], [])):
        result = downloader.fetch_data()
        assert not result
