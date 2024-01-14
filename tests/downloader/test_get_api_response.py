from dataclasses import dataclass
from unittest.mock import patch

from earthquake_data_layer import Downloader, MetadataManager, settings


@dataclass
class MockApiResponse:
    content: dict

    def json(self):
        return self.content


def test_get_api_response():
    # init instances
    expected_key_name = "key1"
    api_key = "api_key1"
    downloader = Downloader(
        metadata_manager=MetadataManager(
            {"keys": {expected_key_name: {"some_date": 10}}}
        )
    )

    # mock response i/o
    mock_response_content = {"response_key": "response_value"}
    mock_response = MockApiResponse(mock_response_content)
    mock_request_params = {"request_params_key": "request_params_value"}
    mock_headers = {"X-RapidAPI-Key": api_key}

    # test
    with patch("requests.get") as mock_get:
        mock_get.return_value = mock_response
        with patch.object(settings, "API_KEYs", {expected_key_name: api_key}):
            result = downloader.get_api_response(
                request_params=mock_request_params, headers=mock_headers, url="test_url"
            )
            assert result == {
                "raw_response": mock_response_content,
                "metadata": {
                    "request_params": mock_request_params,
                    "key_name": expected_key_name,
                },
            }
