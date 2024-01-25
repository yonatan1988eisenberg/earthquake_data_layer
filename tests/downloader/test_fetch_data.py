from copy import deepcopy
from dataclasses import dataclass
from unittest.mock import MagicMock, patch

import pytest

from earthquake_data_layer import (
    Downloader,
    MetadataManager,
    definitions,
    exceptions,
    settings,
)


@dataclass
class MockApiResponse:
    content: dict

    def json(self):
        return self.content


def test_fetch_data_no_requests_params(blank_metadata):
    mock_metadata = deepcopy(blank_metadata)
    mock_metadata["keys"] = {
        "key1": {definitions.TODAY.strftime(definitions.DATE_FORMAT): 0},
        "key2": {definitions.TODAY.strftime(definitions.DATE_FORMAT): 0},
    }

    downloader = Downloader(
        metadata_manager=MetadataManager(mock_metadata), mode="collection"
    )
    with patch.object(settings, "API_KEYs", {"key1": "api_key1", "key2": "api_key2"}):
        with pytest.raises(exceptions.RemainingRequestsError):
            _ = downloader.fetch_data()


def test_fetch_data(blank_metadata, sample_response, inverted_sample_response):
    # desired_remaining_requests has to be an even number
    desired_remaining_requests = 2
    remaining_requests = desired_remaining_requests + settings.REQUESTS_TOLERANCE
    mock_metadata = deepcopy(blank_metadata)
    mock_metadata["keys"] = {
        "key1": {
            definitions.TODAY.strftime(definitions.DATE_FORMAT): remaining_requests
        },
        "key2": {definitions.TODAY.strftime(definitions.DATE_FORMAT): 0},
    }

    downloader = Downloader(
        metadata_manager=MetadataManager(mock_metadata), mode="collection"
    )

    mock_responses = [
        MockApiResponse(sample_response["raw_response"]),
        MockApiResponse(inverted_sample_response["raw_response"]),
    ] * (desired_remaining_requests // 2)

    mock_api_responses = MagicMock()
    mock_api_responses.__iter__.return_value = mock_responses

    with patch.object(settings, "API_KEYs", {"key1": "api_key1", "key2": "api_key2"}):
        with patch(
            "earthquake_data_layer.downloader.requests.get",
            return_value=mock_api_responses,
        ):
            results = downloader.fetch_data()
            assert len(results) == desired_remaining_requests
            assert all(
                (
                    all((key in result for key in ("raw_response", "metadata")))
                    for result in results
                )
            )
