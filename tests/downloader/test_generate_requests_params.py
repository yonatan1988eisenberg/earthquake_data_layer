from copy import deepcopy
from unittest.mock import patch

from earthquake_data_layer import Downloader, MetadataManager, definitions, settings


def test_generate_requests_params_collection(blank_metadata):

    remaining_requests = definitions.MAX_REQUESTS_PER_DAY
    mock_metadata = deepcopy(blank_metadata)
    mock_metadata["keys"] = {
        "key1": {
            definitions.TODAY.strftime(definitions.DATE_FORMAT): remaining_requests
        },
        "key2": {definitions.TODAY.strftime(definitions.DATE_FORMAT): 0},
    }

    downloader = Downloader(metadata_manager=MetadataManager(mock_metadata))
    with patch.object(settings, "API_KEYs", {"key1": "api_key1", "key2": "api_key2"}):
        requests_params, headers = downloader.generate_requests_params()

        assert len(requests_params) == max(
            0, remaining_requests - settings.REQUESTS_TOLERANCE
        )  # Assuming useable_requests remaining requests for key1
        assert len(headers) == max(0, remaining_requests - settings.REQUESTS_TOLERANCE)

    offset = blank_metadata["collection_dates"]["offset"] or 1
    expected_request_params = [
        {
            "count": definitions.MAX_RESULTS_PER_REQUEST,
            "start": offset + (definitions.MAX_RESULTS_PER_REQUEST * request_num),
            "type": settings.DATA_TYPE_TO_FETCH,
        }
        for request_num in range(len(requests_params))
    ]
    assert requests_params == expected_request_params

    expected_headers = [
        {"X-RapidAPI-Key": "api_key1", "X-RapidAPI-Host": settings.API_HOST}
        for _ in range(len(headers))
    ]
    assert headers == expected_headers


def test_generate_requests_params_update(blank_metadata):

    remaining_requests = 100

    mock_metadata = deepcopy(blank_metadata)
    mock_metadata["keys"] = {
        "key1": {
            definitions.TODAY.strftime(definitions.DATE_FORMAT): remaining_requests
        },
        "key2": {definitions.TODAY.strftime(definitions.DATE_FORMAT): 0},
    }

    downloader = Downloader(
        metadata_manager=MetadataManager(mock_metadata), mode="update"
    )
    with patch.object(settings, "API_KEYs", {"key1": "api_key1", "key2": "api_key2"}):
        requests_params, headers = downloader.generate_requests_params()
        assert (
            len(requests_params) == settings.NUM_REQUESTS_FOR_UPDATE
        )  # Assuming useable_requests remaining requests for key1
        assert len(headers) == settings.NUM_REQUESTS_FOR_UPDATE

    offset = blank_metadata["collection_dates"]["offset"] or 1
    expected_request_params = [
        {
            "count": definitions.MAX_RESULTS_PER_REQUEST,
            "start": offset + (definitions.MAX_RESULTS_PER_REQUEST * request_num),
            "type": settings.DATA_TYPE_TO_FETCH,
        }
        for request_num in range(len(requests_params))
    ]
    assert requests_params == expected_request_params

    expected_headers = [
        {"X-RapidAPI-Key": "api_key1", "X-RapidAPI-Host": settings.API_HOST}
        for _ in range(len(headers))
    ]
    assert headers == expected_headers
