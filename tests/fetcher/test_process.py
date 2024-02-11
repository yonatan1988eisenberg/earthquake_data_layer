from tests.utils import MockApiResponse


def test_single_response(mock_fetcher, last_response_content, mock_response_data):
    mock_fetcher.responses = [MockApiResponse(content=last_response_content).json()]
    result = mock_fetcher.process()

    expected_count = last_response_content["metadata"]["count"]
    expected_data = [{"id": 1, **{val: key for key, val in mock_response_data.items()}}]

    assert not result.get("error")
    assert result.get("status")
    assert mock_fetcher.total_count == expected_count
    assert mock_fetcher.data == expected_data


def test_multiple_responses(
    mock_fetcher, first_response_content, last_response_content, mock_response_data
):
    mock_fetcher.responses = [
        MockApiResponse(content=first_response_content).json(),
        MockApiResponse(content=last_response_content).json(),
    ]
    result = mock_fetcher.process()

    expected_count = (
        first_response_content["metadata"]["count"]
        + last_response_content["metadata"]["count"]
    )
    expected_data = [
        {"id": 2, **mock_response_data},
        {"id": 1, **{val: key for key, val in mock_response_data.items()}},
    ]

    assert not result.get("error")
    assert result.get("status")
    assert mock_fetcher.total_count == expected_count
    assert mock_fetcher.data == expected_data
