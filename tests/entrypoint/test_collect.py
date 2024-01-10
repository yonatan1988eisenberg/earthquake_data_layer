from unittest.mock import patch

from collect import DoneCollectingError, StorageConnectionError


def test_collect_success(client):
    # Mock the run_collection function to return a result
    with patch(
        "earthquake_data_layer.entrypoint.run_collection", return_value="some_result"
    ):

        # Send a request to the /collect endpoint
        response = client.get("/collect/some_run_id")

    # Assert that the response status code is 200
    assert response.status_code == 200

    # Assert that the response JSON contains the expected result and status
    assert response.json() == {"result": "some_result", "status": "success"}


def test_collect_done_collecting(client):
    # Mock the run_collection function to raise a DoneCollectingError
    with patch(
        "earthquake_data_layer.entrypoint.run_collection",
        side_effect=DoneCollectingError(),
    ):

        # Send a request to the /collect endpoint
        response = client.get("/collect/some_run_id")

    # Assert that the response status code is 200
    assert response.status_code == 200

    # Assert that the response JSON contains the expected result and status
    assert response.json() == {"result": "done_collecting", "status": "success"}


def test_collect_storage_connection_error(client):
    # Mock the run_collection function to raise a StorageConnectionError
    with patch(
        "earthquake_data_layer.entrypoint.run_collection",
        side_effect=StorageConnectionError("Connection error"),
    ):

        # Send a request to the /collect endpoint
        response = client.get("/collect/some_run_id")

    # Assert that the response status code is 501 (HTTPException status_code for StorageConnectionError)
    assert response.status_code == 501

    # Assert that the response JSON contains the expected error message
    assert response.json() == {"detail": "Connection error"}


def test_collect_generic_error(client):
    # Mock the run_collection function to raise a generic exception
    with patch(
        "earthquake_data_layer.entrypoint.run_collection",
        side_effect=Exception("Some error"),
    ):

        # Send a request to the /collect endpoint
        response = client.get("/collect/some_run_id")

    # Assert that the response status code is 500 (HTTPException status_code for generic error)
    assert response.status_code == 500

    # Assert that the response JSON contains the expected error message
    assert response.json() == {"detail": "Some error"}