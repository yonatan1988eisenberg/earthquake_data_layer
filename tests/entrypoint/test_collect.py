from unittest.mock import patch

from earthquake_data_layer import definitions
from earthquake_data_layer.exceptions import DoneCollectingError, StorageConnectionError


def test_collect_success(client):
    # Mock the run_collection function to return a result
    with patch(
        "earthquake_data_layer.routes.collect.run_collection",
        return_value="some_result",
    ):

        # Send a request to the /collect endpoint
        response = client.get("/collect/some_run_id")

    # Assert that the response status code is 200
    assert response.status_code == 200

    # Assert that the response JSON contains the expected result and status
    assert response.json() == {"result": "some_result", "status": 200}


def test_collect_done_collecting(client):
    # Mock the run_collection function to raise a DoneCollectingError
    with patch(
        "earthquake_data_layer.routes.collect.run_collection",
        side_effect=DoneCollectingError(),
    ):

        # Send a request to the /collect endpoint
        response = client.get("/collect/some_run_id")

    # Assert that the response status code is 200
    assert response.status_code == 200

    # Assert that the response JSON contains the expected result and status
    assert response.json() == {"result": "done_collecting", "status": 200}


def test_collect_storage_connection_error(client):
    # Mock the run_collection function to raise a StorageConnectionError
    with patch(
        "earthquake_data_layer.routes.collect.run_collection",
        side_effect=StorageConnectionError("Connection error"),
    ):

        # Send a request to the /collect endpoint
        response = client.get("/collect/some_run_id")

    # Assert that the response status code is 501 (HTTPException status_code for StorageConnectionError)
    assert response.status_code == definitions.HTTP_COULDNT_CONNECT_TO_STORAGE


def test_collect_generic_error(client):
    # Mock the run_collection function to raise a generic exception
    with patch(
        "earthquake_data_layer.routes.collect.run_collection",
        side_effect=Exception("Some error"),
    ):

        # Send a request to the /collect endpoint
        response = client.get("/collect/some_run_id")

    # Assert that the response status code is 500 (HTTPException status_code for generic error)
    assert response.status_code == 500
