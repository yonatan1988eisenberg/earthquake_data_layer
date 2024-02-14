from unittest.mock import patch

from earthquake_data_layer import definitions


def test_update_success(client, storage):
    # Mock the run_collection function to return a result
    with patch(
        "earthquake_data_layer.routes.update.update_dataset",
        return_value="some_result",
    ):
        with patch("earthquake_data_layer.helpers.Storage", return_value=storage):
            # Send a request to the /collect endpoint
            response = client.get("/update/2020-01-01")

    # Assert that the response status code is 200
    assert response.status_code == 200

    # Assert that the response JSON contains the expected result and status
    assert response.json() == {"result": "some_result", "status_code": 200}


def test_storage_connection_error(client):
    # Mock the run_collection function to raise a StorageConnectionError
    with patch(
        "earthquake_data_layer.helpers.verify_storage_connection", return_value=False
    ):
        # Send a request to the /collect endpoint
        response = client.get("/update/2020-01-01")

    # Assert that the response status code is 501 (HTTPException status_code for StorageConnectionError)
    assert response.status_code == definitions.HTTP_COULDNT_CONNECT_TO_STORAGE


def test_update_generic_error(client, storage):
    # Mock the run_collection function to raise a generic exception
    with patch(
        "earthquake_data_layer.routes.update.update_dataset",
        side_effect=Exception("Some error"),
    ):
        with patch("earthquake_data_layer.helpers.Storage", return_value=storage):
            # Send a request to the /collect endpoint
            response = client.get("/update/2020-01-01")

    # Assert that the response status code is 500 (HTTPException status_code for generic error)
    assert response.status_code == 500
