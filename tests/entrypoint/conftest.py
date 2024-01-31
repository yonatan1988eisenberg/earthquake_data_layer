import pytest
from fastapi.testclient import TestClient

from earthquake_data_layer import app


@pytest.fixture
def client():
    return TestClient(app)
