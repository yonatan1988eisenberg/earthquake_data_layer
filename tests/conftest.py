# pylint: disable=redefined-outer-name,unused-argument,import-outside-toplevel
import os
from unittest.mock import patch

import boto3
import pytest
from moto import mock_s3


@pytest.fixture
def blank_metadata():
    return {
        "collection_dates": {
            "start_date": False,
            "end_date": False,
            "offset": 1,
            "collection_start_date": False,
        },
        "keys": {},
    }


@pytest.fixture
def get_blank_metadata(blank_metadata):
    with patch(
        "earthquake_data_layer.Metadata.get_metadate", return_value=blank_metadata
    ):
        yield


@pytest.fixture(scope="module")
def test_bucket():
    return "test-bucket"


@pytest.fixture(scope="module")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ["AWS_S3_ENDPOINT"] = "http://localhost:5000"


@pytest.fixture(scope="module")
def storage(aws_credentials, test_bucket):
    with mock_s3():
        from earthquake_data_layer import Storage

        s3_client = boto3.client("s3")
        storage = Storage(client=s3_client)
        assert storage.bucket_exists(test_bucket, create=True)
        yield storage


@pytest.fixture(autouse=True)
def clear_bucket(storage, test_bucket):
    yield
    # Clear the bucket after each test
    for obj_key in storage.list_objects(test_bucket):
        storage.remove_object(obj_key, test_bucket)
