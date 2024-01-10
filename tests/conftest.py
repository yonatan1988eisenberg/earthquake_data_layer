# pylint: disable=redefined-outer-name,unused-argument,import-outside-toplevel
import datetime
import os
from collections import Counter

import boto3
import pytest
from moto import mock_s3

from earthquake_data_layer import definitions


@pytest.fixture
def mock_run_metadata():
    return {
        "mode": "collecting",
        "count": 3,
        "data_key": "sample_data_key",
        "responses_ids": ["response1", "response2", "response3"],
        "columns": Counter({"col1": 3, "col2": 3, "col3": 3}),
        "next_run_dates": {"earliest_date": "2023-01-02", "offset": 1},
    }


@pytest.fixture
def blank_metadata():
    return {
        "collection_dates": {
            "start_date": False,
            "end_date": False,
            "offset": False,
            "collection_start_date": False,
        },
        "keys": {},
        "known_columns": ["col1", "col2", "col3"],
    }


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


@pytest.fixture
def num_rows():
    num_rows = 10

    return num_rows


@pytest.fixture
def num_cols():
    num_cols = 5

    return num_cols


@pytest.fixture
def sample_response(num_rows, num_cols):
    mock_data = list()
    for row in range(num_rows):
        row_data = dict()
        for col in range(num_cols):
            row_data[f"row{row}_col{col}"] = row * col
        row_data["date"] = definitions.TODAY.strftime(definitions.EXPECTED_DATE_FORMAT)

        mock_data.append(row_data)

    return {
        "raw_response": {
            "data": mock_data,
            "other_key": "other_value",
            "httpStatus": 200,
        },
        "metadata": {
            "request_params": {
                f"param_{param}": f"value_{param}" for param in range(num_cols)
            },
            "key_name": "sample_key",
        },
    }


@pytest.fixture
def inverted_sample_response(num_rows, num_cols):
    mock_data = list()
    for row in range(num_cols):
        row_data = dict()
        for col in range(num_rows):
            row_data[f"row{row}_col{col}"] = row * col
        row_data["date"] = definitions.TODAY.strftime(definitions.EXPECTED_DATE_FORMAT)

        mock_data.append(row_data)

    return {
        "raw_response": {
            "data": mock_data,
            "other_key": "other_value",
            "httpStatus": 200,
        },
        "metadata": {
            "request_params": {
                f"param_{param}": f"value_{param}" for param in range(5)
            },
            "key_name": "sample_key",
        },
    }


@pytest.fixture
def mock_dates_counter(num_rows, num_cols):
    today_str = definitions.TODAY.strftime(definitions.DATE_FORMAT)
    tomorrow_str = (definitions.TODAY + datetime.timedelta(days=1)).strftime(
        definitions.DATE_FORMAT
    )
    row_date = Counter()
    row_date.update([today_str] * num_rows + [tomorrow_str] * num_cols)

    return row_date
