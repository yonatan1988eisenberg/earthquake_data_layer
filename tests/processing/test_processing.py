# pylint: disable=redefined-outer-name, use-a-generator
from collections import Counter
from copy import deepcopy

import pytest

from earthquake_data_layer import Preprocess


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
    return {
        "raw_response": {
            "data": [
                {f"row{row}_col{col}": row * col for col in range(num_cols)}
                for row in range(num_rows)
            ],
            "other_key": "other_value",
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
    num_cols, num_rows = num_rows, num_cols

    return {
        "raw_response": {
            "data": [
                {f"row{row}_col{col}": row * col for col in range(num_cols)}
                for row in range(num_rows)
            ],
            "other_key": "other_value",
        },
        "metadata": {
            "request_params": {
                f"param_{param}": f"value_{param}" for param in range(5)
            },
            "key_name": "sample_key",
        },
    }


def test_process_response(sample_response, num_rows, num_cols):
    run_id = "sample_run"
    data_key = "sample_data_key"
    responses_ids = []
    count = 0
    columns = Counter()

    (
        metadata,
        processed_data,
        responses_ids,
        count,
        columns,
    ) = Preprocess.process_response(
        sample_response, run_id, data_key, responses_ids, count, columns
    )

    assert "response_id" in metadata
    assert metadata["data_key"] == data_key
    assert len(responses_ids) == 1
    assert count == num_rows
    assert all(
        [
            columns[f"row{row}_col{col}"] == 1
            for row in range(num_rows)
            for col in range(num_cols)
        ]
    )
    assert len(processed_data) == num_rows


def test_bundle(sample_response, inverted_sample_response, num_rows, num_cols):
    run_id = "sample_run"
    data_key = "sample_data_key"
    responses_ids = []
    count = 0
    columns = Counter()
    mode = "collection"
    double_col_names = min(num_rows, num_cols)

    responses = [sample_response, inverted_sample_response]

    responses_metadata = []
    three_d_data = []
    for response in responses:
        metadata, data, responses_ids, count, columns = Preprocess.process_response(
            response, run_id, data_key, responses_ids, count, columns
        )
        responses_metadata.append(metadata)
        three_d_data.append(data)

    data, run_metadata = Preprocess.bundle(
        three_d_data, responses_ids, columns, mode, count, data_key
    )

    assert run_metadata["mode"] == mode
    assert run_metadata["count"] == count
    assert run_metadata["data_key"] == data_key
    assert len(run_metadata["responses_ids"]) == len(responses)
    # each row has different columns, we expect that some will be shared but some woun't
    assert all(
        [
            columns[f"row{row}_col{col}"] == 2
            if row < double_col_names and col < double_col_names
            else columns[f"row{row}_col{col}"] == 1
            for row in range(num_rows)
            for col in range(num_cols)
        ]
    )
    # we also expect that the columns response id will be share among all the rows
    assert columns["response_id"] == count
    assert data == three_d_data[0] + three_d_data[1]


def verify_row(row: dict, data: list[dict]):
    """
    verifies row contains "response_id" and that the rest of the row is in data
    """
    if isinstance(row.pop("response_id"), str):
        return row in data
    return False


def test_preprocess(sample_response, inverted_sample_response):
    responses = [sample_response, inverted_sample_response]
    run_id = "sample_run"
    data_key = "sample_data_key"

    sample_response_data = deepcopy(sample_response["raw_response"]["data"])
    inverted_sample_response_data = deepcopy(
        inverted_sample_response["raw_response"]["data"]
    )
    sample_response_data.extend(inverted_sample_response_data)
    expected_data = sample_response_data

    run_metadata, responses_metadata, data = Preprocess.preprocess(
        responses, run_id, data_key
    )
    assert len(run_metadata["responses_ids"]) == len(responses)
    assert run_metadata["count"] == len(expected_data)
    assert all(
        [
            metadata["response_id"] in run_metadata["responses_ids"]
            for metadata in responses_metadata
        ]
    )
    assert len(expected_data) == len(data)
    assert all([verify_row(row, expected_data) for row in data])
    assert all(
        [
            key in run_metadata
            for key in ("mode", "data_key", "responses_ids", "columns")
        ]
    )
    assert len(responses_metadata) == len(responses)
    assert all([metadata["data_key"] == data_key for metadata in responses_metadata])
    assert all([run_id in metadata["response_id"] for metadata in responses_metadata])
    assert all(["key_name" in metadata for metadata in responses_metadata])
