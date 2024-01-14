from collections import Counter

from earthquake_data_layer.validating import MissingValues


def test_missing_values_execute_no_missing_values(mock_run_metadata):
    # Set up run metadata with no missing values
    run_metadata = mock_run_metadata

    result = MissingValues.execute(run_metadata=run_metadata)

    # Check that there are no missing values reported
    assert result == ("missing_values", None)


def test_missing_values_execute_with_missing_values(mock_run_metadata):
    # Set up run metadata with missing values
    run_metadata = mock_run_metadata
    mock_run_metadata["columns"] -= Counter(["col1", "col2", "col2"])

    result = MissingValues.execute(run_metadata=run_metadata)

    # Check that missing values are reported
    assert result == ("missing_values", {"col1": 1, "col2": 2})
