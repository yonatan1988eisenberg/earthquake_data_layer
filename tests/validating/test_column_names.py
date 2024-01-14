from earthquake_data_layer import MetadataManager
from earthquake_data_layer.validating import ColumnsNames


def test_columns_names_execute_known_columns_vanilla(blank_metadata, mock_run_metadata):
    # Set up metadata manager and run metadata
    metadata_manager = MetadataManager(blank_metadata)
    run_metadata = mock_run_metadata

    result = ColumnsNames.execute(
        metadata_manager=metadata_manager, run_metadata=run_metadata
    )

    # Check that there are no missing or new columns
    assert result == ("column_names", {"missing_columns": [], "new_columns": []})


def test_columns_names_execute_with_new_columns(blank_metadata, mock_run_metadata):
    # Set up metadata manager and run metadata with new columns
    metadata_manager = MetadataManager(blank_metadata)
    run_metadata = mock_run_metadata
    mock_run_metadata.get("columns").update(["col4"])

    result = ColumnsNames.execute(
        metadata_manager=metadata_manager, run_metadata=run_metadata
    )

    # Check that new columns are reported
    assert result == ("column_names", {"missing_columns": [], "new_columns": ["col4"]})


def test_columns_names_execute_with_missing_columns(blank_metadata, mock_run_metadata):
    # Set up metadata manager and run metadata with new columns
    metadata_manager = MetadataManager(blank_metadata)
    run_metadata = mock_run_metadata
    del mock_run_metadata["columns"]["col1"]

    result = ColumnsNames.execute(
        metadata_manager=metadata_manager, run_metadata=run_metadata
    )

    # Check that missing columns are reported
    assert result == ("column_names", {"missing_columns": ["col1"], "new_columns": []})
