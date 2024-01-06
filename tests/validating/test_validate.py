from earthquake_data_layer import MetadataManager, Validate
from earthquake_data_layer.validating import ColumnsNames


def test_validate_default_steps(blank_metadata, mock_run_metadata):
    # Set up metadata manager and run metadata with new columns
    metadata_manager = MetadataManager(blank_metadata)
    run_metadata = mock_run_metadata

    result = Validate.validate(
        metadata_manager=metadata_manager, run_metadata=run_metadata
    )

    # Check that validation reports for each step are present
    assert result == {
        "column_names": {"missing_columns": [], "new_columns": []},
        "missing_values": None,
    }


def test_validate_custom_steps(blank_metadata, mock_run_metadata):
    # Set up metadata manager and run metadata with new columns
    metadata_manager = MetadataManager(blank_metadata)
    run_metadata = mock_run_metadata

    result = Validate.validate(
        steps=[ColumnsNames],
        metadata_manager=metadata_manager,
        run_metadata=run_metadata,
    )

    # Check that validation reports for the specified steps are present
    assert result == {"column_names": {"missing_columns": [], "new_columns": []}}
