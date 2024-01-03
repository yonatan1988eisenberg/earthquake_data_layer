from earthquake_data_layer import MetadataManager, definitions


def test_get_remaining_requests_no_key(blank_metadata):
    metadata_manager = MetadataManager(blank_metadata)
    result = metadata_manager.key_remaining_requests("nonexistent_key")

    assert result == definitions.MAX_REQUESTS_PER_DAY


def test_get_remaining_requests_key_not_used_today():
    mock_metadata_file_content = {"keys": {"api_key": {"2023-01-01": 100}}}
    metadata_manager = MetadataManager(mock_metadata_file_content)
    result = metadata_manager.key_remaining_requests("nonexistent_key")

    result = metadata_manager.key_remaining_requests("api_key")

    assert result == definitions.MAX_REQUESTS_PER_DAY


def test_get_remaining_requests_key_used_today():
    mock_metadata_file_content = {
        "keys": {"api_key": {definitions.TODAY.strftime(definitions.DATE_FORMAT): 50}}
    }
    metadata_manager = MetadataManager(mock_metadata_file_content)
    result = metadata_manager.key_remaining_requests("api_key")

    assert result == 50


def test_update_remaining_requests():
    mock_metadata = {"keys": {"api_key": {"2023-12-27": 100}}}
    metadata_manager = MetadataManager(mock_metadata)

    new_value = 50
    result = metadata_manager.update_key_remaining_requests("api_key", new_value)

    assert result
    assert metadata_manager.key_remaining_requests("api_key") == new_value
