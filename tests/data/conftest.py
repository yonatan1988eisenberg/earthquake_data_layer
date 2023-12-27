# pylint: disable=redefined-outer-name
from unittest.mock import mock_open, patch

import pytest


@pytest.fixture
def mock_metadata_file_content():
    return '{"keys": {}, "latest_update": {"date": "2023-01-01", "offset": 1}}'


@pytest.fixture
def mock_open_function(mock_metadata_file_content):
    m = mock_open(read_data=mock_metadata_file_content)
    with patch("builtins.open", m):
        yield m
