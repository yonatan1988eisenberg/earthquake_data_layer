# pylint: disable=unused-import,redefined-outer-name

import os
import tempfile

from tests.conftest import aws_credentials, storage, test_bucket


def test_existing_and_nonexistent_buckets(storage, test_bucket):
    # Test when the bucket exists
    assert storage.bucket_exists(test_bucket)

    # Test when the bucket doesn't exist
    assert not storage.bucket_exists("nonexistent-bucket")

    # Test creating a bucket if it doesn't exist
    assert storage.bucket_exists("new_bucket", create=True)
    assert storage.bucket_exists("new_bucket")


def test_listing_objects_with_and_without_prefix(storage, test_bucket):
    # Create some mock objects in the bucket
    storage.client.put_object(Bucket=test_bucket, Key="file1.txt", Body="Content1")
    storage.client.put_object(Bucket=test_bucket, Key="file2.txt", Body="Content2")

    # Test listing objects
    objects = storage.list_objects(test_bucket)
    assert objects, ["file1.txt", "file2.txt"]

    # Test listing objects with a prefix
    objects = storage.list_objects(test_bucket, prefix="file1")
    assert objects, ["file1.txt"]


def test_removing_existing_and_nonexistent_objects(storage, test_bucket):
    file_key = "file.txt"

    # Create some mock objects in the bucket
    storage.client.put_object(Bucket=test_bucket, Key=file_key, Body="Content")
    assert file_key in storage.list_objects(test_bucket)

    # Test removing an existing object
    assert storage.remove_object("file.txt", test_bucket)
    assert file_key not in storage.list_objects(test_bucket)

    # Test removing a non-existing object
    assert not storage.remove_object("nonexistent-file", test_bucket)


def test_saving_objects_from_bytes_and_file_path(storage, test_bucket):
    # Test saving a file
    content = b"Test content"
    key = "test.txt"
    assert storage.save_object(content, key, bucket_name=test_bucket)
    assert key in storage.list_objects(test_bucket)

    # Test saving a file with a custom key
    key = "custom/test.txt"
    assert storage.save_object(content, key, bucket_name=test_bucket)
    assert key in storage.list_objects(test_bucket)

    # Test saving a file from a path
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(content)
    try:
        assert storage.save_object(temp_file.name, bucket_name=test_bucket)
        assert os.path.basename(temp_file.name) in storage.list_objects(test_bucket)
    finally:
        os.remove(temp_file.name)


def test_loading_existing_and_nonexistent_objects(storage, test_bucket):
    # Create a mock object in the bucket
    expected_content = "Content"
    storage.client.put_object(Bucket=test_bucket, Key="file.txt", Body=expected_content)

    # Test loading an existing object
    content = storage.load_object("file.txt", bucket_name=test_bucket)
    assert content is not None
    assert content, expected_content

    # Test loading a non-existing object
    try:
        storage.load_object("nonexistent-file", bucket_name=test_bucket)
    except FileNotFoundError:
        assert True
