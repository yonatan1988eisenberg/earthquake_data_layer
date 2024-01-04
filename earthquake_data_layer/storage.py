import datetime
import io
import logging
import os
from typing import Optional, Union

import boto3
from botocore.exceptions import ClientError

from earthquake_data_layer import helpers, settings
from earthquake_data_layer.settings import (
    AWS_ACCESS_KEY_ID,
    AWS_BUCKET_NAME,
    AWS_REGION,
    AWS_S3_ENDPOINT,
    AWS_SECRET_ACCESS_KEY,
)


class Storage:
    """
    Class for handling S3 storage operations.

    Usage:
    - Initialize the class: storage = Storage()
    - Use the methods for various storage operations.

    Example:
    storage.upload_file(file_source='example.txt', object_name='path/to/example.txt')
    """

    def __init__(self, **kwargs):
        """
        Initializes the Storage class.

        Parameters:
        - aws_access_key_id (str, optional): AWS access key ID.
        - aws_secret_access_key (str, optional): AWS secret access key.
        - endpoint_url (str, optional): Custom endpoint URL for S3.
        - region_name (str, optional): AWS region name.
        - client (boto3.client, optional): Custom S3 client. If not provided, a new client is created.
        """
        self.aws_access_key_id = kwargs.get("aws_access_key_id", AWS_ACCESS_KEY_ID)
        self.aws_secret_access_key = kwargs.get(
            "aws_secret_access_key", AWS_SECRET_ACCESS_KEY
        )
        self.endpoint_url = kwargs.get("endpoint_url", AWS_S3_ENDPOINT)
        self.region_name = kwargs.get("region_name", AWS_REGION)

        client = kwargs.get("client")
        if not client:
            client = boto3.client(
                "s3",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                endpoint_url=self.endpoint_url,
                region_name=self.region_name,
            )
        self.client = client

    def bucket_exists(
        self,
        bucket_name: str,
        client: Optional[boto3.client] = None,
        create: bool = False,
    ) -> bool:
        """
        Checks if an S3 bucket exists.

        Parameters:
        - bucket_name (str): The name of the bucket.
        - client (boto3.client, optional): Custom S3 client. If not provided, the class default client is used.
        - create (bool, optional): If True, creates the bucket if it doesn't exist.

        Returns:
        - bool: True if the bucket exists, False otherwise.
        """
        if not client:
            client = self.client

        try:
            client.head_bucket(Bucket=bucket_name)
            return True
        except ClientError as error:
            if error.response["Error"]["Code"] == "404":
                if create:
                    try:
                        client.create_bucket(Bucket=bucket_name)
                        return True
                    except Exception as inner_error:
                        logging.error(inner_error)

                return False
            logging.error(f"Error checking bucket existence: {error}")
            return False

    def list_objects(
        self, bucket_name: str = AWS_BUCKET_NAME, prefix: Optional[str] = ""
    ) -> list[str]:
        """
        List all objects in an S3 bucket.

        Parameters:
        - bucket_name (str): The name of the bucket.
        - prefix (str, optional): The prefix to filter objects.

        Returns:
        - List[str]: List of object keys.
        """
        response = self.client.list_objects(Bucket=bucket_name, Prefix=prefix)
        return [obj["Key"] for obj in response.get("Contents", [])]

    def remove_object(
        self,
        key: str,
        bucket_name: str = AWS_BUCKET_NAME,
        client: Optional[boto3.client] = None,
    ) -> bool:
        """
        Remove an object from an S3 bucket.

        Parameters:
        - key (str): The S3 object key.
        - bucket_name (str): The name of the bucket.
        - client (boto3.client, optional): Custom S3 client. If not provided, the class default client is used.

        Returns:
        - bool: True if the object is removed successfully, False otherwise.
        """
        if not client:
            client = self.client

        try:
            client.delete_object(Bucket=bucket_name, Key=key)
            logging.info(f"Object removed successfully: {key}")
            return True
        except ClientError as e:
            logging.error(f"Error removing object: {e}")
            return False

    def save_object(
        self,
        file_source: Union[str, bytes],
        key: Optional[str] = None,
        bucket_name: str = AWS_BUCKET_NAME,
        client: Optional[boto3.client] = None,
    ) -> bool:
        """
        Save an object to an S3 bucket.

        Parameters:
        - file_source (Union[str, bytes]): The source of the file. It can be a local file path or bytes content.
        - key (str, optional): The S3 object key. If not provided, a key is generated.
        - bucket_name (str): The name of the bucket.
        - client (boto3.client, optional): Custom S3 client. If not provided, the class default client is used.

        Returns:
        - bool: True if the object is saved successfully, False otherwise.
        """
        if not client:
            client = self.client

        try:
            if isinstance(file_source, str):
                if not key:
                    key = os.path.basename(file_source)
                client.upload_file(file_source, bucket_name, key)
            elif isinstance(file_source, bytes):
                if not key:
                    random_string = helpers.random_string(
                        settings.RANDOM_STRING_LENGTH_KEY
                    )
                    key = f"{random_string}_{datetime.datetime.now().strftime('%Y-%m-%d_%H:%m:%S')}"
                client.upload_fileobj(io.BytesIO(file_source), bucket_name, key)
            logging.info(f"File uploaded successfully: {key}")
        except ClientError as e:
            logging.error(f"Error uploading file: {e}")
            return False
        return True

    def load_object(
        self,
        key: str,
        return_as_io: bool = True,
        destination_path: Optional[str] = None,
        bucket_name: str = AWS_BUCKET_NAME,
        client: Optional[boto3.client] = None,
    ) -> Optional[Union[bool, io.BytesIO]]:
        """
        Load an object from an S3 bucket.

        Parameters:
        - key (str): The S3 object key.
        - return_as_io (bool, optional): If True, returns the object content as an io.BytesIO object. If False,
          saves the object to the specified destination_path.
        - destination_path (str, optional): The local path to save the object. Required if return_as_io is False.
        - bucket_name (str): The name of the bucket.
        - client (boto3.client, optional): Custom S3 client. If not provided, the class default client is used.

        Returns:
        - Union[io.BytesIO, bool, None]: If return_as_io is True, returns io.BytesIO object. If return_as_io is False,
          returns True if the object is saved successfully, otherwise None.
        """
        if not client:
            client = self.client

        try:
            if return_as_io:
                file_content = io.BytesIO()
                client.download_fileobj(bucket_name, key, file_content)
                file_content.seek(0)
                logging.info(f"File downloaded successfully: {key}")
                return file_content

            with open(destination_path, "wb") as f:
                client.download_fileobj(bucket_name, key, f)
            logging.info(
                f"File downloaded successfully: {key}\nSaved at {destination_path}"
            )
            return True
        except ClientError as error:
            logging.error(f"Error downloading file: {error}")

        return None
