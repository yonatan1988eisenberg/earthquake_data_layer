import datetime
import json
import logging
from typing import Optional, Union

from botocore.exceptions import ClientError

from earthquake_data_layer import definitions, settings, storage
from earthquake_data_layer.helpers import is_valid_date


class MetadataManager:
    """A class for managing metadata related to earthquake data."""

    def __init__(self, metadata: Optional[dict] = None, **kwargs):

        self.local = kwargs.get("local", settings.LOCAL_METADATA)
        self.bucket = kwargs.get("bucket", settings.AWS_BUCKET_NAME)

        if not metadata:
            metadata = self.get_metadate()
        self.metadata = metadata

    def get_metadate(self) -> dict:
        """
        Fetch metadata from the file or cloud storage.

        Parameters:
        - local (bool): Flag indicating whether to use local storage (default is settings.LOCAL_METADATA).
        - bucket (str): The AWS S3 bucket name (default is settings.AWS_BUCKET_NAME).

        Returns:
        dict: The metadata dictionary.
        """
        if self.local:
            try:
                with open(definitions.METADATA_LOCATION, "r", encoding="utf-8") as file:
                    metadata = json.load(file)

            except (FileNotFoundError, json.JSONDecodeError) as error:
                logging.info(
                    f"Error reading metadata file, returning empty dict: {error}"
                )
                return {}
        else:
            try:
                connection = storage.Storage()
                if definitions.METADATA_KEY in connection.list_objects(
                    self.bucket, definitions.METADATA_KEY
                ):
                    metadata = connection.load_object(
                        definitions.METADATA_KEY, bucket_name=self.bucket
                    )
                    metadata.seek(0)
                    metadata = json.loads(metadata.read().decode("utf-8"))

                else:
                    metadata = {}
            except (ClientError, json.JSONDecodeError) as error:
                logging.info(
                    f"Error reading metadata file, returning empty dict: {error.__traceback__}"
                )
                metadata = {}

        return metadata

    def _save_metadata(self) -> bool:
        """
        Update metadata with the provided dictionary.

        Parameters:
        - metadata (dict): The metadata dictionary to be updated.
        - local (bool): Flag indicating whether to use local storage (default is settings.LOCAL_METADATA).
        - bucket (str): The AWS S3 bucket name (default is settings.AWS_BUCKET_NAME).

        Returns:
        bool: True if the update is successful, False otherwise.
        """
        if self.local:
            try:
                with open(definitions.METADATA_LOCATION, "w", encoding="utf-8") as file:
                    json.dump(self.metadata, file)
                return True
            except Exception as error:
                logging.error(f"Unable to open file: {error.__traceback__}")
                return False
        else:
            try:
                connection = storage.Storage()
                transaction = connection.save_object(
                    json.dumps(self.metadata).encode("utf-8"),
                    definitions.METADATA_KEY,
                    bucket_name=self.bucket,
                )
                assert transaction is True
                return True

            except (ClientError, AssertionError) as error:
                logging.info(f"Error uploading metadata file: {error.__traceback__}")
                return False

    @property
    def collection_dates(self) -> tuple:
        """
        Get the latest collection dates from the metadata file.

        Parameters:
        - metadata (dict, optional): The metadata dictionary (default is None, and it will be fetched if not provided).

        Returns:
        tuple: A tuple containing start_date, end_date, offset, and collection_start_date.
        """

        collection_dates = self.metadata.get("collection_dates", {})
        start_date = collection_dates.get(
            "start_date", definitions.EARLIEST_EARTHQUAKE_DATE
        )
        end_date = collection_dates.get(
            "end_date", definitions.YESTERDAY.strftime(definitions.DATE_FORMAT)
        )
        offset = collection_dates.get("offset", 1)
        collection_start_date = collection_dates.get("collection_start_date", False)
        return start_date, end_date, offset, collection_start_date

    def update_collection_dates(
        self, save: bool = False, **kwargs
    ) -> Union[bool, dict]:
        """
        Update the collection dates in the metadata file.

        Parameters:
        - metadata (dict, optional): The metadata dictionary (default is None, and it will be fetched if not provided).
        - upload (bool): Flag indicating whether to upload the updated metadata (default is False).
        - kwargs: Keyword arguments for start_date, end_date, offset, and collection_start_date.

        Returns:
        bool or dict: True if the update is successful, False otherwise, or the updated metadata.

        Raises:
        ValueError: If date values are not in the expected format.
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        offset = kwargs.get("offset")
        collection_start_date = kwargs.get("collection_start_date")

        collection_dates = self.metadata.get("collection_dates")

        # update dates
        for date_name, date in zip(
            ["start_date", "end_date", "collection_start_date"],
            [start_date, end_date, collection_start_date],
        ):
            date = is_valid_date(date)
            if date:
                collection_dates[date_name] = date
            else:
                raise ValueError(
                    f"{date_name} needs to be a datetime.date object or string in YYYY-MM-DD format"
                )

        # update offset
        if offset:
            collection_dates["offset"] = offset

        # update metadata
        self.metadata["collection_dates"] = collection_dates

        if save:
            return self._save_metadata()

        return True

    def key_remaining_requests(self, key: str) -> int:
        """
        Get today's remaining requests for the specified API key.

        Parameters:
        - key (str): The API key for which to retrieve remaining requests.
        - metadata (dict, optional): The metadata dictionary (default is None, and it will be fetched if not provided).

        Returns:
        int: The remaining number of requests allowed for the specified key today.

        Notes:
        If the key doesn't exist in the metadata or if the key wasn't used today,
        the function generates key metadata with today's date and the maximum requests allowed
        (definitions.MAX_REQUESTS_PER_DAY). It then updates the metadata and saves it.

        Example:
        # >>> remaining_requests = get_remaining_requests("your_api_key")
        # >>> print(remaining_requests)
        150
        """

        keys = self.metadata.get("keys", {})
        key_metadata = keys.get(key)

        # if the key doesn't exist in the metadata or if the key wasn't used today
        if (
            not key_metadata
            or definitions.TODAY.strftime(definitions.DATE_FORMAT)
            not in key_metadata.keys()
        ):
            return definitions.MAX_REQUESTS_PER_DAY

        return key_metadata[definitions.TODAY.strftime(definitions.DATE_FORMAT)]

    def update_key_remaining_requests(
        self,
        key: str,
        requests: int,
    ) -> Union[bool, dict]:
        """
        Update today's remaining requests for the specified API key.

        Parameters:
        - key (str): The API key for which to update remaining requests.
        - requests (int): The new remaining number of requests.
        - metadata (dict, optional): The metadata dictionary (default is None, and it will be fetched if not provided).
        - upload (bool): Flag indicating whether to upload the updated metadata (default is False).

        Returns:
        bool or dict: True if the update is successful, False otherwise, or the updated metadata.
        """
        # get keys metadata
        keys = self.metadata.get("keys", {})

        # update key metadata
        today = datetime.date.today().strftime(definitions.DATE_FORMAT)
        keys[key] = {today: requests}

        # update metadata
        self.metadata["keys"] = keys

        return True

    @property
    def known_columns(self):
        """
        Get the known columns from metadata or return default columns.

        Returns:
        list: List of known columns.
        """
        cols = self.metadata.get("known_columns")
        if not cols:
            return definitions.SEEN_COLUMNS

        return cols
