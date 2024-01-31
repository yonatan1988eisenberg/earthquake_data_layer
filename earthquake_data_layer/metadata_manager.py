import json
from typing import Optional, Union

from botocore.exceptions import ClientError

from earthquake_data_layer import definitions, helpers, settings, storage


class MetadataManager:
    """
    A class for managing metadata related to earthquake data.

    The class responsibilities include fetching, updating, and saving metadata.
    It provides methods for retrieving information such as collection dates,
    remaining requests for API keys, and updating these values.

    metadata structure:
    {
    "collection_dates": {"start_date": YYYY-MM-DD,
                         "end_date": YYYY-MM-DD,
                         "offset": int,
                         "collection_start_date": YYYY-MM-DD
                        },
    "keys": {key_name1: {YYYY-MM-DD: int (num of remaining requests for that date)},
             key_name2: {YYYY-MM-DD: int},
             .
             .
             .}

    }

    Attributes:
    - local (bool): Flag indicating whether to use local storage (default is settings.LOCAL_METADATA).
    - bucket (str): The AWS S3 bucket name (default is settings.AWS_BUCKET_NAME).
    - metadata (dict): The metadata dictionary containing information about collection dates,
      API keys, and other relevant details.

    Methods:
    - __init__(self, metadata: Optional[dict] = None, **kwargs): Class constructor, initializes the MetadataManager.
    - get_metadata(self) -> dict: Fetches metadata from the file or cloud storage.
    - _save_metadata(self) -> bool: Updates metadata with the provided dictionary and saves it.
    - collection_dates(self) -> tuple: Retrieves the latest collection dates from the metadata file.
    - update_collection_dates(self, save: bool = False, **kwargs) -> Union[bool, dict]: Updates collection dates in
    the metadata file.
    - key_remaining_requests(self, key: str) -> int: Retrieves today's remaining requests for the specified API key.
    - update_key_remaining_requests(self, key: str, requests: int) -> Union[bool, dict]: Updates today's remaining
    requests for the specified API key.
    - known_columns(self) -> list: Retrieves known columns from metadata or returns default columns.

    Example:
    # >>> manager = MetadataManager()
    # >>> start_date, end_date, offset, collection_start_date = manager.collection_dates()
    # >>> print(f"Start Date: {start_date}, End Date: {end_date}, Offset: {offset}, Collection Start Date:
    {collection_start_date}")
    # Start Date: 2023-01-01, End Date: 2023-01-02, Offset: 1, Collection Start Date: False
    """

    def __init__(self, metadata: Optional[dict] = None, **kwargs):
        """
        Initialize a MetadataManager instance.

        Parameters:
        - metadata (dict, optional): The initial metadata dictionary. Defaults to None which triggers a get_metadate
        method.
        - local (bool): Flag indicating whether to use local storage. Defaults to settings.LOCAL_METADATA.
        - bucket (str): The AWS S3 bucket name. Defaults to settings.AWS_BUCKET_NAME.
        """

        self.local = kwargs.get("local", settings.LOCAL_METADATA)
        self.bucket = kwargs.get("bucket", settings.AWS_BUCKET_NAME)

        if not metadata:
            metadata = self.get_metadate()
        self.metadata = metadata

        settings.logger.info(
            f"Initiated MetadataManager with metadata: {self.metadata}"
        )

    def get_metadate(self) -> dict:
        """
        Fetch metadata from the file or cloud storage.

        Returns:
        dict: The metadata dictionary.
        """
        if self.local:
            try:
                with open(definitions.METADATA_LOCATION, "r", encoding="utf-8") as file:
                    metadata = json.load(file)

            except (FileNotFoundError, json.JSONDecodeError) as error:
                settings.logger.error(
                    f"Error reading local metadata file, returning empty dict: {error}"
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
                settings.logger.error(
                    f"Error reading cloud metadata file, returning empty dict: {error}"
                )
                metadata = {}

        return metadata

    def _save_metadata(self) -> bool:
        """
        Update metadata with the provided dictionary.

        Returns:
        bool: True if the update is successful, False otherwise.
        """
        if self.local:
            try:
                with open(definitions.METADATA_LOCATION, "w", encoding="utf-8") as file:
                    json.dump(self.metadata, file)

                settings.logger.info("metadata successfully saved")
                return True
            except Exception as error:
                settings.logger.error(f"Unable to save metadata file: {error}")
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
                settings.logger.info("metadata successfully uploaded")
                return True

            except (ClientError, AssertionError) as error:
                settings.logger.error(f"Error uploading metadata file: {error}")
                return False

    @property
    def collection_dates(self) -> tuple:
        """
        Get the latest collection dates from the metadata file.

        Returns:
        tuple: A tuple containing start_date, end_date, offset, and collection_start_date.
        """

        collection_dates = self.metadata.get("collection_dates", {})
        start_date = collection_dates.get("start_date")
        end_date = collection_dates.get("end_date")
        offset = collection_dates.get("offset", 1)
        collection_start_date = collection_dates.get("collection_start_date")

        # the parameter may be missing or set to False, in both cases we'll want to set them to a default value
        params = [start_date, end_date, offset, collection_start_date]
        default_values = (
            settings.EARLIEST_EARTHQUAKE_DATE,
            definitions.YESTERDAY.strftime(definitions.DATE_FORMAT),
            1,
            definitions.TODAY.strftime(definitions.DATE_FORMAT),
        )

        return_values = tuple(
            (
                param or default_value
                for param, default_value in zip(params, default_values)
            )
        )
        settings.logger.debug(f"collection_dates was calculated as {return_values}")
        return return_values

    def update_collection_dates(
        self, save: bool = False, **kwargs
    ) -> Union[bool, dict]:
        """
        Update the collection dates in the metadata file.

        Parameters:
        - save (bool): Flag indicating whether to upload the updated metadata. Defaults to False.
        - kwargs: Keyword arguments for start_date, end_date, offset, and collection_start_date.

        Returns:
        bool or dict: True if the update is successful, False otherwise, or the updated metadata.

        Raises:
        ValueError: If date values are not in the expected format (default definitions.DATE_FORMAT).
        """

        settings.logger.info("updating collection dates")

        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        offset = kwargs.get("offset")
        collection_start_date = kwargs.get("collection_start_date")

        collection_dates = self.metadata.get("collection_dates")
        if not collection_dates:
            collection_dates = dict()

        # update dates
        for date_name, date in zip(
            ["start_date", "end_date", "collection_start_date"],
            [start_date, end_date, collection_start_date],
        ):
            date = helpers.is_valid_date(date)
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

        Returns:
        bool or dict: True if the update is successful, False otherwise, or the updated metadata.
        """
        # get keys metadata
        keys = self.metadata.get("keys", {})

        # update key metadata
        today = definitions.TODAY.strftime(definitions.DATE_FORMAT)
        keys[key] = {today: requests}

        # update metadata
        self.metadata["keys"] = keys

        settings.logger.info("Updated an API keys remaining requests")

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
