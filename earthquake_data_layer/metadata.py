import datetime
import json
import logging

from botocore.exceptions import ClientError

from earthquake_data_layer import definitions, settings, storage
from earthquake_data_layer.helpers import is_valid_date


class Metadata:
    # todo: add docstring
    """module doc string"""

    @classmethod
    def _update_metadata(
        cls,
        metadata: dict,
        local: bool = settings.LOCAL_METADATA,
        bucket: str = settings.AWS_BUCKET_NAME,
    ):
        """
        Update metadata with the provided dictionary.

        Parameters:
        - metadata (dict): The metadata dictionary to be updated.

        Returns:
        bool: True if the update is successful, False otherwise.
        """
        if local:
            try:
                with open(definitions.METADATA_LOCATION, "w", encoding="utf-8") as file:
                    json.dump(metadata, file)
                return True
            except Exception as error:
                logging.error(f"can't open file: {error.__traceback__}")
                return False
        else:
            try:
                connection = storage.Storage()
                transaction = connection.save_object(
                    json.dumps(metadata).encode("utf-8"),
                    definitions.METADATA_KEY,
                    bucket_name=bucket,
                )
                assert transaction is True
                return True

            except (ClientError, AssertionError) as error:
                logging.info(f"Error uploading metadata file: {error.__traceback__}")
                return False

    @classmethod
    def get_collection_dates(cls) -> tuple:
        """
        loads the latest_update date and offset from the metadata file
        """
        metadata = cls.get_metadate()
        collection_dates = metadata.get("collection_dates", {})
        start_date = collection_dates.get("start_date", False)
        end_date = collection_dates.get("end_date", False)
        offset = collection_dates.get("offset", 1)
        collection_start_date = collection_dates.get("collection_start_date", False)
        return start_date, end_date, offset, collection_start_date

    @classmethod
    def update_collection_dates(cls, **kwargs) -> bool:
        """
        updates the collection dates in the metadate file
        """
        start_date = kwargs.get("start_date")
        end_date = kwargs.get("end_date")
        offset = kwargs.get("offset")
        collection_start_date = kwargs.get("collection_start_date")

        metadata = cls.get_metadate()
        collection_dates = metadata.get("collection_dates")

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
        metadata["collection_dates"] = collection_dates
        return cls._update_metadata(metadata)

    @staticmethod
    def get_metadate(
        local: bool = settings.LOCAL_METADATA, bucket: str = settings.AWS_BUCKET_NAME
    ) -> dict:
        """Fetches the metadata from the file/cloud"""
        if local:
            try:
                with open(definitions.METADATA_LOCATION, "r", encoding="utf-8") as file:
                    metadata = json.load(file)

            except (FileNotFoundError, json.JSONDecodeError) as error:
                # Log or handle the exception appropriately
                logging.info(
                    f"Error reading metadata file, returning empty dict: {error}"
                )
                return {}
        else:
            try:
                connection = storage.Storage()
                if definitions.METADATA_KEY in connection.list_objects(
                    bucket, definitions.METADATA_KEY
                ):
                    metadata = connection.load_object(
                        definitions.METADATA_KEY, bucket_name=bucket
                    )
                    metadata = json.loads(metadata.read().decode("utf-8"))

                else:
                    metadata = {}
            except (ClientError, json.JSONDecodeError) as error:
                logging.info(
                    f"Error reading metadata file, returning empty dict: {error.__traceback__}"
                )
                metadata = {}

        return metadata

    @classmethod
    def get_remaining_requests(cls, key: str) -> int:
        """
        Return today's remaining requests for the specified API key.

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

        metadata = cls.get_metadate()
        keys = metadata.get("keys", {})
        key_metadata = keys.get(key)

        today = datetime.date.today().strftime("%Y-%m-%d")

        # if the key doesn't exist in the metadata or if the key wasn't used today
        if not key_metadata or today not in key_metadata.keys():
            return definitions.MAX_REQUESTS_PER_DAY

        return key_metadata[today]

    @classmethod
    def update_remaining_requests(cls, key: str, requests: int) -> bool:
        # get keys metadata
        metadata = cls.get_metadate()
        keys = metadata.get("keys", {})

        # update key metadata
        today = datetime.date.today().strftime("%Y-%m-%d")
        keys[key] = {today: requests}

        # update metadata
        metadata["keys"] = keys

        return cls._update_metadata(metadata)
