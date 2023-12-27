# todo: add  docstring
"""module doc string"""
import datetime
import json

from earthquake_data_layer import definitions


class Data:
    # todo: add docstring
    """module doc string"""

    @classmethod
    def update_metadata(cls, metadata: dict):
        """
        Update metadata with the provided dictionary.

        Parameters:
        - metadata (dict): The metadata dictionary to be updated.

        Returns:
        bool: True if the update is successful, False otherwise.
        """
        try:
            with open(definitions.METADATA_LOCATION, "w", encoding="utf-8") as file:
                json.dump(metadata, file)
            return True
        except Exception:
            # todo: Log or handle the exception appropriately
            return False

    @classmethod
    def get_latest_update(cls) -> tuple[str, int]:
        """
        loads the latest_update date and offset from the metadata file
        """
        metadata = cls.get_metadate()
        latest_update = metadata.get("latest_update", {})
        date = latest_update.get("date")
        offset = latest_update.get("offset")

        if not date:
            date = "1638-01-01"

        if not offset:
            offset = 1

        return date, offset

    @classmethod
    def update_latest_update(cls, date: str, offset: int) -> bool:
        """
        updates the latest_update in the metadate file
        """
        metadata = cls.get_metadate()
        metadata.update({"latest_update": {"date": date, "offset": offset}})
        return cls.update_metadata(metadata)

    @staticmethod
    def get_metadate() -> dict:
        """fetches the metadata from the file"""
        with open(definitions.METADATA_LOCATION, "r", encoding="utf-8") as file:
            metadata = json.load(file)
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
        >>> remaining_requests = get_remaining_requests("your_api_key")
        >>> print(remaining_requests)
        150
        """

        metadata = cls.get_metadate()
        keys = metadata.get("keys", {})
        key_metadata = keys.get(key)

        today = datetime.date.today().strftime("%Y-%m-%d")

        # if the key doesn't exist in the metadata or if the key wasn't used today
        if not key_metadata or today not in key_metadata.keys():
            # generate the key metadata
            key_metadata = {today: definitions.MAX_REQUESTS_PER_DAY}

            # update the metadata and save it
            keys.update(key_metadata)
            metadata.update(keys)
            cls.update_metadata(metadata)

            return definitions.MAX_REQUESTS_PER_DAY

        return key_metadata[today]
