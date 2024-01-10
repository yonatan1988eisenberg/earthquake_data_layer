import datetime
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Literal, Optional, Union

import requests

from earthquake_data_layer import definitions, settings
from earthquake_data_layer.helpers import is_valid_date, key_api2name
from earthquake_data_layer.metadata_manager import MetadataManager


class Downloader:
    """
    A class for downloading data from an API.

    This class handles the retrieval of data from an API, managing API keys, generating request parameters,
    and executing concurrent requests for efficient data collection.

    Attributes:
    - metadata_manager (Optional[MetadataManager]): An instance of MetadataManager for managing metadata.
    - mode (Literal["collection", "update"]): The mode of the downloader ("collection" or "update").

    Properties:
    - keys_metadata: Property to get metadata for API keys.
    - start_end_dates: Property to get start and end dates based on the mode.
    - offset: Property to get the offset based on the mode.

    Methods:
    - get_api_response(request_params: dict, headers: dict, url: str = definitions.API_URL) -> dict:
      Send a GET request to the API and return the JSON response.
    - generate_requests_params(self, **kwargs) -> tuple[list[dict[str, Union[int, str]]], list[dict[str, str]]]:
      Generate a list of request parameters and headers based on the specified inputs.
    - get_base_query_params(self, **kwargs) -> dict: Generate base query parameters for an API request.
    - fetch_data(self, **kwargs) -> list[dict]: Fetch metadata from the API.

    Example:
    # >>> metadata_manager = MetadataManager()
    # >>> downloader = Downloader(metadata_manager, mode="collection")
    # >>> data = downloader.fetch_data(data_type="earthquake")
    # >>> print(f"Downloaded data: {data}")
    # Downloaded data: [{'raw_response': {...}, 'metadata': {'request_params': {...}, 'key_name': 'key1'}}, ...]
    """

    def __init__(
        self,
        metadata_manager: Optional[MetadataManager] = None,
        mode: Literal["collection", "update"] = "collection",
    ):
        """
        Initialize the Downloader class.

        Parameters:
        - metadata_manager (MetadataManager): An instance of MetadataManager for managing metadata.
        - mode (Literal["collection", "update"], optional): The mode of the downloader ("collection" or "update").
          Defaults to "collection".
        """

        self.metadata_manager = metadata_manager
        self.mode = mode

        settings.logger.info("Downloader initiated")

    @property
    def keys_metadata(self) -> dict[str, tuple[str, int]]:
        """
        Property to get metadata for API keys.

        Returns:
        Dict[str, Tuple[str, int]]: A dictionary with key names as keys and tuples of API keys and remaining
        requests as values.
        """

        keys_metadata = dict()
        for key_name, api_key in settings.API_KEYs.items():
            remaining_requests = 0
            if self.mode == "collection":
                keys_remaining_requests = (
                    self.metadata_manager.key_remaining_requests(key_name)
                    - settings.REQUESTS_TOLERANCE
                )
                remaining_requests = max(0, keys_remaining_requests)
            elif self.mode == "update":
                remaining_requests = min(
                    settings.NUM_REQUESTS_FOR_UPDATE,
                    self.metadata_manager.key_remaining_requests(key_name),
                )

            keys_metadata[key_name] = (api_key, remaining_requests)

        settings.logger.debug(f"calculated keys_metadata:\n{keys_metadata}")

        return keys_metadata

    @property
    def start_end_dates(self) -> tuple[Union[str, None], Union[str, None]]:
        """
        Property to get start and end dates based on the mode.

        Returns:
        Tuple[Union[str, None], Union[str, None]]: A tuple with start_date and end_date.
        """
        start_date, end_date = (None, None)

        if self.mode == "collection":
            start_date, end_date, _, _ = self.metadata_manager.collection_dates

        elif self.mode == "update":
            # todo: add update dates to metadata and set start_date = last_update_date
            start_date = (
                definitions.TODAY
                - datetime.timedelta(days=settings.UPDATE_TIME_DELTA_DAYS)
            ).strftime(definitions.DATE_FORMAT)
            end_date = definitions.TODAY.strftime(definitions.DATE_FORMAT)

        settings.logger.debug(
            f"Using {start_date} as start_date and {end_date} as end_date."
        )

        return start_date, end_date

    @property
    def offset(self) -> Union[int, None]:
        """
        Property to get the offset based on the mode.

        Returns:
        Union[int, None]: The offset value.
        """

        offset = None

        if self.mode == "collection":
            _, _, offset, _ = self.metadata_manager.collection_dates

        elif self.mode == "update":
            offset = 1

        return offset

    @staticmethod
    def get_api_response(
        request_params: dict, headers: dict, url: str = definitions.API_URL
    ) -> dict:
        """
        Send a GET request to the API and return the JSON response.

        Parameters:
        - request_params (dict): The parameters for the API request.
        - headers (dict): The headers for the API request.
        - url (str): The URL for the API.

        Returns:
        dict: containing The JSON response from the API, the requests parameters and the used key's name
        """
        try:
            response = requests.get(
                url, headers=headers, params=request_params, timeout=5
            )

            return {
                "raw_response": response.json(),
                "metadata": {
                    "request_params": request_params,
                    "key_name": key_api2name(headers.get("X-RapidAPI-Key")),
                },
            }

        except requests.RequestException as error:
            settings.logger.error("Error in an API request")
            return {
                "error": error,
                "metadata": {
                    "request_params": request_params,
                    "key_name": key_api2name(headers.get("X-RapidAPI-Key")),
                },
            }

    def generate_requests_params(
        self, **kwargs
    ) -> tuple[list[dict[str, Union[int, str]]], list[dict[str, str]]]:
        """
        Generate a list of request parameters and headers based on the specified inputs.

        kwargs:
        - base_query_kwargs (dict, optional): Additional parameters to be included in each request.
          Defaults to an empty dictionary.

        Returns:
        Tuple[List[Dict[str, Union[int, str]]], List[Dict[str, str]]]:
        A tuple containing a list of dictionaries representing request parameters,
        where each dictionary contains keys 'count' for the number of expected results and 'start' for the offset,
        and a list of dictionaries representing headers.
        """

        # Extracting parameters with default values if not provided
        base_query_kwargs = kwargs.get(
            "base_query_kwargs", {"type": settings.DATA_TYPE_TO_FETCH}
        )
        offset = self.offset

        # generate the request parameters and headers
        requests_params = list()
        headers = list()

        for _, (api_key, remaining_requests) in self.keys_metadata.items():

            for _ in range(remaining_requests):
                # generate header
                headers.append(
                    {"X-RapidAPI-Key": api_key, "X-RapidAPI-Host": settings.API_HOST}
                )

                # generate query_parameters
                count = definitions.MAX_RESULTS_PER_REQUEST
                current_offset = offset + (
                    len(requests_params) * definitions.MAX_RESULTS_PER_REQUEST
                )
                requests_params.append(
                    {"count": count, "start": current_offset, **base_query_kwargs}
                )

                # get only NUM_REQUESTS_FOR_UPDATE request when updating
                if (
                    self.mode == "update"
                    and len(requests_params) == settings.NUM_REQUESTS_FOR_UPDATE
                ):
                    break
            # get only NUM_REQUESTS_FOR_UPDATE request when updating
            if (
                self.mode == "update"
                and len(requests_params) == settings.NUM_REQUESTS_FOR_UPDATE
            ):
                break

        settings.logger.info("generated the requests parameters")

        # Returning the lists of request parameters and headers
        return requests_params, headers

    def get_base_query_params(self, **kwargs) -> dict:
        """
        Generate base query parameters for an API request based on the specified inputs.

        Parameters:
        - data_type (str, optional): The type of metadata to query (e.g., "earthquake").
          Defaults to settings.DATA_TYPE_TO_FETCH.

        Returns:
        dict: A dictionary containing base query parameters for the API request.

        Raises:
        ValueError: If the provided start_date is not in the supported format "YYYY-MM-DD".
        """

        # Extracting parameters with default values if not provided
        data_type = kwargs.get("data_type", settings.DATA_TYPE_TO_FETCH)

        # get start/end dates according to mode
        start_date, end_date = self.start_end_dates

        # Set base query params
        base_query_kwargs = {"type": data_type}

        # check if dates were provided, parse them, or set them to default values.
        for date_name, date in zip(
            ["startDate", "endDate"],
            [start_date, end_date],
        ):
            if date:
                if is_valid_date(date):
                    if isinstance(date, datetime.date):
                        date = date.strftime(definitions.DATE_FORMAT)
                else:
                    raise ValueError(
                        f"{date_name} needs to be a datetime.date object or string in YYYY-MM-DD format"
                    )
            # pylint: disable=else-if-used
            else:
                if date_name == "startDate":
                    date = definitions.TODAY - datetime.timedelta(days=7)
                    date = date.strftime(definitions.DATE_FORMAT)
                elif date_name == "endDate":
                    date = definitions.TODAY.strftime(definitions.DATE_FORMAT)

            base_query_kwargs[date_name] = date

        settings.logger.debug(f"calculated base_query_kwargs:\n{base_query_kwargs}")
        return base_query_kwargs

    def fetch_data(self, **kwargs) -> list[dict]:
        """
        Fetch metadata from the API.

        kwargs:

        - data_type: str (default "earthquake") - type of metadata points to return
        """
        try:
            base_query_kwargs = self.get_base_query_params(**kwargs)
            requests_params, headers = self.generate_requests_params(
                base_query_kwargs=base_query_kwargs
            )

            settings.logger.info("Starting calls to API.")
            with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
                api_responses = list(
                    executor.map(self.get_api_response, requests_params, headers)
                )

            settings.logger.info("Finished calls to API.")

            # todo: verify the responses status
            # todo: update keys remaining requests
            return api_responses

        except Exception as error:
            settings.logger.critical(f"Encountered an error:\n{error}")
            sys.exit(1)
