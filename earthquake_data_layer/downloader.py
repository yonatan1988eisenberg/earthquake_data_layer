import datetime
import json
import logging
import math
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Literal, Union

import requests

from earthquake_data_layer import Data, definitions, settings


class Downloader:
    """
    A class for downloading data from an API.
    """

    @staticmethod
    def get_api_response(
        request_params: dict, headers: dict, url: str = definitions.API_URL
    ):
        """
        Send a GET request to the API and return the JSON response.

        Parameters:
        - request_params (dict): The parameters for the API request.
        - headers (dict): The headers for the API request.
        - url (str): The URL for the API.

        Returns:
        dict: The JSON response from the API.
        """
        try:
            response = requests.get(
                url, headers=headers, params=request_params, timeout=5
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.error(f"Error in API request: {e}")
            raise

    @staticmethod
    def generate_requests_params(**kwargs):
        """
        Generate a list of request parameters based on the specified inputs.

        kwargs:
        - total_results (int, optional): The total number of results to be retrieved.
          Defaults to definitions.MAX_RESULTS_PER_REQUEST.
        - offset (int, optional): The initial offset for the first request. Defaults to 1.
        - base_params (dict, optional): Additional parameters to be included in each request.
          Defaults to an empty dictionary.

        Returns:
        list: A list of dictionaries representing request parameters, where each dictionary
              contains keys 'count' for the number of expected results and 'start' for the offset.

        Example:
        >>> total_results = 100
        >>> offset = 5
        >>> base_params = {'startDate': '2023-01-01'}
        >>> requests_params = generate_requests_params(total_results=total_results,
        ...                                            offset=offset,
        ...                                            base_params=base_params)
        >>> print(requests_params)
        [{'count': 20, 'start': 5, 'startDate': '2023-01-01'},
         {'count': 20, 'start': 25, 'startDate': '2023-01-01'},
         {'count': 20, 'start': 45, 'startDate': '2023-01-01'},
         {'count': 20, 'start': 65, 'startDate': '2023-01-01'},
         {'count': 20, 'start': 85, 'startDate': '2023-01-01'}]
        """
        # Extracting parameters with default values if not provided
        total_results = kwargs.get("total_results", definitions.MAX_RESULTS_PER_REQUEST)
        offset = kwargs.get("offset", 1)
        base_params = kwargs.get("base_params", {})

        # Calculating the number of requests needed
        num_requests = math.ceil(total_results / definitions.MAX_RESULTS_PER_REQUEST)
        requests_params = []

        # Iterating through each request
        for i in range(num_requests):
            # Calculating the remaining results and the count for the current request
            remaining_results = total_results - (
                i * definitions.MAX_RESULTS_PER_REQUEST
            )
            count = min(definitions.MAX_RESULTS_PER_REQUEST, remaining_results)

            # Calculating the current offset for the current request
            current_offset = offset + (i * definitions.MAX_RESULTS_PER_REQUEST)

            # Creating a dictionary with request parameters and adding it to the list
            request_params = {"count": count, "start": current_offset, **base_params}
            requests_params.append(request_params)

        # Returning the list of request parameters
        return requests_params

    @staticmethod
    def get_base_query_params(**kwargs) -> dict:
        """
        Generate base query parameters for an API request based on the specified inputs.

        Parameters:
        - data_type (str, optional): The type of data to query (e.g., "earthquake"). Defaults to "earthquake".
        - start_of_time (bool, optional): If True, sets the start date to the earliest supported date (1638-01-01).
        - last_week (bool, optional): If True, sets the start date to one week ago from the current date.
        - start_date (str or datetime.date, optional): If provided, sets the start date to the specified value.
          It should be in the format "YYYY-MM-DD".

        Returns:
        dict: A dictionary containing base query parameters for the API request.

        Raises:
        ValueError: If the provided start_date is not in the supported format "YYYY-MM-DD".

        Example:
        >>> base_query_params = get_base_query_params(data_type="earthquake", last_week=True)
        >>> print(base_query_params)
        {'type': 'earthquake', 'startDate': '2023-12-18'}
        """
        # Extracting parameters with default values if not provided
        data_type = kwargs.get("data_type", "earthquake")
        start_of_time = kwargs.get("start_of_time", False)
        last_week = kwargs.get("last_week", False)
        start_date = kwargs.get("start_date", False)

        # Set base query params
        base_query_kwargs = {"type": data_type}

        if start_of_time:
            base_query_kwargs["startDate"] = "1638-01-01"
        elif last_week:
            requested_date = datetime.date.today() - datetime.timedelta(weeks=1)
            base_query_kwargs["startDate"] = requested_date.strftime("%Y-%m-%d")
        elif not start_date:
            if isinstance(start_date, str):
                # Verify str is in format
                try:
                    datetime.datetime.strptime(start_date, "%Y-%m-%d")
                    base_query_kwargs["startDate"] = start_date
                except ValueError as error:
                    raise ValueError(
                        f"{start_date} is not a supported date format, use YYYY-MM-DD"
                    ) from error
            elif isinstance(start_date, datetime.date):
                base_query_kwargs["startDate"] = start_date.strftime("%Y-%m-%d")

        return base_query_kwargs

    @classmethod
    def get_num_results(cls, num_results: Union[Literal["max"], int]) -> int:
        """
        Calculate the maximum number of results based on available API requests.

        Parameters:
        - num_results (Union[Literal["max"], int]): The desired number of results.
          If "max" is provided, the maximum possible results based on available API requests are returned.

        Returns:
        int: The calculated number of results.

        Raises:
        ValueError: If num_results is an integer less than 1.

        Example:
        >>> get_num_results("max")
        150000  # Assuming 150 remaining requests and MAX_RESULTS_PER_REQUEST is 1000

        >>> get_num_results(50000)
        50000  # If 50000 is within the available API requests limit

        >>> get_num_results(0)
        ValueError: num_results must be larger than 0
        """
        # Validate and handle the "max" case
        if num_results == "max":
            return cls._calculate_max_results()

        # Validate and handle integer input
        if not isinstance(num_results, int) or num_results < 1:
            raise ValueError("num_results must be an integer greater than 0 or 'max'")

        # Calculate the minimum of num_results and the available API requests
        return min(num_results, cls._calculate_max_results())

    @staticmethod
    def _calculate_max_results() -> int:
        """
        Calculate the maximum number of results based on available API requests.

        Returns:
        int: The calculated number of results.
        """
        max_results = 0

        # Calculate the total available results based on remaining API requests
        for key in settings.API_KEYs:
            remaining_requests = Data.get_remaining_requests(key)
            max_results += remaining_requests * definitions.MAX_RESULTS_PER_REQUEST

        return max_results

    @classmethod
    def fetch_data(cls, **kwargs):
        """
        Fetch data from the API.

        kwargs:
        - num_results: str|int (default 'max') - number of results to return. Use 'max' to get the maximum available
        - number of results, taking into account the nuber of results per request and remaining requests for the day.
        - Use an int > 0 to limit the number of results.
        - data_type: str (default "earthquake") - type of data points to return
        - start_of_time: bool (default False) - returns data from the beginning of time (1638)
        - last_week: bool (default False) - returns data from the last week. overrides start_of_time if both are True
        - start_date: str|datetime.date|bool (default False) - a date to begin the search from. str must be
          in YYYY-MM-DD format. overrides start_of_time and last_week if any of them is True
        - offset: int (default 1) - an offset
        - key: str (default settings.API_KEYs[0]) - a valid api key.
        """
        try:
            offset = kwargs.get("offset", 1)
            num_results = cls.get_num_results(kwargs.get("num_results", "max"))
            base_query_kwargs = cls.get_base_query_params(**kwargs)
            requests_params = cls.generate_requests_params(
                total_results=num_results, offset=offset, base_params=base_query_kwargs
            )

            headers = [
                [{"X-RapidAPI-Key": key, "X-RapidAPI-Host": settings.API_HOST}]
                * Data.get_remaining_requests(key)
                for key in settings.API_KEYs
            ][: len(requests_params)]

            with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
                api_responses = list(
                    executor.map(cls.get_api_response, requests_params, headers)
                )

            # todo: Process the API responses as needed
            with open(
                f"{datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}_data",
                "w",
                encoding="utf-8",
            ) as file:
                json.dump(api_responses, file)

        except Exception as error:
            logging.error(f"An error occurred: {error}")
            # Handle the error or re-raise it based on your use case


if __name__ == "__main__":
    Downloader.fetch_data()
