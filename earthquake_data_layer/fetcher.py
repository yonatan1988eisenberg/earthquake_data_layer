import threading
import traceback
from dataclasses import dataclass
from functools import partial
from typing import Optional

import requests
from fake_headers import Headers

from earthquake_data_layer import definitions, settings
from earthquake_data_layer.helpers import (
    add_rows_to_parquet,
    generate_raw_data_key_from_date,
    is_valid_date,
)
from earthquake_data_layer.proxy_generator import ProxiesGenerator

wait_event = threading.Event()


@dataclass
class Fetcher:
    """
    Fetcher class for handling data retrieval and processing from an API.

    Attributes:
        start_date (str): The start date of the time frame for data retrieval.
        end_date (str): The end date of the time frame for data retrieval.
        header: Headers object for managing HTTP headers.
        metadata (dict): Metadata containing information about the execution and status.
        responses (list): List to store API responses.
        data (list): List to store processed data.
        total_count (int): Total number of rows processed.

    Methods:
        fetch_data(**kwargs):
            Fetch earthquake data for a given time frame, process, upload to S3, and return metadata.

        query_api(query_params, retries=5):
            Query the API for earthquake data within the specified time frame.

        generate_query_params(query_params=None):
            Generate query parameters with default values and update them with provided parameters.

        process():
            Process API responses to calculate the total number of rows and extract data.

        upload_data():
            Upload processed data to S3.

    Properties:
        year:
            Get the year from the start_date.

        month:
            Get the month from the start_date.
    """

    start_date: str
    end_date: str
    header = Headers(headers=True, os="windows")
    metadata: Optional[dict] = None
    responses: Optional[list] = None
    data: Optional[list] = None
    total_count: int = 0

    def fetch_data(self, **kwargs):
        """
        Fetch earthquake data for a given time frame, process, upload to S3, and return metadata with the below keys:
        - start_date
        - end_date
        - execution_date
        - status (done/error/etc.)
        - error_massage (if applicable)
        - count (num of rows, if applicable)
        - data_key (location on s3, if applicable)

        kwargs:
        - query_params: dict. overwrite the default query parameters
        """

        # validate input
        for date in (self.start_date, self.end_date):
            if not is_valid_date(date):
                raise ValueError(
                    f"start_date and end_date should be in {definitions.DATE_FORMAT} format"
                )

        settings.logger.info(
            f"starting to fetch the data for the time frame {self.start_date} - {self.end_date}"
        )

        self.metadata = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "execution_date": definitions.TODAY,
        }

        # run the pipeline, return the metadata upon error
        for step in (
            partial(self.query_api, kwargs.get("query_params"), kwargs.get("proxy")),
            partial(self.process),
            partial(self.upload_data),
        ):
            step_result = step()
            self.metadata.update(step_result)
            if self.metadata.get("error"):
                settings.logger.critical(
                    f"{self.year}-{self.month}: encountered an error, status: {self.metadata.get('status')}"
                )
                break

        settings.logger.info(
            f"{self.year}-{self.month}: finished fetching the data. successful: {'error' in self.metadata.keys()}"
        )

        return self.metadata

    def query_api(
        self,
        query_params: Optional[dict] = None,
        proxy_generator: Optional[ProxiesGenerator] = None,
        retries: int = 10,
    ) -> dict:
        """
        Query the API for earthquake data within the specified time frame.

        Args:
            query_params (dict): Query parameters.
            proxy_generator (ProxiesGenerator): an initialized ProxiesGenerator object
            retries (int): number of allowed API exceptions raised

        Returns:
            dict: Status of the API query.
        """
        # generate query params
        query_params = query_params or dict()
        query_params = self.generate_query_params(query_params)

        # get all the data for the time frame, query until the count is < limit or error occurred
        settings.logger.info(f"{self.year}-{self.month}: starting to query the API")
        self.responses = list()

        # a flag to single we're done fetching, a placeholder and retries counter
        done_fetching = False
        last_error = None
        try_ = 0
        while True:
            try:
                settings.logger.debug(
                    f"{self.year}-{self.month} (try {try_}): query num {len(self.responses) + 1}"
                )
                proxy = (
                    proxy_generator.gen()
                    if isinstance(proxy_generator, ProxiesGenerator)
                    else None
                )

                response = requests.get(
                    definitions.API_URL,
                    headers=self.header.generate(),
                    proxies=proxy,
                    params=query_params,
                    timeout=5,
                )

                settings.logger.debug(
                    f"{self.year}-{self.month} (try {try_}): successfully queried"
                )
                settings.logger.debug(
                    f"{self.year}-{self.month} (try {try_}): proxy: {proxy}"
                )
                response = response.json()
                self.responses.append(response)

                # check if we're done
                if response["metadata"]["count"] < definitions.MAX_RESULTS_PER_REQUEST:
                    settings.logger.debug(
                        f"{self.year}-{self.month} (try {try_}): setting 'done_fetching' to True"
                    )
                    done_fetching = True
                    break

                query_params["offset"] += definitions.MAX_RESULTS_PER_REQUEST

            except (requests.RequestException, IndexError) as error:
                last_error = error
                error_traceback = "".join(
                    traceback.format_exception(None, error, error.__traceback__)
                )
                settings.logger.error(
                    f"{self.year}-{self.month} (try {try_}): encountered an error while querying the API: {error_traceback}"
                )

                try_ += 1
                if try_ > retries:
                    break

        settings.logger.info(
            f"{self.year}-{self.month}: finished querying the API, num responses: {len(self.responses)}"
        )

        if not done_fetching:
            return {
                "status": definitions.STATUS_QUERY_API_FAIL,
                "error": repr(last_error),
            }

        return {"status": definitions.STATUS_QUERY_API_SUCCESS}

    def generate_query_params(self, query_params: Optional[dict] = None) -> dict:
        """
        Generate query parameters with default values and update them with provided parameters.

        Args:
            query_params (dict): Additional query parameters.

        Returns:
            dict: Updated query parameters.
        """

        params = {
            "starttime": self.start_date,
            "endtime": self.end_date,
            "limit": definitions.MAX_RESULTS_PER_REQUEST,
            "offset": 1,
            "format": "geojson",
        }

        query_params = query_params or {}
        params.update(query_params)

        settings.logger.info(f"{self.year}-{self.month}: generated query parameters")
        settings.logger.debug(f"{self.year}-{self.month}: {params}")

        return params

    def process(self) -> dict:
        """
        Process API responses to calculate the total number of rows and extract data.

        Returns:
            dict: Status of the processing.
        """

        settings.logger.info(
            f"{self.year}-{self.month}: started processing the responses"
        )

        if len(self.responses) == 0:
            settings.logger.critical(
                f"{self.year}-{self.month}: couldn't fetch healthy responses"
            )
            return {"status": definitions.STATUS_PROCESS_FAIL, "error": True}

        self.data = list()
        for response in self.responses:
            # sum the number of rows
            self.total_count += response["metadata"]["count"]

            # bundle all the data to one list of dictionaries
            response_data = [
                {**feature.get("properties", {}), "id": feature.get("id")}
                for feature in response["features"]
            ]
            self.data.extend(response_data)

        settings.logger.info(
            f"{self.year}-{self.month}: finished processing the responses"
        )

        return {"status": definitions.STATUS_PROCESS_SUCCESS, "count": self.total_count}

    def upload_data(self) -> dict:
        """
        Upload processed data to S3.

        Returns:
            dict: Status of the data upload.
        """

        settings.logger.info(f"{self.year}-{self.month}: started to upload the data")

        key = generate_raw_data_key_from_date(self.year, self.month)

        data_uploaded = add_rows_to_parquet(self.data, key)

        if not data_uploaded:
            settings.logger.critical(
                f"{self.year}-{self.month}: encountered an error while uploading the data"
            )
            return {"status": definitions.STATUS_UPLOAD_DATA_FAIL, "error": True}

        settings.logger.info(f"{self.year}-{self.month}: finished uploading the data")
        return {"data_key": key, "status": definitions.STATUS_UPLOAD_DATA_SUCCESS}

    @property
    def year(self) -> str:
        """
        Get the year from the start_date.

        Returns:
            str: Year.
        """
        return self.start_date[:4]

    @property
    def month(self) -> str:
        """
        Get the month from the start_date.

        Returns:
            str: Month.
        """
        return self.start_date[5:7]
