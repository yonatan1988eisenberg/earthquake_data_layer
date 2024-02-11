from dataclasses import dataclass
from typing import Optional

import requests
from fake_headers import Headers

from earthquake_data_layer import definitions, helpers, settings


@dataclass
class Fetcher:
    start_date: str
    end_date: str
    header = Headers(headers=True, os="windows")
    metadata: Optional[dict] = None
    responses: Optional[list] = None
    data: Optional[list] = None
    total_count: int = 0

    def fetch_data(self, **kwargs):
        """
        takes a start date and an end date and runs the pipeline:
        1. download all the data for those dates (if for any response there's an error the process will skip to 5)
        2. process
        4. save the data to s3
        5. return a dictionary with the below keys:
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
            if not helpers.is_valid_date(date):
                raise ValueError(
                    f"start_date and end_date should be in {definitions.DATE_FORMAT} format"
                )

        settings.logger.info(
            f"starting to fetch the data for the time frame {self.start_date} - {self.end_date}"
        )

        self.metadata = {
            "start_date": self.start_date,
            "end_data": self.end_date,
            "execution_date": definitions.TODAY,
        }

        # run the pipeline, return the metadata upon error
        for step_result in (
            self.query_api(kwargs.get("query_params", {})),
            self.process(),
            self.upload_data(),
        ):
            self.metadata.update(step_result)
            if self.metadata.get("error"):
                break

        settings.logger.info("finished fetching the data")

        return self.metadata

    def query_api(self, query_params) -> dict:

        # generate query params
        query_params = self.generate_query_params(query_params)

        # get all the data for the time frame, query until the count is < limit or error occurred
        settings.logger.info("starting to query the API")
        self.responses = list()
        while True:
            try:
                response = requests.get(
                    definitions.API_URL_,
                    headers=self.header.generate(),
                    params=query_params,
                    timeout=5,
                ).json()

                self.responses.append(response)

                # check if we need more requests
                if response["metadata"]["count"] < definitions.MAX_RESULTS_PER_REQUEST_:
                    break

                query_params["offset"] += definitions.MAX_RESULTS_PER_REQUEST_

            except (requests.RequestException, IndexError) as error:
                settings.logger.critical(
                    f"encountered an error while querying the API: {error}"
                )
                return {"error": error}

        settings.logger.info(
            f"finished querying the API, num responses: {len(self.responses)}"
        )

        return {"status": "successfully queried the API"}

    def generate_query_params(self, query_params: Optional[dict] = None) -> dict:
        """
        generates the default query params and updates them with the provided query_params
        """

        query_params = query_params or dict()

        params = {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "limit": definitions.MAX_RESULTS_PER_REQUEST_,
            "offset": 1,
            "format": "geojson",
        }

        params.update(query_params)

        settings.logger.info("generated query parameters")
        settings.logger.debug(f"{params}")

        return params

    def process(self) -> dict:
        """
        calculates the total number of rows and extract the data from the responses
        """

        settings.logger.info("started processing the responses")
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

        settings.logger.info("finished processing the responses")

        return {"status": "successfully processed responses", "count": self.total_count}

    def upload_data(self):
        data_uploaded = helpers.add_rows_to_parquet(
            self.data,
            key=helpers.generate_data_key_from_date(self.year, self.month),
        )

        if not data_uploaded:
            settings.logger.critical("encountered an error while uploading the data")
            return {"error": "encountered an error while uploading the data"}

        return {"status": "uploaded the data"}

    @property
    def year(self):
        return int(self.start_date[:4])

    @property
    def month(self):
        return int(self.start_date[5:7])
