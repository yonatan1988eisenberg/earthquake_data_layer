import itertools
from collections import Counter
from typing import Literal

from earthquake_data_layer import helpers, settings


class Preprocess:
    @classmethod
    def process_response(
        cls,
        response: dict,
        run_id: str,
        data_key: str,
        responses_ids: list[str],
        count: int,
        columns: Counter,
    ) -> tuple[dict, list[dict], list[str], int, Counter]:
        """
        Process a single response.

        Parameters:
        - response (dict): The response dictionary.
        - run_id (str): The run ID.
        - data_key (str): The data key.
        - responses_ids (list): List to store response IDs.
        - count (int): Current count.
        - columns (Counter): Counter for columns.

        Returns:
        tuple: Metadata, processed data, updated response IDs, updated count, updated columns.
        """

        # Extract response's components
        metadata = response.get("metadata", {})
        raw_response = response.get("raw_response", {})
        data = raw_response.pop("data", [])

        # Generate response_id
        response_id = f"{run_id}_{helpers.random_string(settings.RANDOM_STRING_LENGTH_RESPONSE_ID)}"
        responses_ids.append(response_id)

        # Process the data
        processed_data = [{**row, "response_id": response_id} for row in data]

        # Update columns
        columns.update(itertools.chain.from_iterable(processed_data))

        # Combine metadata and the response's metadata, add items
        metadata = {
            **metadata,
            **raw_response,
            "response_id": response_id,
            "data_key": data_key,
        }

        # Update count
        count += metadata.get("count", 0) or len(processed_data)

        return metadata, processed_data, responses_ids, count, columns

    @classmethod
    def bundle(
        cls,
        three_d_data: list[list[dict]],
        responses_ids: list[str],
        columns: Counter,
        mode: Literal["collection", "update"],
        count: int,
        data_key: str,
    ) -> tuple[list[dict], dict]:
        """
        Bundle metadata, processed data, and other information.

        Parameters:
        - three_d_data (list): List of processed data.
        - responses_ids (list): List of response IDs.
        - columns (Counter): Counter for columns.
        - mode (Literal["collection", "update"]): Collection mode or update mode.
        - count (int): Count of rows.
        - data_key (str): Data key.

        Returns:
        tuple: Merged data, run metadata.
        """

        run_metadata = {
            "mode": mode,
            "count": count,
            "data_key": data_key,
            "responses_ids": responses_ids,
            "columns": columns,
        }

        two_d_data = list(itertools.chain.from_iterable(three_d_data))

        return two_d_data, run_metadata

    @classmethod
    def preprocess(
        cls,
        responses: list[dict],
        run_id: str,
        data_key,
        mode: Literal["collection", "update"] = "collection",
    ) -> tuple[dict, list[dict], list[dict]]:
        """
        Preprocess responses, where each response is expected to be:
        response = {
            "raw_response": response.json(),
            "metadata": {
                         "request_params": request_params,
                         "key_name": key_name,
                        }
            }

        Parameters:
        - run_id (str): The run ID.
        - responses (list): List of response dictionaries.
        - mode (Literal["collection", "update"]): Collection mode or update mode.

        Returns:
        tuple: run metadata, responses metadata and data, where

        run_metadata = {
            "mode": mode,
            "count": count,
            "data_key": data_key,
            "responses_ids": responses_ids,
            "columns": columns,
        }

        responses_metadata = list[{
            metadata,   # originated from responses["metadata]
            raw_response, # originated from responses["raw_response] but excludes the data
            "response_id": response_id,
            "data_key": data_key,
        }]

        data is a single list containing all the data from the input responses
        """
        responses_ids = []
        columns = Counter()
        count = 0

        responses_metadata = []
        three_d_data = []
        for response in responses:
            (
                metadata,
                two_dim_data,
                responses_ids,
                count,
                columns,
            ) = cls.process_response(
                response, run_id, data_key, responses_ids, count, columns
            )
            responses_metadata.append(metadata)
            three_d_data.append(two_dim_data)
        data, run_metadata = cls.bundle(
            three_d_data, responses_ids, columns, mode, count, data_key
        )

        return run_metadata, responses_metadata, data
