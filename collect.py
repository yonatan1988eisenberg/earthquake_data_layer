import json
import logging

import pandas as pd

from earthquake_data_layer import (
    Downloader,
    MetadataManager,
    Preprocess,
    Storage,
    Validate,
    exceptions,
    helpers,
    settings,
)


def run_collection(run_id: str):
    """
    Execute a data collection run.

    Parameters:
    - run_id (str): A unique identifier for the run.

    Raises:
    - EOFError: Raised if no more data collection cycles are needed based on the metadata.
    - RuntimeError: Raised if there is an error during the update and upload process.

    Returns:
    dict: Metadata for the data collection run.

    This function performs the following steps:
    1. Initializes a MetadataManager to manage metadata for the run.
    2. Aborts the run if no more cycles are needed based on the 'done_collecting' flag in the metadata.
    3. Uses a Downloader to fetch data from the API.
    4. Preprocesses the responses using the Preprocess class.
    5. Validates the run using the Validate class and updates the 'validation_report' in the run metadata.
    6. Uploads artifacts to a storage service using the Storage class.
      - Uploads responses metadata as a JSON file.
      - Uploads the processed data as a DataFrame.
      - Updates the 'runs.parquet' file with the run metadata.
    7. Updates metadata and saves it using the 'updated_and_save_metadata' function.
    8. Returns the run metadata if all upload and update operations are successful.
    9. Raises a RuntimeError if any of the upload or update operations fail.

    Note: Ensure proper exception handling and logging are implemented in production.
    """

    logging.info(f"started running with id {run_id}")

    # verify the bucket exist and connection can be established
    storage = Storage()
    if not storage.bucket_exists(settings.AWS_BUCKET_NAME, create=True):
        raise exceptions.StorageConnectionError(
            "couldn't establish connection to the cloud"
        )

    try:
        # get metadata
        metadata_manager = MetadataManager()

        # abort if no more cycles are needed
        if metadata_manager.metadata.get("done_collecting"):
            raise exceptions.DoneCollectingError(
                "According to the metadata, we're done_collecting"
            )

        # fetch data
        downloader = Downloader(metadata_manager)
        responses = downloader.fetch_data()

        # preprocess
        data_key = helpers.generate_data_key(run_id)
        try:
            run_metadata, responses_metadata, data = Preprocess.preprocess(
                responses, run_id, data_key
            )
        except exceptions.NoHealthyRequestsError:
            settings.logger.critical("couldn't fetch healthy responses")
            raise

        # validate
        run_metadata["validation_report"] = Validate.validate(
            metadata_manager=metadata_manager, run_metadata=run_metadata
        )

        # upload artifacts
        connection = Storage()

        # the responses metadata
        responses_metadata_uploaded = connection.save_object(
            json.dumps(responses_metadata).encode("utf-8"),
            helpers.generate_responses_metadata_key(run_id),
        )

        # the data
        data_uploaded = helpers.upload_df(
            pd.DataFrame.from_records(data),
            helpers.generate_data_key(run_id),
            connection,
        )

        # update runs.parquet
        runs_metadata_updated = helpers.add_rows_to_parquet(run_metadata, connection)

        # update metadata and save
        metadata_saved = helpers.updated_and_save_metadata(
            metadata_manager, run_metadata
        )

        # pylint: disable=no-else-return
        if all(
            [
                data_uploaded,
                responses_metadata_uploaded,
                runs_metadata_updated,
                metadata_saved,
            ]
        ):
            settings.logger.info("Data collection run completed successfully.")
            return run_metadata
        else:
            raise RuntimeError(
                f"""Update and upload error:\n
                Responses metadata uploaded: {responses_metadata_uploaded}\n
                Data uploaded: {data_uploaded}\n
                Runs metadata updated: {runs_metadata_updated}\n
                Metadata saved: {metadata_saved}"""
            )
    except Exception as e:
        settings.logger.error(
            f"An error occurred during the data collection run: {e.__traceback__}"
        )
        raise
