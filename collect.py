import json

import click
import pandas as pd

from earthquake_data_layer import (
    Downloader,
    MetadataManager,
    Preprocess,
    Storage,
    Validate,
    definitions,
    helpers,
)


def updated_and_save_metadata(
    metadata_manager: MetadataManager, run_metadata: dict
) -> bool:
    """
    Update collection dates in the metadata manager based on the provided run metadata.

    Parameters:
    - metadata_manager (MetadataManager): The metadata manager instance.
    - run_metadata (dict): Metadata from the current run.

    Returns:
    bool: True if the update and save operation is successful, False otherwise.

    This function updates the collection dates in the metadata manager based on the
    information provided in the run metadata. It adjusts the start date, end date, offset,
    and collection start date according to the logic defined for different scenarios,
    including the first run, last run, and collecting before the required start date.

    Data collections will be made of cycles. collection_start_time will be set when a new one begins and data from
    start_date to {collection_start_time - 1 day} will be gathered in that cycle by conducting as many runs as needed.
    When all the data will be gathered (end_date <= start_date) start_date will be set to collection_start_time and
    collection_start_time will be set to False, indicating a new cycle begins.
    The process repeats until all the data from {run_date - 1 day} to setting.EARLIEST_EARTHQUAKE_DATE is collected.

    If the update is successful, the function returns True; otherwise, it returns False.
    """
    (
        start_date,
        end_date,
        _,
        collection_start_date,
    ) = metadata_manager.collection_dates

    # when more runs are required to complete a cycle
    new_end_date = run_metadata["next_run_dates"]["earliest_date"]
    new_offset = run_metadata["next_run_dates"]["offset"]

    # on the first run of a cycle
    if not collection_start_date:
        collection_start_date = definitions.TODAY.strftime(definitions.DATE_FORMAT)

    # on the last run
    elif new_end_date <= start_date and end_date == definitions.YESTERDAY.strftime(
        definitions.DATE_FORMAT
    ):
        metadata_manager.metadata["done_collecting"] = True

    # at the end of a cycle
    elif new_end_date <= start_date:
        start_date = collection_start_date
        new_end_date = definitions.YESTERDAY.strftime(definitions.DATE_FORMAT)
        new_offset = 1
        collection_start_date = False

    return metadata_manager.update_collection_dates(
        start_date=start_date,
        end_date=new_end_date,
        offset=new_offset,
        collection_start_date=collection_start_date,
        save=True,
    )


@click.command()
@click.option(
    "--run_id",
    default=definitions.TODAY.strftime("%Y-%m-%d_%H-%m-%S"),
    help="a unique id for the run",
)
def run(run_id: str):
    # get metadata
    metadata_manager = MetadataManager()

    # abort if no more cycles are needed
    if metadata_manager.metadata.get("done_collecting"):
        raise EOFError("according to the metadata we're done_collecting")

    # fetch data
    downloader = Downloader(metadata_manager)
    responses = downloader.fetch_data()

    # preprocess
    data_key = helpers.generate_data_key(run_id)
    run_metadata, responses_metadata, data = Preprocess.preprocess(
        responses, run_id, data_key
    )

    # validate
    run_metadata["validation_report"] = Validate.validate(
        metadata_manager=metadata_manager, run_metadata=run_metadata
    )

    # upload artifacts:
    connection = Storage()

    # the responses metadata
    responses_metadata_uploaded = connection.save_object(
        json.dumps(responses_metadata).encode("utf-8"),
        helpers.generate_responses_metadata_key(run_id),
    )

    # the data
    data_uploaded = helpers.upload_df(
        pd.DataFrame.from_records(data), helpers.generate_data_key(run_id), connection
    )

    # update runs.parquet
    runs_metadata_updated = helpers.update_runs_metadata(run_metadata, connection)

    # update metadata and save
    metadata_saved = updated_and_save_metadata(metadata_manager, run_metadata)

    # pylint: disable=no-else-return
    if all(
        [
            data_uploaded,
            responses_metadata_uploaded,
            runs_metadata_updated,
            metadata_saved,
        ]
    ):
        return run_id

    else:
        raise RuntimeError(
            f"""update and upload error:\n
            responses_metadata_uploaded: {responses_metadata_uploaded}\n
            data_uploaded: {data_uploaded}\n
            runs_metadata_updated: {runs_metadata_updated}\n
            metadata_saved: {metadata_saved}"""
        )


# pylint: disable=no-value-for-parameter
if __name__ == "__main__":
    run()
