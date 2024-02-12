import json
from collections.abc import Iterable

from earthquake_data_layer import Fetcher, Storage, definitions, helpers, settings

LOG_MESSAGE_DATASET_MONTHS = "initiated DatasetMonths with time frame {} - {}"
LOG_MESSAGE_DOWNLOAD_DATA = "starting to download data for dates:"
LOG_MESSAGE_SUCCESS = "{}-{}: success"
LOG_MESSAGE_ERROR = "{}-{}: error"
LOG_MESSAGE_INIT_DATASET = "initiating the dataset"
LOG_MESSAGE_INIT_DATASET_COMPLETE = "the dataset was successfully downloaded"
LOG_MESSAGE_INIT_DATASET_INCOMPLETE = "the dataset was not successfully downloaded, check collection.parquet and logs for more details"
LOG_MESSAGE_VERIFY_DATASET = "verifying initial dataset was collected"
LOG_MESSAGE_INIT_DATASET_PATCHING = (
    "the initial dataset was not completely downloaded, patching:"
)


def fetch_months_data(months: Iterable, metadata: dict) -> dict:
    """
    Fetch earthquake data for a given list of months.

    Args:
        months (Iterable): Iterable of tuples representing year and month.
        metadata (dict): Metadata dictionary.

    Returns:
        dict: Updated metadata.
    """
    # verify details key exists and is a dict
    metadata.setdefault("details", {})

    storage = Storage()

    settings.logger.info(LOG_MESSAGE_DOWNLOAD_DATA)

    error_flag = False
    new_rows = list()

    for i, (year, month) in enumerate(months):
        metadata["details"].setdefault(year, {})

        start_date, end_date = helpers.get_month_start_end_dates(year, month)
        fetcher = Fetcher(start_date=start_date, end_date=end_date)
        result = fetcher.fetch_data()

        new_rows.append(result)

        if result.get("status") == definitions.STATUS_UPLOAD_DATA_SUCCESS:
            log_msg = LOG_MESSAGE_SUCCESS.format(year, month)
            settings.logger.info(log_msg)
            metadata["details"][year][
                month
            ] = definitions.STATUS_COLLECTION_METADATA_COMPLETE
        else:
            log_msg = LOG_MESSAGE_ERROR.format(year, month)
            settings.logger.info(log_msg)
            metadata["details"][year][
                month
            ] = definitions.STATUS_COLLECTION_METADATA_INCOMPLETE
            error_flag = True

        if i != 0 and i % settings.COLLECTION_BATCH_SIZE == 0:
            helpers.add_rows_to_parquet(
                new_rows, definitions.COLLECTION_RUNS_KEY, storage=storage
            )
            storage.save_object(
                json.dumps(metadata).encode("utf-8"),
                definitions.COLLECTION_METADATA_KEY,
            )

    helpers.add_rows_to_parquet(
        new_rows, definitions.COLLECTION_RUNS_KEY, storage=storage
    )

    if not error_flag:
        metadata["status"] = definitions.STATUS_COLLECTION_METADATA_COMPLETE

    return metadata


def collect_dataset() -> dict:
    """
    Collect earthquake dataset for the specified date range.

    Returns:
        dict: Metadata of the collected dataset.
    """
    first_date = settings.EARLIEST_EARTHQUAKE_DATE
    last_date = definitions.YESTERDAY.strftime(definitions.DATE_FORMAT)

    metadata = {
        "status": definitions.STATUS_COLLECTION_METADATA_INCOMPLETE,
        "details": {},
        "first_date": first_date,
        "last_date": last_date,
    }

    metadata = fetch_months_data(
        helpers.DatasetMonths(first_date=first_date, last_date=last_date), metadata
    )

    return metadata


def patch_dataset(metadata: dict) -> dict:
    """
    Patch incomplete dates in the dataset.

    Args:
        metadata (dict): Metadata of the dataset.

    Returns:
        dict: Updated metadata after patching.
    """
    incomplete_dates = [
        (year, month)
        for year, months in metadata.get("details", {}).items()
        for month, status in months.items()
        if status != definitions.STATUS_PIPELINE_SUCCESS
    ]

    metadata = fetch_months_data(incomplete_dates, metadata)

    return metadata


def verify_initial_dataset() -> bool:
    """
    Verify if the initial earthquake dataset was collected.

    Returns:
        bool: True if the dataset was successfully collected, False otherwise.
    """
    settings.logger.info(LOG_MESSAGE_VERIFY_DATASET)

    storage = Storage()
    if storage.list_objects(prefix=definitions.COLLECTION_METADATA_KEY):
        settings.logger.info("the initial dataset was initiated")

        collection_metadata = json.loads(
            storage.load_object(definitions.COLLECTION_METADATA_KEY)
            .read()
            .decode("utf-8")
        )
        # pylint: disable=no-else-return
        if (
            collection_metadata.get("status")
            == definitions.STATUS_COLLECTION_METADATA_COMPLETE
        ):
            settings.logger.info(LOG_MESSAGE_INIT_DATASET_COMPLETE)
            return True
        elif (
            collection_metadata.get("status")
            == definitions.STATUS_COLLECTION_METADATA_INCOMPLETE
        ):
            settings.logger.info(LOG_MESSAGE_INIT_DATASET_INCOMPLETE)
            collection_metadata = patch_dataset(collection_metadata)

    else:
        settings.logger.info(LOG_MESSAGE_INIT_DATASET)
        collection_metadata = collect_dataset()

    storage.save_object(
        json.dumps(collection_metadata).encode("utf-8"),
        definitions.COLLECTION_METADATA_KEY,
    )

    if (
        collection_metadata.get("status")
        == definitions.STATUS_COLLECTION_METADATA_COMPLETE
    ):
        settings.logger.info(LOG_MESSAGE_INIT_DATASET_COMPLETE)
        return True

    settings.logger.info(LOG_MESSAGE_INIT_DATASET_INCOMPLETE)
    return False
