import datetime
import json
import logging
import random
import string
import traceback
from collections.abc import Iterable
from copy import deepcopy
from random import choice
from typing import Optional, Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dateutil.relativedelta import relativedelta

from earthquake_data_layer import definitions, settings
from earthquake_data_layer.fetcher import Fetcher
from earthquake_data_layer.metadata_manager import MetadataManager
from earthquake_data_layer.storage import Storage

LOG_MESSAGE_DATASET_MONTHS = "initiated DatasetMonths with time frame {} - {}"
LOG_MESSAGE_DOWNLOAD_DATA = "starting to download data for dates:"
LOG_MESSAGE_SUCCESS = "{}-{}: success"
LOG_MESSAGE_ERROR = "{}-{}: error"


def is_valid_date(
    date: Union[datetime.date, str], str_format: str = definitions.DATE_FORMAT
):
    """
    Check if the input date is valid and convert it to the specified string format.

    Parameters:
    - date (Union[date, str]): The input date, either as a date object or a string.
    - str_format (str): The expected string format for the date. Default is 'YYYY-MM-DD'.

    Returns:
    Union[str, bool]: If the input date is valid, return the date as a string in the
    specified format. If not valid, return False.

    This function checks the validity of the input date, which can be either a date
    object or a string representation. If the input date is a date object, it is
    converted to a string with the specified format. If the input date is a string,
    an attempt is made to parse it as a date, and if successful, it is returned as a
    string in the specified format. If the input is not a valid date, the function
    returns False.
    """

    if isinstance(date, datetime.date):
        return date.strftime(str_format)
    if isinstance(date, str):
        try:
            # Attempt to parse the string as a date
            datetime.datetime.strptime(date, str_format)
            return date
        except ValueError:
            # The string is not in the expected format
            return False

    return False


def generate_data_key(run_id: str):
    """generates a key for a run's data file"""
    return f"data/raw_data/{definitions.TODAY.strftime('%Y')}/{run_id}_data.parquet"


def generate_raw_data_key_from_date(year: str, month: str):
    """generates a key for a data file based on a date"""
    return f"data/raw_data/{year}/{year}_{month}_raw_data.parquet"


def get_month_start_end_dates(year: int, month: int) -> tuple[str, str]:
    """
    takes a year and a month and returns two strings of the first and last day of
    that month in {definitions.DATE_FORMAT} format.
    """
    start_date = datetime.date(year, month, 1)
    end_date = start_date + relativedelta(day=31)

    return start_date.strftime(definitions.DATE_FORMAT), end_date.strftime(
        definitions.DATE_FORMAT
    )


def generate_responses_metadata_key(run_id: str):
    """generates a key for a run's responses' metadata file"""
    return f"data/runs/{definitions.TODAY.strftime('%Y')}/{run_id}.parquet"


def random_string(n: int = 5):
    """returns a random lowercase string of length n"""
    return "".join(random.choices(string.ascii_lowercase, k=n))


def upload_df(df: pd.DataFrame, key: str, storage: Storage) -> bool:
    """
    Uploads a DataFrame to the storage.

    Parameters:
    - df (pd.DataFrame): The DataFrame to upload.
    - key (str): The key to use for storage.
    - storage (Storage): The storage instance.

    Returns:
    bool: True if the upload is successful, False otherwise.
    """
    table = pa.Table.from_pandas(df)
    writer = pa.BufferOutputStream()
    pq.write_table(table, writer)
    return storage.save_object(bytes(writer.getvalue()), key)


def add_rows_to_parquet(
    rows: Union[dict, list[dict]],
    key: str = definitions.RUNS_METADATA_KEY,
    storage: Optional[Storage] = None,
    remove_duplicates=True,
) -> bool:
    """
    uploads the row(s) to the parquet file located at {key}. If the file doesn't exist creates it.

    Parameters:
    - rows (dict | list[dict]): The data to append to the file.
    - key (str): The key for the parquet file. Default is the runs metadata key.
    - storage (Storage): A storage instance, optional.

    Returns:
    bool: True if the update is successful, False otherwise.
    """

    if storage is None:
        storage = Storage()

    if isinstance(rows, dict):
        rows = [rows]

    # load the file from storage and append the new line to it
    try:
        df = pd.read_parquet(storage.load_object(key))
        df = pd.concat([df, pd.DataFrame.from_records(rows)], ignore_index=True)
        if remove_duplicates:
            df = df.drop_duplicates()

    # if the file doesn't exist (first run)
    except FileNotFoundError:
        settings.logger.error(f"Couldn't find {key}")
        df = pd.DataFrame.from_records(rows)

    # upload to storage
    return upload_df(df, key, storage)


def key_api2name(key: str) -> Union[str, bool]:
    """
    Convert API key to key name.

    Parameters:
    - key (str): The API key.

    Returns:
    str: The corresponding key name.
    """
    candidates = [
        key_name for key_name, api_key in settings.API_KEYs.items() if api_key == key
    ]
    match len(candidates):
        case 1:
            return next(iter(candidates))
        case 0:
            return False
        case _:
            rand_candidate = choice(candidates)
            logging.info(
                f"more than one API name matches the key, returning a random choice - {rand_candidate}"
            )
            return rand_candidate


def updated_and_save_metadata(
    metadata_manager: MetadataManager, run_metadata: dict, save: bool = True
) -> bool:
    """
    Update collection dates in the metadata manager based on the provided run metadata.

    Parameters:
    - metadata_manager (MetadataManager): The metadata manager instance.
    - run_metadata (dict): Metadata from the current run.
    - save (bool): if to upload the updated metadata, defaults to True

    Returns:
    bool: True if the update and save operation is successful, False otherwise.

    This function updates the collection dates in the metadata manager based on the
    information provided in the run metadata. It adjusts the start date, end date, offset,
    and collection start date according to the logic defined for different scenarios,
    including the first run, last run, and collecting before the required start date.

    Data collections will be made of cycles. collection_start_time will be set when a new one begins and data from
    start_date to {collection_start_time - 1 day} will be gathered in that cycle by conducting as many runs as needed.
    When all the data will be gathered (new_end_date = start_date) start_date will be set and the rest of the parameters
    will be set to False, indicating a new cycle begins.
    The process repeats until all the data from {run_date - 1 day} to setting.EARLIEST_EARTHQUAKE_DATE is collected,
    at which point start and end date will indicate a date range (from which the data was collected during
    all the cycles) and offset and collection_start_time will be set to False.

    If the update is successful, the function returns True; otherwise, it returns False.

    note: some duplicated rows are expected but no data will be lost.
    """

    (
        start_date,
        _,
        _,
        collection_start_date,
    ) = metadata_manager.collection_dates

    new_start_date = deepcopy(start_date)
    new_end_date = deepcopy(run_metadata["next_run_dates"]["earliest_date"])
    new_offset = deepcopy(run_metadata["next_run_dates"]["offset"])
    new_collection_start_date = deepcopy(collection_start_date)

    first_cycle_run = collection_start_date == definitions.TODAY.strftime(
        definitions.DATE_FORMAT
    )
    last_cycle_run = new_end_date == start_date

    # at the end of a cycle
    if last_cycle_run:
        # collection_start_date - 1 day
        new_start_date = (
            datetime.datetime.strptime(
                new_collection_start_date, definitions.DATE_FORMAT
            )
            - datetime.timedelta(days=1)
        ).strftime(definitions.DATE_FORMAT)
        new_end_date = False
        new_offset = False
        new_collection_start_date = False

    # on the last run
    if last_cycle_run and first_cycle_run:
        metadata_manager.metadata["done_collecting"] = True
        new_end_date = new_start_date
        new_start_date = settings.EARLIEST_EARTHQUAKE_DATE
        new_offset = False
        new_collection_start_date = False

    # this save metadata_manager.metadata["done_collecting"] = True as well
    return metadata_manager.update_collection_dates(
        start_date=new_start_date,
        end_date=new_end_date,
        offset=new_offset,
        collection_start_date=new_collection_start_date,
        save=save,
    )


class DatasetMonths:
    """
    Iterates over months within a given date range.
    """

    def __init__(self, **kwargs):
        """
        Initialize DatasetMonths with optional parameters.

        Args:
            first_date (Union[datetime.datetime, str]): Start date of the iteration.
            last_date (Union[datetime.datetime, str]): End date of the iteration.
            date_format (str): Format of date strings (default: definitions.DATE_FORMAT).
        """
        self.first_date: Union[datetime.datetime, str] = (
            kwargs.get("first_date") or settings.EARLIEST_EARTHQUAKE_DATE
        )
        self.last_date: Union[datetime.datetime, str] = (
            kwargs.get("last_date") or definitions.YESTERDAY
        )
        self.date_format: str = kwargs.get("date_format") or definitions.DATE_FORMAT

        self.verify_input()

        settings.logger.info(
            f"initiated DatasetMonths with time frame {self.first_date.strftime(definitions.DATE_FORMAT)} - {self.last_date.strftime(definitions.DATE_FORMAT)}"
        )

    def verify_input(self):
        """
        Verify and convert input dates to datetime objects if they are strings.
        Raises:
            ValueError: If the date is not a valid date string.
        """
        try:
            if isinstance(self.first_date, str):
                self.first_date = datetime.datetime.strptime(
                    self.first_date, self.date_format
                )
            if isinstance(self.last_date, str):
                self.last_date = datetime.datetime.strptime(
                    self.last_date, self.date_format
                )
        except ValueError as e:
            raise ValueError(f"Invalid input: {e}") from e

    def __iter__(self):
        """
        Initialize iterator for DatasetMonths.
        """
        self.current_month = self.first_date - relativedelta(months=1)
        return self

    def __next__(self):
        """
        Get the next year and month in the iteration.

        Returns:
            Tuple[int, int]: Year and month.
        """
        self.current_month += relativedelta(months=1)

        if self.current_month > self.last_date:
            raise StopIteration

        return self.current_month.year, self.current_month.month


def fetch_months_data(
    months: Iterable,
    metadata: Optional[dict] = None,
    storage: Optional[Storage] = None,
    runs_key: str = definitions.COLLECTION_RUNS_KEY,
    metadata_key: Optional[str] = definitions.COLLECTION_METADATA_KEY,
) -> dict:
    """
    Fetch earthquake data for a given list of months, saves the return value from fetcher.fetch_data() at {runs_key}
    and returns the updated metadata.

    Args:
        months (Iterable): Iterable of tuples representing year and month.
        metadata (dict): Metadata dictionary.
        storage (Storage): a Storage object, optional.
        runs_key (str): where tho save the result of each run, default to definitions.COLLECTION_RUNS_KEY.
        metadata_key (str): where tho save the metadata, default to definitions.COLLECTION_METADATA_KEY.

    Returns:
        dict: Updated metadata.
    """
    if not metadata:
        metadata = dict()

    # verify details key exists and is a dict
    metadata.setdefault("details", {})

    if not storage and any((runs_key, metadata_key)):
        storage = Storage()

    settings.logger.info(LOG_MESSAGE_DOWNLOAD_DATA)

    error_flag = False
    new_rows = list()

    for i, (year, month) in enumerate(months):
        metadata["details"].setdefault(year, {})

        start_date, end_date = get_month_start_end_dates(year, month)
        fetcher = Fetcher(start_date=start_date, end_date=end_date)
        result = fetcher.fetch_data()

        new_rows.append(result)

        if result.get("status") == definitions.STATUS_UPLOAD_DATA_SUCCESS:
            log_msg = LOG_MESSAGE_SUCCESS.format(year, month)
            settings.logger.info(log_msg)
            metadata["details"][year][month] = definitions.STATUS_PIPELINE_SUCCESS
        else:
            log_msg = LOG_MESSAGE_ERROR.format(year, month)
            settings.logger.info(log_msg)
            metadata["details"][year][month] = definitions.STATUS_PIPELINE_FAIL
            error_flag = True

        if i != 0 and i % settings.COLLECTION_BATCH_SIZE == 0:
            if runs_key:
                add_rows_to_parquet(new_rows, runs_key, storage=storage)
            if metadata_key:
                storage.save_object(
                    json.dumps(metadata).encode("utf-8"),
                    metadata_key,
                )

    if runs_key:
        add_rows_to_parquet(new_rows, runs_key, storage=storage)

    if not error_flag:
        metadata["status"] = definitions.STATUS_COLLECTION_METADATA_COMPLETE

    return metadata


def verify_storage_connection(storage: Optional[Storage] = None):
    """attempts to connect to the storage, returns True is successful, False otherwise"""
    if not storage:
        storage = Storage()

    try:
        storage.bucket_exists(create=True)
        return True
    except Exception as error:
        error_traceback = "".join(
            traceback.format_exception(None, error, error.__traceback__)
        )
        settings.logger.critical(f"Could not connect to the cloud:\n {error_traceback}")

        return False
