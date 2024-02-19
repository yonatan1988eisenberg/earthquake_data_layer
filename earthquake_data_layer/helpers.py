import concurrent
import datetime
import json
import math
import random
import string
import traceback
from collections.abc import Iterable
from typing import Optional, Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dateutil.relativedelta import relativedelta

from earthquake_data_layer import definitions, settings
from earthquake_data_layer.proxy_generator import ProxiesGenerator
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
    key: str,
    storage: Optional[Storage] = None,
    remove_duplicates=True,
) -> bool:
    """
    uploads the row(s) to the parquet file located at {key}. If the file doesn't exist creates it.

    Parameters:
    - rows (dict | list[dict]): The data to append to the file.
    - key (str): The key for the parquet file. Default is the runs metadata key.
    - storage (Storage): A storage instance, optional.
    - remove_duplicates (bool): if to drop duplicates, default to True.

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

        settings.logger.debug(
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

    # pylint: disable=import-outside-toplevel
    from earthquake_data_layer.fetcher import Fetcher

    if not metadata:
        metadata = dict()

    # verify details key exists and is a dict
    metadata.setdefault("details", {})

    if not storage and any((runs_key, metadata_key)):
        storage = Storage()

    settings.logger.info(LOG_MESSAGE_DOWNLOAD_DATA)

    num_months = len(list(months))
    num_batches = math.ceil(num_months / settings.COLLECTION_BATCH_SIZE)
    settings.logger.info(f"expecting {num_batches} batch(s)")

    error_flag = False
    new_rows = list()
    months = list(months)
    proxy_generator = ProxiesGenerator()
    for batch in range(num_batches):
        settings.logger.info(f"starting batch {batch + 1}")

        batch_months = months[
            batch
            * settings.COLLECTION_BATCH_SIZE : (batch + 1)
            * settings.COLLECTION_BATCH_SIZE
        ]
        batch_dates = [get_month_start_end_dates(*month) for month in batch_months]
        batch_fetchers = [Fetcher(*date) for date in batch_dates]

        # run the batch concurrently
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=len(batch_fetchers)
        ) as executor:
            futures = [
                executor.submit(fetcher.fetch_data, proxy=proxy_generator)
                for fetcher in batch_fetchers
            ]
            concurrent.futures.wait(futures)
            thread_results = [future.result() for future in futures]

        # process batch results
        for thread_result, (year, month) in zip(thread_results, batch_months):
            new_rows.append(thread_result)
            metadata["details"].setdefault(str(year), {})

            if thread_result.get("status") == definitions.STATUS_UPLOAD_DATA_SUCCESS:
                log_msg = LOG_MESSAGE_SUCCESS.format(year, month)
                settings.logger.info(log_msg)
                metadata["details"][str(year)][
                    month
                ] = definitions.STATUS_PIPELINE_SUCCESS
            else:
                log_msg = LOG_MESSAGE_ERROR.format(year, month)
                settings.logger.info(log_msg)
                metadata["details"][str(year)][month] = definitions.STATUS_PIPELINE_FAIL
                error_flag = True

        settings.logger.info(f"finished batch {batch + 1}")
        # save if keys are provided
        if runs_key:
            settings.logger.debug("saving rows")
            add_rows_to_parquet(new_rows, runs_key, storage=storage)
        if metadata_key:
            settings.logger.debug("saving metadata")
            storage.save_object(
                json.dumps(metadata).encode("utf-8"),
                metadata_key,
            )

    settings.logger.info(f"finished all batches, successful: {error_flag}")

    if not error_flag:
        metadata["status"] = definitions.STATUS_COLLECTION_METADATA_COMPLETE

    return metadata


def verify_storage_connection(storage: Optional[Storage] = None) -> bool:
    """attempts to connect to the storage, returns True is successful, False otherwise"""
    if not storage:
        storage = Storage()

    try:
        if storage.bucket_exists(create=True):
            return True
        return False
    except Exception as error:
        error_traceback = "".join(
            traceback.format_exception(None, error, error.__traceback__)
        )
        settings.logger.critical(f"Could not connect to the cloud:\n {error_traceback}")
        return False
