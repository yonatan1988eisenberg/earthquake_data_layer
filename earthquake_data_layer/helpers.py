import datetime
import logging
import random
import string
from random import choice
from typing import Union

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from earthquake_data_layer import definitions, settings
from earthquake_data_layer.storage import Storage


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


def update_runs_metadata(
    run_metadata: dict, storage: Storage, key: str = definitions.RUNS_METADATA_KEY
) -> bool:
    """
    Updates the runs metadata file in the storage with a new line run_metadata.

    Parameters:
    - run_metadata (dict): The metadata to append.
    - storage (Storage): The storage instance.
    - key (str): The key for the metadata file. Default is the runs metadata key.

    Returns:
    bool: True if the update is successful, False otherwise.
    """
    # load the file from storage and append the new line to it
    try:
        df = pd.read_parquet(storage.load_object(key))
        df = pd.concat(
            [df, pd.DataFrame.from_records([run_metadata])], ignore_index=True
        )
    # if the file doesn't exist (first run)
    except FileNotFoundError:
        df = pd.DataFrame.from_records([run_metadata])

    # upload to storage
    return upload_df(df, key, storage)


def key_api2name(key: str) -> Union[str, bool]:
    # todo: move to helpers
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
