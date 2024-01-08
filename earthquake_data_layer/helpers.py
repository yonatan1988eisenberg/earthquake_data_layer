import datetime
import random
import string
from typing import Union

from earthquake_data_layer import definitions


def is_valid_date(
    date: Union[datetime.date, str], str_format: str = definitions.DATE_FORMAT
):
    if isinstance(date, datetime.date):
        return date.strftime(definitions.DATE_FORMAT)
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
    return f"data/raw_data/{definitions.TODAY.strftime('%Y')}/{run_id}_data.parquet"


def generate_responses_metadata_key(run_id: str):
    return f"data/runs/{definitions.TODAY.strftime('%Y')}/{run_id}.parquet"


def random_string(n: int):
    return "".join(random.choices(string.ascii_lowercase, k=n))
