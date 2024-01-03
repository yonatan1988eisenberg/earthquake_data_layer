import datetime
from typing import Union

from earthquake_data_layer import definitions


def is_valid_date(date: Union[datetime.date, str]):
    if isinstance(date, datetime.date):
        return date.strftime(definitions.DATE_FORMAT)
    if isinstance(date, str):
        try:
            # Attempt to parse the string as a date
            datetime.datetime.strptime(date, definitions.DATE_FORMAT)
            return date
        except ValueError:
            # The string is not in the expected format
            return False

    return False
