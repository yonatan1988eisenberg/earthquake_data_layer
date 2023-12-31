import datetime
from typing import Union


def is_valid_date(date: Union[datetime.date, str]):
    if isinstance(date, datetime.date):
        return date.strftime("%Y-%m-%d")
    if isinstance(date, str):
        try:
            # Attempt to parse the string as a date
            datetime.datetime.strptime(date, "%Y-%m-%d")
            return date
        except ValueError:
            # The string is not in the expected format
            return False

    return False
