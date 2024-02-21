import datetime
from copy import deepcopy

from dateutil.relativedelta import relativedelta

from earthquake_data_layer import definitions, helpers


def update_dataset(last_year: int, last_month: int):
    """collects the data from the 12 months prior to the provided year and month"""

    last_date = datetime.date(last_year, last_month, 1)
    current_month = deepcopy(last_date)
    months = list()
    for _ in range(12):
        months.append((current_month.year, current_month.month))
        current_month -= relativedelta(months=1)

    first_date = datetime.date(months[-1][0], months[-1][1], 1)

    metadata = {
        "status": definitions.STATUS_COLLECTION_METADATA_INCOMPLETE,
        "details": {},
        "first_date": first_date,
        "last_date": last_date,
    }

    metadata = helpers.fetch_months_data(months, metadata, metadata_key=None)

    return metadata
