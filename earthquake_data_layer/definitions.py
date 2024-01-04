# pylint: disable=missing-module-docstring
import datetime
from pathlib import Path

API_URL = "https://everyearthquake.p.rapidapi.com/earthquakesByDate"
MAX_RESULTS_PER_REQUEST = 1000
MAX_REQUESTS_PER_DAY = 150


# metadata locations
METADATA_FILE = "metadata.json"
METADATA_LOCATION = Path(__file__).parent.joinpath(METADATA_FILE)
METADATA_KEY = "/metadata.json"

# fix time and date format
TODAY = datetime.datetime.now()
DATE_FORMAT = "%Y-%m-%d"
