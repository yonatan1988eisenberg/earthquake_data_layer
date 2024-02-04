# pylint: disable=missing-module-docstring
import datetime
from pathlib import Path

# API
API_URL = "https://everyearthquake.p.rapidapi.com/earthquakesByDate"
MAX_RESULTS_PER_REQUEST = 1000
MAX_REQUESTS_PER_DAY = 150
# the advertised MAX_REQUESTS_PER_MIN is 10 but still getting error regarding the rate
MAX_REQUESTS_PER_MIN = 8
MAX_START = 100000


# metadata locations
METADATA_FILE = "metadata.json"
METADATA_LOCATION = Path(__file__).parent.joinpath(METADATA_FILE)
METADATA_KEY = "metadata.json"
RUNS_METADATA_KEY = "data/runs/runs.parquet"
ERRED_RESPONSES_KEY = "data/raw_data/erred_responses.parquet"

# fixed time and date format
TODAY = datetime.datetime.now()
YESTERDAY = TODAY - datetime.timedelta(days=1)
DATE_FORMAT = "%Y-%m-%d"
EXPECTED_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"

# data columns seen in dev
SEEN_COLUMNS = [
    "id",
    "magnitude",
    "type",
    "title",
    "date",
    "time",
    "updated",
    "url",
    "detailUrl",
    "felt",
    "cdi",
    "mmi",
    "alert",
    "status",
    "tsunami",
    "sig",
    "net",
    "code",
    "ids",
    "sources",
    "types",
    "nst",
    "dmin",
    "rms",
    "gap",
    "magType",
    "geometryType",
    "depth",
    "latitude",
    "longitude",
    "place",
    "distanceKM",
    "placeOnly",
    "location",
    "continent",
    "country",
    "subnational",
    "city",
    "locality",
    "postcode",
    "what3words",
    "timezone",
    "locationDetails",
]

# API status codes
HTTP_NO_REMAINING_API_CALLS = 460
HTTP_COULDNT_FETCH_HEALTHY_RESPONSES = 461
HTTP_COULDNT_CONNECT_TO_STORAGE = 462
