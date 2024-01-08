# pylint: disable=missing-module-docstring
import datetime
from pathlib import Path

API_URL = "https://everyearthquake.p.rapidapi.com/earthquakesByDate"
MAX_RESULTS_PER_REQUEST = 1000
MAX_REQUESTS_PER_DAY = 150

# metadata locations
METADATA_FILE = "metadata.json"
METADATA_LOCATION = Path(__file__).parent.joinpath(METADATA_FILE)
METADATA_KEY = "metadata.json"
RUNS_METADATA_KEY = "data/runs/runs.parquet"

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
