# pylint: disable=missing-module-docstring
import datetime
from pathlib import Path

# API
API_URL = "https://everyearthquake.p.rapidapi.com/earthquakesByDate"
MAX_RESULTS_PER_REQUEST = 1000
API_URL_ = base_url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
MAX_RESULTS_PER_REQUEST_ = 20000
MAX_REQUESTS_PER_DAY = 150
# the advertised MAX_REQUESTS_PER_MIN is 10 but still getting error regarding the rate
MAX_REQUESTS_PER_MIN = 8
# restrain by API
MAX_START = 100000
# and therefor:
MAX_REQUESTS_PER_CALL = 100


# metadata locations
METADATA_FILE = "metadata.json"
METADATA_LOCATION = Path(__file__).parent.joinpath(METADATA_FILE)
METADATA_KEY = "metadata.json"
RUNS_METADATA_KEY = "data/runs/runs.parquet"
ERRED_RESPONSES_KEY = "data/raw_data/erred_responses.parquet"
COLLECTION_METADATA_KEY = "data/collection_metadata.json"
COLLECTION_RUNS_KEY = "data/collection.parquet"
UPDATE_RUNS_KEY = "data/update.parquet"

# fixed time and date format
TODAY = datetime.datetime.now()
YESTERDAY = TODAY - datetime.timedelta(days=1)
DATE_FORMAT = "%Y-%m-%d"
EXPECTED_DATA_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"

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
    # columns we're adding
    "response_id",
]

# API status codes
HTTP_NO_REMAINING_API_CALLS = 460
HTTP_COULDNT_FETCH_HEALTHY_RESPONSES = 461
HTTP_COULDNT_CONNECT_TO_STORAGE = 462
HTTP_INVALID_RUN_ID = 463

# pipeline statuses
STATUS_QUERY_API_SUCCESS = "successfully queried the API"
STATUS_QUERY_API_FAIL = "error during API call"
STATUS_PROCESS_SUCCESS = "successfully processed responses"
STATUS_PROCESS_FAIL = ""
STATUS_UPLOAD_DATA_SUCCESS = "successfully uploaded the data"
STATUS_UPLOAD_DATA_FAIL = "error while uploading the data"
STATUS_PIPELINE_SUCCESS = "successfully fetched the data for this time frame"
STATUS_PIPELINE_FAIL = "failed fetching the data for this time frame"

# collection_metadata statuses
STATUS_COLLECTION_METADATA_COMPLETE = "complete"
STATUS_COLLECTION_METADATA_INCOMPLETE = "incomplete"
