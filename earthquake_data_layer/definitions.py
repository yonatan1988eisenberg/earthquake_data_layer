# pylint: disable=missing-module-docstring
import datetime

# API
API_URL = base_url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
MAX_RESULTS_PER_REQUEST = 20000

# metadata and tables keys
COLLECTION_METADATA_KEY = "data/collection_metadata.json"
COLLECTION_RUNS_KEY = "data/collection.parquet"
UPDATE_RUNS_KEY = "data/update.parquet"

# fixed time and date format
TODAY = datetime.datetime.now()
YESTERDAY = TODAY - datetime.timedelta(days=1)
DATE_FORMAT = "%Y-%m-%d"
EXPECTED_DATA_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


# API status codes
HTTP_COULDNT_FETCH_HEALTHY_RESPONSES = 461
HTTP_COULDNT_CONNECT_TO_STORAGE = 462
HTTP_INVALID_RUN_ID = 463

# pipeline statuses
STATUS_QUERY_API_SUCCESS = "successfully queried the API"
STATUS_QUERY_API_FAIL = "error during API call"
STATUS_PROCESS_SUCCESS = "successfully processed responses"
STATUS_PROCESS_FAIL = "couldn't fetch healthy responses"
STATUS_UPLOAD_DATA_SUCCESS = "successfully uploaded the data"
STATUS_UPLOAD_DATA_FAIL = "error while uploading the data"
STATUS_PIPELINE_SUCCESS = "successfully fetched the data for this time frame"
STATUS_PIPELINE_FAIL = "failed fetching the data for this time frame"

# collection_metadata statuses
STATUS_COLLECTION_METADATA_COMPLETE = "complete"
STATUS_COLLECTION_METADATA_INCOMPLETE = "incomplete"
