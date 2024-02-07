# pylint: disable=missing-module-docstring,pointless-string-statement
import logging
import os
import re
import sys

from dotenv import load_dotenv


def get_bool(key):
    return os.getenv(key, "false").casefold() == "true"


def get_api_keys(pattern: str = r"API_KEY\w+") -> dict[str:str]:
    """
    api keys are expected to start with "API_KEY" such as API_KEY1, API_KEY2 etc.
    this function returns a list of them all.
    """
    key_pattern = re.compile(pattern)

    return {
        key_name: api_key
        for key_name, api_key in os.environ.items()
        if key_pattern.match(key_name) and not api_key == ""
    }


load_dotenv()

""" Data Collection """
# how many requests to send when updating the data
NUM_REQUESTS_FOR_UPDATE = 1
# the interval between updates
UPDATE_TIME_DELTA_DAYS = 7
# collect data from as early as
EARLIEST_EARTHQUAKE_DATE = "1900-01-01"
# data point types to fetch, more details can be found at the API homepage
DATA_TYPE_TO_FETCH = "earthquake"
# how many requests to leave in each key when collecting
REQUESTS_TOLERANCE = int(os.getenv("REQUESTS_TOLERANCE", "0"))

""" Metadata Location """
LOCAL_METADATA = False

""" Quasi-unique ID Generations """
# when uploading to storage without a key
RANDOM_STRING_LENGTH_KEY = 5
# when processing the responses
RANDOM_STRING_LENGTH_RESPONSE_ID = 5

""" Environment Variables"""
# api
API_HOST = os.getenv("API_HOST", None)
API_KEYs = get_api_keys()

# earthquake data layer creds
DATA_LAYER_ENDPOINT = os.getenv("DATA_LAYER_ENDPOINT", "localhost")
DATA_LAYER_PORT = os.getenv("DATA_LAYER_PORT", "9000")

# aws
AWS_S3_ENDPOINT = os.getenv("AWS_S3_ENDPOINT", None)
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", None)
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", None)
AWS_REGION = os.getenv("AWS_REGION", None)
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME", None)

# testing
INTEGRATION_TEST = get_bool("INTEGRATION_TEST")

""" Logging """
LOGGER_NAME = os.getenv("LOGGER_NAME", "EDL")
LOGLEVEL = os.getenv("LOG_LEVEL", "ERROR").upper()
log_format = f"""{LOGGER_NAME} :: %(levelname)-8s :: %(asctime)s %(filename)s:%(lineno)d -30s %(message)s"""
logging.basicConfig(
    stream=sys.stdout,
    format=log_format,
)
logger = logging.getLogger(__name__)
logger.setLevel(LOGLEVEL)
