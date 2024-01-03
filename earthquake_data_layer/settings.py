# pylint: disable=missing-module-docstring
import os
import re

from dotenv import load_dotenv


def get_api_keys(pattern: str = r"API_KEY\w+") -> dict[str:str]:
    """
    api keys are expected to start with "API_KEY" such as API_KEY1, API_KEY2 etc.
    this function returns a list of them all.
    """
    key_pattern = re.compile(pattern)

    return {
        key_name: api_key
        for key_name, api_key in os.environ.items()
        if key_pattern.match(key_name)
    }


load_dotenv()

# debug
DEBUG = False

# api
API_HOST = os.getenv("API_HOST", None)
API_KEYs = get_api_keys()
NUM_REQUESTS_FOR_UPDATE = 1
UPDATE_TIME_DELTA_DAYS = 7
DATA_TYPE_TO_FETCH = "earthquake"
# how many requests to leave in each key when collecting
REQUESTS_TOLERANCE = 0


# aws
AWS_S3_ENDPOINT = os.getenv("AWS_S3_ENDPOINT", None)
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", None)
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", None)
AWS_REGION = os.getenv("AWS_REGION", None)
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME", None)

# metadata
LOCAL_METADATA = False

# logging
LOGLEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
