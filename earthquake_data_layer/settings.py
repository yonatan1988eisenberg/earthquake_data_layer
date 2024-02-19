# pylint: disable=missing-module-docstring,pointless-string-statement
import logging
import os
import sys

from dotenv import load_dotenv


def get_bool(key):
    return os.getenv(key, "false").casefold() == "true"


load_dotenv()

""" Data Collection """
# collect data from as early as
EARLIEST_EARTHQUAKE_DATE = "1900-01-01"
# save the runs data every n months completed
COLLECTION_BATCH_SIZE = 50
SLEEP_EVERY_N_REQUESTS = 40
COLLECTION_SLEEP_TIME = 3000
# url to test proxy is working
IP_VERIFYING_URL = "http://httpbin.org/ip"


""" Quasi-unique ID Generations """
# when uploading to storage without a key
RANDOM_STRING_LENGTH_KEY = 5

""" Environment Variables"""
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
log_format = f"""{LOGGER_NAME} :: %(levelname)-8s :: %(asctime)s %(filename)s:%(lineno)d %(message)s"""
logging.basicConfig(
    stream=sys.stdout,
    format=log_format,
)
logger = logging.getLogger(__name__)
logger.setLevel(LOGLEVEL)
