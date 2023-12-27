# pylint: disable=missing-module-docstring
from pathlib import Path

API_URL = "https://everyearthquake.p.rapidapi.com/earthquakesByDate"
MAX_RESULTS_PER_REQUEST = 1000
MAX_REQUESTS_PER_DAY = 150

# metadata location
METADATA_FILE = "meta.json"
METADATA_LOCATION = Path(__file__).parent.joinpath(METADATA_FILE)
