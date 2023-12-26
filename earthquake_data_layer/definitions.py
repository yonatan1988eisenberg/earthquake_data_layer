from pathlib import Path

API_URL = "https://everyearthquake.p.rapidapi.com/earthquakesByDate"
MAX_RESULTS = 1000
MAX_REQUESTS = 150

# metadata location
METADATA_FILE = "meta.json"
METADATA_LOCATION = Path(__file__).parent.joinpath(METADATA_FILE)
