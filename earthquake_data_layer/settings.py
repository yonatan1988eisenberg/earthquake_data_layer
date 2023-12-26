import os
import re
from dotenv import load_dotenv


def get_api_keys(pattern: str = r'API_KEY\w+') -> list[str]:
    """
    api keys are expected to start with "API_KEY" such as API_KEY1, API_KEY2 etc.
    this function returns a list of them all.
    """
    key_pattern = re.compile(pattern)

    return [val for key, val in os.environ.items() if key_pattern.match(key)]


load_dotenv()

# debug
DEBUG = False

# api
API_HOST = os.getenv("API_HOST", None)
API_KEYs = get_api_keys()

# logging
LOGLEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
