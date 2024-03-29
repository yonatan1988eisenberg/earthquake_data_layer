from threading import Thread
from time import sleep

from fastapi import FastAPI, status
from uvicorn import run

import collect_dataset
from earthquake_data_layer import settings
from earthquake_data_layer.routes.update import update_router

app = FastAPI()

app.include_router(update_router)


@app.get("/", description="healthcheck")
def read_root():
    return {"message": "Up and running", "status": status.HTTP_200_OK}


def collect_initial_dataset():
    """verifies the collection dataset was successfully and completely downloaded, retries every {settings.SLEEP_TIME}"""
    settings.logger.info("Verifying initial dataset was collected")
    verified = False
    while not verified:

        if settings.INTEGRATION_TEST:
            verified = True
            continue

        verified = collect_dataset.verify_initial_dataset()
        if not verified:
            settings.logger.info(
                f"failed to collect the initial dataset, retrying in {settings.COLLECTION_SLEEP_TIME} seconds"
            )
            sleep(settings.COLLECTION_SLEEP_TIME)

    settings.logger.info("The initial dataset was collected")


def start():
    Thread(target=collect_initial_dataset).start()

    settings.logger.info("Starting app")
    run(app, host=settings.DATA_LAYER_ENDPOINT, port=int(settings.DATA_LAYER_PORT))


if __name__ == "__main__":
    start()
