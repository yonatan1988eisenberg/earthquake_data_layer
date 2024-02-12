from threading import Thread
from time import sleep

from fastapi import FastAPI, status
from uvicorn import run

import collect_dataset
from earthquake_data_layer import settings
from earthquake_data_layer.routes.collect import collect_router

app = FastAPI()

app.include_router(collect_router)


@app.get("/", description="healthcheck")
def read_root():
    return {"message": "Up and running", "status": status.HTTP_200_OK}


def collect_initial_dataset():
    """verifies the collection dataset was successfully and completely downloaded, retries every 6 hour"""
    settings.logger.info("Verifying initial dataset was collected")
    result = False
    while not result:
        result = collect_dataset.verify_initial_dataset()
        if not result:
            # retry every 6 hours
            sleep(21600)


def start():
    Thread(target=collect_initial_dataset).start()

    settings.logger.info("Starting app")
    run(app, host=settings.DATA_LAYER_ENDPOINT, port=int(settings.DATA_LAYER_PORT))


if __name__ == "__main__":
    start()
