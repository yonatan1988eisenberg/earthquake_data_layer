from fastapi import FastAPI, status
from uvicorn import run

from earthquake_data_layer import settings
from earthquake_data_layer.routes.collect import collect_router

app = FastAPI()

app.include_router(collect_router)


@app.get("/", description="healthcheck")
def read_root():
    return {"message": "Up and running", "status": status.HTTP_200_OK}


def start():
    settings.logger.info("Starting app")
    run(app, host=settings.DATA_LAYER_ENDPOINT, port=int(settings.DATA_LAYER_PORT))


if __name__ == "__main__":
    start()
