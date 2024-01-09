from fastapi import FastAPI, HTTPException
from uvicorn import run

from collect import DoneCollectingError, StorageConnectionError
from collect import run as run_collection
from earthquake_data_layer import settings

app = FastAPI()


@app.get("/", description="healthcheck")
def read_root():
    return {"message": "Up and running", "status": "success"}


@app.get(
    "/collect/{run_id}",
    description="used to initialize a data collection run. "
    "returns the run_id if successful, raises an exception otherwise",
)
def collect(run_id: str):
    try:
        result = run_collection(run_id)
        return {"result": result, "status": "success"}

    except DoneCollectingError:
        return {"result": "done_collecting", "status": "success"}

    except StorageConnectionError as error:
        raise HTTPException(status_code=501, detail=str(error)) from error

    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error)) from error


def start():
    run(app, host=settings.EDL_ENDPOINT, port=settings.EDL_PORT)


if __name__ == "__main__":
    start()
