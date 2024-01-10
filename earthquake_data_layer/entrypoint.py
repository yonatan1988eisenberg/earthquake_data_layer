from fastapi import FastAPI, HTTPException
from uvicorn import run

from collect import DoneCollectingError, StorageConnectionError, run_collection
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

    settings.logger.info(f"incoming get request at /collect/{run_id}")

    try:
        result = run_collection(run_id)
        settings.logger.info("Success! return results")
        return {"result": result, "status": "success"}

    except DoneCollectingError:
        settings.logger.info("It looks like we're done collecting..")
        return {"result": "done_collecting", "status": "success"}

    except StorageConnectionError as error:
        settings.logger.critical(f"Could not connect to the cloud:\n {error}")
        raise HTTPException(status_code=501, detail=str(error)) from error

    except RuntimeError as error:
        settings.logger.critical(f"Encountered a Runtime Error:\n {error}")
        raise HTTPException(status_code=501, detail=str(error)) from error

    except Exception as error:
        settings.logger.critical(f"Encountered and error:\n {error}")
        raise HTTPException(status_code=500, detail=str(error)) from error


def start():
    settings.logger.info("Starting app")
    run(app, host=settings.EDL_ENDPOINT, port=settings.EDL_PORT)


if __name__ == "__main__":
    start()
