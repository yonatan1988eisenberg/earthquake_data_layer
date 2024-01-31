# pylint: disable=raise-missing-from
from fastapi import FastAPI, HTTPException, status
from uvicorn import run

from collect import run_collection
from earthquake_data_layer import definitions, settings
from earthquake_data_layer.exceptions import (
    DoneCollectingError,
    NoHealthyRequestsError,
    RemainingRequestsError,
    StorageConnectionError,
)

app = FastAPI()


@app.get("/", description="healthcheck")
def read_root():
    return {"message": "Up and running", "status": status.HTTP_200_OK}


@app.get(
    "/collect/{run_id}",
    description="used to initialize a data collection run. "
    "returns the run_id if successful, raises an exception otherwise",
)
def collect(run_id: str):
    settings.logger.info(f"incoming get request at /collect/{run_id}")

    if settings.INTEGRATION_TEST:
        return {"result": "test_result", "status": status.HTTP_200_OK}

    try:
        result = run_collection(run_id)
        settings.logger.info("Success! return results")
        return {"result": result, "status": status.HTTP_200_OK}

    except DoneCollectingError:
        settings.logger.info("It looks like we're done collecting..")
        return {"result": "done_collecting", "status": status.HTTP_200_OK}

    except RemainingRequestsError:
        settings.logger.info("No remaining API calls")
        raise HTTPException(
            status_code=definitions.HTTP_NO_REMAINING_API_CALLS,
            detail="No remaining API calls",
        )

    except NoHealthyRequestsError:
        settings.logger.info("couldn't fetch healthy responses")
        raise HTTPException(
            status_code=definitions.HTTP_COULDNT_FETCH_HEALTHY_RESPONSES,
            detail="couldn't fetch healthy responses",
        )

    except StorageConnectionError as error:
        settings.logger.critical(
            f"Could not connect to the cloud:\n {error.__traceback__}"
        )
        raise HTTPException(
            status_code=definitions.HTTP_COULDNT_CONNECT_TO_STORAGE,
            detail="Could not connect to the cloud",
        )

    except RuntimeError as error:
        settings.logger.critical(
            f"Encountered a Runtime Error:\n {error.__traceback__}"
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Encountered a Runtime Error",
        )

    except Exception as error:
        settings.logger.critical(f"Encountered an error:\n {error.__traceback__}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Encountered an error",
        )


def start():
    settings.logger.info("Starting app")
    run(app, host=settings.DATA_LAYER_ENDPOINT, port=int(settings.DATA_LAYER_PORT))


if __name__ == "__main__":
    start()
