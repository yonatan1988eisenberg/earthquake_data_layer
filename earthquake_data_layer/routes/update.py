# pylint: disable=raise-missing-from
import datetime
import traceback

from fastapi import APIRouter, HTTPException, status

from earthquake_data_layer import definitions, exceptions, helpers, settings
from update_dataset import update_dataset

INVALID_RUN_ID_MASSAGE = f"Invalid run_id format, expecting {definitions.DATE_FORMAT}"

update_router = APIRouter()


@update_router.get(
    "/update/{run_id}", description="used to initialize a data update run."
)
def update(run_id: str):
    settings.logger.info(f"incoming get request at /collect/{run_id}")

    # verify input
    if not helpers.is_valid_date(run_id):
        settings.logger.critical(INVALID_RUN_ID_MASSAGE)
        raise HTTPException(
            status_code=definitions.HTTP_INVALID_RUN_ID,
            detail=INVALID_RUN_ID_MASSAGE,
        )

    if settings.INTEGRATION_TEST:
        return {"result": "test_result", "status_code": status.HTTP_200_OK}

    # verify connection to storage
    if not helpers.verify_storage_connection():
        raise HTTPException(
            status_code=definitions.HTTP_COULDNT_CONNECT_TO_STORAGE,
            detail="Could not connect to the cloud",
        )

    last_date = datetime.datetime.strptime(
        run_id, definitions.DATE_FORMAT
    ) - datetime.timedelta(days=1)

    try:
        result = update_dataset(last_date.year, last_date.month)
        settings.logger.info("Success! returning results")
        return {"result": result, "status_code": status.HTTP_200_OK}

    except exceptions.NoHealthyRequestsError:
        raise HTTPException(
            status_code=definitions.HTTP_COULDNT_FETCH_HEALTHY_RESPONSES,
            detail="couldn't fetch healthy responses",
        )

    except RuntimeError as error:
        error_traceback = "".join(
            traceback.format_exception(None, error, error.__traceback__)
        )
        settings.logger.critical(f"Encountered a Runtime Error:\n {error_traceback}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Encountered a Runtime Error",
        )

    except Exception as error:
        error_traceback = "".join(
            traceback.format_exception(None, error, error.__traceback__)
        )
        settings.logger.critical(f"Encountered an error:\n {error_traceback}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Encountered an error",
        )
