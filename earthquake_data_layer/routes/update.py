# pylint: disable=raise-missing-from
import datetime
import traceback

from fastapi import APIRouter, HTTPException, status

from earthquake_data_layer import definitions, exceptions, helpers, settings
from update_dataset import update_dataset

INVALID_DATE_MASSAGE = f"Invalid date format, expecting {definitions.DATE_FORMAT}"

update_router = APIRouter()


@update_router.get(
    "/update/{date}", description="used to initialize a data update run."
)
def update(date: str):
    settings.logger.info(f"incoming get request at /collect/{date}")

    # verify input
    if not helpers.is_valid_date(date):
        settings.logger.critical(INVALID_DATE_MASSAGE)
        raise HTTPException(
            status_code=definitions.HTTP_INVALID_DATE,
            detail=INVALID_DATE_MASSAGE,
        )

    if settings.INTEGRATION_TEST:
        return {"result": "test_result", "status_code": status.HTTP_200_OK}

    # verify connection to storage
    if not helpers.verify_storage_connection():
        raise HTTPException(
            status_code=definitions.HTTP_COULD_NOT_CONNECT_TO_STORAGE,
            detail="Could not connect to the cloud",
        )

    last_date = datetime.datetime.strptime(date, definitions.DATE_FORMAT)

    try:
        result = update_dataset(last_date.year, last_date.month)
        settings.logger.info("Success! returning results")
        return {"result": result, "status_code": status.HTTP_200_OK}

    except exceptions.NoHealthyRequestsError:
        raise HTTPException(
            status_code=definitions.HTTP_COULD_NOT_FETCH_HEALTHY_RESPONSES,
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
