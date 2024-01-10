from abc import ABC, abstractmethod
from typing import Optional, Union

from earthquake_data_layer import MetadataManager, settings


class ValidationStep(ABC):
    """a scheme for a validation step"""

    name: str

    @classmethod
    @abstractmethod
    def execute(cls, **kwargs) -> tuple[str, Union[dict, None]]:
        pass

    @classmethod
    def log_error(cls, error_massage: str) -> tuple[str, dict]:
        settings.logger.error(f"{error_massage} when validating {cls.name}")
        return cls.name, {"error": error_massage}


class ColumnsNames(ValidationStep):
    """
    Verifies all the known columns exist and that there are no new columns.
    """

    name: str = "column_names"

    @classmethod
    def execute(cls, **kwargs) -> tuple[str, Union[dict, None]]:
        """
        Execute the column names verification step.

        Parameters:
        - metadata_manager (MetadataManager): The metadata manager instance.
        - run_metadata (dict): The run metadata.

        Returns:
        tuple: Step name and a report containing missing and new columns.
        """
        metadata_manager: MetadataManager = kwargs.get("metadata_manager")
        run_metadata: dict = kwargs.get("run_metadata")

        if not metadata_manager or not run_metadata:
            # raise TypeError("Missing argument, one of [metadata_manager, run_metadata]")
            return cls.log_error(
                "Missing argument, one of [metadata_manager, run_metadata]"
            )

        known_cols = metadata_manager.known_columns
        run_columns = run_metadata.get("columns")

        report = dict()
        new_cols = list()
        known_cols_in_run = list()
        if run_columns:
            for col in run_columns:
                if col not in known_cols:
                    new_cols.append(col)
                else:
                    known_cols_in_run.append(col)

        report["missing_columns"] = list(set(known_cols).difference(known_cols_in_run))
        report["new_columns"] = new_cols

        return cls.name, report


class MissingValues(ValidationStep):
    """
    Returns a dictionary {column: number of missing values} or None if there are none.
    """

    name: str = "missing_values"

    @classmethod
    def execute(cls, **kwargs) -> tuple[str, Union[dict, None]]:
        """
        Execute the missing values verification step.

        Parameters:
        - run_metadata (dict): The run metadata.

        Returns:
        tuple: Step name and a report containing columns with missing values.
        """
        run_metadata: dict = kwargs.get("run_metadata")

        if not run_metadata:
            # raise TypeError("Missing argument: run_metadata")
            return cls.log_error("Missing argument: run_metadata")

        report = dict()
        expected_num_rows = run_metadata.get("count")
        columns = run_metadata.get("columns")

        if not expected_num_rows or not columns:
            # raise ValueError("Run metadata is missing a key, one of [count, columns]")
            return cls.log_error(
                "Run metadata is missing a key, one of [count, columns]"
            )

        for col, values in columns.items():
            if values != expected_num_rows:
                report[col] = expected_num_rows - values

        if not report:
            return cls.name, None

        return cls.name, report


class Validate:
    """
    Validates run metadata with a series of steps.
    """

    steps: Optional[list] = [ColumnsNames, MissingValues]

    @classmethod
    def validate(cls, steps: Optional[list] = None, **kwargs):
        """
        Execute the validation process with specified steps.

        Parameters:
        - steps (list): List of validation steps to execute.
        - kwargs: Additional keyword arguments required for all the steps.

        Returns:
        dict: A dictionary containing validation reports for each step.
        """

        settings.logger.info("Starting validation")

        report = dict()

        if steps is None:
            steps = cls.steps

        for step in steps:
            step_name, step_report = step.execute(**kwargs)
            report[step_name] = step_report

        settings.logger.debug(f"Validation finished. report: {report}")

        return report
