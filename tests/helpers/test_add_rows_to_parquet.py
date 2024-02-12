from io import BytesIO
from unittest.mock import patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from earthquake_data_layer import definitions, helpers


# todo: rewrite tests with current data scheme, add test to make sure duplicates are removed
def test_file_dont_exist_single_row(sample_response, storage):
    with (
        patch("earthquake_data_layer.Storage.list_objects"),
        patch(
            "earthquake_data_layer.helpers.upload_df", return_value=True
        ) as upload_func,
    ):
        result = helpers.add_rows_to_parquet(
            rows=sample_response, key=definitions.ERRED_RESPONSES_KEY, storage=storage
        )
        uploaded_df = upload_func.call_args[0][0]

        assert result
        pd.testing.assert_frame_equal(
            pd.DataFrame.from_records([sample_response]), uploaded_df
        )


def test_file_exist(sample_response, inverted_sample_response, storage):
    initial_df = pd.DataFrame.from_records([sample_response])
    table = pa.Table.from_pandas(initial_df)
    writer = pa.BufferOutputStream()
    pq.write_table(table, writer)
    expected_loaded_df = pd.read_parquet(BytesIO(writer.getvalue()))
    expected_uploaded_df = pd.concat(
        [expected_loaded_df, pd.DataFrame.from_records([inverted_sample_response])],
        ignore_index=True,
    )

    with (
        patch("earthquake_data_layer.Storage.list_objects"),
        patch(
            "earthquake_data_layer.helpers.upload_df", return_value=True
        ) as upload_func,
        patch(
            "earthquake_data_layer.Storage.load_object",
            return_value=BytesIO(writer.getvalue()),
        ),
    ):
        result = helpers.add_rows_to_parquet(
            rows=inverted_sample_response,
            key=definitions.ERRED_RESPONSES_KEY,
            storage=storage,
            remove_duplicates=False,
        )
        uploaded_df = upload_func.call_args[0][0]

        assert result
        pd.testing.assert_frame_equal(expected_uploaded_df, uploaded_df)
