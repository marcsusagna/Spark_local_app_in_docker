from pyspark.sql import (
    SparkSession
)

import src.datasets_logic.top_tracks_logic as df_logic
from src.data_health_checks.base_data_health_checks import (
    null_check,
    uniqueness_check,
    invalid_string,
    timestamp_format_check
)
from src.constants import INPUT_DIR

if __name__ == "__main__":
    spark_session = (
        SparkSession
        .builder
        .appName("Data Health App")
        .getOrCreate()
    )
    # Silence logs to be able to see checks results
    spark_session.sparkContext.setLogLevel("OFF")

    # Read dataset
    input_df = df_logic.read_input_file(
        spark_session=spark_session,
        file_path=INPUT_DIR+"userid-timestamp-artid-artname-traid-traname.tsv",
    )

    # The spark transformation is based on some data assumptions that need to be checked every time new data needs to be processed
    # Otherwise the computation might be wrong. Depending on the severity of breaking the assumption, the checks should
    # either fail the transform or just warn about it

    null_check(input_df, columns_to_check=["_c0", "_c1"], except_on_fail=True)
    # Check for correct timestamp formatting (assumption 2)
    timestamp_format_check(
        input_df,
        column_to_check="_c1",
        expected_format=df_logic.TIMESTAMP_FORMAT,
        except_on_fail=True
    )

    uniqueness_check(input_df, columns_to_check=["_c0", "_c1"], except_on_fail=False)

    # Check for:
    # Artist name doesn't include the given delimiter between artist name and track name (assumption 4)

    invalid_string(
        input_df,
        columns_to_check=["_c2", "_c5"],
        invalid_string=df_logic.ARTIST_TRACK_DELIMITER,
        # If this check fails, the consequence is a top played track might not be easily recognizable by the name
        except_on_fail=False
    )

