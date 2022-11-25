import sys

from pyspark.sql import (
    SparkSession
)

import src.datasets_logic.top_tracks_logic as df_logic
import src.utils as utils
from src.constants import INPUT_DIR, OUTPUT_DIR

if __name__ == "__main__":
    spark_session = (
        SparkSession
        .builder
        .appName("App to obtain top tracks")
        .getOrCreate()
    )
    input_df = df_logic.read_input_file(
        spark_session=spark_session,
        file_path=INPUT_DIR+"userid-timestamp-artid-artname-traid-traname.tsv",
    )

    processed_input_df = df_logic.process_input_file(input_df)

    plays_with_session = df_logic.obtain_session_id(
        processed_input_df,
        session_duration_min=df_logic.SESSION_THRESHOLD_MINUTES
    )

    plays_with_session.cache()

    top_sessions = df_logic.obtain_top_session(
        plays_with_session,
        num_sessions_to_keep=df_logic.NUM_TOP_SESSIONS
    )
    top_tracks = df_logic.obtain_most_popular_tracks(
        plays_with_session,
        top_sessions,
        num_tracks_to_keep=df_logic.NUM_TOP_TRACKS
    )

    # Repartition to 1 since the output is just 10 rows
    utils.write_to_disk(
        df=top_tracks,
        output_dir=OUTPUT_DIR,
        file_name="top_tracks",
        num_output_partitions=1
    )

    # Print query plan for analysis
    sys.stdout = open("output/query_plan.txt", "w")
    top_tracks.explain(mode="simple")
    sys.stdout.close()