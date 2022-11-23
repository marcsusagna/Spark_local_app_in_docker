from pyspark.sql import (
    SparkSession,
    types as T,
)

import datetime

import src.datasets_logic.top_tracks_logic as df_logic
import src.utils as utils
from src.constants import INPUT_DIR, OUTPUT_DIR

spark_session = (
    SparkSession
    .builder
    .master("local[8]")
    .config("spark.executor.memory", "4g")
    .appName("My processing app")
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
    file_name="top_tracks.tsv",
    num_output_partitions=1
)

top_tracks.repartition(1).write.option("header", True).option("delimiter", "\t").csv(output_dir+file_name)

top_tracks.write.csv("output/my_csv.csv")