from pyspark.sql import (
    DataFrame,
    SparkSession,
    functions as F,
    types as T,
    Window
)
# Constants
TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"
INPUT_FILE_DELIMITER = "\t"
ARTIST_TRACK_DELIMITER = "--/"
SESSION_THRESHOLD_MINUTES = 20
NUM_TOP_SESSIONS = 20
NUM_TOP_TRACKS = 10

# Assumptions on input dataframe that will need to be monitored:
# 1) user_id (_c0) and track_start (_c1) are not null. Check behavior: Fail
# (meaningless window function, could aggregate over users for which user id is null)
# 2) track_start (_c1) always abides with TIMESTAMP_FORMAT. Check behavior: Fail (tracks attached to wrong session)
# 3) Columns (_c0) and (_c1) don't have quotes to be scaped because they are not free text, hence (_c3) can't be null
# No need to monitor.
# 4) Combination of user_id and track_start_timestamp should be unique. Check behavior: Warn. Doesn't invalidate logic
# but raises a warning on the quality of the feed
# 5) Artist name (_c3) and track_name (_c5) Don't have ARTIST_TRACK_DELIMITER in them. Check behavior: warn
# A violation would just make the track_name harder to retrieve.


def read_input_file(spark_session: SparkSession, file_path: str):
    """
    scaping needs to be improved
    :param spark_session:
    :param file_path:
    :return:
    """
    input_df = (
        spark_session
        .read
        .option("quote", "\"")
        .option("escape", "\'")
        .csv(file_path, sep=INPUT_FILE_DELIMITER, header=False)
    )
    return input_df


def process_input_file(input_df: DataFrame) -> DataFrame:
    """
    Obtain target schema for further processing
    :param input_df: Onboarded input file
    :return:
    """
    # Why are we creating column full_track_name?
    # Best would be to use track_id to identify song counts in the last step, however, 11% are null (and not due to parsing)
    # Therefore, we create full_track_name for the following reasons:
    # 1) Be able to better identify the track in case it is picked as top 10 (two artists can have the same song name)
    # 2) Remove null values in track_name due to quotes in column artist_name (_c3)

    # Dropping columns that are useless to reduce data shuffling
    input_df = (
        input_df
        .withColumn(
            "full_track_name",
            F.concat_ws(ARTIST_TRACK_DELIMITER, F.col("_c3"), F.coalesce(F.col("_c5"), F.lit("")))
        )
        # We are explicit with the format to ensure input comes in a constant format
        .withColumn("track_start_ts", F.to_timestamp(F.col("_c1"), TIMESTAMP_FORMAT))
        .selectExpr([
            "_c0 as user_id",
            "track_start_ts",
            "full_track_name"
        ])
    )

    return input_df


def obtain_session_id(all_plays_df: DataFrame, session_duration_min: int) -> DataFrame:
    """
    Compare each track played with the previoulsy played track for each user. Use the minutes between the tracks
    as a way to identify sessions.

    A session is identified by the user_id column and the session_id_ts, which is the starting ts of the session.

    :param all_plays_df: Dataframe with columns: user_id, track_start_ts and full_track_name
    :param session_duration_min: Minutes between songs to be considered of the same session
    :return: DF with user_id, track_start_ts, full_track_name and session_id_ts
    """

    window_for_lag = (
        Window
        .partitionBy("user_id")
        .orderBy(F.col("track_start_ts").asc())
    )

    window_for_session_id = (
        Window
        .partitionBy("user_id")
        .orderBy(F.col("track_start_ts").asc())
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    # Create session_id_ts for tracks that initiate a new session
    # Could do it without storing tmp columns, but for readibility / debugging, the intermediate columns are kept
    all_plays_df_with_session = (
        all_plays_df
        .withColumn("lagged_ts", F.lag(F.col("track_start_ts")).over(window_for_lag))
        .withColumn("mins_between_songs",
                    (F.col("track_start_ts").cast(T.LongType()) - F.col("lagged_ts").cast(T.LongType())) / 60)
        .withColumn(
            "session_id_ts",
            F.when(
                F.col("mins_between_songs") > session_duration_min, F.col("track_start_ts")
            ).when(
                # First ever played track by a user
                F.col("mins_between_songs").isNull(), F.col("track_start_ts")
            ).otherwise(
                F.lit(None).cast(T.TimestampType()))
        )
        # Populate session id ts for tracks that don't start a session
        # Tracks that start a session will use the value of session_id_ts, as it is the max until that row
        .withColumn("session_id_ts", F.max("session_id_ts").over(window_for_session_id))
        .selectExpr(
            "user_id",
            "track_start_ts",
            "full_track_name",
            "session_id_ts"
        )
    )

    return all_plays_df_with_session


def obtain_top_session(df_with_session: DataFrame, num_sessions_to_keep: int) -> DataFrame:
    """
    Obtain top longest num_sessions_to_keep by counting number of entries per session

    :param df_with_session: df with all tracks played and the necessary columns user_id and session_id_ts
    :param num_sessions_to_keep: how many sessions to be considered top
    :return: Dataframe containing the user_id, session_id_ts for the top longest sessions
    """
    top_sessions = (
        df_with_session
        .groupBy("user_id", "session_id_ts")
        .agg(
            F.count("*").alias("session_length")
        )
        .orderBy(F.col("session_length").desc()).limit(num_sessions_to_keep)
        .drop("session_length")
    )
    return top_sessions


def obtain_most_popular_tracks(
        df_tracks_played: DataFrame,
        df_top_session: DataFrame,
        num_tracks_to_keep: int) -> DataFrame:
    """
    Count track plays within the top longest sessions

    :param df_tracks_played: output df of obtain_session_id
    :param df_top_session: output df of obtain_top_session
    :param num_tracks_to_keep: number of tracks considered the most popular
    :return: DataFrame with 3 columns (artist_name, track_name and number of times a song was played) for the top songs
    """
    # Broadcast join since df_top_session is most likely below threshold
    tracks_in_top_sessions = (
        df_tracks_played
        .join(
            df_top_session,
            on=["user_id", "session_id_ts"],
            how="inner"
        )
    )

    top_tracks = (
        tracks_in_top_sessions
        .groupBy("full_track_name")
        .agg(
            F.count("*").alias("track_plays")
        )
        .orderBy(F.col("track_plays").desc())
        .limit(num_tracks_to_keep)
    )

    top_tracks = (
        top_tracks
        .withColumn("split_name", F.split(F.col("full_track_name"), ARTIST_TRACK_DELIMITER))
        .withColumn("artist_name", F.col("split_name").getItem(0))
        .withColumn("track_name", F.col("split_name").getItem(1))
        .selectExpr(
            "artist_name",
            "track_name",
            "track_plays"
        )
    )
    return top_tracks
