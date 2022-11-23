from pyspark.sql import (
    DataFrame,
    functions as F,
    types as T,
    Window
)
def obtain_session_id(all_plays_df: DataFrame, session_duration_min: int) -> DataFrame:
    """
    Compare each track played with the previoulsy played track for each user. Use the minutes between the tracks
    as a way to identify sessions.

    A session is identified by the user_id column and the session_id_ts, which is the starting ts of the session.

    :param all_plays_df: Dataframe with columns: user_id, track_start_ts and full_track_name
    :param session_duration_min: Minutes between songs to be considered of the same session
    :return: DF with user_id, track_start_ts, full_track_name and session_id_ts
    """

    windor_for_lag = (
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

    # Create session_id for tracks that initiate a new session
    # Could do it without storing tmp columns, but for readibility / debugging, the intermediate columns are kept
    all_plays_df_with_session = (
        all_plays_df
        .withColumn("lagged_ts", F.lag(F.col("track_start_ts")).over(windor_for_lag))
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
        # Populate session id for tracks that don't start a session
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
    Obtain top longest num_sessions_to_keep

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

    :param df_tracks_played: output df of obtain_session_id
    :param df_top_session: output df of obtain_top_session
    :param num_tracks_to_keep: number of tracks considered the most popular
    :return: DataFrame with 3 columns (artist_name, track_name and number of times a song was played) for the top songs
    """
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
        .withColumn("artist_name", F.split(F.col("full_track_name"), "--/").getItem(0))
        .withColumn("track_name", F.split(F.col("full_track_name"), "--/").getItem(1))
        .selectExpr(
            "artist_name",
            "track_name",
            "track_plays"
        )
    )
    return top_tracks