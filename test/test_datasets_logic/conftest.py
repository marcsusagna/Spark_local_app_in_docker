import pytest
import datetime
from pyspark.sql import(
    SparkSession,
    types as T
)


@pytest.fixture(scope="module")
def spark_session():
    """
    Defined as module to be reused among all tests within the same module (usually have similar sizes)
    :return:
    """
    spark_session = (
        SparkSession
        .builder
        .master("local[1]")
        .config("spark.executor.memory", "2g")
        .appName("My processing app")
        .getOrCreate()
    )
    return spark_session


@pytest.fixture(scope="module")
def input_df(spark_session):
    """
    All tests within this module use this input df, no need to create it one per each function, so we scope it to module
    :param spark_session:
    :return:
    """
    test_data = [
        # session 1 user 1
        {"user_id": "U0001", "track_start_ts": datetime.datetime(2009, 5, 4, 23, 8, 57),
         "full_track_name": "artist_1--/track_1"},
        {"user_id": "U0001", "track_start_ts": datetime.datetime(2009, 5, 4, 23, 9, 57),
         "full_track_name": "artist_1--/track_2"},
        {"user_id": "U0001", "track_start_ts": datetime.datetime(2009, 5, 4, 23, 10, 57),
         "full_track_name": "artist_1--/track_3"},
        {"user_id": "U0001", "track_start_ts": datetime.datetime(2009, 5, 4, 23, 11, 57),
         "full_track_name": "artist_1--/track_4"},
        {"user_id": "U0001", "track_start_ts": datetime.datetime(2009, 5, 4, 23, 15, 57),
         "full_track_name": "artist_1--/track_1"},
        {"user_id": "U0001", "track_start_ts": datetime.datetime(2009, 5, 4, 23, 17, 57),
         "full_track_name": "artist_1--/track_5"},
        # session 2 user 1
        {"user_id": "U0001", "track_start_ts": datetime.datetime(2009, 5, 7, 11, 2, 57),
         "full_track_name": "artist_2--/track_3"},
        {"user_id": "U0001", "track_start_ts": datetime.datetime(2009, 5, 7, 11, 9, 57),
         "full_track_name": "artist_2--/track_4"},
        {"user_id": "U0001", "track_start_ts": datetime.datetime(2009, 5, 7, 11, 10, 57),
         "full_track_name": "artist_2--/track_2"},
        # session 3 user 1
        {"user_id": "U0001", "track_start_ts": datetime.datetime(2009, 5, 7, 11, 31, 57),
         "full_track_name": "artist_1--/track_2"},
        {"user_id": "U0001", "track_start_ts": datetime.datetime(2009, 5, 7, 11, 32, 45),
         "full_track_name": "artist_1--/track_4"},

        # session 1 user 2
        {"user_id": "U0002", "track_start_ts": datetime.datetime(2009, 9, 4, 23, 8, 57),
         "full_track_name": "artist_1--/track_1"},
        {"user_id": "U0002", "track_start_ts": datetime.datetime(2009, 9, 4, 23, 9, 57),
         "full_track_name": "artist_1--/track_2"},
        # session 2 user 2
        {"user_id": "U0002", "track_start_ts": datetime.datetime(2009, 12, 10, 13, 1, 57),
         "full_track_name": "artist_1--/track_3"},
        # session 3 user 2
        {"user_id": "U0002", "track_start_ts": datetime.datetime(2009, 12, 11, 13, 1, 57),
         "full_track_name": "artist_1--/track_3"},
        # session 4 user 2
        {"user_id": "U0002", "track_start_ts": datetime.datetime(2009, 12, 12, 13, 1, 57),
         "full_track_name": "artist_1--/track_3"},

        # session 1 user 3
        {"user_id": "U0003", "track_start_ts": datetime.datetime(2009, 11, 4, 7, 24, 57),
         "full_track_name": "artist_2--/track_1"},
        {"user_id": "U0003", "track_start_ts": datetime.datetime(2009, 11, 4, 7, 26, 57),
         "full_track_name": "artist_2--/track_2"},
        {"user_id": "U0003", "track_start_ts": datetime.datetime(2009, 11, 4, 7, 29, 57),
         "full_track_name": "artist_2--/track_5"},
        {"user_id": "U0003", "track_start_ts": datetime.datetime(2009, 11, 4, 7, 32, 57),
         "full_track_name": "artist_2--/track_2"},
    ]

    input_df = spark_session.createDataFrame(
        test_data,
        T.StructType([
            T.StructField("user_id", T.StringType()),
            T.StructField("track_start_ts", T.TimestampType()),
            T.StructField("full_track_name", T.StringType())
        ])
    )
    return input_df


@pytest.fixture(scope="function")
def top_sessions_expected_df(spark_session):
    """
    This dataset will only be seen by one test, so no need to keep it in memory for more than one test
    :param spark_session:
    :return:
    """
    expected_data = [
        {"user_id": "U0001", "session_id_ts": datetime.datetime(2009, 5, 4, 23, 8, 57)},
        {"user_id": "U0003", "session_id_ts": datetime.datetime(2009, 11, 4, 7, 24, 57)},
        {"user_id": "U0001", "session_id_ts": datetime.datetime(2009, 5, 7, 11, 2, 57)}
    ]
    expected_df = spark_session.createDataFrame(
        expected_data,
        T.StructType([
            T.StructField("user_id", T.StringType()),
            T.StructField("session_id_ts", T.TimestampType())
        ])
    )

    return expected_df


@pytest.fixture(scope="function")
def top_tracks_expected_df(spark_session):
    expected_data = [
        {"artist_name": "artist_2", "track_name": "track_2", "track_plays": 3},
        {"artist_name": "artist_1", "track_name": "track_1", "track_plays": 2},
    ]
    expected_df = spark_session.createDataFrame(
        expected_data,
        T.StructType([
            T.StructField("artist_name", T.StringType()),
            T.StructField("track_name", T.StringType()),
            T.StructField("track_plays", T.LongType()),
        ])
    )

    return expected_df