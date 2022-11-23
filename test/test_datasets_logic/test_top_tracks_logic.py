import pytest
from src.datasets_logic.top_tracks_logic import (
    obtain_session_id,
    obtain_top_session,
    obtain_most_popular_tracks
)

pytest.mark.usefixtures("spark_session", "input_df", "top_sessions_expected_df", "top_tracks_expected_df")
class TestTopTracksLogic:
    """
    Test Suite for the logic to compute the top tracks out of the longest sessions
    """
    #def __init__(self, spark_session, input_df):
    #    self.spark_session = spark_session
    #    self.input_df = input_df

    def test_top_sessions(self, input_df, top_sessions_expected_df):
        plays_with_session = obtain_session_id(input_df, session_duration_min=20)
        top_sessions = obtain_top_session(plays_with_session, num_sessions_to_keep=3)
        assert top_sessions.collect() == top_sessions_expected_df.collect()

    def test_top_tracks(self, input_df, top_tracks_expected_df):
        plays_with_session = obtain_session_id(input_df, session_duration_min=20)
        top_sessions = obtain_top_session(plays_with_session, num_sessions_to_keep=3)
        top_tracks = obtain_most_popular_tracks(plays_with_session, top_sessions, 2)
        assert top_tracks.collect() == top_tracks_expected_df.collect()