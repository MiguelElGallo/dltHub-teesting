from pipeline import chess
from pipeline import load_data_with_retry
from pydantic import BaseModel, Field
from typing import List
import json
from pytest import fixture
import dlt 
from unittest.mock import patch

MAX_PLAYERS = 5

class Profile(BaseModel):
    avatar: str
    player_id: int
    id_: str = Field(..., alias="@id")
    url: str
    name: str
    username: str
    title: str
    followers: int
    country: str
    location: str
    last_online: int
    joined: int
    status: str
    is_streamer: str
    verified: str
    league: str
    streaming_platforms: List[str]

class Player(BaseModel):
    players: List[str]

@fixture
def sample_data():
    try:
        with open("test/player.json", "r") as f:
            player_data = json.load(f)
        player = Player(**player_data)

        with open("test/profile.json", "r") as f:
            profile_data = json.load(f)
        profile = Profile(**profile_data)
        # ...use 'player' and 'profile' here...
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error: {e}")
    finally:
        return player, profile

def  test_init(sample_data):
    player, profile = sample_data
    assert player is not None
    assert profile is not None

@patch("pipeline._get_data_with_retry")
def test_pipe_run(mocked_getdata, sample_data):
    def mock_side_effect(*args, **kwargs):
        path = args[1]
        if path.startswith("player"):
            return sample_data[0].dict()
        elif path.startswith("profiles"):
            return sample_data[1].dict()
        return {}
    mocked_getdata.side_effect = mock_side_effect
    # Following pipeline.py we will createa a pipeline object
    # and run it
    pipeline = dlt.pipeline(
    pipeline_name="chess_pipeline",
    destination="duckdb",
    dataset_name="chess_data",
    )
    data = chess(max_players=1)
    load_data_with_retry(pipeline, data)



