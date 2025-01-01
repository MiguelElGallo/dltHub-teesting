from unittest import mock
from pipeline import chess
from pipeline import load_data_with_retry
from pydantic import BaseModel, Field
from typing import List, Tuple
import json
from pytest import fixture
import dlt 
from unittest.mock import patch,  MagicMock

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
def sample_data()-> Tuple[Player, Profile]:
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


def test_pipe_run( sample_data):
    player, profile = sample_data
    
    mock_get_data = MagicMock()
    def mock_side_effect(path, *args, **kwargs):
        if path.startswith("titled"):
            return player.dict()
        elif path.startswith("player"):
            return profile.dict()
        elif path.contains("games"):
            return {[]}
        return {}  # Default case

    mock_get_data.side_effect = mock_side_effect
    
    with patch.object(chess, '_get_data_with_retry', mock_get_data):
        pipeline = dlt.pipeline(
            pipeline_name="chess_pipeline",
            destination="duckdb",
            dataset_name="chess_data",
        )
        data = chess(max_players=1)
        load_data_with_retry(pipeline, data)



