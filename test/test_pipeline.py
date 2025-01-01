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


@patch("pipeline.get_data_with_retry")
def test_pipe_run(mock_get_data, sample_data):
    player, profile = sample_data
    
    def mock_side_effect(path, *args, **kwargs):
        if path.startswith("titled"):
            return player.dict()
        elif path.startswith("player") and not path.endswith("games"):
            return profile.dict()
        elif "games" in path:
            return {"games": []}
        return {}
        
    mock_get_data.side_effect = mock_side_effect

    pipeline = dlt.pipeline(
        pipeline_name="chess_pipeline", 
        destination="duckdb",
        dataset_name="chess_data"
    )
    
    data = chess(max_players=1)
    info = load_data_with_retry(pipeline, data)
    
    assert info is not None
    assert mock_get_data.call_count >= 3  # Called for players, profiles and games
    mock_get_data.assert_any_call("titled/GM")

@patch("pipeline._get_data_with_retry")
def test_pipe_run_error(mock_get_data, sample_data):
    mock_get_data.side_effect = Exception("API Error")
    
    pipeline = dlt.pipeline(
        pipeline_name="chess_pipeline",
        destination="duckdb", 
        dataset_name="chess_data"
    )
    
    data = chess(max_players=1)
    

@patch("pipeline._get_data_with_retry") 
def test_pipe_empty_data(mock_get_data, sample_data):
    mock_get_data.return_value = {"players": []}
    
    pipeline = dlt.pipeline(
        pipeline_name="chess_pipeline",
        destination="duckdb",
        dataset_name="chess_data" 
    )
    
    data = chess(max_players=1)
    info = load_data_with_retry(pipeline, data)
    
    assert info is not None
    mock_get_data.assert_called_once_with("titled/GM")
