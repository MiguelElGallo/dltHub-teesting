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

# Exact data to mock the API response
# Case 1: Get players
# ['0blivi0usspy', '123lt', '124chess', '1977ivan', '1stsecond']
# Case 2: Get profile
# {'player_id': 360558673, '@id': 'https://api.chess.com/pub/player/0blivi0usspy', 'url': 'https://www.chess.com/member/0blivi0usspy', 'username': '0blivi0usspy', 'title': 'GM', 'followers': 8, 'country': 'https://api.chess.com/pub/country/GB', 'last_online': 1735748231, 'joined': 1714573164, 'status': 'premium', 'is_streamer': False, 'verified': False, 'league': 'Champion', 'streaming_platforms': []}
# Case 3: Get games
# [{'url': 'https://www.chess.com/game/live/60386454607', 'pgn': '[Event "Live Chess"]\n[Site "Chess.com"]\n[Date "2022.10.24"]\n[Round "-"]\n[White "124chess"]\n[Black "JSlaby"]\n[Result "1-0"]\n[CurrentPosition "2r1r3/q5k1/2p2pp1/2Q4p/pP2PPnP/P4BP1/1BP3K1/R2R4 b - -"]\n[Timezone "UTC"]\n[ECO "A07"]\n[ECOUrl "https://www.chess.com/openings/Kings-Indian-Attack-2...c6"]\n[UTCDate "2022.10.24"]\n[UTCTime "21:37:11"]\n[WhiteElo "2587"]\n[BlackElo "2567"]\n[TimeControl "60"]\n[Termination "124chess won by resignation"]\n[StartTime "21:37:11"]\n[EndDate "2022.10.24"]\n[EndTime "21:39:03"]\n[Link "https://www.chess.com/game/live/60386454607"]\n\n1. Nf3 {[%clk 0:01:00]} 1... c6 {[%clk 0:01:00]} 2. g3 {[%clk 0:00:59.8]} 2... d5 {[%clk 0:00:59.9]} 3. Bg2 {[%clk 0:00:59.5]} 3... Bg4 {[%clk 0:00:59.6]} 4. O-O {[%clk 0:00:59.4]} 4... e6 {[%clk 0:00:58.4]} 5. d3 {[%clk 0:00:58.6]} 5... Nd7 {[%clk 0:00:57.5]} 6. Nbd2 {[%clk 0:00:58.1]} 6... Ngf6 {[%clk 0:00:57]} 7. b3 {[%clk 0:00:57.3]} 7... Bc5 {[%clk 0:00:56.2]} 8. Bb2 {[%clk 0:00:56.8]} 8... a5 {[%clk 0:00:55.7]} 9. a3 {[%clk 0:00:48.2]} 9... Qe7 {[%clk 0:00:54.3]} 10. e4 {[%clk 0:00:46.1]} 10... dxe4 {[%clk 0:00:53]} 11. dxe4 {[%clk 0:00:46]} 11... e5 {[%clk 0:00:52.5]} 12. Qe1 {[%clk 0:00:44.4]} 12... O-O {[%clk 0:00:51]} 13. Nh4 {[%clk 0:00:43.7]} 13... Rfe8 {[%clk 0:00:48.2]} 14. h3 {[%clk 0:00:42.8]} 14... Be6 {[%clk 0:00:46.4]} 15. Ndf3 {[%clk 0:00:41.6]} 15... g6 {[%clk 0:00:44.4]} 16. Nxe5 {[%clk 0:00:40.2]} 16... Nxe5 {[%clk 0:00:43.7]} 17. Bxe5 {[%clk 0:00:40.1]} 17... Nd7 {[%clk 0:00:38.2]} 18. Bb2 {[%clk 0:00:38.8]} 18... b5 {[%clk 0:00:36.5]} 19. Nf3 {[%clk 0:00:36.8]} 19... a4 {[%clk 0:00:33.7]} 20. b4 {[%clk 0:00:35.8]} 20... Ba7 {[%clk 0:00:32.4]} 21. Qc3 {[%clk 0:00:34.3]} 21... f6 {[%clk 0:00:28.9]} 22. Rfd1 {[%clk 0:00:33.1]} 22... Rac8 {[%clk 0:00:27.6]} 23. Nd4 {[%clk 0:00:31.8]} 23... Bc4 {[%clk 0:00:25.6]} 24. h4 {[%clk 0:00:26.1]} 24... Ne5 {[%clk 0:00:25]} 25. f4 {[%clk 0:00:24.4]} 25... Ng4 {[%clk 0:00:21.9]} 26. Bf3 {[%clk 0:00:23.7]} 26... Qd7 {[%clk 0:00:18.6]} 27. Kg2 {[%clk 0:00:22.7]} 27... h5 {[%clk 0:00:15.6]} 28. Nxb5 {[%clk 0:00:20.2]} 28... Qf7 {[%clk 0:00:11.3]} 29. Nxa7 {[%clk 0:00:18.6]} 29... Qxa7 {[%clk 0:00:10.9]} 30. Qxc4+ {[%clk 0:00:17.9]} 30... Kg7 {[%clk 0:00:07.8]} 31. Qc5 {[%clk 0:00:15.8]} 1-0\n', 'time_control': '60', 'end_time': 1666647543, 'rated': True, 'tcn': 'gvYQowZJfo6Eeg0Slt5Zbl!Tjr9IcjWGiq70mCJCtCSKde8!vF98pxESlv2UvKZKjKTZKjXHFvGyrzIWes1Tfd46vBSAxFZKnDKEov0Zgo3NBHZ1HW1WsA!2AI', 'uuid': '053bb03e-53e4-11ed-8792-78ac4409ff3c', 'initial_setup': 'rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1', 'fen': '2r1r3/q5k1/2p2pp1/2Q4p/pP2PPnP/P4BP1/1BP3K1/R2R4 b - -', 'time_class': 'bullet', 'rules': 'chess', 'white': {...}, 'black': {...}}]
@patch("pipeline.get_data_with_retry")
def test_pipe_run(mock_get_data, sample_data):
    player, profile = sample_data
    
    def mock_side_effect(path:str, *args, **kwargs):
        if path.startswith("titled"):
            return {"players": ["0blivi0usspy", "123lt", "124chess", "1977ivan", "1stsecond"]}
        elif path.startswith("player") and not path.__contains__("games"):
            return {
                "player_id": 360558673,
                "@id": "https://api.chess.com/pub/player/0blivi0usspy",
                "url": "https://www.chess.com/member/0blivi0usspy",
                "username": "0blivi0usspy",
                "title": "GM",
                "followers": 8,
                "country": "https://api.chess.com/pub/country/GB",
                "last_online": 1735748231,
                "joined": 1714573164,
                "status": "premium",
                "is_streamer": False,
                "verified": False,
                "league": "Champion",
                "streaming_platforms": []
            }
        elif path.startswith("player") and  path.__contains__("games"):
            return {
                "games": [
                    {
                        "url": "https://www.chess.com/game/live/60386454607",
                        # ...example game data from the comment...
                    }
                ]
            }
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
