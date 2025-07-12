import requests
import pandas as pd
from pathlib import Path
import json
from .convert_mlbam import ConvertMLBAM
from .chadwick_cols import chadwick_dtypes, cwgame_dtypes
from .parse_game import ParseGame
from tqdm import tqdm
from .utils import get_year_events


class ParseSeason:
    def __init__(self, year: int):
        self.year = year
        self.convert_mlbam = ConvertMLBAM()
        self.df = pd.DataFrame(columns=chadwick_dtypes.keys())  # type: ignore
        self.df = self.df.astype(chadwick_dtypes)
        self.game_info = pd.DataFrame(columns=cwgame_dtypes.keys())  # type: ignore
        self.game_info = self.game_info.astype(cwgame_dtypes)

    def get_schedule(self):
        url = (
            f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&startDate={self.year}-01-01&endDate={self.year}-12-31"
        )
        r = requests.get(url)
        r.raise_for_status()
        schedule = r.json()
        return schedule

    def parse(self):
        try:
            df = get_year_events(self.year)
            if not df.empty:
                self.df = pd.concat([self.df.reset_index(drop=True), df])
        except KeyError:
            pass
        schedule = self.get_schedule()
        games = set()
        for date in schedule["dates"]:
            for game in date["games"]:
                # Regular season only
                if not game["gameType"] == "R":
                    continue
                # Only finished games
                if not game["status"]["codedGameState"] == "F":
                    continue
                if str(game["gamePk"]) in self.df["mlbam_id"].values:
                    continue
                games.add(game["link"])
        if not games:
            return
        cwd = Path(__file__).parent
        event_types_list = json.loads(open(Path(__file__).parent / "eventTypes.json").read())
        for game in tqdm(list(games), desc="Games", position=0, leave=True):
            game_data = requests.get(f"https://statsapi.mlb.com{game}").json()
            parse_game = ParseGame(game_data, self.convert_mlbam, event_types_list)
            parse_game.parse()
            parse_game.parse_game_info()
            parse_game.df["mlbam_id"] = game_data["gamePk"]
            self.df = pd.concat([self.df, parse_game.df])
            self.game_info = pd.concat([self.game_info, pd.DataFrame([parse_game.game_info])])
        self.df = self.df.reset_index(drop=True)
        self.game_info = self.game_info.reset_index(drop=True)
        return self.df, self.game_info
