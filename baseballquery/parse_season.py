import requests
import pandas as pd
from .convert_mlbam import ConvertMLBAM
from .chadwick_cols import chadwick_dtypes
from .parse_game import ParseGame
from tqdm import tqdm

class ParseSeason:
    def __init__(self, year: int):
        self.year = year
        self.convert_mlbam = ConvertMLBAM()
        self.df = pd.DataFrame(columns=chadwick_dtypes.keys())  # type: ignore
        self.df = self.df.astype(chadwick_dtypes)

    def get_schedule(self):
        url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&startDate={self.year}-01-01&endDate={self.year}-12-31"
        r = requests.get(url)
        r.raise_for_status()
        schedule = r.json()
        return schedule

    def parse(self):
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
                games.add(game["link"])
        if not games:
            return
        for game in tqdm(games, desc=" Games", position=0, leave=True):
            game_data = requests.get(f"https://statsapi.mlb.com{game}").json()
            parse_game = ParseGame(game_data, self.convert_mlbam)
            parse_game.parse()
            self.df = pd.concat([self.df, parse_game.df])
        return self.df