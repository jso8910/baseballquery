import requests
import pandas as pd
from pathlib import Path
import json
from .convert_mlbam import ConvertMLBAM
from .chadwick_cols import chadwick_dtypes, cwgame_dtypes
from .parse_game import ParseGame
import tqdm
from tqdm.asyncio import tqdm_asyncio
from .utils import get_year_events
import msgspec
import aiohttp
import asyncio


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

    async def fetch(self, session, url):
        async with session.get(url) as response:
            if response.status != 200:
                response.raise_for_status()
            return await response.json()

    async def download_data(self, game_set):
        # data_l = []
        async with aiohttp.ClientSession(json_serialize=msgspec.json.decode) as session:
            tasks = []
            for game in list(game_set):
                tasks.append(asyncio.create_task(self.fetch(session, f"https://statsapi.mlb.com{game}")))
            results = await tqdm_asyncio.gather(*tasks, desc="Fetching Data", position=1, leave=False)
        return results

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
        event_types_list = json.loads(open(Path(__file__).parent / "eventTypes.json").read())
        data_l = asyncio.run(self.download_data(games))
        game_df_l = []
        game_info_df_l = []
        for game_data in tqdm.tqdm(data_l, desc="Parsing games", position=1, leave=False):
            parse_game = ParseGame(game_data, self.convert_mlbam, event_types_list)
            parse_game.parse()
            parse_game.parse_game_info()
            parse_game.df["mlbam_id"] = game_data["gamePk"]
            game_df_l.append(parse_game.df)
            game_info_df_l.append(pd.DataFrame([parse_game.game_info]))
        del data_l # Free memory
        df_new = pd.concat(game_df_l)
        df_new = df_new.astype(chadwick_dtypes)
        self.df = pd.concat([self.df, df_new])
        print(self.df)
        self.df = self.df.reset_index(drop=True)

        self.game_info = pd.concat([self.game_info, *game_info_df_l])
        self.game_info = self.game_info.astype(cwgame_dtypes)
        self.game_info = self.game_info.reset_index(drop=True)
        return self.df, self.game_info
