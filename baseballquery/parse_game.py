import pandas as pd
from datetime import datetime
from .parse_plate_appearance import ParsePlateAppearance
from .convert_mlbam import ConvertMLBAM
from .chadwick_cols import chadwick_dtypes, cwgame_dtypes
import numpy as np


class ParseGame:
    def __init__(self, game: dict, convert_id: ConvertMLBAM, event_types_list: list[dict]):
        self.game = game
        # self.df = pd.DataFrame(columns=chadwick_dtypes.keys())  # type: ignore
        # self.df = self.df.astype(chadwick_dtypes)
        self.data = []
        self.starting_lineup_away = {}
        self.starting_lineup_home = {}
        self.convert_id = convert_id
        self.event_types_list = event_types_list
        away_players = self.game["liveData"]["boxscore"]["teams"]["away"]["players"]
        for player, _ in away_players.items():
            if away_players[player].get("battingOrder", "").endswith("00"):
                # ID is in format IDXXXXXX, so remove the ID
                self.starting_lineup_away[int(away_players[player]["battingOrder"][0])] = (
                    self.convert_id.mlbam_to_retro(int(player[2:]))
                )
        home_players = self.game["liveData"]["boxscore"]["teams"]["home"]["players"]
        for player, _ in home_players.items():
            if home_players[player].get("battingOrder", "").endswith("00"):
                self.starting_lineup_home[int(home_players[player]["battingOrder"][0])] = (
                    self.convert_id.mlbam_to_retro(int(player[2:]))
                )

        self.positions = {}
        for player, _ in away_players.items():
            if not away_players[player].get("allPositions", None):
                continue
            self.positions[int(player[2:])] = int(away_players[player]["allPositions"][0]["code"])

        for player, _ in home_players.items():
            if not home_players[player].get("allPositions", None):
                continue
            self.positions[int(player[2:])] = int(home_players[player]["allPositions"][0]["code"])

        self.player_lineup_spots = {}
        for player, _ in away_players.items():
            if not away_players[player].get("battingOrder", None):
                continue
            self.player_lineup_spots[self.convert_id.mlbam_to_retro(int(player[2:]))] = int(
                away_players[player]["battingOrder"][0]
            )

        for player, _ in home_players.items():
            if not home_players[player].get("battingOrder", None):
                continue
            self.player_lineup_spots[self.convert_id.mlbam_to_retro(int(player[2:]))] = int(
                home_players[player]["battingOrder"][0]
            )

        self.away_starting_pitcher = self.convert_id.mlbam_to_retro(
            self.game["liveData"]["boxscore"]["teams"]["away"]["pitchers"][0]
        )
        self.home_starting_pitcher = self.convert_id.mlbam_to_retro(
            self.game["liveData"]["boxscore"]["teams"]["home"]["pitchers"][0]
        )

        self.home_team = self.game["gameData"]["teams"]["home"]["teamCode"].upper()
        self.away_team = self.game["gameData"]["teams"]["away"]["teamCode"].upper()
        # Reconstruction. In the format "XXXYYYYMMDD0". Works unless there's a double header with a game postponed
        if self.game["gameData"]["game"]["doubleHeader"] == "N":
            self.game_id = f"{self.home_team}{''.join(self.game['gameData']['game']['id'].split('/')[:3])}0"
        else:
            self.game_id = f"{self.home_team}{''.join(self.game['gameData']['game']['id'].split('/')[:3])}{self.game['gameData']['game']['id'][-1]}"

        self.home_score = 0
        self.away_score = 0

        self.game_info: dict[str, int|str|None] = {key: None for key in cwgame_dtypes.keys()}

    def parse_game_info(self):
        self.game_info["GAME_ID"] = self.game_id
        # dt = datetime.strptime(self.game["gameData"]["datetime"]["officialDate"], "%Y-%m-%d")
        date = self.game["gameData"]["datetime"]["officialDate"]
        dt = datetime.fromisoformat(date)
        self.game_info["GAME_DY"] = dt.weekday()
        self.game_info["START_GAME_TM"] = int(self.game["gameData"]["datetime"]["time"].replace(":", ""))
        self.game_info["DAYNIGHT_PARK_CD"] = "N" if self.game["gameData"]["datetime"]["dayNight"] == "night" else "D"
        self.game_info["PARK_ID"] = None
        self.game_info["ATTEND_PARK_CT"] = self.game["gameData"]["gameInfo"]["attendance"]
        self.game_info["TEMP_PARK_CT"] = int(self.game["gameData"]["weather"]["temp"])
        wind = self.game["gameData"]["weather"]["wind"].split(", ")[1]
        if wind == "In From CF":
            self.game_info["WIND_DIRECTION"] = 6
        elif wind == "In From LF":
            self.game_info["WIND_DIRECTION"] = 5
        elif wind == "In From RF":
            self.game_info["WIND_DIRECTION"] = 7
        elif wind == "L To R":
            self.game_info["WIND_DIRECTION"] = 4
        elif wind == "Out To CF":
            self.game_info["WIND_DIRECTION"] = 2
        elif wind == "Out To LF":
            self.game_info["WIND_DIRECTION"] = 1
        elif wind == "Out To RF":
            self.game_info["WIND_DIRECTION"] = 3
        elif wind == "R To L":
            self.game_info["WIND_DIRECTION"] = 8
        else:
            self.game_info["WIND_DIRECTION"] = 0
        self.game_info["WIND_SPEED_PARK_CT"] = int(self.game["gameData"]["weather"]["wind"].split(" ")[0])
        self.game_info["FIELD_PARK_CD"] = 0
        self.game_info["PRECIP_PARK_CT"] = 0
        self.game_info["SKY_PARK_CD"] = 0
        self.game_info["MINUTES_GAME_CT"] = self.game["gameData"]["gameInfo"]["gameDurationMinutes"]

        game_decisions = self.game["liveData"]["decisions"]
        if game_decisions.get("winner", None):
            self.game_info["WIN_PIT_ID"] = self.convert_id.mlbam_to_retro(int(game_decisions["winner"]["id"]))
        else:
            self.game_info["WIN_PIT_ID"] = None
        if game_decisions.get("loser", None):
            self.game_info["LOSE_PIT_ID"] = self.convert_id.mlbam_to_retro(int(game_decisions["loser"]["id"]))
        else:
            self.game_info["LOSE_PIT_ID"] = None
        if game_decisions.get("save", None):
            self.game_info["SAVE_PIT_ID"] = self.convert_id.mlbam_to_retro(int(game_decisions["save"]["id"]))
        else:
            self.game_info["SAVE_PIT_ID"] = None

        self.game_info["FINAL_INN_CT"] = self.game["liveData"]["linescore"]["currentInning"]
        self.game_info["FINAL_HOME_SCORE_CT"] = self.game["liveData"]["linescore"]["teams"]["home"]["runs"]
        self.game_info["FINAL_AWAY_SCORE_CT"] = self.game["liveData"]["linescore"]["teams"]["away"]["runs"]

    def parse(self):
        runners = [None, None, None]
        runner_resp_pit_id = [None, None, None]
        old_inning_topbot = True
        away_pitcher = [self.away_starting_pitcher, "?"]
        home_pitcher = [self.home_starting_pitcher, "?"]
        for idx, plate_appearance in enumerate(self.game["liveData"]["plays"]["allPlays"]):
            if plate_appearance["about"]["isTopInning"] != old_inning_topbot:
                runners = [None, None, None]
                runner_resp_pit_id = [None, None, None]
                old_inning_topbot = not old_inning_topbot
            if len(plate_appearance["playEvents"]) == 0:
                # This sometimes happens (eg https://www.mlb.com/gameday/rockies-vs-giants/2024/07/27/745307/final/summary/all)
                # Where there is a random empty plate appearance. This one was after a game ending challenge, that could be why
                continue
            eventTypes = {event["code"]: event for event in self.event_types_list}
            # eventTypes documentation from https://statsapi.mlb.com/api/v1/eventTypes
            # Custom proxy property for foul_error
            eventTypes["foul_error"] = eventTypes["error"]
            pa = ParsePlateAppearance(
                plate_appearance,
                self.game["liveData"]["plays"]["allPlays"][:idx],
                self.game_id,
                self.away_team,
                self.home_team,
                self.starting_lineup_away,  # type: ignore
                self.starting_lineup_home,  # type: ignore
                self.positions,
                self.player_lineup_spots,
                self.away_starting_pitcher,
                self.home_starting_pitcher,
                away_pitcher,
                home_pitcher,
                self.away_score,
                self.home_score,
                self.convert_id,
                runners,  # type: ignore
                runner_resp_pit_id,  # type: ignore
                eventTypes,
            )
            pa.parse()
            # self.df = pd.concat([self.df, pa.df], ignore_index=True)
            self.data.extend(pa.data)
            if plate_appearance["about"]["isTopInning"]:
                self.away_score += sum(elem["EVENT_RUNS_CT"] for elem in pa.data)
            else:
                self.home_score += sum(elem["EVENT_RUNS_CT"] for elem in pa.data)

        self.df = pd.DataFrame(self.data, columns=chadwick_dtypes.keys())   # type: ignore

        self._calculate_fate_runs_vectorized()

    def _calculate_fate_runs_vectorized(self):
        """Vectorized FATE_RUNS_CT calculation"""
        if self.df.empty:
            return
        
        # Create a mask for away team batting
        away_batting = self.df["BAT_TEAM_ID"] == self.df["AWAY_TEAM_ID"]
        
        # Calculate base scores (before adding EVENT_RUNS_CT)
        base_scores = np.where(away_batting, self.df["AWAY_SCORE_CT"], self.df["HOME_SCORE_CT"])
        
        # Add event runs to get total scores
        total_scores = base_scores + self.df["EVENT_RUNS_CT"]
        
        # Add total_scores as a temporary column
        self.df["_temp_total_scores"] = total_scores
        
        # Group by inning and team
        grouped = self.df.groupby(["INN_CT", "BAT_TEAM_ID"])
        
        # Calculate the final score for each group (last total score in each inning)
        final_scores = grouped["_temp_total_scores"].transform('last')
        
        # FATE_RUNS_CT = final score of inning - current total score
        self.df["FATE_RUNS_CT"] = final_scores - self.df["_temp_total_scores"]
        
        # Remove the temporary column
        self.df.drop("_temp_total_scores", axis=1, inplace=True)