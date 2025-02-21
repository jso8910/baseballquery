import pandas as pd
from pathlib import Path
from .parse_plate_appearance import ParsePlateAppearance
from .convert_mlbam import ConvertMLBAM
from .chadwick_cols import chadwick_dtypes


class ParseGame:
    def __init__(self, game: dict, convert_id: ConvertMLBAM):
        self.game = game
        self.df = pd.DataFrame(columns=chadwick_dtypes.keys())  # type: ignore
        self.df = self.df.astype(chadwick_dtypes)
        self.starting_lineup_away = {}
        self.starting_lineup_home = {}
        self.convert_id = convert_id
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
            self.player_lineup_spots[self.convert_id.mlbam_to_retro(int(player[2:]))] = int(away_players[player]["battingOrder"][0])

        for player, _ in home_players.items():
            if not home_players[player].get("battingOrder", None):
                continue
            self.player_lineup_spots[self.convert_id.mlbam_to_retro(int(player[2:]))] = int(home_players[player]["battingOrder"][0])

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
            )
            pa.parse()
            self.df = pd.concat([self.df, pa.df], ignore_index=True)
            if plate_appearance["about"]["isTopInning"]:
                self.away_score += pa.df["EVENT_RUNS_CT"].sum()
            else:
                self.home_score += pa.df["EVENT_RUNS_CT"].sum()

        innings = self.df.groupby(["INN_CT", "BAT_TEAM_ID"])
        for _, inning in innings:
            inning["FATE_RUNS_CT"] = inning["AWAY_SCORE_CT"] if inning["BAT_TEAM_ID"].iloc[0] == inning["AWAY_TEAM_ID"].iloc[0] else inning["HOME_SCORE_CT"]
            inning["FATE_RUNS_CT"] += inning["EVENT_RUNS_CT"]
            inning["FATE_RUNS_CT"] = inning["FATE_RUNS_CT"].iloc[-1] - inning["FATE_RUNS_CT"]
            self.df.update(inning)
