import pandas as pd
from pathlib import Path
from .parse_plate_appearance import ParsePlateAppearance
from .convert_mlbam import ConvertMLBAM

chadwick_dtypes = {
    "GAME_ID": "object",  #
    "AWAY_TEAM_ID": "object",  #
    "INN_CT": "int64",  #
    "OUTS_CT": "int64",  #
    "BALLS_CT": "int64",  #
    "STRIKES_CT": "int64",  #
    "AWAY_SCORE_CT": "int64",  #
    "HOME_SCORE_CT": "int64",  #
    "RESP_BAT_ID": "object",  #
    "RESP_BAT_HAND_CD": "object",  #
    "RESP_PIT_ID": "object",  #
    "RESP_PIT_HAND_CD": "object",  #
    "BASE1_RUN_ID": "object",  #
    "BASE2_RUN_ID": "object",  #
    "BASE3_RUN_ID": "object",  #
    "BAT_FLD_CD": "int64",  #
    "BAT_LINEUP_ID": "int64",
    "EVENT_CD": "int64",  #
    "AB_FL": "bool",  #
    "H_CD": "int64",  #
    "SH_FL": "bool",  #
    "SF_FL": "bool",  #
    "EVENT_OUTS_CT": "int64",  #
    "DP_FL": "bool",  #
    "TP_FL": "bool",  #
    "RBI_CT": "int64",  #
    "WP_FL": "bool",  #
    "PB_FL": "bool",  #
    "BATTEDBALL_CD": "object",  #
    "BAT_DEST_ID": "int64",  #
    "RUN1_DEST_ID": "int64",  #
    "RUN2_DEST_ID": "int64",  #
    "RUN3_DEST_ID": "int64",  #
    "RUN1_SB_FL": "bool",  #
    "RUN2_SB_FL": "bool",  #
    "RUN3_SB_FL": "bool",  #
    "RUN1_CS_FL": "bool",  #
    "RUN2_CS_FL": "bool",  #
    "RUN3_CS_FL": "bool",  #
    "RUN1_PK_FL": "bool",  #
    "RUN2_PK_FL": "bool",  #
    "RUN3_PK_FL": "bool",  #
    "RUN1_RESP_PIT_ID": "object",  #
    "RUN2_RESP_PIT_ID": "object",  #
    "RUN3_RESP_PIT_ID": "object",  #
    "HOME_TEAM_ID": "object",  #
    "BAT_TEAM_ID": "object",  #
    "FLD_TEAM_ID": "object",  #
    "PA_TRUNC_FL": "bool",  #
    "START_BASES_CD": "int64",  #
    "END_BASES_CD": "int64",  #
    "RESP_PIT_START_FL": "bool",  #
    "PA_BALL_CT": "int64",  #
    "PA_OTHER_BALL_CT": "int64",  #
    "PA_STRIKE_CT": "int64",  #
    "PA_OTHER_STRIKE_CT": "int64",  #
    "EVENT_RUNS_CT": "int64",  #
    "BAT_SAFE_ERR_FL": "bool",  #
    "FATE_RUNS_CT": "int64",
    "RESP_BAT_START_FL": "bool",  #
    "MLB_STATSAPI_APPROX": "bool",
}

mlbam_to_retro_team_name = {
    "AZ": "ARI",
    "BAL": "BAL",
    "BOS": "BOS",
    "CHC": "CHN",
    "CIN": "CIN",
    "CLE": "CLE",
    "COL": "COL",
    "DET": "DET",
    "HOU": "HOU",
    "KC": "KCA",
    "LAD": "LAN",
    "WSH": "WAS",
    "NYM": "NYN",
    "ATH": "OAK",
}


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

        self.away_starting_pitcher = self.convert_id.mlbam_to_retro(self.game["gameData"]["probablePitchers"]["away"]["id"])
        self.home_starting_pitcher = self.convert_id.mlbam_to_retro(self.game["gameData"]["probablePitchers"]["home"]["id"])

        self.home_team = self.game["gameData"]["teams"]["home"]["teamCode"].upper()
        self.away_team = self.game["gameData"]["teams"]["away"]["teamCode"].upper()
        # Reconstruction. In the format "XXXYYYYMMDD0". Doesn't work with doubleheaders to add a 1 at the end
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
        for idx, plate_appearance in enumerate(self.game["liveData"]["plays"]["allPlays"]):
            if plate_appearance["about"]["isTopInning"] != old_inning_topbot:
                runners = [None, None, None]
                runner_resp_pit_id = [None, None, None]
                old_inning_topbot = not old_inning_topbot
            if len(plate_appearance["playEvents"]) == 0:
                # This sometimes happens (eg https://www.mlb.com/gameday/rockies-vs-giants/2024/07/27/745307/final/summary/all)
                # Where there is a random empty plate appearance. This one was after a game ending challenge, that could be why
                continue
            pa = ParsePlateAppearance(plate_appearance, self.game["liveData"]["plays"]["allPlays"][:idx], self.game_id, self.away_team, self.home_team, self.starting_lineup_away, self.starting_lineup_home, self.positions, self.away_starting_pitcher, self.home_starting_pitcher, [self.away_starting_pitcher], [self.home_starting_pitcher], self.away_score, self.home_score, self.convert_id, runners, runner_resp_pit_id)  # type: ignore
            pa.parse()
            self.df = pd.concat([self.df, pa.df], ignore_index=True)
            if plate_appearance["about"]["isTopInning"]:
                self.away_score += pa.df["EVENT_RUNS_CT"].sum()
            else:
                self.home_score += pa.df["EVENT_RUNS_CT"].sum()

        ## Temporary testing code
        cwd = Path(__file__).parent
        original_cw = pd.read_hdf(cwd / "chadwick.hdf5", key=f"year_{self.game_id[3:7]}")
        game = original_cw[original_cw["GAME_ID"] == self.game_id]
        cols_to_test = ["EVENT_CD", "BALLS_CT", "STRIKES_CT", "OUTS_CT", "START_BASES_CD", "END_BASES_CD", "BAT_FLD_CD", "HOME_SCORE_CT", "AWAY_SCORE_CT", "EVENT_RUNS_CT", "RESP_BAT_ID", "RESP_PIT_ID", "BASE1_RUN_ID", "BASE2_RUN_ID", "BASE3_RUN_ID"]
        print(self.game_id)
        for idx, row in self.df.iterrows():
            for col in cols_to_test:
                if pd.isna(row[col]) and pd.isna(game[col].iloc[idx]):    # type: ignore
                    continue
                if row[col] != game[col].iloc[idx]: # type: ignore
                    # Weird edge case... can't tell the difference
                    if col == "EVENT_CD" and row[col] == 2 and game[col].iloc[idx] == 18 and row["BAT_SAFE_ERR_FL"] == True:    # type: ignore
                        continue
                    # Retrosheet considers subs of the DH as DH, not PH. MLBAM considers them as PH
                    if col == "BAT_FLD_CD" and row[col] == 11 and game[col].iloc[idx] == 10:    # type: ignore
                        continue
                    print(col, "mismatch")
                    print(row[col], game.iloc[idx][col])    # type: ignore
                    print(row)
                    print(game.iloc[idx])   # type: ignore
                    print()