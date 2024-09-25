from pandas import pd   # type: ignore
from pathlib import Path
from .parse_plate_appearance import ParsePlateAppearance
from .convert_mlbam import ConvertMLBAM

chadwick_dtypes = {
    "GAME_ID": "object",#
    "AWAY_TEAM_ID": "object",#
    "INN_CT": "int64",#
    "OUTS_CT": "int64",#
    "BALLS_CT": "int64",#
    "STRIKES_CT": "int64",#
    "AWAY_SCORE_CT": "int64",
    "HOME_SCORE_CT": "int64",
    "RESP_BAT_ID": "object",#
    "RESP_BAT_HAND_CD": "object",#
    "RESP_PIT_ID": "object",#
    "RESP_PIT_HAND_CD": "object",#
    "BASE1_RUN_ID": "object",#
    "BASE2_RUN_ID": "object",#
    "BASE3_RUN_ID": "object",#
    "BAT_FLD_CD": "int64",
    "BAT_LINEUP_ID": "int64",
    "EVENT_CD": "int64",
    "AB_FL": "bool",
    "H_CD": "int64",
    "SH_FL": "bool",
    "SF_FL": "bool",
    "EVENT_OUTS_CT": "int64",
    "DP_FL": "bool",
    "TP_FL": "bool",
    "RBI_CT": "int64",#
    "WP_FL": "bool",
    "PB_FL": "bool",
    "BATTEDBALL_CD": "object",
    "BAT_DEST_ID": "int64",
    "RUN1_DEST_ID": "int64",#
    "RUN2_DEST_ID": "int64",#
    "RUN3_DEST_ID": "int64",#
    "RUN1_SB_FL": "bool",#
    "RUN2_SB_FL": "bool",#
    "RUN3_SB_FL": "bool",#
    "RUN1_CS_FL": "bool",#
    "RUN2_CS_FL": "bool",#
    "RUN3_CS_FL": "bool",#
    "RUN1_PK_FL": "bool",#
    "RUN2_PK_FL": "bool",#
    "RUN3_PK_FL": "bool",#
    "RUN1_RESP_PIT_ID": "object",#
    "RUN2_RESP_PIT_ID": "object",#
    "RUN3_RESP_PIT_ID": "object",#
    "HOME_TEAM_ID": "object",#
    "BAT_TEAM_ID": "object",#
    "FLD_TEAM_ID": "object",#
    "PA_TRUNC_FL": "bool",
    "START_BASES_CD": "int64",#
    "END_BASES_CD": "int64",#
    "PIT_START_FL": "bool",
    "RESP_PIT_START_FL": "bool",
    "PA_BALL_CT": "int64",
    "PA_OTHER_BALL_CT": "int64",
    "PA_STRIKE_CT": "int64",
    "PA_OTHER_STRIKE_CT": "int64",
    "EVENT_RUNS_CT": "int64",#
    "BAT_SAFE_ERR_FL": "bool",
    "FATE_RUNS_CT": "int64",
    "RESP_BAT_START_FL": "bool",
}


class ParseGame:
    def __init__(self, game: dict, convert_id: ConvertMLBAM): # type: ignore
        self.game = game    # type: ignore
        self.df = pd.DataFrame(columns=chadwick_dtypes.keys())   # type: ignore
        self.df = self.df.astype(chadwick_dtypes)    # type: ignore
        self.starting_lineup_away = {}
        self.starting_lineup_home = {}
        self.convert_id = convert_id
        away_players = self.game["liveData"]["boxscore"]["teams"]["away"]["players"]    # type: ignore
        for _, player in away_players.items():  # type: ignore
            if away_players[player].get("battingOrder", "").endswith("00"):   # type: ignore
                self.starting_lineup_away[int(away_players[player]["battingOrder"][0])] = (self.convert_id.mlbam_to_retro(player))   # type: ignore
        home_players = self.game["liveData"]["boxscore"]["teams"]["home"]["players"]    # type: ignore
        for _, player in home_players.items():  # type: ignore
            if home_players[player].get(battingOrder, "").endswith("00"):   # type: ignore
                self.starting_lineup_home[int(away_players[player]["battingOrder"][0])] = (self.convert_id.mlbam_to_retro(player))   # type: ignore

        self.away_starting_pitcher = self.convert_id.mlbam_to_retro(self.player_lookup["gameData"]["probablePitchers"]["away"]["id"])    # type: ignore
        self.home_starting_pitcher = self.convert_id.mlbam_to_retro(self.player_lookup["gameData"]["probablePitchers"]["home"]["id"])    # type: ignore

        self.home_team = self.game["gameData"]["teams"]["home"]["abbreviation"]    # type: ignore
        self.away_team = self.game["gameData"]["teams"]["away"]["abbreviation"]    # type: ignore
        # Reconstruction. In the format "XXXYYYYMMDD0". Doesn't work with doubleheaders to add a 1 at the end
        self.game_id = f"{self.home_team}{"".join(self.game['gameData']['game']['id'].split(/)[:3])}0"  # type: ignore


    def parse(self):
        # TODO: Reset this between innings, figure out how to add the manfred runner. I assume that can be done within ParsePlateAppearance (since it'll probably be in the event thing)
        runners = [None, None, None]
        runner_resp_pit_id = [None, None, None]
        for plate_appearance in self.game["liveData"]["plays"]["allPlays"]: # type: ignore
            if len(plate_appearance["playEvents"]) == 0:    # type: ignore
                # This sometimes happens (eg https://www.mlb.com/gameday/rockies-vs-giants/2024/07/27/745307/final/summary/all)
                # Where there is a random empty plate appearance. This one was after a game ending challenge, that could be why
                continue
            pa = ParsePlateAppearance(plate_appearance, self.game_id, self.away_team, self.home_team, self.starting_lineup_away, self.starting_lineup_home, self.away_starting_pitcher, self.home_starting_pitcher, runners, runner_resp_pit_id)    # type: ignore
            pa.parse()
            self.df = pd.concat([self.df, pa.df], ignore_index=True)   # type: ignore
