import pandas as pd
from convert_mlbam import ConvertMLBAM
from copy import deepcopy
import json
from pathlib import Path

"""

For the runner things, just move this details thing and replace the results thing with the details thing. It's inside the playEvents
"details": {
                                "description": "Ronald Acuna Jr.  out at 3rd, catcher Nick Fortes to first baseman Yuli Gurriel to third baseman Jean Segura.   Matt Olson to 2nd.",
                                "event": "Runner Out",
                                "eventType": "other_out",
                                "awayScore": 0,
                                "homeScore": 4,
                                "isScoringPlay": false,
                                "isOut": false,
                                "hasReview": false
                            },
Also move the count thing which is on the same level
And remove every playEvent after it (as well as the actual playEvent thing which contains the details) before passing it to parse plate appearance


0: unknown (obs)
1: none (obs)
2: Generic out (out)
3: Strikeout (K)
4: Stolen base (SB)
5: Defensive indifference (DI)
6: Caught stealing (CS)
7: Pickoff error (POE) (obs)
8: Pickoff (PO)
9: Wild pitch (WP)
10: Passed ball (PB)
11: Balk (BK)
12: Other advance/out advancing (OA) (figure out where this is used.... is it always an out?. it appears to never be an out)
13: Foul error (FE)
14: Walk (BB)
15: Intentional walk (IBB)
16: Hit by pitch (HBP)
17: Interference (INT)
18: Error (E)
19: Fielder's choice (FC)
20: Single (1B)
21: Double (2B)
22: Triple (3B)
23: Home run (HR)
24: missing play (obs)

Data from 2023:

# Event_type field
In [22]: list(df["event_type"].unique())
Out[22]:
['NaN',
 'caught_stealing_3b',
 'strikeout_double_play',
 'double_play',
 'fielders_choice',
 'force_out',
 'grounded_into_double_play',
 'field_error',
 'double',
 'catcher_interf',
 'triple',
 'strikeout',
 'walk',
 'home_run',
 'single',
 'sac_bunt',
 'other_out',
 'sac_fly',
 'hit_by_pitch',
 'caught_stealing_2b',
 'caught_stealing_home',
 'stolen_base_2b',
 'field_out',
 'fielders_choice_out',
 'sac_fly_double_play',
 'intent_walk',
 'pickoff_caught_stealing_home']


# Event field
In [23]: list(df["event"].unique())
Out[23]:
['Double',
 'Groundout',
 'NaN',
 'Hit By Pitch',
 'Walk',
 'Pop Out',
 'Runner Out',
 'Catcher Interference',
 'Fielders Choice Out',
 'Caught Stealing 3B',
 'Flyout',
 'Caught Stealing Home',
 'Forceout',
 'Fielders Choice',
 'Sac Fly Double Play',
 'Bunt Lineout',
 'Triple',
 'Field Error',
 'Bunt Pop Out',
 'Bunt Groundout',
 'Sac Fly',
 'Caught Stealing 2B',
 'Single',
 'Strikeout',
 'Strikeout Double Play',
 'Home Run',
 'Stolen Base 2B',
 'Double Play',
 'Lineout',
 'Intent Walk',
 'Sac Bunt',
 'Pickoff Caught Stealing Home',
 'Grounded Into DP']

# All pairs of event and event_type
 In [28]: [g for g, _ in df.group_by(["event", "event_type"])]
Out[28]:
[('NaN', 'NaN'),
 ('Sac Fly Double Play', 'sac_fly_double_play'),
 ('Double Play', 'double_play'),
 ('Caught Stealing 2B', 'caught_stealing_2b'),
 ('Grounded Into DP', 'grounded_into_double_play'),
 ('Single', 'single'),
 ('Stolen Base 2B', 'stolen_base_2b'),
 ('Fielders Choice Out', 'fielders_choice_out'),
 ('Field Error', 'field_error'),
 ('Strikeout Double Play', 'strikeout_double_play'),
 ('Groundout', 'field_out'),
 ('Bunt Lineout', 'field_out'),
 ('Sac Fly', 'sac_fly'),
 ('Caught Stealing 3B', 'caught_stealing_3b'),
 ('Fielders Choice', 'fielders_choice'),
 ('Catcher Interference', 'catcher_interf'),
 ('Caught Stealing Home', 'caught_stealing_home'),
 ('Intent Walk', 'intent_walk'),
 ('Flyout', 'field_out'),
 ('Pop Out', 'field_out'),
 ('Pickoff Caught Stealing Home', 'pickoff_caught_stealing_home'),
 ('Bunt Groundout', 'field_out'),
 ('Runner Out', 'other_out'),
 ('Lineout', 'field_out'),
 ('Sac Bunt', 'sac_bunt'),
 ('Double', 'double'),
 ('Home Run', 'home_run'),
 ('Forceout', 'force_out'),
 ('Triple', 'triple'),
 ('Walk', 'walk'),
 ('Hit By Pitch', 'hit_by_pitch'),
 ('Bunt Pop Out', 'field_out'),
 ('Strikeout', 'strikeout')]


# Duplicated event_type in pair with event
 In [32]: l = [g for g, _ in df.group_by(["event", "event_type"])]

In [33]: second = [a[1] for a in l]

In [34]: for a in l:
    ...:     if second.count(a[1]) > 1:
    ...:         print(a)
    ...:
('Groundout', 'field_out')
('Flyout', 'field_out')
('Lineout', 'field_out')
('Pop Out', 'field_out')
('Bunt Pop Out', 'field_out')
('Bunt Groundout', 'field_out')
('Bunt Lineout', 'field_out')


from MephistonOwl on Discord:
common_out_codes = {
    "Bunt Groundout": "B",
    "Bunt Pop Out": "B",
    "Flyout": "F",
    "Groundout": "G",
    "Lineout": "L",
    "Pop Out": "P",
    "Sac Bunt": "SAC",
    "Sac Fly": "SF",
}

double_play_codes = {
    "Grounded Into DP": "DP",
    "Double Play": "DP",
    "Strikeout Double Play": "KDP",
    "Triple Play": "TP",
    "Sac Fly Double Play": "SFDP",
}

reach_codes = {
    "strikeout": "K",
    "field_error": "E",
    "walk": "BB",
    "intent_walk": "IBB",
    "force_out": "FC",
    "fielders_choice": "FC",
    "fielders_choice_out": "FC",
    "hit_by_pitch": "HBP",
    "double_play": "DP",
    "catcher_interf": "CI",
}

base_stealing_codes = {
    "stolen_base_1b": "SB",
    "stolen_base_2b": "SB",
    "stolen_base_3b": "SB",
    "stolen_base_home": "SB",
    "pickoff_1b": "PO",
    "pickoff_2b": "PO",
    "pickoff_3b": "PO",
    "pickoff_home": "PO",
    "caught_stealing_1b": "CS",
    "caught_stealing_2b": "CS",
    "caught_stealing_3b": "CS",
    "caught_stealing_home": "CS",
    "pickoff_caught_stealing_2b": "POCS",
    "pickoff_caught_stealing_3b": "POCS",
    "pickoff_caught_stealing_home": "POCS",
    "defensive_indiff": "DI",
}

poor_pitches_codes = {
    "wild_pitch": "WP",
    "passed_ball": "PB",
    "balk": "BLK",
}

error_codes = {
    "error": "E",
    "field_error": "E",
    "pickoff_error_1b": "POE",
    "pickoff_error_2b": "POE",
    "pickoff_error_3b": "POE",
}

error_dropped_foul_ball_player_codes = {
    "by pitcher": 1,
    "by catcher": 2,
    "by first baseman": 3,
    "by secnd baseman": 4,
    "by third baseman": 5,
    "by shortstop": 6,
    "by left fielder": 7,
    "by center fielder": 8,
    "by right fielder": 9,
}
"""

chadwick_dtypes = {
    "GAME_ID": "object",
    "AWAY_TEAM_ID": "object",
    "INN_CT": "int64",
    "OUTS_CT": "int64",
    "BALLS_CT": "int64",
    "STRIKES_CT": "int64",
    "AWAY_SCORE_CT": "int64",
    "HOME_SCORE_CT": "int64",
    "RESP_BAT_ID": "object",
    "RESP_BAT_HAND_CD": "object",
    "RESP_PIT_ID": "object",
    "RESP_PIT_HAND_CD": "object",
    "BASE1_RUN_ID": "object",
    "BASE2_RUN_ID": "object",
    "BASE3_RUN_ID": "object",
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
    "RBI_CT": "int64",
    "WP_FL": "bool",
    "PB_FL": "bool",
    "BATTEDBALL_CD": "object",
    "BAT_DEST_ID": "int64",
    "RUN1_DEST_ID": "int64",
    "RUN2_DEST_ID": "int64",
    "RUN3_DEST_ID": "int64",
    "RUN1_SB_FL": "bool",
    "RUN2_SB_FL": "bool",
    "RUN3_SB_FL": "bool",
    "RUN1_CS_FL": "bool",
    "RUN2_CS_FL": "bool",
    "RUN3_CS_FL": "bool",
    "RUN1_PK_FL": "bool",
    "RUN2_PK_FL": "bool",
    "RUN3_PK_FL": "bool",
    "RUN1_RESP_PIT_ID": "object",
    "RUN2_RESP_PIT_ID": "object",
    "RUN3_RESP_PIT_ID": "object",
    "HOME_TEAM_ID": "object",
    "BAT_TEAM_ID": "object",
    "FLD_TEAM_ID": "object",
    "PA_TRUNC_FL": "bool",
    "START_BASES_CD": "int64",
    "END_BASES_CD": "int64",
    "PIT_START_FL": "bool",
    "RESP_PIT_START_FL": "bool",
    "PA_BALL_CT": "int64",
    "PA_OTHER_BALL_CT": "int64",
    "PA_STRIKE_CT": "int64",
    "PA_OTHER_STRIKE_CT": "int64",
    "EVENT_RUNS_CT": "int64",
    "BAT_SAFE_ERR_FL": "bool",
    "FATE_RUNS_CT": "int64",
    "RESP_BAT_START_FL": "bool",
}

chadwick_defaults = {
    "GAME_ID": None,
    "AWAY_TEAM_ID": None,
    "INN_CT": None,
    "OUTS_CT": None,
    "BALLS_CT": None,
    "STRIKES_CT": None,
    "AWAY_SCORE_CT": None,
    "HOME_SCORE_CT": None,
    "RESP_BAT_ID": None,
    "RESP_BAT_HAND_CD": None,
    "RESP_PIT_ID": None,
    "RESP_PIT_HAND_CD": None,
    "BASE1_RUN_ID": pd.NA,
    "BASE2_RUN_ID": pd.NA,
    "BASE3_RUN_ID": pd.NA,
    "BAT_FLD_CD": None,
    "BAT_LINEUP_ID": None,
    "EVENT_CD": None,
    "AB_FL": False,
    "H_CD": 0,
    "SH_FL": False,
    "SF_FL": False,
    "EVENT_OUTS_CT": 0,
    "DP_FL": False,
    "TP_FL": False,
    "RBI_CT": 0,
    "WP_FL": False,
    "PB_FL": False,
    "BATTEDBALL_CD": None,
    "BAT_DEST_ID": 0,
    "RUN1_DEST_ID": 0,
    "RUN2_DEST_ID": 0,
    "RUN3_DEST_ID": 0,
    "RUN1_SB_FL": False,
    "RUN2_SB_FL": False,
    "RUN3_SB_FL": False,
    "RUN1_CS_FL": False,
    "RUN2_CS_FL": False,
    "RUN3_CS_FL": False,
    "RUN1_PK_FL": False,
    "RUN2_PK_FL": False,
    "RUN3_PK_FL": False,
    "RUN1_RESP_PIT_ID": pd.NA,
    "RUN2_RESP_PIT_ID": pd.NA,
    "RUN3_RESP_PIT_ID": pd.NA,
    "HOME_TEAM_ID": None,
    "BAT_TEAM_ID": None,
    "FLD_TEAM_ID": None,
    "PA_TRUNC_FL": False,
    "START_BASES_CD": 0,
    "END_BASES_CD": 0,
    "PIT_START_FL": False,
    "RESP_PIT_START_FL": False,
    "PA_BALL_CT": 0,
    "PA_OTHER_BALL_CT": 0,
    "PA_STRIKE_CT": 0,
    "PA_OTHER_STRIKE_CT": 0,
    "EVENT_RUNS_CT": 0,
    "BAT_SAFE_ERR_FL": False,
    "FATE_RUNS_CT": 0,
    "RESP_BAT_START_FL": False,
}


class ParsePlateAppearance:
    def __init__(self, plate_appearance: dict, game_id: str, away_team: str, home_team: str, starting_lineup_away: list[str], starting_lineup_home: list[str], away_starting_pitcher: str, home_starting_pitcher: str, convert_id: ConvertMLBAM, runners: list[str | None], resp_pitchers: list[str | None], top_level_pa: bool = True) -> None:  # type: ignore
        self.plate_appearance = plate_appearance  # type: ignore
        self.game_id = game_id
        self.away_team = away_team
        self.home_team = home_team
        self.starting_lineup_away = starting_lineup_away
        self.starting_lineup_home = starting_lineup_home
        self.away_starting_pitcher = away_starting_pitcher
        self.home_starting_pitcher = home_starting_pitcher
        self.convert_id = convert_id
        self.df = pd.DataFrame(columns=list(chadwick_dtypes.keys()))  # type: ignore
        self.df = self.df.astype(chadwick_dtypes)  # type: ignore
        self.runners = runners
        self.resp_pitchers = resp_pitchers
        self.top_level_pa = top_level_pa

    def parse(self) -> None:
        event_type_to_cwevent = {
            "pickoff_1b": 8,
            "pickoff_2b": 8,
            "pickoff_3b": 8,
            "pitcher_step_off": 100,
            "pickoff_error_1b": 8,
            "pickoff_error_2b": 8,
            "pickoff_error_3b": 8,
            "batter_timeout": 100,
            "mound_visit": 100,
            "no_pitch": 100,
            "single": 20,
            "double": 21,
            "triple": 22,
            "home_run": 23,
            # 'double_play': 11,             # NOTE: Need special case for this. Pretty sure it's always in play, fielded, out. Seems to be a mix of fielder's choice double play and line into double play and fly into double play
            "field_error": 18,
            "error": 18,
            "field_out": 2,
            "fielders_choice": 19,
            "fielders_choice_out": 19,
            "force_out": 2,
            "grounded_into_double_play": 2,
            "grounded_into_triple_play": 2,
            "strikeout": 3,
            "strike_out": 3,
            "strikeout_double_play": 3,
            "strikeout_triple_play": 3,
            # 'triple_play': 11,            # NOTE: Similar to double_play. Not sure how to identify a fielder's choice with this...
            # 'sac_fly': 11,                # NOTE: Can be either out or error. If not is_out, then it's an error (18). Otherwise an out (2)
            "catcher_interf": 17,
            "batter_interference": 2,  # Not in my sample. Assume it's an out. But cwevent doesn't document interference either (and interference (17) is always CI)
            "fielder_interference": 12,  # Not in my sample. Assume it's some advance. But cwevent doesn't document interference either (and interference (17) is always CI)
            "runner_interference": 12,  # Not in my sample. Assume it's some out. But cwevent doesn't document interference either (and interference (17) is always CI)
            "fan_interference": 2,  # Not in my sample. Assume it's some out (surely if it were a homer converted to a hit, it would be eg double). But cwevent doesn't document interference either (and interference (17) is always CI)
            "batter_turn": 100,  # NOTE: probably doesnt matter
            "ejection": 100,
            "cs_double_play": 6,
            "defensive_indiff": 5,
            "sac_fly_double_play": 2,
            # 'sac_bunt': 2,                # NOTE: Can be either out or error. If not is_out, then it's an error (18). Otherwise an out (2)
            "sac_bunt_double_play": 2,
            "walk": 14,
            "intent_walk": 15,
            "hit_by_pitch": 16,
            "injury": 100,
            "os_ruling_pending_prior": 100,
            "os_ruling_pending_primary": 100,
            "at_bat_start": 100,
            "passed_ball": 10,
            "other_advance": 12,  # I think? But not used in my 1000 games from 2022
            "runner_double_play": 12,  # Not sure. Not used in my sample I checked
            "runner_placed": 100,  # Manfred runner!
            "pitching_substitution": 100,
            "offensive_substitution": 100,
            "defensive_switch": 100,
            "umpire_substitution": 100,
            "pitcher_switch": 100,
            "game_advisory": 100,
            "stolen_base": 4,
            "stolen_base_2b": 4,
            "stolen_base_3b": 4,
            "stolen_base_home": 4,
            "caught_stealing": 6,
            "caught_stealing_2b": 6,
            "caught_stealing_3b": 6,
            "caught_stealing_home": 6,
            "defensive_substitution": 100,
            "pickoff_caught_stealing_2b": 6,
            "pickoff_caught_stealing_3b": 6,
            "pickoff_caught_stealing_home": 6,
            "balk": 11,
            "forced_balk": 11,
            "wild_pitch": 9,
            "other_out": 12,
        }

        movement_indices = set()
        for runner_event in self.plate_appearance["runners"]:
            movement_indices.add(runner_event["details"]["playIndex"])

        for movement_index in movement_indices:
            if movement_index == len(self.plate_appearance["playEvents"]) - 1:
                continue
            modified_pa = deepcopy(self.plate_appearance)
            # Only include playEvents up to the runner event
            modified_pa["playEvents"] = modified_pa["playEvents"][: movement_index + 1]
            modified_pa["runners"] = list(
                filter(lambda x: x["details"]["playIndex"] == movement_index, modified_pa["runners"])
            )
            sub_pa = ParsePlateAppearance(
                modified_pa,
                self.game_id,
                self.away_team,
                self.home_team,
                self.starting_lineup_away,
                self.starting_lineup_home,
                self.away_starting_pitcher,
                self.home_starting_pitcher,
                self.convert_id,
                self.runners,
                self.resp_pitchers,
                top_level_pa=False,
            )
            sub_pa.parse()
            self.df = pd.concat([self.df, sub_pa.df], ignore_index=True)
        self.plate_appearance["runners"] = list(
            filter(
                lambda x: x["details"]["playIndex"] == len(self.plate_appearance["playEvents"]) - 1,
                self.plate_appearance["runners"],
            )
        )

        # Merge result into last entry of playEvents
        self.plate_appearance["playEvents"][-1]["details"] = (
            self.plate_appearance["playEvents"][-1]["details"] | self.plate_appearance["result"]
        )

        row: dict[str, None | str | float | int | bool] = {
            col: chadwick_defaults[col] for col in chadwick_dtypes.keys()
        }
        row["GAME_ID"] = self.game_id
        row["AWAY_TEAM_ID"] = self.away_team
        row["HOME_TEAM_ID"] = self.home_team
        if self.plate_appearance["about"]["isTopInning"]:  # type: ignore
            row["BAT_TEAM_ID"] = self.away_team
            row["FLD_TEAM_ID"] = self.home_team
        else:
            row["BAT_TEAM_ID"] = self.home_team
            row["FLD_TEAM_ID"] = self.away_team
        row["INN_CT"] = self.plate_appearance["about"]["inning"]  # type: ignore
        row["OUTS_CT"] = self.plate_appearance["playEvents"][-1]["count"]["outs"]

        # This count object adds 1 to balls and strikes if the last event was a ball or strike. We don't want this
        if not self.plate_appearance["playEvents"][-1].get("isBaseRunningPlay", False):
            row["BALLS_CT"] = (
                self.plate_appearance["playEvents"][-1]["count"]["balls"]
                - self.plate_appearance["playEvents"][-1]["details"]["isBall"]
            )
            row["STRIKES_CT"] = (
                self.plate_appearance["playEvents"][-1]["count"]["strikes"]
                - self.plate_appearance["playEvents"][-1]["details"]["isStrike"]
            )
        else:  # When it's a running event, the last pitch is the second to last event
            row["BALLS_CT"] = (
                self.plate_appearance["playEvents"][-1]["count"]["balls"]
                - self.plate_appearance["playEvents"][-2]["details"]["isBall"]
            )
            row["STRIKES_CT"] = (
                self.plate_appearance["playEvents"][-1]["count"]["strikes"]
                - self.plate_appearance["playEvents"][-2]["details"]["isStrike"]
            )

        # Get the batter and pitcher
        # TODO: Implement substitutions
        # TODO: Implement BAT_LINEUP_ID, BAT_FLD_CD, BATTEDBALL_CD, AWAY_SCORE_CT, HOME_SCORE_CT, PIT_START_FL, RESP_PIT_START_FL, FATE_RUNS_CT, RESP_BAT_START_FL
        row["RESP_BAT_ID"] = self.convert_id.mlbam_to_retro(self.plate_appearance["matchup"]["batter"]["id"])  # type: ignore
        row["RESP_BAT_HAND_CD"] = self.plate_appearance["matchup"]["batSide"]["code"]  # type: ignore
        row["RESP_PIT_ID"] = self.convert_id.mlbam_to_retro(self.plate_appearance["matchup"]["pitcher"]["id"])  # type: ignore
        row["RESP_PIT_HAND_CD"] = self.plate_appearance["matchup"]["pitchHand"]["code"]  # type: ignore

        # Update BASE_RUN_IDs for runners
        row["START_BASES_CD"] = 0
        for i, runner in enumerate(self.runners):
            if runner == None:
                continue
            row[f"BASE{i+1}_RUN_ID"] = runner
            row[f"RUN{i+1}_RESP_PIT_ID"] = self.resp_pitchers[i]
            row[f"START_BASES_CD"] += 2**i
        # Calculate runs scored on play and other baserunning events
        runs_scored = 0
        rbis = 0
        for runner in self.plate_appearance["runners"]:  # type: ignore
            start_base = runner["movement"]["originBase"]  # type: ignore
            end_base = runner["movement"]["end"]  # type: ignore
            try:
                end_base = int(end_base[0])  # type: ignore
            except:
                pass
            if runner["details"]["isScoringEvent"]:
                runs_scored += 1
                end_base = 4
                if runner["details"]["earned"] and runner["details"]["teamUnearned"]:
                    end_base = 6
                elif not runner["details"]["earned"]:
                    end_base = 5
                if runner["details"]["rbi"]:
                    rbis += 1
            if runner["movement"]["isOut"]:
                end_base = 0
            if start_base in ["1B", "2B", "3B"]:
                start_base = int(start_base[0])  # type: ignore
                row[f"RUN{start_base}_DEST_ID"] = end_base
                row[f"RUN{start_base}_SB_FL"] = False
                row[f"RUN{start_base}_CS_FL"] = False
                row[f"RUN{start_base}_PK_FL"] = False
                if runner["details"]["eventType"].startswith("pickoff_caught_stealing") or runner["details"]["eventType"].startswith("caught_stealing"):  # type: ignore
                    row[f"RUN{start_base}_CS_FL"] = True
                elif runner["details"]["eventType"].startswith("pickoff"):  # type: ignore
                    row[f"RUN{start_base}_PK_FL"] = True
                elif runner["details"]["eventType"].startswith("stolen_base"):  # type: ignore
                    row[f"RUN{start_base}_SB_FL"] = True
                elif runner["details"]["eventType"] == "wild_pitch":
                    row["WP_FL"] = True
                elif runner["details"]["eventType"] == "passed_ball":
                    row["PB_FL"] = True
            elif start_base == None:
                row["BAT_DEST_ID"] = end_base
            else:
                print(self.plate_appearance)  # type: ignore
                print(start_base)  # type: ignore
            if end_base in [1, 2, 3]:
                pid = self.convert_id.mlbam_to_retro(runner["details"]["runner"]["id"])  # type: ignore

                # It's possible that this runner has already been removed from runners (eg if they were on second and a previous runner advanced to second)
                # So don't remove them if they're not there
                if pid in self.runners:
                    idx = self.runners.index(pid)
                    self.runners[idx] = None
                    self.resp_pitchers[idx] = None
                self.runners[end_base - 1] = pid
                if runner["details"]["responsiblePitcher"] == None:
                    self.resp_pitchers[end_base - 1] = row["RESP_PIT_ID"]
                else:
                    self.resp_pitchers[end_base - 1] = self.convert_id.mlbam_to_retro(runner["details"]["responsiblePitcher"]["id"])  # type: ignore
            elif end_base >= 4:
                pid = self.convert_id.mlbam_to_retro(runner["details"]["runner"]["id"])  # type: ignore
                if pid in self.runners:
                    idx = self.runners.index(pid)
                    self.resp_pitchers[idx] = None
                    self.runners[idx] = None
            else:
                pid = self.convert_id.mlbam_to_retro(runner["details"]["runner"]["id"])  # type: ignore
                if pid in self.runners and start_base != None:
                    self.runners[start_base - 1] = None
                    self.resp_pitchers[start_base - 1] = None

        row["EVENT_RUNS_CT"] = runs_scored
        row["RBI_CT"] = rbis
        row["END_BASES_CD"] = 0
        for i, runner in enumerate(self.runners):
            if runner == None:
                continue
            row["END_BASES_CD"] += 2**i

        ## Process event type
        event_type = self.plate_appearance["playEvents"][-1]["details"]["eventType"]

        # eventTypes documentation from https://statsapi.mlb.com/api/v1/eventTypes
        event_types_list = json.loads(open(Path(__file__).parent / "eventTypes.json").read())
        eventTypes = {event["code"]: event for event in event_types_list}

        # Special cases for sac_bunt and sac_fly
        if event_type == "sac_bunt":
            row["SH_FL"] = True
            if self.plate_appearance["playEvents"][-1]["details"]["isOut"]:
                event_code = 2  # out
            else:
                event_code = 18  # error
        elif event_type == "sac_fly":
            row["SF_FL"] = True
            event_type = "field_out"
            if self.plate_appearance["playEvents"][-1]["details"]["isOut"]:
                event_code = 2  # out
            else:
                event_code = 18  # error
        elif event_type == "double_play" or event_type == "triple_play":
            # Check if it's a fielder's choice. Note: very janky but it works and it doesn't _really_ matter if it's perfect
            if "fielder's choice" in self.plate_appearance["playEvents"][-1]["details"]["description"]:
                event_code = 19
            else:
                event_code = 2
        else:
            # Get the event code
            event_code = event_type_to_cwevent.get(event_type, 100)
            if event_code == 100:
                print(event_type)
                raise ValueError("Unknown event type")
        row["EVENT_CD"] = event_code

        # Check if the event is an at bat (PA but not catcher's interference or sacrifice)
        if (
            eventTypes[event_type]["plateAppearance"]
            and not row["SF_FL"]
            and not row["SH_FL"]
            and not event_type == "catcher_interf"
        ):
            row["AB_FL"] = True

        # If this is the top level PA (ie not a sub PA), and the event is not a plate appearance, set PA_TRUNC_FL
        if self.top_level_pa and not eventTypes[event_type]["plateAppearance"]:
            row["PA_TRUNC_FL"] = True

        # Set H_CD
        if event_code in [20, 21, 22, 23]:
            # Single (20 - 19 = 1), Double (21 - 19 = 2), Triple (22 - 19 = 3), Home Run (23 - 19 = 4)
            row["H_CD"] = event_code - 19

        # Set DP_FL and TP_FL
        if event_type == "double_play":
            row["DP_FL"] = True
        elif event_type == "triple_play":
            row["TP_FL"] = True

        # Outs
        row["EVENT_OUTS_CT"] = self.plate_appearance["count"]["outs"] - row["OUTS_CT"]

        # Balls
        if self.top_level_pa:
            row["PA_BALL_CT"] = self.plate_appearance["count"]["balls"]
        else:
            # This should always work
            row["PA_BALL_CT"] = self.plate_appearance["playEvents"][-1]["count"]["balls"]

        # Strikes
        if self.top_level_pa:
            row["PA_STRIKE_CT"] = self.plate_appearance["count"]["strikes"]
        else:
            # This should always work
            row["PA_STRIKE_CT"] = self.plate_appearance["playEvents"][-1]["count"]["strikes"]

        # Count auto balls and strikes
        row["PA_OTHER_BALL_CT"] = 0
        row["PA_OTHER_STRIKE_CT"] = 0
        for pitch_idx in self.plate_appearance["pitchIndex"]:
            if pitch_idx >= len(self.plate_appearance["playEvents"]):
                continue
            pitch = self.plate_appearance["playEvents"][pitch_idx]
            if not pitch["isPitch"] and pitch["details"].get("isBall", False):
                row["PA_OTHER_BALL_CT"] += 1
            elif not pitch["isPitch"] and pitch["details"].get("isStrike", False):
                row["PA_OTHER_STRIKE_CT"] += 1

        self.df = pd.concat([self.df, pd.DataFrame([row])], ignore_index=True)
