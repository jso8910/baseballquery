import pandas as pd
from convert_mlbam import ConvertMLBAM
from copy import deepcopy
import json
from pathlib import Path
import requests

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
    "RESP_PIT_START_FL": "bool",
    "PA_BALL_CT": "int64",
    "PA_OTHER_BALL_CT": "int64",
    "PA_STRIKE_CT": "int64",
    "PA_OTHER_STRIKE_CT": "int64",
    "EVENT_RUNS_CT": "int64",
    "BAT_SAFE_ERR_FL": "bool",
    "FATE_RUNS_CT": "int64",
    "RESP_BAT_START_FL": "bool",
    "MLB_STATSAPI_APPROX": "bool",
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
    "RESP_PIT_START_FL": False,
    "PA_BALL_CT": 0,
    "PA_OTHER_BALL_CT": 0,
    "PA_STRIKE_CT": 0,
    "PA_OTHER_STRIKE_CT": 0,
    "EVENT_RUNS_CT": 0,
    "BAT_SAFE_ERR_FL": False,
    "FATE_RUNS_CT": 0,
    "RESP_BAT_START_FL": False,
    "MLB_STATSAPI_APPROX": True,
}


class ParsePlateAppearance:
    def __init__(self, plate_appearance: dict, prev_game_plays: dict, game_id: str, away_team: str, home_team: str, starting_lineup_away: list[str], starting_lineup_home: list[str], positions: dict[int, int], away_starting_pitcher: str, home_starting_pitcher: str, away_pitcher: list[str], home_pitcher: list[str], away_score: int, home_score: int, convert_id: ConvertMLBAM, runners: list[str | None], resp_pitchers: list[str | None], top_level_pa: bool = True) -> None:
        self.plate_appearance = plate_appearance
        self.prev_game_plays = prev_game_plays
        self.game_id = game_id
        self.away_team = away_team
        self.home_team = home_team
        self.starting_lineup_away = starting_lineup_away
        self.starting_lineup_home = starting_lineup_home
        self.away_pitcher = away_pitcher
        self.home_pitcher = home_pitcher
        self.away_score = away_score
        self.home_score = home_score
        self.positions = positions
        self.away_starting_pitcher = away_starting_pitcher
        self.home_starting_pitcher = home_starting_pitcher
        self.convert_id = convert_id
        self.df = pd.DataFrame(columns=list(chadwick_dtypes.keys()))
        self.df = self.df.astype(chadwick_dtypes)
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
            "field_error": 2,
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
            "pickoff_caught_stealing_2b": 8,
            "pickoff_caught_stealing_3b": 8,
            "pickoff_caught_stealing_home": 8,
            "balk": 11,
            "forced_balk": 11,
            "wild_pitch": 9,
            "other_out": 12,
        }

        movement_indices = set()
        for runner_event in self.plate_appearance["runners"]:
            movement_indices.add(runner_event["details"]["playIndex"])

        # When ParsePlateAppearance is run, pinch runner subs will be processed
        latest_runner_subs_processed = -1
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
                self.prev_game_plays,
                self.game_id,
                self.away_team,
                self.home_team,
                self.starting_lineup_away,
                self.starting_lineup_home,
                self.positions,
                self.away_starting_pitcher,
                self.home_starting_pitcher,
                self.away_pitcher,
                self.home_pitcher,
                self.away_score,
                self.home_score,
                self.convert_id,
                self.runners,
                self.resp_pitchers,
                top_level_pa=False,
            )
            latest_runner_subs_processed = movement_index
            sub_pa.parse()
            self.df = pd.concat([self.df, sub_pa.df], ignore_index=True)
        self.plate_appearance["runners"] = list(
            filter(
                lambda x: x["details"]["playIndex"] == len(self.plate_appearance["playEvents"]) - 1,
                self.plate_appearance["runners"],
            )
        )

        # Merge result into last entry of playEvents if this is the actual PA result
        if self.top_level_pa:
            self.plate_appearance["playEvents"][-1]["details"] = (
                self.plate_appearance["playEvents"][-1]["details"] | self.plate_appearance["result"]
            )

        row: dict[str, None | str | float | int | bool] = {
            col: chadwick_defaults[col] for col in chadwick_dtypes.keys()
        }
        row["GAME_ID"] = self.game_id
        row["AWAY_TEAM_ID"] = self.away_team
        row["HOME_TEAM_ID"] = self.home_team
        if self.plate_appearance["about"]["isTopInning"]:
            row["BAT_TEAM_ID"] = self.away_team
            row["FLD_TEAM_ID"] = self.home_team
        else:
            row["BAT_TEAM_ID"] = self.home_team
            row["FLD_TEAM_ID"] = self.away_team
        row["INN_CT"] = self.plate_appearance["about"]["inning"]
        row["OUTS_CT"] = self.plate_appearance["playEvents"][-1]["count"]["outs"]

        # The previous event thing should be the count before the event
        if not self.plate_appearance["playEvents"][-1].get("isBaseRunningPlay", False) and not self.plate_appearance["playEvents"][-1]["type"] == "pickoff":
            if len(self.plate_appearance["playEvents"]) > 1:
                row["BALLS_CT"] = self.plate_appearance["playEvents"][-2]["count"]["balls"]
                row["STRIKES_CT"] = self.plate_appearance["playEvents"][-2]["count"]["strikes"]
            else:
                row["BALLS_CT"] = 0
                row["STRIKES_CT"] = 0
        else:
            if len(self.plate_appearance["playEvents"]) > 1:
                row["OUTS_CT"] = self.plate_appearance["playEvents"][-2]["count"]["outs"]
            else:
                row["OUTS_CT"] = self.plate_appearance["playEvents"][-1]["count"]["outs"]
            # If it's a baserunning play (or pickoff), the last one will be the baserunning event, the second to last will be the pitch
            # and the third to last will be the previous count
            if len(self.plate_appearance["playEvents"]) > 2:
                row["BALLS_CT"] = self.plate_appearance["playEvents"][-3]["count"]["balls"]
                row["STRIKES_CT"] = self.plate_appearance["playEvents"][-3]["count"]["strikes"]
            else:
                row["BALLS_CT"] = 0
                row["STRIKES_CT"] = 0

        # Get the batter and pitcher
        # TODO: Implement substitutions
        # TODO: Implement BAT_LINEUP_ID, AWAY_SCORE_CT, HOME_SCORE_CT, RESP_PIT_START_FL, FATE_RUNS_CT, RESP_BAT_START_FL
        row["RESP_BAT_ID"] = self.convert_id.mlbam_to_retro(self.plate_appearance["matchup"]["batter"]["id"])
        row["RESP_BAT_HAND_CD"] = self.plate_appearance["matchup"]["batSide"]["code"]
        row["RESP_PIT_ID"] = self.convert_id.mlbam_to_retro(self.plate_appearance["matchup"]["pitcher"]["id"])
        row["RESP_PIT_HAND_CD"] = self.plate_appearance["matchup"]["pitchHand"]["code"]
        if self.plate_appearance["about"]["isTopInning"]:
            self.away_pitcher = [row["RESP_PIT_ID"], row["RESP_PIT_HAND_CD"]]
        else:
            self.home_pitcher = [row["RESP_PIT_ID"], row["RESP_PIT_HAND_CD"]]

        # Process substitutions and runner placement in extra innings
        for event_idx in self.plate_appearance["actionIndex"]:
            if event_idx >= len(self.plate_appearance["playEvents"]):
                continue
            event = self.plate_appearance["playEvents"][event_idx]
            if event["details"]["eventType"] == "runner_placed":
                base = event["base"]
                player = self.convert_id.mlbam_to_retro(event["player"]["id"])
                self.runners[base - 1] = player
                self.resp_pitchers[base - 1] = row["RESP_PIT_ID"]
            if not event.get("isSubstitution", False):
                continue
            if event["position"]["abbreviation"] == "PR":
                if event["index"] <= latest_runner_subs_processed:
                    continue
                old_code = self.convert_id.mlbam_to_retro(event["replacedPlayer"]["id"])
                new_code = self.convert_id.mlbam_to_retro(event["player"]["id"])
                self.runners[self.runners.index(old_code)] = new_code
            elif event["position"]["abbreviation"] == "PH":
                new_code = self.convert_id.mlbam_to_retro(event["player"]["id"])
                old_code = self.convert_id.mlbam_to_retro(event["replacedPlayer"]["id"])
                # If the strikeout should be charged to the old hitter
                if event["count"]["strikes"] == 2 and self.plate_appearance["playEvents"][-1]["count"]["strikes"] == 3:
                    row["RESP_BAT_ID"] = old_code
                    # We need to make another request to get the batter handedness
                    bat_old = requests.get(f"https://statsapi.mlb.com{event['replacedPlayer']['link']}").json()
                    row["RESP_BAT_HAND_CD"] = bat_old["people"][0]["batSide"]["code"]

                    # Sadly we can't get for certain which hand the player batted with, so in this very rare circumstance
                    # where the strikeout is charged to the old hitter, we assume
                    if row["RESP_BAT_HAND_CD"] == "S":
                        row["RESP_BAT_HAND_CD"] = "L"
                    continue
                # Otherwise, the play should actually already have the correct ID
            elif event["position"]["abbreviation"] == "P":
                new_code = self.convert_id.mlbam_to_retro(event["player"]["id"])
                count = (event["count"]["balls"], event["count"]["strikes"])
                # Rule 10.17(g)(1)
                charged_to_reliever = count in ((3,0), (3,1), (3,2), (2,1), (2,0))
                if charged_to_reliever and self.plate_appearance["playEvents"][-1]["count"]["balls"] == 4:
                    row["RESP_PIT_ID"] = self.away_pitcher[0] if self.plate_appearance["about"]["isTopInning"] else self.home_pitcher[0]
                    row["RESP_PIT_HAND_ID"] = self.away_pitcher[1] if self.plate_appearance["about"]["isTopInning"] else self.home_pitcher[1]
                # Should already be correct!
                if self.plate_appearance["about"]["isTopInning"]:
                    self.away_pitcher = [new_code, self.plate_appearance["matchup"]["pitchHand"]["code"]]
                else:
                    self.home_pitcher = [new_code, self.plate_appearance["matchup"]["pitchHand"]["code"]]
            elif event["details"]["eventType"] in ("defensive_substitution", "defensive_switch"):
                self.positions[event["player"]["id"]] = int(event["position"]["code"])

        # If they were a pinch runner, they're now 0 (this means they batted around)
        if self.positions[self.plate_appearance["matchup"]["batter"]["id"]] == 12:
            self.positions[self.plate_appearance["matchup"]["batter"]["id"]] = 0
        # This is so dumb. Why is there not a field for RESP_BAT_FLD_CD I hate this so much.
        row["BAT_FLD_CD"] = self.positions[self.plate_appearance["matchup"]["batter"]["id"]]

        # In case they bat around, the pinch hitter should no longer be 11
        if self.top_level_pa and self.positions[self.plate_appearance["matchup"]["batter"]["id"]] == 11:
            self.positions[self.plate_appearance["matchup"]["batter"]["id"]] = 0

        # Check whether batter and hitter are starter
        if row["RESP_BAT_ID"] in self.starting_lineup_away or row["RESP_BAT_ID"] in self.starting_lineup_home:
            row["RESP_BAT_START_FL"] = True
        if row["RESP_PIT_ID"] in self.starting_lineup_away or row["RESP_PIT_ID"] in self.starting_lineup_home:
            row["RESP_PIT_START_FL"] = True

        # Update BASE_RUN_IDs for runners
        row["START_BASES_CD"] = 0
        for i, runner in enumerate(self.runners):
            if runner == None:
                continue
            row[f"BASE{i+1}_RUN_ID"] = runner
            row[f"RUN{i+1}_RESP_PIT_ID"] = self.resp_pitchers[i]
            row[f"START_BASES_CD"] += 2**i

        # We don't want any baserunning events from the same origin and different destinations
        origin_bases = set()
        for idx in reversed(range(len(self.plate_appearance["runners"]))):
            if self.plate_appearance["runners"][idx]["movement"]["originBase"] in origin_bases:
                self.plate_appearance["runners"].pop(idx)
            origin_bases.add(self.plate_appearance["runners"][idx]["movement"]["originBase"])

        # Calculate runs scored on play and other baserunning events
        runs_scored = 0
        rbis = 0
        for runner in self.plate_appearance["runners"]:
            start_base = runner["movement"]["originBase"]
            end_base = runner["movement"]["end"]
            try:
                end_base = int(end_base[0])
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
                start_base = int(start_base[0])
                row[f"RUN{start_base}_DEST_ID"] = end_base
                row[f"RUN{start_base}_SB_FL"] = False
                row[f"RUN{start_base}_CS_FL"] = False
                row[f"RUN{start_base}_PK_FL"] = False
                if runner["details"]["eventType"].startswith("pickoff_caught_stealing") or runner["details"]["eventType"].startswith("caught_stealing"):
                    row[f"RUN{start_base}_CS_FL"] = True
                elif runner["details"]["eventType"].startswith("pickoff"):
                    row[f"RUN{start_base}_PK_FL"] = True
                elif runner["details"]["eventType"].startswith("stolen_base"):
                    row[f"RUN{start_base}_SB_FL"] = True
                elif runner["details"]["eventType"] == "wild_pitch":
                    row["WP_FL"] = True
                elif runner["details"]["eventType"] == "passed_ball":
                    row["PB_FL"] = True
            elif start_base == None:
                row["BAT_DEST_ID"] = end_base
                if runner["details"]["eventType"] in ("field_error", "error"):
                    row["BAT_SAFE_ERR_FL"] = True
            else:
                print(self.plate_appearance)
                print(start_base)
            if end_base in [1, 2, 3]:
                pid = self.convert_id.mlbam_to_retro(runner["details"]["runner"]["id"])

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
                    self.resp_pitchers[end_base - 1] = self.convert_id.mlbam_to_retro(runner["details"]["responsiblePitcher"]["id"])
            elif end_base >= 4:
                pid = self.convert_id.mlbam_to_retro(runner["details"]["runner"]["id"])
                if pid in self.runners:
                    idx = self.runners.index(pid)
                    self.resp_pitchers[idx] = None
                    self.runners[idx] = None
            else:
                pid = self.convert_id.mlbam_to_retro(runner["details"]["runner"]["id"])
                if pid in self.runners and start_base != None:
                    self.runners[start_base - 1] = None
                    self.resp_pitchers[start_base - 1] = None

        row["EVENT_RUNS_CT"] = runs_scored
        if self.plate_appearance["about"]["isTopInning"]:
            row["AWAY_SCORE_CT"] = self.away_score + self.df["EVENT_RUNS_CT"].sum()
            row["HOME_SCORE_CT"] = self.home_score
        else:
            row["AWAY_SCORE_CT"] = self.away_score
            row["HOME_SCORE_CT"] = self.home_score + self.df["EVENT_RUNS_CT"].sum()
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

        # Batted ball type
        if self.plate_appearance["playEvents"][-1].get("hitData"):
            batted_ball = self.plate_appearance["playEvents"][-1]["hitData"]["trajectory"]
            if batted_ball == "popup":
                row["BATTEDBALL_CD"] = "P"
            elif batted_ball == "line_drive":
                row["BATTEDBALL_CD"] = "L"
            elif batted_ball == "ground_ball":
                row["BATTEDBALL_CD"] = "G"
            elif batted_ball == "fly_ball":
                row["BATTEDBALL_CD"] = "F"

        self.df = pd.concat([self.df, pd.DataFrame([row])], ignore_index=True)
