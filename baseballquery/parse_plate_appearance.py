import pandas as pd
from .convert_mlbam import ConvertMLBAM
from copy import deepcopy
import requests
from collections import defaultdict
from .chadwick_cols import chadwick_dtypes, chadwick_defaults
import msgspec.json as mjson

class ParsePlateAppearance:
    # Class level cache
    _player_cache = {}
    _event_type_to_cwevent = {
            "pickoff_1b": 8,
            "pickoff_2b": 8,
            "pickoff_3b": 8,
            "pitcher_step_off": 99,
            "pickoff_error_1b": 8,
            "pickoff_error_2b": 8,
            "pickoff_error_3b": 8,
            "batter_timeout": 99,
            "mound_visit": 99,
            "no_pitch": 99,
            "single": 20,
            "double": 21,
            "triple": 22,
            "home_run": 23,
            # 'double_play': 11,             # NOTE: Need special case for this. Pretty sure it's always in play, fielded, out. Seems to be a mix of fielder's choice double play and line into double play and fly into double play
            "field_error": 18,
            "error": 12,
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
            "batter_turn": 99,  # NOTE: probably doesnt matter
            "ejection": 99,
            "cs_double_play": 6,
            "defensive_indiff": 5,
            "sac_fly_double_play": 2,
            # 'sac_bunt': 2,                # NOTE: Can be either out or error. If not is_out, then it's an error (18). Otherwise an out (2)
            "sac_bunt_double_play": 2,
            "walk": 14,
            "intent_walk": 15,
            "hit_by_pitch": 16,
            "injury": 99,
            "os_ruling_pending_prior": 99,
            "os_ruling_pending_primary": 99,
            "at_bat_start": 99,
            "passed_ball": 10,
            "other_advance": 12,  # I think? But not used in my 1000 games from 2022
            "runner_double_play": 12,  # Not sure. Not used in my sample I checked
            "runner_placed": 99,  # Manfred runner!
            "pitching_substitution": 99,
            "offensive_substitution": 99,
            "defensive_switch": 99,
            "umpire_substitution": 99,
            "pitcher_switch": 99,
            "game_advisory": 99,
            "stolen_base": 4,
            "stolen_base_2b": 4,
            "stolen_base_3b": 4,
            "stolen_base_home": 4,
            "caught_stealing": 6,
            "caught_stealing_2b": 6,
            "caught_stealing_3b": 6,
            "caught_stealing_home": 6,
            "defensive_substitution": 99,
            "pickoff_caught_stealing_2b": 8,
            "pickoff_caught_stealing_3b": 8,
            "pickoff_caught_stealing_home": 8,
            "balk": 11,
            "forced_balk": 11,
            "wild_pitch": 9,
            "other_out": 12,
            "foul_error": 13,
        }
    def __init__(
        self,
        plate_appearance: dict,
        prev_game_plays: dict,
        game_id: str,
        away_team: str,
        home_team: str,
        starting_lineup_away: dict[int, str],
        starting_lineup_home: dict[int, str],
        positions: dict[int, int],
        player_lineup_spots: dict[str, int],
        away_starting_pitcher: str,
        home_starting_pitcher: str,
        away_pitcher: list[str],
        home_pitcher: list[str],
        away_score: int,
        home_score: int,
        convert_id: ConvertMLBAM,
        runners: list[str | None],
        resp_pitchers: list[str | None],
        event_types: dict,
        top_level_pa: bool = True,
        ghost_runner_added: bool = False,
        run_scored_ct_prev: int = 0,
    ) -> None:
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
        self.player_lineup_spots = player_lineup_spots
        self.away_starting_pitcher = away_starting_pitcher
        self.home_starting_pitcher = home_starting_pitcher
        self.convert_id = convert_id
        # self.df = pd.DataFrame(columns=chadwick_dtypes.keys())  # type: ignore
        # self.df = self.df.astype(chadwick_dtypes)
        self.data = []
        self.runners = runners
        self.resp_pitchers = resp_pitchers
        self.top_level_pa = top_level_pa
        self.ghost_runner_added = ghost_runner_added
        self.run_scored_ct_prev = run_scored_ct_prev
        self.event_types = event_types

    def parse(self) -> None:
        row: dict[str, None | str | float | int | bool] = {
            col: chadwick_defaults[col] for col in chadwick_dtypes.keys()
        }
                # Set all counts
        counts: list[bool] = [
            True, # 0-0
            False, # 0-1
            False, # 0-2
            False, # 1-0
            False, # 1-1
            False, # 1-2
            False, # 2-0
            False, # 2-1
            False, # 2-2
            False, # 3-0
            False, # 3-1
            False, # 3-2
        ]
        for pitch in self.plate_appearance["playEvents"]:
            # event = self.plate_appearance["playEvents"][pitch]
            if not pitch.get("count", None):
                continue
            count = pitch["count"]
            count_tup = (count["balls"], count["strikes"])
            # I have to do it this way because the counts include 3 strikes and 4 balls... sigh
            if count_tup == (0, 0):
                counts[0] = True
            elif count_tup == (0, 1):
                counts[1] = True
            elif count_tup == (0, 2):
                counts[2] = True
            elif count_tup == (1, 0):
                counts[3] = True
            elif count_tup == (1, 1):
                counts[4] = True
            elif count_tup == (1, 2):
                counts[5] = True
            elif count_tup == (2, 0):
                counts[6] = True
            elif count_tup == (2, 1):
                counts[7] = True
            elif count_tup == (2, 2):
                counts[8] = True
            elif count_tup == (3, 0):
                counts[9] = True
            elif count_tup == (3, 1):
                counts[10] = True
            elif count_tup == (3, 2):
                counts[11] = True
        row["0-0"] = counts[0]
        row["0-1"] = counts[1]
        row["0-2"] = counts[2]
        row["1-0"] = counts[3]
        row["1-1"] = counts[4]
        row["1-2"] = counts[5]
        row["2-0"] = counts[6]
        row["2-1"] = counts[7]
        row["2-2"] = counts[8]
        row["3-0"] = counts[9]
        row["3-1"] = counts[10]
        row["3-2"] = counts[11]

        movement_indices = set()
        for runner_event in self.plate_appearance["runners"]:
            if runner_event["details"]["playIndex"] > len(self.plate_appearance["playEvents"]) - 1:
                continue
            movement_indices.add(runner_event["details"]["playIndex"])

        for action_event in self.plate_appearance["actionIndex"]:
            if action_event > len(self.plate_appearance["playEvents"]) - 1:
                continue
            if (
                self.plate_appearance["playEvents"][action_event].get("isBaseRunningPlay", False)
                and self.plate_appearance["playEvents"][action_event]["details"]["eventType"] == "error"
                and self.plate_appearance["playEvents"][action_event - 1]["details"]["code"] == "F"
            ):
                # Custom foul_error field
                self.plate_appearance["playEvents"][action_event]["details"]["eventType"] = "foul_error"
                movement_indices.add(action_event)

        # When ParsePlateAppearance is run, pinch runner subs will be processed
        latest_runner_subs_processed = -1
        for movement_index in sorted(list(movement_indices)):
            if movement_index == len(self.plate_appearance["playEvents"]) - 1:
                continue
            modified_pa = mjson.decode(mjson.encode(self.plate_appearance))
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
                self.player_lineup_spots,
                self.away_starting_pitcher,
                self.home_starting_pitcher,
                self.away_pitcher,
                self.home_pitcher,
                self.away_score,
                self.home_score,
                self.convert_id,
                self.runners,
                self.resp_pitchers,
                self.event_types,
                top_level_pa=False,
                ghost_runner_added=self.ghost_runner_added,
                run_scored_ct_prev=sum(elem["EVENT_RUNS_CT"] for elem in self.data),
            )
            # After the first sub_pa has been parsed, the ghost runner has already been added. Don't add again!
            self.ghost_runner_added = True
            latest_runner_subs_processed = movement_index
            sub_pa.parse()
            # self.df = pd.concat([self.df, sub_pa.df], ignore_index=True)
            self.data.extend(sub_pa.data)
        self.plate_appearance["runners"] = list(
            filter(
                lambda x: x["details"]["playIndex"] == len(self.plate_appearance["playEvents"]) - 1,
                self.plate_appearance["runners"],
            )
        )

        # Save resp_pitchers
        original_resp_pitchers = self.resp_pitchers.copy()

        # Save runners
        original_runners_list = self.runners.copy()

        # Initialize RUNx_DEST_ID
        for idx, runner in enumerate(self.runners):
            if runner is not None:
                row[f"RUN{idx + 1}_DEST_ID"] = idx + 1

        # Merge result into last entry of playEvents if this is the actual PA result
        if self.top_level_pa:
            self.plate_appearance["playEvents"][-1]["details"] = (
                self.plate_appearance["playEvents"][-1]["details"] | self.plate_appearance["result"]
            )

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

        # Handle outs, balls, strikes
        pitches = [
            p
            for p in self.plate_appearance["pitchIndex"]
            if p < len(self.plate_appearance["playEvents"])
            and self.plate_appearance["playEvents"][p]["type"] not in ("pickoff", "stepoff")
        ]
        # Exclude the most recent pitch if it has runner going UNLESS it's a foul ball
        pitches_no_runner = pitches.copy()
        if self.plate_appearance["playEvents"][-1]["details"]["eventType"] not in (
            "wild_pitch",
            "passed_ball",
            "foul_error",
        ):
            if (
                len(pitches) != 0
                and self.plate_appearance["playEvents"][pitches_no_runner[-1]]["details"].get("runnerGoing", False)
                and self.plate_appearance["playEvents"][pitches_no_runner[-1]]["details"]["code"] != "F"
            ):
                pitches_no_runner.pop(-1)
            # Sometimes, there is a caught stealing where the runner wasn't "going" before the pitch
            elif (
                len(pitches) != 0
                and self.plate_appearance["playEvents"][-1]["details"]["eventType"].startswith("caught_stealing")
                and not self.plate_appearance["playEvents"][pitches_no_runner[-1]]["details"].get("runnerGoing", False)
            ):
                pitches_no_runner.pop(-1)
        if len(pitches) == 0:
            row["OUTS_CT"] = self.plate_appearance["playEvents"][0]["count"]["outs"]
        else:
            row["OUTS_CT"] = self.plate_appearance["playEvents"][pitches[-1]]["count"]["outs"]

        if (
            len(pitches) > 1
            and not self.plate_appearance["playEvents"][-1].get("isBaseRunningPlay", False)
            and self.plate_appearance["playEvents"][-1]["type"] != "pickoff"
            and not self.plate_appearance["playEvents"][-1]["details"]["eventType"].startswith("pickoff")
        ):
            row["BALLS_CT"] = self.plate_appearance["playEvents"][pitches[-2]]["count"]["balls"]
            row["STRIKES_CT"] = self.plate_appearance["playEvents"][pitches[-2]]["count"]["strikes"]
        elif self.plate_appearance["playEvents"][-1]["type"] == "pickoff" or self.plate_appearance["playEvents"][-1][
            "details"
        ]["eventType"].startswith("pickoff"):
            # Special case for POCS where the pickoff is done by the pitcher throwing home on a pitch. Eg stealing home
            if self.plate_appearance["playEvents"][-1]["details"]["eventType"].startswith(
                "pickoff_caught_stealing"
            ) and self.plate_appearance["playEvents"][-1]["details"].get("runnerGoing", False):
                row["BALLS_CT"] = self.plate_appearance["playEvents"][pitches[-2]]["count"]["balls"]
                row["STRIKES_CT"] = self.plate_appearance["playEvents"][pitches[-2]]["count"]["strikes"]
            # If this is a catcher pickoff after eg a strikeout
            elif self.plate_appearance["playEvents"][-1]["details"].get(
                "fromCatcher", False
            ) and not self.plate_appearance["playEvents"][-1]["details"]["eventType"].startswith("pickoff"):
                if len(pitches) > 1:
                    row["BALLS_CT"] = self.plate_appearance["playEvents"][pitches[-2]]["count"]["balls"]
                    row["STRIKES_CT"] = self.plate_appearance["playEvents"][pitches[-2]]["count"]["strikes"]
                else:
                    row["BALLS_CT"] = 0
                    row["STRIKES_CT"] = 0
            elif len(pitches) > 0:
                row["BALLS_CT"] = self.plate_appearance["playEvents"][pitches[-1]]["count"]["balls"]
                row["STRIKES_CT"] = self.plate_appearance["playEvents"][pitches[-1]]["count"]["strikes"]
            else:
                row["BALLS_CT"] = 0
                row["STRIKES_CT"] = 0
        elif self.plate_appearance["playEvents"][-1].get("isBaseRunningPlay", False):
            # Distance the correct pitch to get the count from is from the end
            dist_from_last = 1
            if self.plate_appearance["playEvents"][-1]["details"]["eventType"] in (
                "wild_pitch",
                "passed_ball",
                "foul_error",
            ):
                dist_from_last = 2
            # This indicates a catcher pickoff likely so dist_from_last is 2
            if self.plate_appearance["playEvents"][-1]["details"][
                "eventType"
            ] == "other_out" and not self.plate_appearance["playEvents"][pitches[-1]]["details"].get(
                "runnerGoing", False
            ):
                dist_from_last = 2
            # Error = advance on the play, so we need to go back 2
            if self.plate_appearance["playEvents"][-1]["details"]["eventType"] == "error":
                dist_from_last = 2
            if (
                self.plate_appearance["playEvents"][-1]["details"]["eventType"].startswith("stolen_base")
                and self.plate_appearance["playEvents"][-2]["type"] == "pickoff"
                and self.plate_appearance["playEvents"][-2]["details"]["fromCatcher"]
            ):
                row["BALLS_CT"] = self.plate_appearance["playEvents"][pitches[-1]]["count"]["balls"]
                row["STRIKES_CT"] = self.plate_appearance["playEvents"][pitches[-1]]["count"]["strikes"]
            elif len(pitches_no_runner) >= dist_from_last:
                row["BALLS_CT"] = self.plate_appearance["playEvents"][pitches_no_runner[-dist_from_last]]["count"][
                    "balls"
                ]
                row["STRIKES_CT"] = self.plate_appearance["playEvents"][pitches_no_runner[-dist_from_last]]["count"][
                    "strikes"
                ]
            else:
                row["BALLS_CT"] = 0
                row["STRIKES_CT"] = 0
        else:
            row["BALLS_CT"] = 0
            row["STRIKES_CT"] = 0

        # Get the batter and pitcher
        row["RESP_BAT_ID"] = self.convert_id.mlbam_to_retro(self.plate_appearance["matchup"]["batter"]["id"])
        row["RESP_BAT_HAND_CD"] = self.plate_appearance["matchup"]["batSide"]["code"]
        row["RESP_PIT_ID"] = self.convert_id.mlbam_to_retro(self.plate_appearance["matchup"]["pitcher"]["id"])
        row["RESP_PIT_HAND_CD"] = self.plate_appearance["matchup"]["pitchHand"]["code"]

        resp_pit_is_current_pitcher = True
        ghost_runner = None
        ghost_runner_original_base = -1
        # Process substitutions and runner placement in extra innings
        for event_idx in self.plate_appearance["actionIndex"]:
            if event_idx >= len(self.plate_appearance["playEvents"]):
                continue
            event = self.plate_appearance["playEvents"][event_idx]
            if event["details"]["eventType"] == "runner_placed" and not self.ghost_runner_added:
                base = event["base"]
                player = self.convert_id.mlbam_to_retro(event["player"]["id"])
                ghost_runner = player
                ghost_runner_original_base = base
                if player not in self.runners:  # If they advanced by eg a wild pitch, they will have already been added
                    self.runners[base - 1] = player
                    self.resp_pitchers[base - 1] = row["RESP_PIT_ID"]
                    original_resp_pitchers[base - 1] = row["RESP_PIT_ID"]
                    row[f"BASE{base}_RUN_ID"] = player
                    row[f"RUN{base}_RESP_PIT_ID"] = row["RESP_PIT_ID"]
                    row[f"RUN{base}_DEST_ID"] = base
                else:
                    self.resp_pitchers[self.runners.index(player)] = row["RESP_PIT_ID"]
                    row[f"RUN{self.runners.index(player) + 1}_RESP_PIT_ID"] = row["RESP_PIT_ID"]
            if not event.get("isSubstitution", False):
                continue
            if event["position"]["abbreviation"] == "PR":
                if event["index"] <= latest_runner_subs_processed:
                    continue
                old_code = self.convert_id.mlbam_to_retro(event["replacedPlayer"]["id"])
                new_code = self.convert_id.mlbam_to_retro(event["player"]["id"])
                if old_code in self.runners:
                    self.runners[self.runners.index(old_code)] = new_code
                self.positions[event["player"]["id"]] = 12
            elif event["position"]["abbreviation"] == "PH":
                new_code = self.convert_id.mlbam_to_retro(event["player"]["id"])
                old_code = self.convert_id.mlbam_to_retro(event["replacedPlayer"]["id"])
                # If the strikeout should be charged to the old hitter
                if event["count"]["strikes"] == 2 and self.plate_appearance["playEvents"][-1]["count"]["strikes"] == 3:
                    row["RESP_BAT_ID"] = old_code
                    # We need to make another request to get the batter handedness
                    if event['replacedPlayer']['link'] not in self._player_cache:
                        bat_old = requests.get(f"https://statsapi.mlb.com{event['replacedPlayer']['link']}").json()
                        self._player_cache[event['replacedPlayer']['link']] = bat_old
                    else:
                        bat_old = self._player_cache[event['replacedPlayer']['link']]
                    row["RESP_BAT_HAND_CD"] = bat_old["people"][0]["batSide"]["code"]

                    # Sadly we can't get for certain which hand the player batted with, so in this very rare circumstance
                    # where the strikeout is charged to the old hitter, we assume
                    if row["RESP_BAT_HAND_CD"] == "S":
                        row["RESP_BAT_HAND_CD"] = "L"
                self.positions[event["player"]["id"]] = 11

                # Otherwise, the play should actually already have the correct ID
            elif event["position"]["abbreviation"] == "P":
                new_code = self.convert_id.mlbam_to_retro(event["player"]["id"])

                # If there is a ghost runner, we need to update the ghost runner's pitcher to the first pitcher in the at bat (not the substituted one)
                if ghost_runner is not None:
                    self.resp_pitchers[self.runners.index(ghost_runner)] = (
                        self.home_pitcher[0] if self.plate_appearance["about"]["isTopInning"] else self.away_pitcher[0]
                    )
                    row[f"RUN{self.runners.index(ghost_runner) + 1}_RESP_PIT_ID"] = (
                        self.home_pitcher[0] if self.plate_appearance["about"]["isTopInning"] else self.away_pitcher[0]
                    )
                    original_resp_pitchers[ghost_runner_original_base - 1] = (
                        self.home_pitcher[0] if self.plate_appearance["about"]["isTopInning"] else self.away_pitcher[0]
                    )
                    ghost_runner = None
                count = (event["count"]["balls"], event["count"]["strikes"])
                # Rule 10.17(g)(1) (ok it might not actually be this number, but you get the point)
                not_charged_to_reliever = count in ((3, 0), (3, 1), (3, 2), (2, 1), (2, 0))
                if not_charged_to_reliever and self.plate_appearance["playEvents"][-1]["count"]["balls"] == 4:
                    resp_pit_is_current_pitcher = False
                    row["RESP_PIT_ID"] = (
                        self.home_pitcher[0] if self.plate_appearance["about"]["isTopInning"] else self.away_pitcher[0]
                    )
                    row["RESP_PIT_HAND_CD"] = (
                        self.home_pitcher[1] if self.plate_appearance["about"]["isTopInning"] else self.away_pitcher[1]
                    )
                # If there were any previous plays, we need to change RESP_PIT_ID and RESP_PIT_START_FL for the previous baserunning plays
                # for i, _ in self.df.iterrows():
                for i in range(len(self.data)):
                    pitcher = self.home_pitcher if self.plate_appearance["about"]["isTopInning"] else self.away_pitcher
                    # self.df.loc[i, "RESP_PIT_ID"] = pitcher[0]  # type: ignore
                    self.data[i]["RESP_PIT_ID"] = pitcher[0]  # type: ignore
                    # self.df.loc[i, "RESP_PIT_HAND_CD"] = pitcher[1]  # type: ignore
                    self.data[i]["RESP_PIT_HAND_CD"] = pitcher[1]  # type: ignore
                    if pitcher[0] == self.away_starting_pitcher or pitcher[0] == self.home_starting_pitcher:
                        # self.df.loc[i, "RESP_PIT_START_FL"] = True  # type: ignore
                        self.data[i]["RESP_PIT_START_FL"] = True  # type: ignore
                if self.plate_appearance["about"]["isTopInning"]:
                    self.home_pitcher[0] = new_code
                    self.home_pitcher[1] = self.plate_appearance["matchup"]["pitchHand"]["code"]
                else:
                    self.away_pitcher[0] = new_code
                    self.away_pitcher[1] = self.plate_appearance["matchup"]["pitchHand"]["code"]
                # Don't forget to update the position (in case of a position player pitching)
                self.positions[event["player"]["id"]] = 1
            elif event["details"]["eventType"] in ("defensive_substitution", "defensive_switch"):
                self.positions[event["player"]["id"]] = int(event["position"]["code"])

        # Don't want to update the pitcher if not a top level PA (before the substitutions are processed)
        # or if the RESP_PIT isn't the actual current pitcher
        if self.top_level_pa and resp_pit_is_current_pitcher:
            if self.plate_appearance["about"]["isTopInning"]:
                self.home_pitcher[0] = row["RESP_PIT_ID"]
                self.home_pitcher[1] = row["RESP_PIT_HAND_CD"]
            else:
                self.away_pitcher[0] = row["RESP_PIT_ID"]
                self.away_pitcher[1] = row["RESP_PIT_HAND_CD"]

        # If they were a pinch runner, they're now 0 (this means they batted around)
        if self.positions[self.plate_appearance["matchup"]["batter"]["id"]] == 12:
            self.positions[self.plate_appearance["matchup"]["batter"]["id"]] = 0
        # Why is there not a field for RESP_BAT_FLD_CD?
        row["BAT_FLD_CD"] = self.positions[self.plate_appearance["matchup"]["batter"]["id"]]

        # In case they bat around, the pinch hitter should no longer be 11
        if self.top_level_pa and self.positions[self.plate_appearance["matchup"]["batter"]["id"]] == 11:
            self.positions[self.plate_appearance["matchup"]["batter"]["id"]] = 0

        # Check whether batter and hitter are starter
        if row["RESP_BAT_ID"] in list(self.starting_lineup_away.values()) + [self.away_starting_pitcher] + list(
            self.starting_lineup_home.values()
        ) + [self.home_starting_pitcher]:
            row["RESP_BAT_START_FL"] = True
        if row["RESP_PIT_ID"] == self.away_starting_pitcher or row["RESP_PIT_ID"] == self.home_starting_pitcher:
            row["RESP_PIT_START_FL"] = True

        # Update BASE_RUN_IDs for runners
        row["START_BASES_CD"] = 0
        for i, runner in enumerate(self.runners):
            if runner == None:
                continue
            row[f"BASE{i+1}_RUN_ID"] = runner
            row[f"RUN{i+1}_RESP_PIT_ID"] = self.resp_pitchers[i]
            row[f"START_BASES_CD"] += 2**i

        # We don't want any baserunning events from the same origin and different destinations on the same play
        origin_bases = set()
        origin_base_runner_events = defaultdict(list)
        original_runners = mjson.decode(mjson.encode(self.plate_appearance["runners"]))
        # The latest one is the longest advance
        for idx in reversed(range(len(self.plate_appearance["runners"]))):
            if (
                self.plate_appearance["runners"][idx]["movement"]["originBase"],
                self.plate_appearance["runners"][idx]["details"]["playIndex"],
            ) in origin_bases:
                origin_base_runner_events[
                    (
                        self.plate_appearance["runners"][idx]["movement"]["originBase"],
                        self.plate_appearance["runners"][idx]["details"]["playIndex"],
                    )
                ].append(self.plate_appearance["runners"].pop(idx))
            origin_bases.add(
                (
                    self.plate_appearance["runners"][idx]["movement"]["originBase"],
                    self.plate_appearance["runners"][idx]["details"]["playIndex"],
                )
            )
            origin_base_runner_events[
                (
                    self.plate_appearance["runners"][idx]["movement"]["originBase"],
                    self.plate_appearance["runners"][idx]["details"]["playIndex"],
                )
            ].append(self.plate_appearance["runners"][idx])

        # Calculate runs scored on play and other baserunning events
        runs_scored = 0
        rbis = 0
        out_runner_resp_pit = None
        out_runner_base = -1
        special_pickoff = False
        for runner in self.plate_appearance["runners"]:
            start_base = runner["movement"]["originBase"]
            end_base = runner["movement"]["end"]

            # Sometimes start_base == end_base and this completely breaks the parser
            # (normally, it's to add some throwing error that didn't result in an advance, I think resulting in a runner safely returning)
            if start_base == end_base:
                continue

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
                # Sometimes the longest advance doesn't tell us whether there was a SB, CS, etc (eg if there's an SB then error)
                for e in origin_base_runner_events[(runner["movement"]["originBase"], runner["details"]["playIndex"])]:
                    if e["details"]["eventType"].startswith("caught_stealing") and not e["details"][
                        "movementReason"
                    ].startswith("r_adv"):
                        row[f"RUN{start_base}_CS_FL"] = True
                        if self.plate_appearance["playEvents"][-1]["details"]["eventType"] == "other_out":
                            self.plate_appearance["playEvents"][-1]["details"][
                                "eventType"
                            ] = f"caught_stealing_{start_base + 1}b"
                    elif e["details"]["eventType"].startswith("pickoff_caught_stealing") and not e["details"][
                        "movementReason"
                    ].startswith("r_adv"):
                        row[f"RUN{start_base}_CS_FL"] = True
                        row[f"RUN{start_base}_PK_FL"] = True
                    elif (
                        e["details"]["eventType"].startswith("pickoff")
                        and e["details"]["movementReason"] != "r_adv_play"
                    ):
                        row[f"RUN{start_base}_PK_FL"] = True
                    elif e["details"]["eventType"].startswith("stolen_base") and not e["details"][
                        "movementReason"
                    ].startswith("r_adv"):
                        row[f"RUN{start_base}_SB_FL"] = True
                    elif e["details"]["eventType"] == "wild_pitch":
                        row["WP_FL"] = True
                    elif e["details"]["eventType"] == "passed_ball":
                        row["PB_FL"] = True
                    elif (
                        e["details"]["movementReason"] == "r_out_returning" and e["details"]["eventType"] == "other_out"
                    ):
                        # Sometimes, the second to last event has the pickoff thing...
                        if (
                            e["details"]["playIndex"] > 0
                            and self.plate_appearance["playEvents"][e["details"]["playIndex"] - 1]["type"] == "pickoff"
                        ):
                            special_pickoff = True
                            row[f"RUN{start_base}_PK_FL"] = True
            elif start_base == None:
                row["BAT_DEST_ID"] = end_base
                # Mostly for passed ball/wild pitch dropped third strike
                for e in origin_base_runner_events[(runner["movement"]["originBase"], runner["details"]["playIndex"])]:
                    if e["details"]["eventType"] == "wild_pitch":
                        row["WP_FL"] = True
                    elif e["details"]["eventType"] == "passed_ball":
                        row["PB_FL"] = True
                    if e["details"]["eventType"] == "field_error":
                        row["BAT_SAFE_ERR_FL"] = True
            else:
                print(self.plate_appearance)
                print(start_base)
            if end_base in [1, 2, 3]:
                pid = self.convert_id.mlbam_to_retro(runner["details"]["runner"]["id"])

                if runner["details"]["responsiblePitcher"] != None:
                    self.resp_pitchers[end_base - 1] = self.convert_id.mlbam_to_retro(
                        runner["details"]["responsiblePitcher"]["id"]
                    )
                elif start_base in [1, 2, 3]:
                    self.resp_pitchers[end_base - 1] = original_resp_pitchers[start_base - 1]
                else:
                    self.resp_pitchers[end_base - 1] = row["RESP_PIT_ID"]

                # It's possible that this runner has already been removed from runners (eg if they were on second and a previous runner advanced to second)
                # So don't remove them if they're not there
                if pid in self.runners:
                    idx = self.runners.index(pid)
                    self.runners[idx] = None
                    self.resp_pitchers[idx] = None
                self.runners[end_base - 1] = pid
            elif end_base >= 4:
                pid = self.convert_id.mlbam_to_retro(runner["details"]["runner"]["id"])
                if pid in self.runners:
                    idx = self.runners.index(pid)
                    self.resp_pitchers[idx] = None
                    self.runners[idx] = None
            else:
                pid = self.convert_id.mlbam_to_retro(runner["details"]["runner"]["id"])
                if (
                    pid in original_runners_list
                    and (start_base == None and out_runner_base == -1)
                    or (start_base is not None and start_base > out_runner_base)
                ):
                    out_runner_resp_pit = original_resp_pitchers[start_base - 1]
                    out_runner_base = start_base
                if pid in self.runners and start_base != None:
                    self.runners[start_base - 1] = None
                    self.resp_pitchers[start_base - 1] = None

        row["EVENT_RUNS_CT"] = runs_scored
        if self.plate_appearance["about"]["isTopInning"]:
            # run_scored_ct_prev is the number of runs scored by
            # row["AWAY_SCORE_CT"] = self.away_score + self.df["EVENT_RUNS_CT"].sum() + self.run_scored_ct_prev
            row["AWAY_SCORE_CT"] = self.away_score + sum(elem["EVENT_RUNS_CT"] for elem in self.data) + self.run_scored_ct_prev
            row["HOME_SCORE_CT"] = self.home_score
        else:
            row["AWAY_SCORE_CT"] = self.away_score
            # row["HOME_SCORE_CT"] = self.home_score + self.df["EVENT_RUNS_CT"].sum() + self.run_scored_ct_prev
            row["HOME_SCORE_CT"] = self.home_score + sum(elem["EVENT_RUNS_CT"] for elem in self.data) + self.run_scored_ct_prev
        row["RBI_CT"] = rbis
        row["END_BASES_CD"] = 0
        for i, runner in enumerate(self.runners):
            if runner == None:
                continue
            row["END_BASES_CD"] += 2**i

        ## Process event type
        event_type = self.plate_appearance["playEvents"][-1]["details"]["eventType"]

        # Special cases for sac_bunt and sac_fly
        if event_type in ("sac_bunt", "sac_bunt_double_play"):
            row["SH_FL"] = True
            if self.plate_appearance["playEvents"][-1]["details"]["isOut"]:
                event_code = 2  # out
            # If the batter made it to any base (ie a safe advance without an out), it's a fielder's choice
            elif any(r["movement"]["originBase"] == None and not r["movement"]["isOut"] for r in original_runners):
                r = next(
                    r for r in original_runners if r["movement"]["originBase"] == None and not r["movement"]["isOut"]
                )
                if any("error" in c["credit"] for c in r["credits"]):
                    event_code = 18
                    row["BAT_SAFE_ERR_FL"] = True
                else:
                    event_code = 19  # fielder's choice
            else:
                event_code = 18  # error
            if event_type == "sac_hit_double_play":
                row["DP_FL"] = True
        elif event_type in ("sac_fly", "sac_fly_double_play"):
            row["SF_FL"] = True
            if self.plate_appearance["playEvents"][-1]["details"]["isOut"]:
                event_code = 2  # out
            elif any(r["movement"]["originBase"] == None and not r["movement"]["isOut"] for r in original_runners):
                r = next(
                    r for r in original_runners if r["movement"]["originBase"] == None and not r["movement"]["isOut"]
                )
                if any("error" in c["credit"] for c in r["credits"]):
                    event_code = 18
                    row["BAT_SAFE_ERR_FL"] = True
                else:
                    event_code = 19  # fielder's choice
            else:
                event_code = 18  # error
            if event_type == "sac_fly_double_play":
                row["DP_FL"] = True
        elif event_type == "double_play" or event_type == "triple_play":
            # It seems that retrosheet doesn't add the fielder's choice flag to double plays :((
            event_code = 2
        else:
            # Get the event code
            event_code = self._event_type_to_cwevent.get(event_type, 100)
            # Sometimes the event code is 2 but the batter still reaches on an error
            if event_code == 2:
                try:
                    r = next(
                        r
                        for r in original_runners
                        if r["movement"]["originBase"] == None and not r["movement"]["isOut"]
                    )
                    if any("error" in c["credit"] for c in r["credits"]):
                        row["BAT_SAFE_ERR_FL"] = True
                except StopIteration:
                    pass
            if event_code == 100:
                print(event_type)
                raise ValueError("Unknown event type")

        # Special case where other_out is a pickoff
        if event_type == "other_out" and special_pickoff:
            event_code = 8

        # Foul error
        if event_code == 13:
            row["BATTEDBALL_CD"] = "F"
            if "pop" in self.plate_appearance["playEvents"][-1]["details"]["description"].lower():
                row["BATTEDBALL_CD"] = "P"

        # This happens with interference errors on a pickoff! EVENT_CD should be 8 and PK_FL should be set!
        if (
            event_type == "error"
            and len(self.plate_appearance["playEvents"]) > 1
            and self.plate_appearance["playEvents"][-2]["type"] == "pickoff"
        ):
            event_code = 8
            row[f"RUN{self.plate_appearance["playEvents"][-2]["details"]["code"]}_PK_FL"] = True
        row["EVENT_CD"] = event_code

        # Check if the event is an at bat (PA but not catcher's interference or sacrifice or walk or HBP)
        if (
            self.event_types[event_type]["plateAppearance"]
            and not row["SF_FL"]
            and not row["SH_FL"]
            and not event_code in (14, 15, 16, 17)
        ):
            row["AB_FL"] = True

        # If this is the top level PA (ie not a sub PA), and the event is not a plate appearance, set PA_TRUNC_FL for all events
        if self.top_level_pa and not self.event_types[event_type]["plateAppearance"]:
            row["PA_TRUNC_FL"] = True
            # for i, _ in self.df.iterrows():
            #     self.df.loc[i, "PA_TRUNC_FL"] = True  # type: ignore
            for i in range(len(self.data)):
                self.data[i]["PA_TRUNC_FL"] = True  # type: ignore

        # Set H_CD
        if event_code in [20, 21, 22, 23]:
            # Single (20 - 19 = 1), Double (21 - 19 = 2), Triple (22 - 19 = 3), Home Run (23 - 19 = 4)
            row["H_CD"] = event_code - 19
            # This can be incorrectly set sometimes
            row["BAT_SAFE_ERR_FL"] = False

        # Outs
        if self.top_level_pa:
            row["EVENT_OUTS_CT"] = self.plate_appearance["count"]["outs"] - row["OUTS_CT"]
        else:
            row["EVENT_OUTS_CT"] = self.plate_appearance["playEvents"][-1]["count"]["outs"] - row["OUTS_CT"]

        # Set DP_FL and TP_FL
        if not row["SF_FL"] and not row["SH_FL"]:
            if row["EVENT_OUTS_CT"] == 2:
                row["DP_FL"] = True
            elif row["EVENT_OUTS_CT"] == 3:
                row["TP_FL"] = True

        # Balls and Strikes
        row["PA_STRIKE_CT"] = 0
        row["PA_BALL_CT"] = 0
        row["PA_OTHER_BALL_CT"] = 0
        row["PA_OTHER_STRIKE_CT"] = 0
        for pitch_idx in self.plate_appearance["pitchIndex"]:
            if pitch_idx >= len(self.plate_appearance["playEvents"]):
                continue
            pitch = self.plate_appearance["playEvents"][pitch_idx]
            # For some reason, auto strikes and balls don't count...
            if pitch["details"]["code"] in ("AS", "AC", "AB"):
                continue
            if pitch["details"].get("isStrike", False) or pitch["details"].get("isInPlay", False):
                row["PA_STRIKE_CT"] += 1
            elif pitch["details"].get("isBall", False):
                row["PA_BALL_CT"] += 1

            if not pitch["isPitch"] and pitch["details"].get("isBall", False):
                row["PA_OTHER_BALL_CT"] += 1
            elif not pitch["isPitch"] and pitch["details"].get("isStrike", False):
                row["PA_OTHER_STRIKE_CT"] += 1

        # Handle pitcher responsibility on a fielder's choice (or when the batter reaches base but there's an out)
        if (
            event_code == 19
            or row["BAT_DEST_ID"] != 0
            and row["EVENT_OUTS_CT"] > 0
            and event_code not in (20, 21, 22, 23)
        ):
            # This is example 3 in rule 9.16(g). If the runner is out, the ex-pitcher is now responsible for the last runner before the out
            # ie runners on first and second, if the runner is out advancing from first to second, the ex-pitcher is responsible for
            # the batter now on first, not the runner on third
            if out_runner_resp_pit:
                for i in reversed(range(out_runner_base)):
                    if i != 0:
                        if row[f"RUN{i}_DEST_ID"] in [1, 2, 3] and self.resp_pitchers[row[f"RUN{i}_DEST_ID"] - 1] == row["RESP_PIT_ID"]:  # type: ignore
                            self.resp_pitchers[row[f"RUN{i}_DEST_ID"] - 1] = out_runner_resp_pit  # type: ignore
                            break
                    else:
                        self.resp_pitchers[row["BAT_DEST_ID"] - 1] = out_runner_resp_pit  # type: ignore
        # Batted ball type. Sometimes on catcher's interference, hitData is populated despite no _real_ BIP
        if self.plate_appearance["playEvents"][-1].get("hitData") and row["EVENT_CD"] != 17:
            batted_ball = self.plate_appearance["playEvents"][-1]["hitData"]["trajectory"]
            if batted_ball == "popup":
                row["BATTEDBALL_CD"] = "P"
            elif batted_ball == "line_drive":
                row["BATTEDBALL_CD"] = "L"
            elif batted_ball == "ground_ball":
                row["BATTEDBALL_CD"] = "G"
            elif batted_ball == "fly_ball":
                row["BATTEDBALL_CD"] = "F"
            elif batted_ball == "bunt_grounder":
                row["BATTEDBALL_CD"] = "G"
            elif batted_ball == "bunt_popup":
                row["BATTEDBALL_CD"] = "P"
            elif batted_ball == "bunt_line_drive":
                row["BATTEDBALL_CD"] = "L"

        row["BAT_LINEUP_ID"] = self.player_lineup_spots[row["RESP_BAT_ID"]]
        # self.df = pd.concat([self.df, pd.DataFrame([row])], ignore_index=True)
        self.data.append(row)
