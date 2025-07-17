import pandas as pd 
from .stat_calculator import BattingStatsCalculator, PitchingStatsCalculator
from .utils import get_years, get_linear_weights
from collections import defaultdict


class StatSplits:
    def __init__(self, start_year: int = 0, end_year: int = 0, years_list: list[int] | None = None, events: pd.DataFrame | None = None):
        """
        Parent class. Should not be instantiated directly
        """

        self.linear_weights = get_linear_weights()  # type: ignore
        self.sql_query_where = defaultdict(str)
        self.sql_custom_columns_where = defaultdict(str)
        self.custom_table_cols = defaultdict(str)
        self.custom_select = defaultdict(str)

        years = get_years()
        if years_list is not None:
            all_years = set(years_list)
            years = [year for year in years if year in all_years]
            self.sql_query_where["year"] = f"events.year IN ({', '.join([str(year) for year in years])})"
        else:
            if start_year not in years:
                raise ValueError(
                    f"Start year {start_year} not found in database. Did you remember to run baseballquery.update_data()?"
                )
            if end_year not in years:
                raise ValueError(
                    f"End year {end_year} not found in database. Did you remember to run baseballquery.update_data()"
                )
            self.sql_query_where["year"] = f"{start_year} <= events.year AND events.year <= {end_year}"

        self.stats: pd.DataFrame = pd.DataFrame()
        self.split = "year"
        self.find = "player"
        self.wins_included = False

    def set_split(self, split: str):
        """
        Set the split to be used for calculating pitching stats.

        Parameters:
        split (str): 'year', 'month', 'career', 'game'
        """
        split = split.lower()
        assert split in [
            "year",
            "month",
            "career",
            "game",
        ], f"Invalid split {split}. Valid splits are 'year', 'month', 'career', 'game'"
        self.split = split

    def set_subdivision(self, subdivision: str):
        """
        Set the sub-division to be used for calculating pitching stats.

        Parameters:
        subdivision (str): 'player' for individual players, 'team' for team totals
        """
        subdivision = subdivision.lower()
        assert subdivision in [
            "player",
            "team",
        ], f"Invalid sub-division {subdivision}. Valid sub-divisions are 'player', 'team'"
        self.find = subdivision

    def set_days_of_week(self, days_of_week: list[str]):
        """
        Limit the data to only include games played on certain days of the week.

        Parameters:
        days_of_week (list): List of days of the week to include. Valid values are "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"
        """
        if len(days_of_week) == 0:
            self.sql_query_where.pop("day_week", None)
            return
        assert all(
            day.capitalize()
            in [
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
                "Sunday",
            ]
            for day in days_of_week
        ), "Invalid day of week"
        for idx, day in enumerate(days_of_week):
            days_of_week[idx] = day.capitalize()
        self.sql_query_where["day_week"] = f"cwgame.GAME_DY IN ({', '.join([f'\'{day}\'' for day in days_of_week])})"

        self.wins_included = False

    def set_batter_handedness_pa(self, handedness: str):
        """
        Limit the data to only include plate appearances with batters hitting on a certain side of the plate.
        Switch hitters are considered the handedness they're currently hitting with.
        If data is unknown (not likely to happen after ~1970s or 1980s) it will be excluded.

        Parameters:
        handedness (str): 'R' for right-handed batters, 'L' for left-handed batters
        """
        handedness = handedness.upper()
        assert handedness in [
            "R",
            "L",
        ], "Invalid handedness. Valid values are 'R' and 'L'"
        self.sql_query_where["bat_hand_pa"] = f"events.RESP_BAT_HAND_CD = '{handedness}'"

    def set_pitcher_handedness(self, handedness: str):
        """
        Limit the data to only include plate appearances with pitchers pitching with a certain hand
        If data is unknown (not likely to happen after ~1970s or 1980s) it will be excluded.

        If someone is still using this by the time switch pitchers start to dominate pitching, open an issue on GitHub.

        Parameters:
        handedness (str): 'R' for right-handed pitchers, 'L' for left-handed pitchers
        """
        handedness = handedness.upper()
        assert handedness in [
            "R",
            "L",
        ], "Invalid handedness. Valid values are 'R' or 'L'"
        self.sql_query_where["pit_hand_pa"] = f"events.RESP_PIT_HAND_CD = '{handedness}'"

    def set_batter_starter(self, starter: bool):
        """
        Limit the data to only include plate appearances with batters who started the game.

        Parameters:
        starter (bool): True for starters, False for non-starters
        """
        self.sql_query_where["bat_starter"] = f"events.RESP_BAT_START_FL = '{int(starter)}'"

    def set_pitcher_starter(self, starter: bool):
        """
        Limit the data to only include plate appearances with pitchers who started the game.

        Parameters:
        starter (bool): True for starters, False for non-starters
        """
        self.sql_query_where["pit_starter"] = f"events.RESP_PIT_START_FL = '{int(starter)}'"

    def set_batter_lineup_pos(self, lineup_pos: list[int]):
        """
        Limit the data to only include plate appearances with batters who batted in a certain lineup position.

        Parameters:
        lineup_pos (int): 1-9 for lineup position
        """
        assert all(1 <= i <= 9 for i in lineup_pos), "Invalid lineup position"
        self.sql_query_where["bat_lineup_pos"] = f"events.BAT_LINEUP_ID IN ({', '.join([str(pos) for pos in lineup_pos])})"

    def set_player_field_position(self, field_pos: list[int]):
        """
        Limit the data to only include plate appearances with players who played a certain field position.

        Parameters:
        field_pos (int): 1-12 for field position.
            - 1-9 are the standard fielding positions, 10 is the DH, 11 is a pinch hitter, 12 is a pinch runner (this last one almost certainly will return 0 results)
        """
        assert all(1 <= i <= 12 for i in field_pos), "Invalid field position"
        self.sql_query_where["bat_fld_pos"] = f"events.BAT_FLD_CD IN ({', '.join([str(pos) for pos in field_pos])})"

    def set_batter_home(self, home: bool):
        """
        Limit the data to only include plate appearances with batters who batted at home or away.

        Parameters:
        home (bool): True for home, False for away
        """
        if home:
            self.sql_query_where["bat_team_home"] = "events.HOME_TEAM_ID = events.BAT_TEAM_ID"
        else:
            self.sql_query_where["bat_team_away"] = "events.HOME_TEAM_ID != events.BAT_TEAM_ID"

    def set_pitcher_home(self, home: bool):
        """
        Limit the data to only include plate appearances with pitchers who pitched at home or away.

        Parameters:
        home (bool): True for home, False for away
        """
        if home:
            self.sql_query_where["pit_team_home"] = "events.HOME_TEAM_ID = events.FLD_TEAM_ID"
        else:
            self.sql_query_where["pit_team_away"] = "events.HOME_TEAM_ID != events.FLD_TEAM_ID"

    def set_pitching_team(self, teams: list[str]):
        """
        Limit the data to only include games with certain teams pitching.

        Parameters:
        teams (list): List of team abbreviations (the retrosheet ones, e.g. "BOS", "NYA")
        """
        if len(teams) == 0:
            self.sql_query_where.pop("fld_team_id", None)
            return
        assert all(len(team) == 3 for team in teams), "Invalid team abbreviation. Team abbreviations must be exactly 3 uppercase alphabetic characters."
        assert all(len(team) == 3 for team in teams), "Invalid team abbreviation"
        assert all(team.isupper() for team in teams), "Team abbreviations must be uppercase"
        assert all(team.isalpha() for team in teams), "Team abbreviations must be alphabetic"
        self.sql_query_where["fld_team_id"] = f"events.FLD_TEAM_ID IN ({', '.join(f'\'{team}\'' for team in teams)})"

    def set_batting_team(self, teams: list[str]):
        """
        Limit the data to only include PAs with certain teams batting.

        Parameters:
        teams (list): List of team abbreviations (the retrosheet ones, e.g. "BOS", "NYA")
        """
        if len(teams) == 0:
            self.sql_query_where.pop("bat_team_id", None)
            return
        assert all(len(team) == 3 for team in teams), "Invalid team abbreviation. Team abbreviations must be exactly 3 uppercase alphabetic characters."
        assert all(len(team) == 3 for team in teams), "Invalid team abbreviation"
        assert all(team.isupper() for team in teams), "Team abbreviations must be uppercase"
        assert all(team.isalpha() for team in teams), "Team abbreviations must be alphabetic"
        self.sql_query_where["bat_team_id"] = f"events.BAT_TEAM_ID IN ({', '.join(f'\'{team}\'' for team in teams)})"

    def set_innings(self, innings: list[int]):
        """
        Limit the data to only include PAs with certain innings.

        Parameters:
        innings (list[int]): 1-infinity for the inning number
        """
        if len(innings) == 0:
            self.sql_query_where.pop("inn_ct", None)
            return
        assert all(1 <= inning for inning in innings), "Invalid inning"
        self.sql_query_where["inn_ct"] = f"events.INN_CT IN ({', '.join([str(inning) for inning in innings])})"

    def set_outs(self, outs: list[int]):
        """
        Limit the data to only include PAs with certain number of outs.

        Parameters:
        outs (list[int]): 0-2 for the number of outs
        """
        if len(outs) == 0:
            self.sql_query_where.pop("outs_ct", None)
            return
        assert all(0 <= out < 3 for out in outs), "Invalid number of outs"
        self.sql_query_where["outs_ct"] = f"events.OUTS_CT IN ({', '.join([str(out) for out in outs])})"

    def set_count(self, counts: list[str]):
        """
        Limit the data to only include PAs which contain certain counts.

        Parameters:
        count (list[str]): List of counts in the format "balls-strikes" (e.g. "0-0", "1-2", "3-2")
        """
        if len(counts) == 0:
            self.sql_query_where.pop("count", None)
            return
        assert all(len(c) == 3 and c[1] == '-' for c in counts), "Invalid count format. Must be in the format 'balls-strikes' (e.g. '0-0', '1-2', '3-2')"
        assert all(c[0].isdigit() and c[2].isdigit() for c in counts), "Invalid count format. Must be in the format 'balls-strikes' (e.g. '0-0', '1-2', '3-2')"
        queries_count = []
        for count in counts:
            balls, strikes = count.split('-')
            queries_count.append(f"events.\"{balls}-{strikes}\" = 1")
        self.sql_query_where["count"] = f"({' OR '.join(queries_count)})"

    def set_strikes_end(self, strikes: list[int]):
        """
        Limit the data to only include PAs that end with certain number of strikes (e.g. 3 for a strikeout, 1 for a hit on a 3-1 count).

        With these functions, if you want to get at bats which ended on an 0-2 count, you would use set_strikes_end([2, 3]) and set_balls_end([0,]).

        Parameters:
        strikes (list[int]): 0-3 for the number of strikes
        """
        if len(strikes) == 0:
            self.sql_query_where.pop("strikes_ct", None)
            return
        assert all(0 <= strike <= 3 for strike in strikes), "Invalid number of strikes"
        self.sql_query_where["strikes_ct"] = f"events.STRIKES_CT IN ({', '.join([str(strike) for strike in strikes])})"

    def set_balls_end(self, balls: list[int]):
        """
        Limit the data to only include PAs that end with certain number of balls (e.g. 4 for a walk, 3 for a hit on a 3-2 count).

        Parameters:
        balls (list[int]): 0-4 for the number of balls
        """
        if len(balls) == 0:
            self.sql_query_where.pop("balls_ct", None)
            return
        assert all(0 <= ball <= 4 for ball in balls), "Invalid number of balls"
        self.sql_query_where["balls_ct"] = f"events.BALLS_CT IN ({', '.join([str(ball) for ball in balls])})"

    def set_home_score(self, scores: list[int]):
        """
        Limit the data to only include PAs with a certain home team score.

        Parameters:
        scores (list[int]): Any integer for the home team score
        """
        if len(scores) == 0:
            self.sql_query_where.pop("home_score_ct", None)
            return
        assert all(score >= 0 for score in scores), "Invalid home team score"
        self.sql_query_where["home_score_ct"] = f"events.HOME_SCORE_CT IN ({', '.join([str(score) for score in scores])})"

    def set_away_score(self, scores: list[int]):
        """
        Limit the data to only include PAs with a certain away team score.

        Parameters:
        scores (list[int]): Any integer for the away team score
        """
        if len(scores) == 0:
            self.sql_query_where.pop("away_score_ct", None)
            return
        assert all(score >= 0 for score in scores), "Invalid away team score"
        self.sql_query_where["away_score_ct"] = f"events.AWAY_SCORE_CT IN ({', '.join([str(score) for score in scores])})"

    def set_base_situation(self, base_situations: list[int]):
        """
        Limit the data to only include PAs with certain base situations at the start of the play (e.g. if a runner on first steals second, the base situation would be 0b001 at the start of the play).

        Parameters:
        base_situation (list[int]): List of integers no more than 2^3 for the base situation. 0 is empty, 1 is occupied. For example, 0b111 = 7 = bases loaded, 0b000 = 0 = bases empty, 0b001 = 1 = runner on first, 0b100 = 4 = runner on third
        """
        if len(base_situations) == 0:
            self.sql_query_where.pop("start_bases_cd", None)
            return
        assert all((0 <= base_situation < 8) for base_situation in base_situations), "Invalid base situation"  # type: ignore
        self.sql_query_where["start_bases_cd"] = f"events.START_BASES_CD IN ({', '.join([str(base_situation) for base_situation in base_situations])})"

    def filter_stats_by_innings(self, home_team: str, conditions: list[dict], return_opposing_stats: bool = False):
        """
        Limit the data to only include games in which a certain stat is equal to a certain value at the start of a specific inning.
        Recommended to not be used with set_subdivision(player) as it will not have eg W/L records,
        but if you want to see how individual players perform when their team is doing well, you're welcome to.
        If a game did not reach an inning (eg bottom of 9th in home team win, or if a game ends early before the 9th inning) the game will not be included.

        Parameters:
        home_team (bool): Which team is batting—True for home team, False for away team
        conditions (list[dict]): List of dictionaries containing the following keys:
            - inning (int): The inning number
            - top (bool): True for top of the inning, False for bottom of the inning
            - stat (str): The stat to filter by (e.g. "OUTS_CT", "RUNS_CT")
            - value (int): The value to filter by
            - operator (str): The operator to use for filtering (default is "=")
        return_opposing_stats (bool): Whether to return the stats achieved by home_team or the opposing team. If True, will return the opposite of home_team.
        """
        col_names = []
        assert isinstance(conditions, list), "Invalid col_names type"
        assert home_team in ["home", "away", "either"], "Invalid value for home_team"
        for item in conditions:
            inning = item["inning"]
            top = item["top"]
            stat = item["stat"]
            value = item["value"]
            operator = item["operator"]
            assert operator in ["=", "<", ">", "<=", ">=", "!="], "Invalid operator"
            # Ensure input is santized
            assert isinstance(inning, int) and inning > 0, "Invalid inning number"
            assert isinstance(top, bool), "Invalid top/bottom inning value"
            assert isinstance(value, int), "Invalid value"
            assert all(c.isalnum() or c == "_" for c in stat), "Invalid stat name"


            # Create a WHERE clause to apply to subqueries (but we don't want subquery recursion)
            sql_where_str = self.sql_query_where["year"]
            col_name = f"{stat}_by_start_{'top' if top else 'bottom'}_{inning}"
            col_names.append(col_name)
            if isinstance(self, BattingStatSplits):
                isBatting = True
            elif isinstance(self, PitchingStatSplits):
                isBatting = False
            else:
                raise ValueError("Invalid StatSplits subclass. Must be BattingStatSplits or PitchingStatSplits.")

            if stat in ["SCORE", "SCORE_DIFF"]:
                if stat == "SCORE":
                    stat_home = "events.HOME_SCORE_CT"
                    stat_away = "events.AWAY_SCORE_CT"
                elif stat == "SCORE_DIFF":
                    stat_home = "events.HOME_SCORE_CT - events.AWAY_SCORE_CT"
                    stat_away = "events.AWAY_SCORE_CT - events.HOME_SCORE_CT"
                else:
                    raise ValueError()  # Should never happen, but just in case. If I forget to update the code when adding new values, it will raise an error
                if top:
                    # Top of the inning: home team hasn't batted yet in this inning
                    self.custom_table_cols[f"{col_name}_home"] = f"""
                        SELECT events.GAME_ID, FIRST_VALUE({stat_home}) OVER (
                            PARTITION BY events.GAME_ID
                            ORDER BY events.INN_CT DESC
                        ) as "{col_name}_home"
                        FROM events
                        WHERE {sql_where_str} AND events.INN_CT = {inning} and events.BAT_TEAM_ID != events.HOME_TEAM_ID
                        GROUP BY events.GAME_ID
                    """
                    # Away team: get the first value in this inning for the away team
                    self.custom_table_cols[f"{col_name}_away"] = f"""
                        SELECT events.GAME_ID, FIRST_VALUE({stat_away}) OVER (
                            PARTITION BY events.GAME_ID
                            ORDER BY events.INN_CT
                        ) as "{col_name}_away"
                        FROM events
                        WHERE {sql_where_str} AND events.INN_CT = {inning} and events.BAT_TEAM_ID != events.HOME_TEAM_ID
                        GROUP BY events.GAME_ID
                    """
                else:
                    # Bottom of the inning: get the first value in this inning for the home team
                    self.custom_table_cols[f"{col_name}_home"] = f"""
                        SELECT events.GAME_ID, FIRST_VALUE({stat_home}) OVER (
                            PARTITION BY events.GAME_ID
                            ORDER BY events.INN_CT
                        ) as "{col_name}_home"
                        FROM events
                        WHERE {sql_where_str} AND events.INN_CT = {inning} and events.BAT_TEAM_ID = events.HOME_TEAM_ID
                        GROUP BY events.GAME_ID
                    """
                    # Away team:
                    self.custom_table_cols[f"{col_name}_away"] = f"""
                        SELECT events.GAME_ID, FIRST_VALUE({stat_away}) OVER (
                            PARTITION BY events.GAME_ID
                            ORDER BY events.INN_CT DESC
                        ) as "{col_name}_away"
                        FROM events
                        WHERE {sql_where_str} AND events.INN_CT = {inning} and events.BAT_TEAM_ID != events.HOME_TEAM_ID
                        GROUP BY events.GAME_ID
                    """
            else:
                # Create two temporary columns—one to measure the home team and one to measure the away team
                # Since the home team bats in the bottom of the inning, the current inning will never be included
                self.custom_table_cols[f"{col_name}_home"] = f"""
                        SELECT events.GAME_ID, sum(CASE WHEN events.INN_CT < {inning} and events.BAT_TEAM_ID {"=" if isBatting else "!="} events.HOME_TEAM_ID THEN events.\"{stat}\" ELSE 0 END) as "{col_name}_home"
                        FROM events
                        WHERE {sql_where_str}
                        GROUP BY events.GAME_ID
                        HAVING SUM(CASE WHEN events.INN_CT = {inning} AND events.BAT_TEAM_ID {'!=' if top else '='} events.HOME_TEAM_ID THEN 1 ELSE 0 END) > 0
                """
                # Since the away team bats in the top of the inning, it will be included if we want the stats going into the bottom of the inning
                self.custom_table_cols[f"{col_name}_away"] = f"""
                        SELECT events.GAME_ID, sum(CASE WHEN events.INN_CT {"<" if top else "<="} {inning} and events.BAT_TEAM_ID {"!=" if isBatting else "="} events.HOME_TEAM_ID THEN events.\"{stat}\" ELSE 0 END) as "{col_name}_away"
                        FROM events
                        WHERE {sql_where_str}
                        GROUP BY events.GAME_ID
                        HAVING SUM(CASE WHEN events.INN_CT = {inning} AND events.BAT_TEAM_ID {'!=' if top else '='} events.HOME_TEAM_ID THEN 1 ELSE 0 END) > 0
                """

            if home_team == "home":
                self.sql_custom_columns_where[f"subquery_{col_name}_{home_team}_{top}_{value}_{operator}"] = f"\"{col_name}_home_cte\".\"{col_name}_home\" {operator} {value}"
            elif home_team == "away":
                self.sql_custom_columns_where[f"subquery_{col_name}_{home_team}_{top}_{value}_{operator}"] = f"\"{col_name}_away_cte\".\"{col_name}_away\" {operator} {value}"
            else:
                # If home_team is "either", we need to check both home and away teams
                self.sql_custom_columns_where[f"subquery_{col_name}_{home_team}_{top}_{value}_{operator}"] = f"(\"{col_name}_home_cte\".\"{col_name}_home\" {operator} {value} OR \"{col_name}_away_cte\".\"{col_name}_away\" {operator} {value})"

        # Only return stats for the relevant team (either the team which achieved the outcome or the opposing team)
        if (home_team == "home" and not return_opposing_stats) or (home_team == "away" and return_opposing_stats):
            if isinstance(self, BattingStatSplits):
                team_check_str = "events.HOME_TEAM_ID = events.BAT_TEAM_ID"
            elif isinstance(self, PitchingStatSplits):
                team_check_str = "events.HOME_TEAM_ID = events.FLD_TEAM_ID"
            else:
                raise ValueError("Invalid StatSplits subclass")
            self.sql_query_where["query_list_inning"] = team_check_str
        elif (home_team == "away" and not return_opposing_stats) or (home_team == "home" and return_opposing_stats):
            if isinstance(self, BattingStatSplits):
                team_check_str = "events.HOME_TEAM_ID != events.BAT_TEAM_ID"
            elif isinstance(self, PitchingStatSplits):
                team_check_str = "events.HOME_TEAM_ID != events.FLD_TEAM_ID"
            else:
                raise ValueError("Invalid StatSplits subclass")
            self.sql_query_where["query_list_inning"] = team_check_str
        elif home_team == "either":
            # Make sure we only include PAs where the team batting or pitching fulfills the condition
            if isinstance(self, BattingStatSplits):
                team_check_str = "events.BAT_TEAM_ID"
            elif isinstance(self, PitchingStatSplits):
                team_check_str = "events.FLD_TEAM_ID"
            else:
                raise ValueError("Invalid StatSplits subclass")
            self.sql_query_where["query_list_inning"] = f"""(
            (
                {team_check_str} {"=" if not return_opposing_stats else "!="} events.HOME_TEAM_ID AND
                {" AND ".join(f"\"{col_names[i]}_home_cte\".\"{col_names[i]}_home\" {item["operator"]} {item["value"]}" for i, item in enumerate(conditions))}
            ) OR (
                {team_check_str} {"!=" if not return_opposing_stats else "="} events.HOME_TEAM_ID AND
                {" AND ".join(f"\"{col_names[i]}_away_cte\".\"{col_names[i]}_away\" {item["operator"]} {item["value"]}" for i, item in enumerate(conditions))}
            ))"""

        # We want to get the win/loss record for these games. If "either" is selected, we want to get the win/loss record for any team that fulfilled the condition
        # Only select win/loss record if self.find is "team"
        if self.find != "team":
            return

        if (home_team == "home" and not return_opposing_stats) or (home_team == "away" and return_opposing_stats):
            self.custom_select[f"win" if isinstance(self, BattingStatSplits) else f"loss"] = f"""
                    CASE WHEN (cwgame.FINAL_HOME_SCORE_CT > cwgame.FINAL_AWAY_SCORE_CT) THEN events.GAME_ID ELSE NULL END
            """
            self.custom_select[f"loss" if isinstance(self, BattingStatSplits) else f"win"] = f"""
                    CASE WHEN (cwgame.FINAL_HOME_SCORE_CT < cwgame.FINAL_AWAY_SCORE_CT) THEN events.GAME_ID ELSE NULL END
            """
        elif (home_team == "away" and not return_opposing_stats) or (home_team == "home" and return_opposing_stats):
            self.custom_select[f"win" if isinstance(self, BattingStatSplits) else f"loss"] = f"""
                    CASE WHEN (cwgame.FINAL_AWAY_SCORE_CT > cwgame.FINAL_HOME_SCORE_CT) THEN events.GAME_ID ELSE NULL END
            """
            self.custom_select[f"loss" if isinstance(self, BattingStatSplits) else f"win"] = f"""
                    CASE WHEN (cwgame.FINAL_AWAY_SCORE_CT < cwgame.FINAL_HOME_SCORE_CT) THEN events.GAME_ID ELSE NULL END
            """
        elif home_team == "either":
            if isinstance(self, BattingStatSplits):
                self.custom_select[f"win" if isinstance(self, BattingStatSplits) else f"loss"] = f"""
                        CASE WHEN (events.BAT_TEAM_ID {"=" if not return_opposing_stats else "!="} events.HOME_TEAM_ID AND cwgame.FINAL_HOME_SCORE_CT {">" if not return_opposing_stats else "<"} cwgame.FINAL_AWAY_SCORE_CT AND {" AND ".join(f"\"{col_names[i]}_home_cte\".\"{col_names[i]}_home\" {item["operator"]} {item["value"]}" for i, item in enumerate(conditions))}) THEN (events.GAME_ID)
                        WHEN (events.BAT_TEAM_ID {"!=" if not return_opposing_stats else "="} events.HOME_TEAM_ID AND cwgame.FINAL_AWAY_SCORE_CT {">" if not return_opposing_stats else "<"} cwgame.FINAL_HOME_SCORE_CT AND {" AND ".join(f"\"{col_names[i]}_away_cte\".\"{col_names[i]}_away\" {item["operator"]} {item["value"]}" for i, item in enumerate(conditions))}) THEN (events.GAME_ID)
                        ELSE NULL END
                """
                self.custom_select[f"loss" if isinstance(self, BattingStatSplits) else f"win"] = f"""
                        CASE WHEN (events.BAT_TEAM_ID {"=" if not return_opposing_stats else "!="} events.HOME_TEAM_ID AND cwgame.FINAL_HOME_SCORE_CT {"<" if not return_opposing_stats else ">"} cwgame.FINAL_AWAY_SCORE_CT AND {" AND ".join(f"\"{col_names[i]}_home_cte\".\"{col_names[i]}_home\" {item["operator"]} {item["value"]}" for i, item in enumerate(conditions))}) THEN (events.GAME_ID)
                        WHEN (events.BAT_TEAM_ID {"!=" if not return_opposing_stats else "="} events.HOME_TEAM_ID AND cwgame.FINAL_AWAY_SCORE_CT {"<" if not return_opposing_stats else ">"} cwgame.FINAL_HOME_SCORE_CT AND {" AND ".join(f"\"{col_names[i]}_away_cte\".\"{col_names[i]}_away\" {item["operator"]} {item["value"]}" for i, item in enumerate(conditions))}) THEN (events.GAME_ID)
                        ELSE NULL END
                """
            else:
                self.custom_select[f"win"] = f"""
                        CASE WHEN (events.FLD_TEAM_ID {"=" if not return_opposing_stats else "!="} events.HOME_TEAM_ID AND cwgame.FINAL_HOME_SCORE_CT {">" if not return_opposing_stats else "<"} cwgame.FINAL_AWAY_SCORE_CT AND {" AND ".join(f"\"{col_names[i]}_home_cte\".\"{col_names[i]}_home\" {item["operator"]} {item["value"]}" for i, item in enumerate(conditions))}) THEN (events.GAME_ID)
                        WHEN (events.FLD_TEAM_ID {"!=" if not return_opposing_stats else "="} events.HOME_TEAM_ID AND cwgame.FINAL_AWAY_SCORE_CT {">" if not return_opposing_stats else "<"} cwgame.FINAL_HOME_SCORE_CT AND {" AND ".join(f"\"{col_names[i]}_away_cte\".\"{col_names[i]}_away\" {item["operator"]} {item["value"]}" for i, item in enumerate(conditions))}) THEN (events.GAME_ID)
                        ELSE NULL END
                """
                self.custom_select[f"loss"] = f"""
                        CASE WHEN (events.FLD_TEAM_ID {"=" if not return_opposing_stats else "!="} events.HOME_TEAM_ID AND cwgame.FINAL_HOME_SCORE_CT {"<" if not return_opposing_stats else ">"} cwgame.FINAL_AWAY_SCORE_CT AND {" AND ".join(f"\"{col_names[i]}_home_cte\".\"{col_names[i]}_home\" {item["operator"]} {item["value"]}" for i, item in enumerate(conditions))}) THEN (events.GAME_ID)
                        WHEN (events.FLD_TEAM_ID {"!=" if not return_opposing_stats else "="} events.HOME_TEAM_ID AND cwgame.FINAL_AWAY_SCORE_CT {"<" if not return_opposing_stats else ">"} cwgame.FINAL_HOME_SCORE_CT AND {" AND ".join(f"\"{col_names[i]}_away_cte\".\"{col_names[i]}_away\" {item["operator"]} {item["value"]}" for i, item in enumerate(conditions))}) THEN (events.GAME_ID)
                        ELSE NULL END
                """
        self.wins_included = True


class BattingStatSplits(StatSplits):
    def __init__(self, start_year: int = 0, end_year: int = 0, years_list: list[int] | None = None, events: pd.DataFrame | None = None):
        """
        Class to calculate batting splits.
        """
        super().__init__(start_year, end_year, years_list=years_list, events=events)
        self.batting_calculator: BattingStatsCalculator | None = None

    def calculate_stats(self):
        """
        Calculate batting stats based on the set splits.
        This method should be run after all splits have been set.
        """
        # Formulate "WHERE" clause for SQL query
        where_clauses = [self.sql_query_where[key] for key in self.sql_query_where]
        where_clause = " AND ".join(where_clauses)

        # Formulate custom columns for SQL query
        custom_clauses = [self.sql_custom_columns_where[key] for key in self.sql_custom_columns_where]
        custom_column_where = " AND ".join(custom_clauses)

        custom_cols = self.custom_table_cols

        custom_select = ", ".join([f"{col} AS \"{alias}\"" for alias, col in self.custom_select.items()])

        self.batting_calculator = BattingStatsCalculator(self.linear_weights, find=self.find, split=self.split, query_where=where_clause, custom_column_where=custom_column_where, custom_cols=custom_cols, custom_select=custom_select, wins_included=self.wins_included)  # type: ignore
        self.batting_calculator.calculate_all_stats()
        self.stats = self.batting_calculator.stats

    def set_score_diff(self, score_diff: list[int]):
        """
        Limit the data to only include PAs with a certain score difference (positive means batting team is leading).

        Parameters:
        score_diff (list[int]): Any integer for the score difference
        """
        if len(score_diff) == 0:
            self.sql_query_where.pop("score_diff", None)
            return
        assert all(isinstance(diff, int) for diff in score_diff), "Invalid score difference. Must be a list of integers."
        self.sql_query_where["score_diff"] = f"CASE WHEN events.BAT_TEAM_ID = events.HOME_TEAM_ID THEN (events.HOME_SCORE_CT - events.AWAY_SCORE_CT) ELSE (events.AWAY_SCORE_CT - events.HOME_SCORE_CT) END IN ({', '.join([str(diff) for diff in score_diff])})"


class PitchingStatSplits(StatSplits):
    def __init__(self, start_year: int = 0, end_year: int = 0, years_list: list[int] | None = None, events: pd.DataFrame | None = None):
        """
        Class to calculate pitching splits.
        """
        super().__init__(start_year, end_year, years_list=years_list, events=events)
        self.pitching_calculator: PitchingStatsCalculator | None = None

    def calculate_stats(self):
        """
        Calculate batting stats based on the set splits.
        This method should be run after all splits have been set.
        """
        # Formulate "WHERE" clause for SQL query
        where_clauses = [self.sql_query_where[key] for key in self.sql_query_where]
        where_clause = " AND ".join(where_clauses)

        # Formulate custom columns for SQL query
        custom_clauses = [self.sql_custom_columns_where[key] for key in self.sql_custom_columns_where]
        custom_column_where = " AND ".join(custom_clauses)

        custom_cols = self.custom_table_cols

        custom_select = ", ".join([f"{col} AS \"{alias}\"" for alias, col in self.custom_select.items()])

        self.pitching_calculator = PitchingStatsCalculator(self.linear_weights, find=self.find, split=self.split, query_where=where_clause, custom_cols=custom_cols, custom_column_where=custom_column_where, custom_select=custom_select, wins_included=self.wins_included)  # type: ignore
        self.pitching_calculator.calculate_all_stats()
        self.stats = self.pitching_calculator.stats

    def set_score_diff(self, score_diff: list[int]):
        """
        Limit the data to only include PAs with a certain score difference (positive means batting team is leading).

        Parameters:
        score_diff (list[int]): Any integer for the score difference
        """
        if len(score_diff) == 0:
            self.sql_query_where.pop("score_diff", None)
            return
        assert all(isinstance(diff, int) for diff in score_diff), "Invalid score difference. Must be a list of integers."
        self.sql_query_where["score_diff"] = f"CASE WHEN events.FLD_TEAM_ID = events.HOME_TEAM_ID THEN (events.HOME_SCORE_CT - events.AWAY_SCORE_CT) ELSE (events.AWAY_SCORE_CT - events.HOME_SCORE_CT) END IN ({', '.join([str(diff) for diff in score_diff])})"
