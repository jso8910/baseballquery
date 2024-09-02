from sys import setswitchinterval
import h5py
from pathlib import Path
import pandas as pd
from .stat_calculator import BattingStatsCalculator, PitchingStatsCalculator

class StatSplits:
    def __init__(self, start_year: int, end_year: int):
        """
        Parent class. Should not be instantiated directly
        """
        cwd = Path(__file__).parent
        self.chadwick = cwd / "chadwick.hdf5"
        with h5py.File(self.chadwick) as f:
            years: list[str] = list(f.keys())

        if f"year_{start_year}" not in years:
            raise ValueError(f"Start year {start_year} not found in database")
        if f"year_{end_year}" not in years:
            raise ValueError(f"End year {end_year} not found in database")
        events_years_list = []
        for year in range(start_year, end_year + 1):
            events_years_list.append(pd.read_hdf(self.chadwick, f"year_{year}"))    # type: ignore

        cwd = Path(__file__).parent
        self.linear_weights = pd.read_csv(cwd / "linear_weights.csv")    # type: ignore
        self.events = pd.concat(events_years_list)  # type: ignore
        self.stats: pd.DataFrame|None = None
        self.split = "year"
        self.find = "player"

    def set_split(self, split: str):
        """
        Set the split to be used for calculating pitching stats.

        Parameters:
        split (str): 'year', 'month', 'career', 'game'
        """
        split = split.lower()
        assert split in ["year", "month", "career", "game"], f"Invalid split {split}. Valid splits are 'year', 'month', 'career', 'game'"
        self.split = split

    def set_subdivision(self, subdivision: str):
        """
        Set the sub-division to be used for calculating pitching stats.

        Parameters:
        subdivision (str): 'player' for individual players, 'team' for team totals
        """
        subdivision = subdivision.lower()
        assert subdivision in ["player", "team"], f"Invalid sub-division {subdivision}. Valid sub-divisions are 'player', 'team'"
        self.find = subdivision

    def set_days_of_week(self, days_of_week: list[str]):
        """
        Limit the data to only include games played on certain days of the week.

        Parameters:
        days_of_week (list): List of days of the week to include. Valid values are "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"
        """
        assert all(day.capitalize() in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"] for day in days_of_week), "Invalid day of week"
        for idx, day in enumerate(days_of_week):
            days_of_week[idx] = day.capitalize()
        self.events = self.events[pd.to_datetime(self.events["GAME_ID"].str.slice(3, -1)).dt.day_name().isin(days_of_week)]   # type: ignore

    def set_batter_handedness_pa(self, handedness: str):
        """
        Limit the data to only include plate appearances with batters hitting on a certain side of the plate.
        Switch hitters are considered the handedness they're currently hitting with.
        If data is unknown (not likely to happen after ~1970s or 1980s) it will be excluded.

        Parameters:
        handedness (str): 'R' for right-handed batters, 'L' for left-handed batters
        """
        handedness = handedness.upper()
        assert handedness in ["R", "L"], "Invalid handedness. Valid values are 'R' and 'L'"
        self.events = self.events[self.events["RESP_BAT_HAND_CD"] == handedness]    # type: ignore

    def set_batter_handedness(self, handedness: str):
        """
        The same as set_batter_handedness_pa, but allows for switch hitters. If a hitter has more than 5 PA from each side of the plate, they're considered switch.

        Parameters:
        handedness (str): 'R' for right-handed batters, 'L' for left-handed batters, 'S' for switch hitters
        """
        handedness = handedness.upper()
        assert handedness in ["R", "L", "S"], "Invalid handedness. Valid values are 'R', 'L', and 'S'"
        if handedness == "S":
            counts = self.events[self.events["RESP_BAT_ID"].values == self.events["RESP_BAT_ID"]]["RESP_BAT_HAND_CD"].value_counts()    # type: ignore
            self.events = self.events[(counts["L"] >= 5) & (counts["R"] >= 5)]    # type: ignore
        else:
            self.events = self.events[self.events["RESP_BAT_HAND_CD"] == handedness]    # type: ignore

    def set_pitcher_handedness(self, handedness: str):
        """
        Limit the data to only include plate appearances with pitchers pitching with a certain hand
        If data is unknown (not likely to happen after ~1970s or 1980s) it will be excluded.

        If someone is still using this by the time switch pitchers start to dominate pitching, open an issue on GitHub.

        Parameters:
        handedness (str): 'R' for right-handed pitchers, 'L' for left-handed pitchers
        """
        handedness = handedness.upper()
        assert handedness in ["R", "L", "S"], "Invalid handedness. Valid values are 'R' or 'L'"
        self.events = self.events[self.events["RESP_BAT_HAND_CD"] == handedness]    # type: ignore

    def set_batter_starter(self, starter: bool):
        """
        Limit the data to only include plate appearances with batters who started the game.

        Parameters:
        starter (bool): True for starters, False for non-starters
        """
        self.events = self.events[self.events["RESP_BAT_START_FL"] == starter]    # type: ignore

    def set_pitcher_starter(self, starter: bool):
        """
        Limit the data to only include plate appearances with pitchers who started the game.

        Parameters:
        starter (bool): True for starters, False for non-starters
        """
        self.events = self.events[self.events["RESP_PIT_START_FL"] == starter]    # type: ignore

    def set_batter_lineup_pos(self, lineup_pos: int):
        """
        Limit the data to only include plate appearances with batters who batted in a certain lineup position.

        Parameters:
        lineup_pos (int): 1-9 for lineup position
        """
        assert 1 <= lineup_pos <= 9, "Invalid lineup position"
        self.events = self.events[self.events["BAT_LINEUP_ID"] == lineup_pos]    # type: ignore

    def set_player_field_position(self, field_pos: int):
        """
        Limit the data to only include plate appearances with players who played a certain field position.

        Parameters:
        field_pos (int): 1-12 for field position.
            - 1-9 are the standard fielding positions, 10 is the DH, 11 is a pinch hitter, 12 is a pinch runner (this last one almost certainly will return 0 results)
        """
        assert 1 <= field_pos <= 12, "Invalid field position"
        self.events = self.events[self.events["BAT_FLD_CD"] == field_pos]    # type: ignore

    def set_batter_home(self, home: bool):
        """
        Limit the data to only include plate appearances with batters who batted at home or away.

        Parameters:
        home (bool): True for home, False for away
        """
        if home:
            self.events = self.events[self.events["HOME_TEAM_ID"] == self.events["BAT_TEAM_ID"]]    # type: ignore
        else:
            self.events = self.events[self.events["HOME_TEAM_ID"] != self.events["BAT_TEAM_ID"]]    # type: ignore

    def set_pitcher_home(self, home: bool):
        """
        Limit the data to only include plate appearances with pitchers who pitched at home or away.

        Parameters:
        home (bool): True for home, False for away
        """
        if home:
            self.events = self.events[self.events["HOME_TEAM_ID"] == self.events["FLD_TEAM_ID"]]    # type: ignore
        else:
            self.events = self.events[self.events["HOME_TEAM_ID"] != self.events["FLD_TEAM_ID"]]    # type: ignore

    def set_pitching_team(self, teams: list[str]):
        """
        Limit the data to only include games with certain teams pitching.

        Parameters:
        teams (list): List of team abbreviations (the retrosheet ones, e.g. "BOS", "NYA")
        """
        assert all(len(team) == 3 for team in teams), "Invalid team abbreviation"
        self.events = self.events[self.events["FLD_TEAM_ID"].isin(teams)]    # type: ignore

    def set_batting_team(self, teams: list[str]):
        """
        Limit the data to only include PAs with certain teams batting.

        Parameters:
        teams (list): List of team abbreviations (the retrosheet ones, e.g. "BOS", "NYA")
        """
        assert all(len(team) == 3 for team in teams), "Invalid team abbreviation"
        self.events = self.events[self.events["BAT_TEAM_ID"].isin(teams)]    # type: ignore

    def set_innings(self, innings: list[int]):
        """
        Limit the data to only include PAs with certain innings.

        Parameters:
        innings (list[int]): 1-infinity for the inning number
        """
        assert all(1 <= inning for inning in innings), "Invalid inning"
        self.events = self.events[self.events["INN_CT"].isin(innings)]    # type: ignore

    def set_outs(self, outs: list[int]):
        """
        Limit the data to only include PAs with certain number of outs.

        Parameters:
        outs (list[int]): 0-2 for the number of outs
        """
        assert all(0 <= out < 3 for out in outs), "Invalid number of outs"
        self.events = self.events[self.events["OUTS_CT"].isin(outs)]    # type: ignore

    def set_strikes(self, strikes: list[int]):
        """
        Limit the data to only include PAs that end with certain number of strikes (e.g. 3 for a strikeout, 1 for a hit on a 3-1 count).

        Parameters:
        strikes (list[int]): 0-3 for the number of strikes
        """
        assert all(0 <= strike <= 3 for strike in strikes), "Invalid number of strikes"
        self.events = self.events[self.events["STRIKES_CT"].isin(strikes)]    # type: ignore

    def set_balls(self, balls: list[int]):
        """
        Limit the data to only include PAs that end with certain number of balls (e.g. 4 for a walk, 3 for a hit on a 3-2 count).

        Parameters:
        balls (list[int]): 0-4 for the number of balls
        """
        assert all(0 <= ball <= 4 for ball in balls), "Invalid number of balls"
        self.events = self.events[self.events["BALLS_CT"].isin(balls)]    # type: ignore

    def set_score_diff(self, score_diff: list[int]):
        """
        Limit the data to only include PAs with a certain score difference (positive means home team is leading).

        Parameters:
        score_diff (list[int]): Any integer for the score difference
        """
        self.events = self.events[(self.events["HOME_SCORE_CT"] - self.events["AWAY_SCORE_CT"]).isin(score_diff)]    # type: ignore

    def set_home_score(self, scores: list[int]):
        """
        Limit the data to only include PAs with a certain home team score.

        Parameters:
        scores (list[int]): Any integer for the home team score
        """
        self.events = self.events[self.events["HOME_SCORE_CT"].isin(scores)]    # type: ignore

    def set_away_score(self, scores: list[int]):
        """
        Limit the data to only include PAs with a certain away team score.

        Parameters:
        scores (list[int]): Any integer for the away team score
        """
        self.events = self.events[self.events["AWAY_SCORE_CT"].isin(scores)]    # type: ignore

    def set_base_situation(self, base_situations: list[str]):
        """
        Limit the data to only include PAs with certain base situations at the start of the play (e.g. if a runner on first steals second, the base situation would be 0b001 at the start of the play).

        Parameters:
        base_situation (list[int]): List of integers no more than 2^3 for the base situation. 0 is empty, 1 is occupied. For example, 0b111 = 7 = bases loaded, 0b000 = 0 = bases empty, 0b001 = 1 = runner on first, 0b100 = 4 = runner on third
        """
        assert all((0 <= base_situation < 8) for base_situation in base_situations), "Invalid base situation"   # type: ignore
        self.events = self.events[self.events["START_BASES_CD"].isin(base_situations)]    # type: ignore

class BattingStatSplits(StatSplits):
    def __init__(self, start_year: int, end_year: int):
        """
        Class to calculate batting splits. Keep in mind that once you limit a split (other than "set_split" and "set_subdivision"), you cannot go back to the original data.
        """
        super().__init__(start_year, end_year)
        self.batting_calculator: BattingStatsCalculator|None = None

    def calculate_stats(self):
        """
        Calculate batting stats based on the set splits.
        This method should be run after all splits have been set.
        """

        self.batting_calculator = BattingStatsCalculator(self.events, self.linear_weights, find=self.find, split=self.split) # type: ignore
        self.batting_calculator.calculate_all_stats()
        self.stats = self.batting_calculator.stats


class PitchingStatSplits(StatSplits):
    def __init__(self, start_year: int, end_year: int):
        """
        Class to calculate pitching splits. Keep in mind that once you limit a split (other than "set_split" and "set_subdivision"), you cannot go back to the original data.
        """
        super().__init__(start_year, end_year)
        self.pitching_calculator: PitchingStatsCalculator|None = None 

    def calculate_stats(self):
        """
        Calculate batting stats based on the set splits.
        This method should be run after all splits have been set.
        """

        self.pitching_calculator = PitchingStatsCalculator(self.events, self.linear_weights, find=self.find, split=self.split) # type: ignore
        self.pitching_calculator.calculate_all_stats()
        self.stats = self.pitching_calculator.stats

