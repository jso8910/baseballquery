import pandas as pd
from tqdm import tqdm
import warnings
from pandas.errors import SettingWithCopyWarning
from typing_extensions import override
import numpy as np


class StatCalculator:
    def __init__(self, events: pd.DataFrame, linear_weights: pd.DataFrame, find: str="player", split: str="year"):
        """
        Parent class for all stat calculators. This class should not be instantiated directly.
        """
        warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)
        self.info_columns = [ # Each column that isn't applicable (eg game_id if you set month) will be set to N/A
            "player_id",
            "team",
            "year",
            "month",
            "day",
            "game_id",
            "start_year",
            "end_year",
        ]
        self.basic_stat_columns = []
        self.calculated_stat_columns = []
        self.linear_weights = linear_weights
        self.events = events
        self.events.loc[:, "year"] = self.events.loc[:, "GAME_ID"].str.slice(3, 7).astype(int)  # type: ignore
        self.events.loc[:, "month"] = self.events.loc[:, "GAME_ID"].str.slice(7, 9).astype(int)   # type: ignore
        self.events.loc[:, "day"] = self.events.loc[:, "GAME_ID"].str.slice(9, 11).astype(int)  # type: ignore
        for year in self.linear_weights["year"].unique():    # type: ignore
            if year not in self.linear_weights["year"].unique():    # type: ignore
                raise ValueError(f"Linear weights must have values for all years in the events data. Missing year: {year}")

        self.find = find
        if self.find not in ["player", "team"]:
            raise ValueError(f"find must be 'player' or 'team', not '{self.find}'")
        self.split = split
        if self.split not in ["year", "month", "career", "game"]:
            raise ValueError(f"split must be 'year', 'month', 'career', 'day', or 'game', not '{self.split}'")

        # Dummy self.stats DataFrame to be overwritten by the child class
        self.stats: pd.DataFrame = pd.DataFrame(columns=self.info_columns + self.basic_stat_columns + self.calculated_stat_columns) # type: ignore


    def calculate_all_stats(self):
        self.calculate_basic_stats()
        self.calculate_advanced_stats()


    def calculate_basic_stats(self) -> None:
        raise NotImplementedError("calculate_basic_stats must be implemented in the child class.")

    def calculate_advanced_stats(self) -> None:
        raise NotImplementedError("calculate_advanced_stats must be implemented in the child class.")


    def create_player_row(self, player_id: str = pd.NA, team: str = pd.NA, year: int = pd.NA, month: int = pd.NA, day: int = pd.NA, game_id: str = pd.NA):   # type: ignore
        self.stats.loc[len(self.stats)] = [player_id, team, year, month, day, game_id, year, year] + [0] * len(self.stats.columns[8:])    # type: ignore

    def get_player_row(self, player_id: str = pd.NA, team: str = pd.NA, year: int = pd.NA, month: int = pd.NA, day: int = pd.NA, game_id: str = pd.NA) -> int:    # type: ignore

        player_row = self.stats.index[  # type: ignore
            ((self.stats["player_id"] == player_id) | (pd.isna(self.stats["player_id"]) & pd.isna(player_id)))  # type: ignore
            & ((self.stats["team"] == team) | (pd.isna(self.stats["team"]) & pd.isna(team)))    # type: ignore
            & ((self.stats["year"] == year) | (pd.isna(self.stats["year"]) & pd.isna(year)))    # type: ignore
            & ((self.stats["month"] == month) | (pd.isna(self.stats["month"]) & pd.isna(month)))    # type: ignore
            & ((self.stats["day"] == day) | (pd.isna(self.stats["day"]) & pd.isna(day)))    # type: ignore
            & ((self.stats["game_id"] == game_id) | (pd.isna(self.stats["game_id"]) & pd.isna(game_id)))    # type: ignore
        ]
        if len(player_row) == 0:    # type: ignore
            self.stats.loc[len(self.stats)] = [player_id, team, year, month, day, game_id] + [0] * len(self.stats.columns[6:])    # type: ignore
            player_row = [self.stats.index[-1]]    # type: ignore

        return player_row[0]   # type: ignore

class BattingStatsCalculator(StatCalculator):
    def __init__(self, events: pd.DataFrame, linear_weights: pd.DataFrame, find: str="player", split: str="year"):
        """
        Args:
            events (dd.DataFrame): A Dask DataFrame that contains the events data.
            linear_weights (pd.DataFrame): A DataFrame that contains the linear weights for each event. Make sure that you have the linear weights for any year you're including in the events. If not, there will be an error.
            find (str): The split of the data. It can be "player" or "team".
            split (str): The split of the data. It can be "year", "month", "career", "day", or "game".
        """
        super().__init__(events, linear_weights, find, split)
        self.basic_stat_columns = [
            "G",
            "PA",
            "AB",
            "H",
            "1B",
            "2B",
            "3B",
            "HR",
            "UBB",
            "IBB",
            "HBP",
            "SF",
            "SH",
            "K",
            "DP",
            "TP",
            "SB",
            "CS",
            "ROE",
            "FC",
            "R",
            "RBI",
            "GB",
            "LD",
            "FB",
            "PU",
        ]
        self.calculated_stat_columns = [
            "AVG",
            "OBP",
            "SLG",
            "OPS",
            "ISO",
            "BABIP",
            "BB%",
            "K%",
            "K/BB",
            "wOBA",
            "wRAA",
            "wRC",
            "wRC+",
            "GB%",
            "LD%",
            "FB%",
            "PU%"
        ]

        self.stats: pd.DataFrame = pd.DataFrame(columns=self.info_columns + self.basic_stat_columns + self.calculated_stat_columns) # type: ignore
        dtypes_dict = {}
        dtypes_dict.update({column: "object" for column in self.info_columns}) # type: ignore
        dtypes_dict.update({column: "int64" for column in self.basic_stat_columns})    # type: ignore
        dtypes_dict.update({column: "float64" for column in self.calculated_stat_columns})   # type: ignore
        self.stats = self.stats.astype(dtypes_dict)    # type: ignore


    @override
    def calculate_basic_stats(self):
        # A list which contains the columns that are being grouped (based on split and find)
        to_group_by: list[str] = []
        if self.split == "year":
            to_group_by.append("year")
        elif self.split == "month":
            to_group_by.append("year")
            to_group_by.append("month")
        elif self.split == "day":
            to_group_by.append("year")
            to_group_by.append("month")
            to_group_by.append("day")
        elif self.split == "game":
            to_group_by.append("GAME_ID")

        if self.find == "player":
            to_group_by.append("RESP_BAT_ID")
        elif self.find == "team":
            to_group_by.append("BAT_TEAM_ID")

        # Create a row for each player grouping
        groups = self.events.groupby(to_group_by)   # type: ignore
        for _, group in tqdm(groups):
            # Set year, month, day, and game_id based on the grouping and what's relevant. pd.NA is used for irrelevant columns (based on find and split)
            if self.split == "year":
                year = group.iloc[0]["year"]    # type: ignore
                month = pd.NA
                day = pd.NA
                game_id = pd.NA
            elif self.split == "month":
                year = group.iloc[0]["year"]    # type: ignore
                month = group.iloc[0]["month"]  # type: ignore
                day = pd.NA
                game_id = pd.NA
            elif self.split == "career":
                year = pd.NA
                month = pd.NA
                day = pd.NA
                game_id = pd.NA
            elif self.split == "day":
                year = group.iloc[0]["year"]    # type: ignore
                month = group.iloc[0]["month"]  # type: ignore
                day = group.iloc[0]["day"]  # type: ignore
                game_id = pd.NA
            elif self.split == "game":
                year = group.iloc[0]["year"]    # type: ignore
                month = group.iloc[0]["month"]  # type: ignore
                day = group.iloc[0]["day"]  # type: ignore
                game_id = group.iloc[0]["GAME_ID"]  # type: ignore
            if self.find == "player":
                player_id = group.iloc[0]["RESP_BAT_ID"]    # type: ignore
                team = pd.NA
            elif self.find == "team":
                player_id = pd.NA
                team = group.iloc[0]["BAT_TEAM_ID"] # type: ignore
            self.create_player_row(player_id, team, year, month, day, game_id)   # type: ignore
            player_row_idx = len(self.stats) - 1
            self.stats.loc[player_row_idx, "start_year"] = group["year"].min()    # type: ignore
            self.stats.loc[player_row_idx, "end_year"] = group["year"].max()    # type: ignore
            for stat in self.basic_stat_columns:
                # These need to be handled separately because they belong to a runner rather than a hitter
                if stat in ["SB", "CS"] and self.find == "player":
                    continue
                if stat == "G":
                    # The number of games in this sample is the number of unique GAME_IDs
                    self.stats.loc[player_row_idx, stat] = group["GAME_ID"].nunique()    # type: ignore
                    continue
                self.stats.loc[player_row_idx, stat] = group[stat].sum()    # type: ignore

        # Calculate and correctly credit SBs and CSs
        if self.find == "player":
            sb_cs = self.events[(self.events["SB"] != 0) | (self.events["CS"] != 0)]    # type: ignore
            for _, event in tqdm(sb_cs.iterrows(), total=len(sb_cs)):   # type: ignore
                if self.split == "year":
                    year = event["year"]    # type: ignore
                    month = pd.NA
                    day = pd.NA
                    game_id = pd.NA
                elif self.split == "month":
                    year = event["year"]    # type: ignore
                    month = event["month"]  # type: ignore
                    day = pd.NA
                    game_id = pd.NA
                elif self.split == "career":
                    year = pd.NA
                    month = pd.NA
                    day = pd.NA
                    game_id = pd.NA
                elif self.split == "day":
                    year = event["year"]    # type: ignore
                    month = event["month"]  # type: ignore
                    day = event["day"]      # type: ignore
                    game_id = pd.NA
                elif self.split == "game":
                    year = event["year"]    # type: ignore
                    month = event["month"]  # type: ignore
                    day = event["day"]      # type: ignore
                    game_id = event["GAME_ID"]   # type: ignore
                if self.find == "player":
                    player_id = event["RESP_BAT_ID"]    # type: ignore
                    team = pd.NA
                elif self.find == "team":
                    player_id = pd.NA
                    team = event["BAT_TEAM_ID"] # type: ignore
                if event["RUN1_SB_FL"] == True:
                    runner_player_id = event["BASE1_RUN_ID"]    # type: ignore
                    runner_row_idx = self.get_player_row(runner_player_id, team, year, month, day, game_id)   # type: ignore
                    self.stats.loc[runner_row_idx, "SB"] += 1
                if event["RUN2_SB_FL"] == True:
                    runner_player_id = event["BASE2_RUN_ID"]    # type: ignore
                    runner_row_idx = self.get_player_row(runner_player_id, team, year, month, day, game_id)  # type: ignore
                    self.stats.loc[runner_row_idx, "SB"] += 1
                if event["RUN3_SB_FL"] == True:
                    runner_player_id = event["BASE3_RUN_ID"]    # type: ignore
                    runner_row_idx = self.get_player_row(runner_player_id, team, year, month, day, game_id) # type: ignore
                    self.stats.loc[runner_row_idx, "SB"] += 1
                if event["RUN1_CS_FL"] == True:
                    runner_player_id = event["BASE1_RUN_ID"]    # type: ignore
                    runner_row_idx = self.get_player_row(runner_player_id, team, year, month, day, game_id)  # type: ignore
                    self.stats.loc[runner_row_idx, "CS"] += 1
                if event["RUN2_CS_FL"] == True:
                    runner_player_id = event["BASE2_RUN_ID"]    # type: ignore
                    runner_row_idx = self.get_player_row(runner_player_id, team, year, month, day, game_id)  # type: ignore 
                    self.stats.loc[runner_row_idx, "CS"] += 1
                if event["RUN3_CS_FL"] == True:
                    runner_player_id = event["BASE3_RUN_ID"]    # type: ignore
                    runner_row_idx = self.get_player_row(runner_player_id, team, year, month, day, game_id) # type: ignore
                    self.stats.loc[runner_row_idx, "CS"] += 1

    @override
    def calculate_advanced_stats(self):
        self.stats["AVG"] = self.stats["H"] / self.stats["AB"]
        self.stats["OBP"] = (self.stats["H"] + self.stats["UBB"] + self.stats["IBB"] + self.stats["HBP"]) / (self.stats["PA"])
        self.stats["SLG"] = (self.stats["1B"] + 2 * self.stats["2B"] + 3 * self.stats["3B"] + 4 * self.stats["HR"]) / self.stats["AB"]
        self.stats["OPS"] = self.stats["OBP"] + self.stats["SLG"]
        self.stats["ISO"] = self.stats["SLG"] - self.stats["AVG"]
        self.stats["BABIP"] = (self.stats["H"] - self.stats["HR"]) / (self.stats["AB"] - self.stats["K"] - self.stats["HR"] + self.stats["SF"])
        self.stats["BB%"] = (self.stats["UBB"] + self.stats["IBB"]) / self.stats["PA"]
        self.stats["K%"] = self.stats["K"] / self.stats["PA"]
        self.stats["K/BB"] = self.stats["K%"] / self.stats["BB%"]

        # Add averaged linear weights to copy of dataframe
        # Thanks to e-motta on stack overflow for helping me out with this (https://stackoverflow.com/a/78937450/27155705) 
        # The flaw is that it doesn't take into account the number of PAs per year, just a naive average
        year_range = np.array(range(self.linear_weights["year"].min(), self.linear_weights["year"].max() + 1))  # type: ignore
        # 3D boolean matrix to say which years should be added to the average for each player row
        m = (self.stats["start_year"].values <= year_range[:, None, None]) & (year_range[:, None, None] <= self.stats["end_year"].values)   # type: ignore
        # Aligning all the columns with the year_range
        values = self.linear_weights.set_index("year").reindex(year_range).values[:, :, None]    # type: ignore
        new_values = ((values * m).sum(axis=0) / m.sum(axis=0))    # type: ignore
        stats_with_linear_weights = self.stats.copy()
        stats_with_linear_weights.loc[:, [f"{elem}_lw" for elem in self.linear_weights.columns[1:]]] = new_values.T    # type: ignore
        self.stats["wOBA"] = (
            # Calculate the mean of linear weights between the start and end year for the player
            # the flaw is that it doesn't take into account the number of PAs in each year
            stats_with_linear_weights["UBB_lw"] * stats_with_linear_weights["UBB"] +
            stats_with_linear_weights["HBP_lw"] * stats_with_linear_weights["HBP"] +
            stats_with_linear_weights["1B_lw"] * stats_with_linear_weights["1B"] +
            stats_with_linear_weights["2B_lw"] * stats_with_linear_weights["2B"] +
            stats_with_linear_weights["3B_lw"] * stats_with_linear_weights["3B"] +
            stats_with_linear_weights["HR_lw"] * stats_with_linear_weights["HR"]
        ) / (self.stats["PA"] - self.stats["IBB"])

        lg_woba_avg = stats_with_linear_weights["avg_woba_lw"]    # type: ignore
        lg_runs_pa = stats_with_linear_weights["lg_runs_pa_lw"]    # type: ignore
        # Average wRC per PA = runs per PA (since wOBA - league wOBA = 0)
        league_wrc_pa = stats_with_linear_weights["lg_runs_pa_lw"]    # type: ignore

        self.stats["wRAA"] = ((self.stats["wOBA"] - lg_woba_avg) / stats_with_linear_weights["woba_scale_lw"]) * self.stats["PA"]
        self.stats["wRC"] = (
            self.stats["wRAA"] + lg_runs_pa*self.stats["PA"]
        )
        self.stats["wRC+"] = ((self.stats["wRC"] / self.stats["PA"]) / league_wrc_pa) * 100
        self.stats["GB%"] = self.stats["GB"] / (self.stats["GB"] + self.stats["LD"] + self.stats["FB"] + self.stats["PU"])
        self.stats["LD%"] = self.stats["LD"] / (self.stats["GB"] + self.stats["LD"] + self.stats["FB"] + self.stats["PU"])
        self.stats["FB%"] = self.stats["FB"] / (self.stats["GB"] + self.stats["LD"] + self.stats["FB"] + self.stats["PU"])
        self.stats["PU%"] = self.stats["PU"] / (self.stats["GB"] + self.stats["LD"] + self.stats["FB"] + self.stats["PU"])


class PitchingStatsCalculator(StatCalculator):
    def __init__(self, events: pd.DataFrame, linear_weights: pd.DataFrame, find: str="player", split: str="year"):
        """
        Args:
            events (dd.DataFrame): A Dask DataFrame that contains the events data.
            linear_weights (pd.DataFrame): A DataFrame that contains the linear weights for each event. Any rows other than the first row are ignored, so average the linear weights if necessary.
            find (str): The split of the data. It can be "player" or "team".
            split (str): The split of the data. It can be "year", "month", "career", "day", or "game".
        """
        super().__init__(events, linear_weights, find, split)

        self.basic_stat_columns = [
            "G",
            "GS",
            "IP",
            "TBF",
            "AB",
            "H",
            # These 3 run ones need to be handled separately taking into account RUN_N_RESP_PIT_ID
            "R",
            "ER",
            "UER",

            "1B",
            "2B",
            "3B",
            "HR",
            "UBB",
            "IBB",
            "HBP",
            "DP",
            "TP",
            "WP",
            "BK",
            "K",
            "P",
            "GB",
            "LD",
            "FB",
            "PU",
            "SH",
            "SF"
        ]
        self.calculated_stat_columns = [
            "ERA",
            "FIP",
            "xFIP",
            "WHIP",
            "ERA-",
            "FIP-",
            "xFIP-",
            "BABIP",
            "BB%",
            "K%",
            "K-BB%",
            "K/BB",
            "BB/9",
            "K/9",
            "wOBA",
            "HR/FB%",
        ]
        self.stats: pd.DataFrame = pd.DataFrame(columns=self.info_columns + self.basic_stat_columns + self.calculated_stat_columns) # type: ignore
        dtypes_dict = {}
        dtypes_dict.update({column: "object" for column in self.info_columns}) # type: ignore
        dtypes_dict.update({column: "int64" for column in self.basic_stat_columns})    # type: ignore
        dtypes_dict.update({column: "float64" for column in self.calculated_stat_columns})   # type: ignore
        dtypes_dict["IP"] = "float64"
        self.stats = self.stats.astype(dtypes_dict)    # type: ignore

    @override
    def create_player_row(self, player_id: str = pd.NA, team: str = pd.NA, year: int = pd.NA, month: int = pd.NA, day: int = pd.NA, game_id: str = pd.NA):   # type: ignore
        # This override is needed because IP isn't an integer
        column = [player_id, team, year, month, day, game_id, year, year] + [0] * len(self.stats.columns[8:])    # type: ignore
        column[8 + self.basic_stat_columns.index("IP")] = 0.0   # type: ignore
        self.stats.loc[len(self.stats)] = column

    @override
    def calculate_basic_stats(self):
        # A list which contains the columns that are being grouped (based on split and find)
        to_group_by: list[str] = []
        if self.split == "year":
            to_group_by.append("year")
        elif self.split == "month":
            to_group_by.append("year")
            to_group_by.append("month")
        elif self.split == "day":
            to_group_by.append("year")
            to_group_by.append("month")
            to_group_by.append("day")
        elif self.split == "game":
            to_group_by.append("GAME_ID")

        if self.find == "player":
            to_group_by.append("RESP_PIT_ID")
        elif self.find == "team":
            to_group_by.append("FLD_TEAM_ID")

        # Create a row for each player grouping
        groups = self.events.groupby(to_group_by)   # type: ignore
        for _, group in tqdm(groups):
            # Set year, month, day, and game_id based on the grouping and what's relevant. pd.NA is used for irrelevant columns (based on find and split)
            if self.split == "year":
                year = group.iloc[0]["year"]    # type: ignore
                month = pd.NA
                day = pd.NA
                game_id = pd.NA
            elif self.split == "month":
                year = group.iloc[0]["year"]    # type: ignore
                month = group.iloc[0]["month"]  # type: ignore
                day = pd.NA
                game_id = pd.NA
            elif self.split == "career":
                year = pd.NA
                month = pd.NA
                day = pd.NA
                game_id = pd.NA
            elif self.split == "day":
                year = group.iloc[0]["year"]    # type: ignore
                month = group.iloc[0]["month"]  # type: ignore
                day = group.iloc[0]["day"]  # type: ignore
                game_id = pd.NA
            elif self.split == "game":
                year = group.iloc[0]["year"]    # type: ignore
                month = group.iloc[0]["month"]  # type: ignore
                day = group.iloc[0]["day"]  # type: ignore
                game_id = group.iloc[0]["GAME_ID"]  # type: ignore
            if self.find == "player":
                player_id = group.iloc[0]["RESP_PIT_ID"]    # type: ignore
                team = pd.NA
            elif self.find == "team":
                player_id = pd.NA
                team = group.iloc[0]["FLD_TEAM_ID"] # type: ignore
            self.create_player_row(player_id, team, year, month, day, game_id)   # type: ignore
            player_row_idx = len(self.stats) - 1
            self.stats.loc[player_row_idx, "start_year"] = group["year"].min()    # type: ignore
            self.stats.loc[player_row_idx, "end_year"] = group["year"].max()    # type: ignore
            for stat in self.basic_stat_columns:
                # These need to be handled separately because they may belong to another pitcher
                if stat in ["R", "UER", "ER"] and self.find == "player":
                    continue
                elif stat == "UER" and self.find == "team":
                    # This includes runs unearned for the team
                    self.stats.loc[player_row_idx, stat] = group["UER"].sum() + group["T_UER"].sum()    # type: ignore
                    continue
                elif stat == "ER" and self.find == "team":
                    # This includes runs earned for the team (earned runs - team unearned runs)
                    self.stats.loc[player_row_idx, stat] = group["ER"].sum() - group["T_UER"].sum()   # type: ignore
                    continue
                if stat == "G":
                    # The number of games in this sample is the number of unique GAME_IDs
                    self.stats.loc[player_row_idx, stat] = group["GAME_ID"].nunique()    # type: ignore
                    continue
                if stat == "GS":
                    for game_id in group["GAME_ID"].unique():    # type: ignore
                        game = group[group["GAME_ID"] == game_id]   # type: ignore
                        if game["PIT_START_FL"].iloc[0] == True:    # type: ignore
                            self.stats.loc[player_row_idx, stat] += 1
                    continue
                if stat == "IP":
                    self.stats.loc[player_row_idx, stat] = group["EVENT_OUTS_CT"].sum() / 3   # type: ignore
                    continue
                if stat == "TBF":
                    self.stats.loc[player_row_idx, stat] = group["PA"].sum()    # type: ignore
                    continue
                self.stats.loc[player_row_idx, stat] = group[stat].sum()    # type: ignore

        # Calculate and correctly credit runs
        if self.find == "player":
            runs_scored = self.events[(self.events["R"] != 0)]    # type: ignore
            for _, event in tqdm(runs_scored.iterrows(), total=len(runs_scored)): # type: ignore
                if self.split == "year":
                    year = event["year"]    # type: ignore
                    month = pd.NA
                    day = pd.NA
                    game_id = pd.NA
                elif self.split == "month":
                    year = event["year"]    # type: ignore
                    month = event["month"]  # type: ignore
                    day = pd.NA
                    game_id = pd.NA
                elif self.split == "career":
                    year = pd.NA
                    month = pd.NA
                    day = pd.NA
                    game_id = pd.NA
                elif self.split == "day":
                    year = event["year"]    # type: ignore
                    month = event["month"]  # type: ignore
                    day = event["day"]  # type: ignore
                    game_id = pd.NA
                elif self.split == "game":
                    year = event["year"]    # type: ignore
                    month = event["month"]  # type: ignore
                    day = event["day"]  # type: ignore
                    game_id = event["GAME_ID"]  # type: ignore
                if self.find == "player":
                    player_id = event["RESP_BAT_ID"]    # type: ignore
                    team = pd.NA
                elif self.find == "team":
                    player_id = pd.NA
                    team = event["BAT_TEAM_ID"] # type: ignore
                if event["BAT_DEST_ID"] >= 4:
                    pitcher_player_id = event["RESP_PIT_ID"]   # type: ignore
                    pitcher_row_idx = self.get_player_row(pitcher_player_id, team, year, month, day, game_id)   # type: ignore
                    self.stats.loc[pitcher_row_idx, "R"] += 1
                    if event["BAT_DEST_ID"] in [4, 6]:
                        self.stats.loc[pitcher_row_idx, "ER"] += 1 
                    else:
                        self.stats.loc[pitcher_row_idx, "UER"] += 1

                if event["RUN1_DEST_ID"] >= 4:
                    pitcher_player_id = event["RUN1_RESP_PIT_ID"]   # type: ignore
                    pitcher_row_idx = self.get_player_row(pitcher_player_id, team, year, month, day, game_id)   # type: ignore
                    self.stats.loc[pitcher_row_idx, "R"] += 1
                    if event["RUN1_DEST_ID"] in [4, 6]:
                        self.stats.loc[pitcher_row_idx, "ER"] += 1 
                    else:
                        self.stats.loc[pitcher_row_idx, "UER"] += 1

                if event["RUN2_DEST_ID"] >= 4:
                    pitcher_player_id = event["RUN2_RESP_PIT_ID"]   # type: ignore
                    pitcher_row_idx = self.get_player_row(pitcher_player_id, team, year, month, day, game_id)   # type: ignore
                    self.stats.loc[pitcher_row_idx, "R"] += 1
                    if event["RUN2_DEST_ID"] in [4, 6]:
                        self.stats.loc[pitcher_row_idx, "ER"] += 1 
                    else:
                        self.stats.loc[pitcher_row_idx, "UER"] += 1

                if event["RUN3_DEST_ID"] >= 4:
                    pitcher_player_id = event["RUN3_RESP_PIT_ID"]   # type: ignore
                    pitcher_row_idx = self.get_player_row(pitcher_player_id, team, year, month, day, game_id)   # type: ignore
                    self.stats.loc[pitcher_row_idx, "R"] += 1
                    if event["RUN3_DEST_ID"] in [4, 6]:
                        self.stats.loc[pitcher_row_idx, "ER"] += 1 
                    else:
                        self.stats.loc[pitcher_row_idx, "UER"] += 1

    @override
    def calculate_advanced_stats(self):
        # Add averaged linear weights to copy of dataframe
        # Thanks to e-motta on stack overflow for helping me out with this (https://stackoverflow.com/a/78937450/27155705) 
        # The flaw is that it doesn't take into account the number of PAs per year, just a naive average
        year_range = np.array(range(self.linear_weights["year"].min(), self.linear_weights["year"].max() + 1))  # type: ignore
        # 3D boolean matrix to say which years should be added to the average for each player row
        m = (self.stats["start_year"].values <= year_range[:, None, None]) & (year_range[:, None, None] <= self.stats["end_year"].values)   # type: ignore
        # Aligning all the columns with the year_range
        values = self.linear_weights.set_index("year").reindex(year_range).values[:, :, None]    # type: ignore
        new_values = ((values * m).sum(axis=0) / m.sum(axis=0))    # type: ignore
        stats_with_linear_weights = self.stats.copy()
        stats_with_linear_weights.loc[:, [f"{elem}_lw" for elem in self.linear_weights.columns[1:]]] = new_values.T    # type: ignore
        league_era = stats_with_linear_weights["lg_era_lw"]    # type: ignore
        fip_constant = stats_with_linear_weights["fip_constant_lw"]    # type: ignore
        lg_hr_fb = stats_with_linear_weights["lg_hr_fb_lw"]    # type: ignore

        self.stats["ERA"] = (self.stats["ER"] / self.stats["IP"]) * 9
        self.stats["FIP"] = fip_constant + (
            13*self.stats["HR"] +
            3*(self.stats["UBB"] + self.stats["IBB"] + self.stats["HBP"]) -
            2*self.stats["K"]
        ) / self.stats["IP"]
        self.stats["xFIP"] = fip_constant + (
            13*(lg_hr_fb * (self.stats["FB"] + self.stats["PU"]))+
            3*(self.stats["UBB"] + self.stats["IBB"] + self.stats["HBP"]) -
            2*self.stats["K"]
        ) / self.stats["IP"]
        self.stats["WHIP"] = (self.stats["H"] + self.stats["UBB"] + self.stats["IBB"]) / self.stats["IP"]

        self.stats["ERA-"] = (self.stats["ERA"] / league_era) * 100
        self.stats["FIP-"] = (self.stats["FIP"] / league_era) * 100
        self.stats["xFIP-"] = (self.stats["xFIP"] / league_era) * 100

        self.stats["BABIP"] = (self.stats["H"] - self.stats["HR"]) / (self.stats["AB"] - self.stats["K"] - self.stats["HR"] + self.stats["SF"])
        self.stats["BB%"] = (self.stats["UBB"] + self.stats["IBB"]) / self.stats["TBF"]
        self.stats["K%"] = self.stats["K"] / self.stats["TBF"]
        self.stats["K-BB%"] = self.stats["K%"] - self.stats["BB%"]
        self.stats["K/BB"] = self.stats["K%"] / self.stats["BB%"]
        self.stats["BB/9"] = 9 * self.stats["UBB"] / self.stats["IP"]
        self.stats["K/9"] = 9 * self.stats["K"] / self.stats["IP"]

        self.stats["wOBA"] = (
            # Calculate the mean of linear weights between the start and end year for the player
            # the flaw is that it doesn't take into account the number of PAs in each year
            stats_with_linear_weights["UBB_lw"] * stats_with_linear_weights["UBB"] +
            stats_with_linear_weights["HBP_lw"] * stats_with_linear_weights["HBP"] +
            stats_with_linear_weights["1B_lw"] * stats_with_linear_weights["1B"] +
            stats_with_linear_weights["2B_lw"] * stats_with_linear_weights["2B"] +
            stats_with_linear_weights["3B_lw"] * stats_with_linear_weights["3B"] +
            stats_with_linear_weights["HR_lw"] * stats_with_linear_weights["HR"]
        ) / (self.stats["TBF"] - self.stats["IBB"])
        self.stats["HR/FB%"] = self.stats["HR"] / (self.stats["FB"] + self.stats["PU"])
