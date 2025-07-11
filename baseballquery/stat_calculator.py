import pandas as pd  # type: ignore
from tqdm import tqdm
from typing_extensions import override
import numpy as np
from functools import reduce


class StatCalculator:
    def __init__(
        self,
        events: pd.DataFrame,
        linear_weights: pd.DataFrame,
        find: str = "player",
        split: str = "year",
    ):
        """
        Parent class for all stat calculators. This class should not be instantiated directly.
        """
        self.info_columns = [  # Each column that isn't applicable (eg game_id if you set month) will be set to N/A
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
        self.events.loc[:, "month"] = self.events.loc[:, "GAME_ID"].str.slice(7, 9).astype(int)  # type: ignore
        self.events.loc[:, "day"] = self.events.loc[:, "GAME_ID"].str.slice(9, 11).astype(int)  # type: ignore
        for year in self.linear_weights["year"].unique():  # type: ignore
            if year not in self.linear_weights["year"].unique():  # type: ignore
                raise ValueError(
                    f"Linear weights must have values for all years in the events data. Missing year: {year}"
                )

        self.find = find
        if self.find not in ["player", "team"]:
            raise ValueError(f"find must be 'player' or 'team', not '{self.find}'")
        self.split = split
        if self.split not in ["year", "month", "career", "game"]:
            raise ValueError(f"split must be 'year', 'month', 'career', 'day', or 'game', not '{self.split}'")

        # Dummy self.stats DataFrame to be overwritten by the child class
        self.stats: pd.DataFrame = pd.DataFrame(columns=self.info_columns + self.basic_stat_columns + self.calculated_stat_columns)  # type: ignore
        self.stats_l = []

    def calculate_all_stats(self):
        self.calculate_basic_stats()
        self.calculate_advanced_stats()

    def calculate_basic_stats(self) -> None:
        raise NotImplementedError("calculate_basic_stats must be implemented in the child class.")

    def calculate_advanced_stats(self) -> None:
        raise NotImplementedError("calculate_advanced_stats must be implemented in the child class.")


class BattingStatsCalculator(StatCalculator):
    def __init__(
        self,
        events: pd.DataFrame,
        linear_weights: pd.DataFrame,
        find: str = "player",
        split: str = "year",
    ):
        """
        Args:
            events (pd.DataFrame): A Pandas DataFrame that contains the events data.
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
            "PU%",
        ]

        self.stats: pd.DataFrame = pd.DataFrame(columns=self.info_columns + self.basic_stat_columns + self.calculated_stat_columns)
        dtypes_dict = {}
        dtypes_dict.update({column: "object" for column in self.info_columns})
        dtypes_dict.update({column: "int64" for column in self.basic_stat_columns})
        dtypes_dict.update({column: "float64" for column in self.calculated_stat_columns})
        self.stats = self.stats.astype(dtypes_dict)
        self.stats_l = []

    @override
    def calculate_basic_stats(self):
        # Alias some columns to others to make gropupby.agg work
        self.events["player_id"] = self.events["RESP_BAT_ID"]
        self.events["team"] = self.events["BAT_TEAM_ID"]
        self.events["G"] = self.events["GAME_ID"]
        self.events["game_id"] = self.events["GAME_ID"]
        self.events["start_year"] = self.events["year"]
        self.events["end_year"] = self.events["year"]

        # A list which contains the columns that are being grouped (based on split and find)
        to_group_by: list[str] = []
        if self.find == "player":
            to_group_by.append("RESP_BAT_ID")
        elif self.find == "team":
            to_group_by.append("BAT_TEAM_ID")

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

        # If we are sorting by player, we need to create extra events for stolen bases (if runner A is on first base and steals second, we need to create an event for the player on first base stealing second with RESP_BAT_ID = A)
        if self.find == "player":
            stolen_first = self.events[self.events["RUN1_SB_FL"] != 0].copy()
            stolen_first["RESP_BAT_ID"] = stolen_first["BASE1_RUN_ID"]
            stolen_second = self.events[self.events["RUN2_SB_FL"] != 0].copy()
            stolen_second["RESP_BAT_ID"] = stolen_second["BASE2_RUN_ID"]
            stolen_third = self.events[self.events["RUN3_SB_FL"] != 0].copy()
            stolen_third["RESP_BAT_ID"] = stolen_third["BASE3_RUN_ID"]
            # Remove all other values (stats, etc) from the stolen bases events
            stolen_first.loc[:, self.basic_stat_columns] = 0
            stolen_second.loc[:, self.basic_stat_columns] = 0
            stolen_third.loc[:, self.basic_stat_columns] = 0
            # Set SB to 1 for the stolen bases events
            stolen_first["SB"] = 1
            stolen_second["SB"] = 1
            stolen_third["SB"] = 1

            # Set SB to 0 for the original events where the player is stealing a base
            self.events.loc[self.events["RUN1_SB_FL"] != 0, "SB"] = 0
            self.events.loc[self.events["RUN2_SB_FL"] != 0, "SB"] = 0
            self.events.loc[self.events["RUN3_SB_FL"] != 0, "SB"] = 0

            # Do the same for CS
            caught_first = self.events[self.events["RUN1_CS_FL"] != 0].copy()
            caught_first["RESP_BAT_ID"] = caught_first["BASE1_RUN_ID"]
            caught_second = self.events[self.events["RUN2_CS_FL"] != 0].copy()
            caught_second["RESP_BAT_ID"] = caught_second["BASE2_RUN_ID"]
            caught_third = self.events[self.events["RUN3_CS_FL"] != 0].copy()
            caught_third["RESP_BAT_ID"] = caught_third["BASE3_RUN_ID"]
            # Remove all other values (stats, etc) from the CS events
            caught_first.loc[:, self.basic_stat_columns] = 0
            caught_second.loc[:, self.basic_stat_columns] = 0
            caught_third.loc[:, self.basic_stat_columns] = 0
            # Set CS to 1 for the stolen bases events
            caught_first["CS"] = 1
            caught_second["CS"] = 1
            caught_third["CS"] = 1

            # Set CS to 0 for the original events where the player is stealing a base
            self.events.loc[self.events["RUN1_CS_FL"] != 0, "CS"] = 0
            self.events.loc[self.events["RUN2_CS_FL"] != 0, "CS"] = 0
            self.events.loc[self.events["RUN3_CS_FL"] != 0, "CS"] = 0
            self.events = pd.concat([self.events, stolen_first, stolen_second, stolen_third, caught_first, caught_second, caught_third], ignore_index=True)
            
        # Create a row for each player grouping
        groups = self.events.groupby(to_group_by)
        to_group_by = [elem for elem in to_group_by if elem != "RESP_BAT_ID"]

        if self.split == "year":
            year = "first"
            month = lambda _: pd.NA
            day = lambda _: pd.NA
            game_id = lambda _: pd.NA
        elif self.split == "month":
            year = "first"
            month = "first"
            day = lambda _: pd.NA
            game_id = lambda _: pd.NA
        elif self.split == "career":
            year = lambda _: pd.NA
            month = lambda _: pd.NA
            day = lambda _: pd.NA
            game_id = lambda _: pd.NA
        elif self.split == "day":
            year = "first"
            month = "first"
            day = "first"
            game_id = lambda _: pd.NA
        elif self.split == "game":
            year = "first"
            month = "first"
            day = "first"
            game_id = "first"
        else:
            raise ValueError(f"split must be 'year', 'month', 'career', 'day', or 'game', not '{self.split}'")
        if self.find == "player":
            player_id = "first"
            team = lambda _: pd.NA
        elif self.find == "team":
            player_id = lambda _: pd.NA
            team = "first"
        else: # Assume aggregating by both ig? But shouldn't happen
            player_id = "first"
            team = "first"

        self.stats_l = groups.agg({
            "player_id": player_id,
            "team": team,
            "year": year,
            "month": month,
            "day": day,
            "game_id": game_id,
            "start_year": "min",
            "end_year": "max",
            "G": "nunique",
            **{stat: "sum" for stat in self.basic_stat_columns if stat not in ["G"]},
        })

        self.stats = pd.DataFrame(self.stats_l, columns=self.stats.columns)
        del self.stats_l

    @override
    def calculate_advanced_stats(self):
        self.stats["AVG"] = self.stats["H"] / self.stats["AB"]
        self.stats["OBP"] = (self.stats["H"] + self.stats["UBB"] + self.stats["IBB"] + self.stats["HBP"]) / (
            self.stats["PA"]
        )
        self.stats["SLG"] = (
            self.stats["1B"] + 2 * self.stats["2B"] + 3 * self.stats["3B"] + 4 * self.stats["HR"]
        ) / self.stats["AB"]
        self.stats["OPS"] = self.stats["OBP"] + self.stats["SLG"]
        self.stats["ISO"] = self.stats["SLG"] - self.stats["AVG"]
        self.stats["BABIP"] = (self.stats["H"] - self.stats["HR"]) / (
            self.stats["AB"] - self.stats["K"] - self.stats["HR"] + self.stats["SF"]
        )
        self.stats["BB%"] = (self.stats["UBB"] + self.stats["IBB"]) / self.stats["PA"]
        self.stats["K%"] = self.stats["K"] / self.stats["PA"]
        self.stats["K/BB"] = self.stats["K%"] / self.stats["BB%"]

        # Add averaged linear weights to copy of dataframe
        # Thanks to e-motta on stack overflow for helping me out with this (https://stackoverflow.com/a/78937450/27155705)
        # The flaw is that it doesn't take into account the number of PAs per year, just a naive average
        year_range = np.array(range(self.linear_weights["year"].min(), self.linear_weights["year"].max() + 1))  # type: ignore
        # 3D boolean matrix to say which years should be added to the average for each player row
        m = (self.stats["start_year"].values <= year_range[:, None, None]) & (year_range[:, None, None] <= self.stats["end_year"].values)  # type: ignore
        # Aligning all the columns with the year_range
        values = self.linear_weights.set_index("year").reindex(year_range).values[:, :, None]  # type: ignore
        new_values = (values * m).sum(axis=0) / m.sum(axis=0)  # type: ignore
        stats_with_linear_weights = self.stats.copy()
        stats_with_linear_weights.loc[:, [f"{elem}_lw" for elem in self.linear_weights.columns[1:]]] = new_values.T  # type: ignore
        self.stats["wOBA"] = (
            # Calculate the mean of linear weights between the start and end year for the player
            # the flaw is that it doesn't take into account the number of PAs in each year
            stats_with_linear_weights["UBB_lw"] * stats_with_linear_weights["UBB"]
            + stats_with_linear_weights["HBP_lw"] * stats_with_linear_weights["HBP"]
            + stats_with_linear_weights["1B_lw"] * stats_with_linear_weights["1B"]
            + stats_with_linear_weights["2B_lw"] * stats_with_linear_weights["2B"]
            + stats_with_linear_weights["3B_lw"] * stats_with_linear_weights["3B"]
            + stats_with_linear_weights["HR_lw"] * stats_with_linear_weights["HR"]
        ) / (self.stats["PA"] - self.stats["IBB"])

        lg_woba_avg = stats_with_linear_weights["avg_woba_lw"]  # type: ignore
        lg_runs_pa = stats_with_linear_weights["lg_runs_pa_lw"]  # type: ignore
        # Average wRC per PA = runs per PA (since wOBA - league wOBA = 0)
        league_wrc_pa = stats_with_linear_weights["lg_runs_pa_lw"]  # type: ignore

        self.stats["wRAA"] = (
            (self.stats["wOBA"] - lg_woba_avg) / stats_with_linear_weights["woba_scale_lw"]
        ) * self.stats["PA"]
        self.stats["wRC"] = self.stats["wRAA"] + lg_runs_pa * self.stats["PA"]
        self.stats["wRC+"] = ((self.stats["wRC"] / self.stats["PA"]) / league_wrc_pa) * 100
        self.stats["GB%"] = self.stats["GB"] / (
            self.stats["GB"] + self.stats["LD"] + self.stats["FB"] + self.stats["PU"]
        )
        self.stats["LD%"] = self.stats["LD"] / (
            self.stats["GB"] + self.stats["LD"] + self.stats["FB"] + self.stats["PU"]
        )
        self.stats["FB%"] = self.stats["FB"] / (
            self.stats["GB"] + self.stats["LD"] + self.stats["FB"] + self.stats["PU"]
        )
        self.stats["PU%"] = self.stats["PU"] / (
            self.stats["GB"] + self.stats["LD"] + self.stats["FB"] + self.stats["PU"]
        )


class PitchingStatsCalculator(StatCalculator):
    def __init__(
        self,
        events: pd.DataFrame,
        linear_weights: pd.DataFrame,
        find: str = "player",
        split: str = "year",
    ):
        """
        Args:
            events (pd.DataFrame): A Pandas DataFrame that contains the events data.
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
            "SF",
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
            "LOB%",
        ]
        self.stats: pd.DataFrame = pd.DataFrame(columns=self.info_columns + self.basic_stat_columns + self.calculated_stat_columns)  # type: ignore
        dtypes_dict = {}
        dtypes_dict.update({column: "object" for column in self.info_columns})  # type: ignore
        dtypes_dict.update({column: "int" for column in self.basic_stat_columns})  # type: ignore
        dtypes_dict.update({column: "float" for column in self.calculated_stat_columns})  # type: ignore
        dtypes_dict["IP"] = "float"
        self.stats = self.stats.astype(dtypes_dict)  # type: ignore
        self.stats_l = []

    @override
    def calculate_basic_stats(self):
        # Convert the event outs to a float (so we can divide by 3 later)
        self.events["EVENT_OUTS_CT"] = self.events["EVENT_OUTS_CT"].astype(float)  # type: ignore

        # Alias some columns to others to make groupby.agg work
        self.events["player_id"] = self.events["RESP_BAT_ID"]
        self.events["team"] = self.events["BAT_TEAM_ID"]
        self.events["game_id"] = self.events["GAME_ID"]
        self.events["start_year"] = self.events["year"]
        self.events["end_year"] = self.events["year"]
        self.events["G"] = self.events["GAME_ID"]
        self.events.loc[self.events["RESP_PIT_START_FL"] == 1, "GS"] = self.events[self.events["RESP_PIT_START_FL"] == 1]["GAME_ID"]  # type: ignore
        self.events.loc[self.events["RESP_PIT_START_FL"] != 1, "GS"] = pd.NA  # type: ignore
        self.events["IP"] = self.events["EVENT_OUTS_CT"]
        self.events["TBF"] = self.events["PA"]

        # Add phantom events for run scoring
        # If a run scores, we need to add an event for the pitcher that is responsible for the run
        if self.find == "player":
            run_score_0 = self.events[self.events["BAT_DEST_ID"] >= 4].copy()
            run_score_1 = self.events[self.events["RUN1_DEST_ID"] >= 4].copy()
            run_score_2 = self.events[self.events["RUN2_DEST_ID"] >= 4].copy()
            run_score_3 = self.events[self.events["RUN3_DEST_ID"] >= 4].copy()
            # Set the RESP_PIT_ID to the pitcher that is responsible for the run
            run_score_1["RESP_PIT_ID"] = run_score_1["RUN1_RESP_PIT_ID"]
            run_score_2["RESP_PIT_ID"] = run_score_2["RUN2_RESP_PIT_ID"]
            run_score_3["RESP_PIT_ID"] = run_score_3["RUN3_RESP_PIT_ID"]
            # Remove all other values (stats, etc) from the run scoring events
            run_score_0.loc[:, self.basic_stat_columns] = 0
            run_score_1.loc[:, self.basic_stat_columns] = 0
            run_score_2.loc[:, self.basic_stat_columns] = 0
            run_score_3.loc[:, self.basic_stat_columns] = 0
            # Set R, ER, and UER to 1 for the run scoring events
            run_score_0["R"] = 1
            run_score_1["R"] = 1
            run_score_2["R"] = 1
            run_score_3["R"] = 1
            run_score_0.loc[run_score_0["BAT_DEST_ID"].isin((4, 6)), "ER"] = 1
            run_score_0.loc[run_score_0["BAT_DEST_ID"].isin((5, 7)), "UER"] = 1
            run_score_1.loc[run_score_1["RUN1_DEST_ID"].isin((4, 6)), "ER"] = 1
            run_score_1.loc[run_score_1["RUN1_DEST_ID"].isin((5, 7)), "UER"] = 1
            run_score_2.loc[run_score_2["RUN2_DEST_ID"].isin((4, 6)), "ER"] = 1
            run_score_2.loc[run_score_2["RUN2_DEST_ID"].isin((5, 7)), "UER"] = 1
            run_score_3.loc[run_score_3["RUN3_DEST_ID"].isin((4, 6)), "ER"] = 1
            run_score_3.loc[run_score_3["RUN3_DEST_ID"].isin((5, 7)), "UER"] = 1
            # Set R, ER, and UER to 0 for the original events where the player is scoring a run
            self.events.loc[self.events["BAT_DEST_ID"] >= 4, "R"] = 0
            self.events.loc[self.events["RUN1_DEST_ID"] >= 4, "R"] = 0
            self.events.loc[self.events["RUN2_DEST_ID"] >= 4, "R"] = 0
            self.events.loc[self.events["RUN3_DEST_ID"] >= 4, "R"] = 0

            self.events.loc[self.events["BAT_DEST_ID"] >= 4, "ER"] = 0
            self.events.loc[self.events["RUN2_DEST_ID"] >= 4, "ER"] = 0
            self.events.loc[self.events["RUN3_DEST_ID"] >= 4, "ER"] = 0
            self.events.loc[self.events["RUN1_DEST_ID"] >= 4, "ER"] = 0

            self.events.loc[self.events["BAT_DEST_ID"] >= 4, "UER"] = 0
            self.events.loc[self.events["RUN1_DEST_ID"] >= 4, "UER"] = 0
            self.events.loc[self.events["RUN2_DEST_ID"] >= 4, "UER"] = 0
            self.events.loc[self.events["RUN3_DEST_ID"] >= 4, "UER"] = 0
            # Concatenate the original events with the run scoring events
            self.events = pd.concat([self.events, run_score_0, run_score_1, run_score_2, run_score_3], ignore_index=True)
        elif self.find == "team":
            self.events["UER"] = self.events["UER"] + self.events["T_UER"]
            self.events["ER"] = self.events["ER"] - self.events["T_UER"]

        # A list which contains the columns that are being grouped (based on split and find)
        to_group_by: list[str] = []
        if self.find == "player":
            to_group_by.append("RESP_PIT_ID")
        elif self.find == "team":
            to_group_by.append("FLD_TEAM_ID")

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

        # Create a row for each player grouping
        groups = self.events.groupby(to_group_by)
        if self.split == "year":
            year = "first"
            month = lambda _: pd.NA
            day = lambda _: pd.NA
            game_id = lambda _: pd.NA
        elif self.split == "month":
            year = "first"
            month = "first"
            day = lambda _: pd.NA
            game_id = lambda _: pd.NA
        elif self.split == "career":
            year = lambda _: pd.NA
            month = lambda _: pd.NA
            day = lambda _: pd.NA
            game_id = lambda _: pd.NA
        elif self.split == "day":
            year = "first"
            month = "first"
            day = "first"
            game_id = lambda _: pd.NA
        elif self.split == "game":
            year = "first"
            month = "first"
            day = "first"
            game_id = "first"
        else:
            raise ValueError(f"split must be 'year', 'month', 'career', 'day', or 'game', not '{self.split}'")
        if self.find == "player":
            player_id = "first"
            team = lambda _: pd.NA
        elif self.find == "team":
            player_id = lambda _: pd.NA
            team = "first"
        else:
            player_id = "first"
            team = "first"

        self.stats_l = groups.agg({
            "player_id": player_id,
            "team": team,
            "year": year,
            "month": month,
            "day": day,
            "game_id": game_id,
            "start_year": "min",
            "end_year": "max",
            "G": "nunique",
            "GS": "nunique",
            **{stat: "sum" for stat in self.basic_stat_columns if stat not in ["G", "GS"]},
        })
        self.stats_l["IP"] = self.stats_l["IP"] / 3

        self.stats = pd.DataFrame(self.stats_l, columns=self.stats.columns)
        del self.stats_l

    @override
    def calculate_advanced_stats(self):
        # Add averaged linear weights to copy of dataframe
        # Thanks to e-motta on stack overflow for helping me out with this (https://stackoverflow.com/a/78937450/27155705)
        # The flaw is that it doesn't take into account the number of PAs per year, just a naive average
        year_range = np.array(range(self.linear_weights["year"].min(), self.linear_weights["year"].max() + 1))  # type: ignore
        # 3D boolean matrix to say which years should be added to the average for each player row
        m = (self.stats["start_year"].values <= year_range[:, None, None]) & (year_range[:, None, None] <= self.stats["end_year"].values)  # type: ignore
        # Aligning all the columns with the year_range
        values = self.linear_weights.set_index("year").reindex(year_range).values[:, :, None]  # type: ignore
        new_values = (values * m).sum(axis=0) / m.sum(axis=0)  # type: ignore
        stats_with_linear_weights = self.stats.copy()
        stats_with_linear_weights.loc[:, [f"{elem}_lw" for elem in self.linear_weights.columns[1:]]] = new_values.T  # type: ignore
        league_era = stats_with_linear_weights["lg_era_lw"]  # type: ignore
        fip_constant = stats_with_linear_weights["fip_constant_lw"]  # type: ignore
        lg_hr_fb = stats_with_linear_weights["lg_hr_fb_lw"]  # type: ignore

        self.stats["ERA"] = (self.stats["ER"] / self.stats["IP"]) * 9
        self.stats["FIP"] = (
            fip_constant
            + (
                13 * self.stats["HR"]
                + 3 * (self.stats["UBB"] + self.stats["IBB"] + self.stats["HBP"])
                - 2 * self.stats["K"]
            )
            / self.stats["IP"]
        )
        self.stats["xFIP"] = (
            fip_constant
            + (
                13 * (lg_hr_fb * (self.stats["FB"] + self.stats["PU"]))
                + 3 * (self.stats["UBB"] + self.stats["IBB"] + self.stats["HBP"])
                - 2 * self.stats["K"]
            )
            / self.stats["IP"]
        )
        self.stats["WHIP"] = (self.stats["H"] + self.stats["UBB"] + self.stats["IBB"]) / self.stats["IP"]

        self.stats["ERA-"] = (self.stats["ERA"] / league_era) * 100
        self.stats["FIP-"] = (self.stats["FIP"] / league_era) * 100
        self.stats["xFIP-"] = (self.stats["xFIP"] / league_era) * 100

        self.stats["BABIP"] = (self.stats["H"] - self.stats["HR"]) / (
            self.stats["AB"] - self.stats["K"] - self.stats["HR"] + self.stats["SF"]
        )
        self.stats["BB%"] = (self.stats["UBB"] + self.stats["IBB"]) / self.stats["TBF"]
        self.stats["K%"] = self.stats["K"] / self.stats["TBF"]
        self.stats["K-BB%"] = self.stats["K%"] - self.stats["BB%"]
        self.stats["K/BB"] = self.stats["K%"] / self.stats["BB%"]
        self.stats["BB/9"] = 9 * self.stats["UBB"] / self.stats["IP"]
        self.stats["K/9"] = 9 * self.stats["K"] / self.stats["IP"]
    
        self.stats["LOB%"] = (
            (self.stats["H"] + self.stats["UBB"] + self.stats["IBB"] + self.stats["HBP"] - self.stats["R"]) /
            (self.stats["H"] + self.stats["UBB"] + self.stats["IBB"] + self.stats["HBP"] - 1.4*self.stats["HR"])
        )

        self.stats["wOBA"] = (
            # Calculate the mean of linear weights between the start and end year for the player
            # the flaw is that it doesn't take into account the number of PAs in each year
            stats_with_linear_weights["UBB_lw"] * stats_with_linear_weights["UBB"]
            + stats_with_linear_weights["HBP_lw"] * stats_with_linear_weights["HBP"]
            + stats_with_linear_weights["1B_lw"] * stats_with_linear_weights["1B"]
            + stats_with_linear_weights["2B_lw"] * stats_with_linear_weights["2B"]
            + stats_with_linear_weights["3B_lw"] * stats_with_linear_weights["3B"]
            + stats_with_linear_weights["HR_lw"] * stats_with_linear_weights["HR"]
        ) / (self.stats["TBF"] - self.stats["IBB"])
        self.stats["HR/FB%"] = self.stats["HR"] / (self.stats["FB"] + self.stats["PU"])
