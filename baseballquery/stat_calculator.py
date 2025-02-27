import pandas as pd  # type: ignore
from tqdm import tqdm
from typing_extensions import override
import numpy as np


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

    def create_player_row(self, player_id: str = pd.NA, team: str = pd.NA, year: int = pd.NA, month: int = pd.NA, day: int = pd.NA, game_id: str = pd.NA):  # type: ignore
        column: dict[str, int | str | float] = {key: 0 for key in self.stats.columns}
        column["player_id"] = player_id
        column["team"] = team
        column["year"] = year
        column["month"] = month
        column["day"] = day
        column["game_id"] = game_id
        column["start_year"] = year
        column["end_year"] = year
        self.stats_l.append(column)


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

        self.stats: pd.DataFrame = pd.DataFrame(columns=self.info_columns + self.basic_stat_columns + self.calculated_stat_columns)  # type: ignore
        dtypes_dict = {}
        dtypes_dict.update({column: "object" for column in self.info_columns})  # type: ignore
        dtypes_dict.update({column: "int64" for column in self.basic_stat_columns})  # type: ignore
        dtypes_dict.update({column: "float64" for column in self.calculated_stat_columns})  # type: ignore
        self.stats = self.stats.astype(dtypes_dict)  # type: ignore
        self.stats_l = []

    @override
    def calculate_basic_stats(self):
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

        # Create a row for each player grouping
        groups = self.events.groupby(to_group_by)  # type: ignore
        to_group_by = [elem for elem in to_group_by if elem != "RESP_BAT_ID"]
        run1_groups = self.events[self.events["SB"] + self.events["CS"] != 0].groupby(["BASE1_RUN_ID"] + to_group_by)
        run2_groups = self.events[self.events["SB"] + self.events["CS"] != 0].groupby(["BASE2_RUN_ID"] + to_group_by)
        run3_groups = self.events[self.events["SB"] + self.events["CS"] != 0].groupby(["BASE3_RUN_ID"] + to_group_by)

        # Create a dictionary with the player_id as the key and a list of the groupings as the value
        groups_list = {}
        empty_df = pd.DataFrame(columns=self.events.columns)
        for n, group in groups:
            groups_list[n] = [empty_df] * 4
            groups_list[n][0] = group
        for n, group in run1_groups:
            if n in groups_list:
                groups_list[n][1] = group
            else:
                groups_list[n] = [empty_df] * 4
                groups_list[n][1] = group
        for n, group in run2_groups:
            if n in groups_list:
                groups_list[n][2] = group
            else:
                groups_list[n] = [empty_df] * 4
                groups_list[n][2] = group
        for n, group in run3_groups:
            if n in groups_list:
                groups_list[n][3] = group
            else:
                groups_list[n] = [empty_df] * 4
                groups_list[n][3] = group

        for group, run1_groups, run2_groups, run3_groups in tqdm(groups_list.values(), total=groups.ngroups):
            # Set year, month, day, and game_id based on the grouping and what's relevant. pd.NA is used for irrelevant columns (based on find and split)
            if not group.empty:
                group_with_info = group
            elif not run1_groups.empty:
                group_with_info = run1_groups
            elif not run2_groups.empty:
                group_with_info = run2_groups
            elif not run3_groups.empty:
                group_with_info = run3_groups
            if self.split == "year":
                year = group_with_info.iloc[0]["year"]  # type: ignore
                month = pd.NA
                day = pd.NA
                game_id = pd.NA
            elif self.split == "month":
                year = group_with_info.iloc[0]["year"]  # type: ignore
                month = group_with_info.iloc[0]["month"]  # type: ignore
                day = pd.NA
                game_id = pd.NA
            elif self.split == "career":
                year = pd.NA
                month = pd.NA
                day = pd.NA
                game_id = pd.NA
            elif self.split == "day":
                year = group_with_info.iloc[0]["year"]  # type: ignore
                month = group_with_info.iloc[0]["month"]  # type: ignore
                day = group_with_info.iloc[0]["day"]  # type: ignore
                game_id = pd.NA
            elif self.split == "game":
                year = group_with_info.iloc[0]["year"]  # type: ignore
                month = group_with_info.iloc[0]["month"]  # type: ignore
                day = group_with_info.iloc[0]["day"]  # type: ignore
                game_id = group_with_info.iloc[0]["GAME_ID"]  # type: ignore
            if self.find == "player":
                player_id = group_with_info.iloc[0]["RESP_BAT_ID"]  # type: ignore
                team = pd.NA
            elif self.find == "team":
                player_id = pd.NA
                team = group_with_info.iloc[0]["BAT_TEAM_ID"]  # type: ignore
            self.create_player_row(player_id, team, year, month, day, game_id)  # type: ignore
            player_row_idx = len(self.stats) - 1
            self.stats_l[player_row_idx]["start_year"] = group["year"].min()  # type: ignore
            self.stats_l[player_row_idx]["end_year"] = group["year"].max()  # type: ignore
            for stat in self.basic_stat_columns:
                # These need to be handled separately because they belong to a runner rather than a hitter
                # if stat in ["SB", "CS"] and self.find == "player":
                #     continue
                if stat == "SB" and self.find == "player":
                    self.stats_l[player_row_idx][stat] = run1_groups["RUN1_SB_FL"].sum() + run2_groups["RUN2_SB_FL"].sum() + run3_groups["RUN3_SB_FL"].sum()  # type: ignore
                    self.stats_l[player_row_idx]["SBO"] = 0
                    continue
                elif stat == "CS" and self.find == "player":
                    self.stats_l[player_row_idx][stat] = (
                        run1_groups["RUN1_CS_FL"].sum()
                        + run2_groups["RUN2_CS_FL"].sum()
                        + run3_groups["RUN3_CS_FL"].sum()
                    )
                    self.stats_l[player_row_idx]["CSO"] = 0
                    continue
                elif stat == "G":
                    # The number of games in this sample is the number of unique GAME_IDs
                    self.stats_l[player_row_idx][stat] = group["GAME_ID"].nunique()  # type: ignore
                    continue
                self.stats_l[player_row_idx][stat] = group[stat].sum()  # type: ignore
        self.stats = pd.DataFrame(self.stats_l, columns=self.stats.columns)  # type: ignore

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
    def create_player_row(self, player_id: str = pd.NA, team: str = pd.NA, year: int = pd.NA, month: int = pd.NA, day: int = pd.NA, game_id: str = pd.NA):  # type: ignore
        # This override is needed because IP isn't an integer
        column: dict[str, int | str | float] = {key: 0 for key in self.stats.columns}
        column["player_id"] = player_id
        column["team"] = team
        column["year"] = year
        column["month"] = month
        column["day"] = day
        column["game_id"] = game_id
        column["start_year"] = year
        column["end_year"] = year
        column["IP"] = 0.0
        self.stats_l.append(column)

    @override
    def calculate_basic_stats(self):
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
        groups = self.events.groupby(to_group_by)  # type: ignore
        to_group_by = [elem for elem in to_group_by if elem != "RESP_PIT_ID"]
        run1_groups = self.events[self.events["R"] != 0].groupby(["RUN1_RESP_PIT_ID"] + to_group_by)
        run2_groups = self.events[self.events["R"] != 0].groupby(["RUN2_RESP_PIT_ID"] + to_group_by)
        run3_groups = self.events[self.events["R"] != 0].groupby(["RUN3_RESP_PIT_ID"] + to_group_by)

        # Create a dictionary with the player_id as the key and a list of the groupings as the value
        groups_list = {}
        empty_df = pd.DataFrame(columns=self.events.columns)
        for n, group in groups:
            groups_list[n] = [empty_df] * 4
            groups_list[n][0] = group
        for n, group in run1_groups:
            if n in groups_list:
                groups_list[n][1] = group
            else:
                groups_list[n] = [empty_df] * 4
                groups_list[n][1] = group
        for n, group in run2_groups:
            if n in groups_list:
                groups_list[n][2] = group
            else:
                groups_list[n] = [empty_df] * 4
                groups_list[n][2] = group
        for n, group in run3_groups:
            if n in groups_list:
                groups_list[n][3] = group
            else:
                groups_list[n] = [empty_df] * 4
                groups_list[n][3] = group

        for group, run1_group, run2_group, run3_group in tqdm(groups_list.values(), total=groups.ngroups):
            # Set year, month, day, and game_id based on the grouping and what's relevant. pd.NA is used for irrelevant columns (based on find and split)
            if not group.empty:
                group_with_info = group
            elif not run1_group.empty:
                group_with_info = run1_group
            elif not run2_group.empty:
                group_with_info = run2_group
            elif not run3_group.empty:
                group_with_info = run3_group
            if self.split == "year":
                year = group_with_info.iloc[0]["year"]  # type: ignore
                month = pd.NA
                day = pd.NA
                game_id = pd.NA
            elif self.split == "month":
                year = group_with_info.iloc[0]["year"]  # type: ignore
                month = group_with_info.iloc[0]["month"]  # type: ignore
                day = pd.NA
                game_id = pd.NA
            elif self.split == "career":
                year = pd.NA
                month = pd.NA
                day = pd.NA
                game_id = pd.NA
            elif self.split == "day":
                year = group_with_info.iloc[0]["year"]  # type: ignore
                month = group_with_info.iloc[0]["month"]  # type: ignore
                day = group_with_info.iloc[0]["day"]  # type: ignore
                game_id = pd.NA
            elif self.split == "game":
                year = group_with_info.iloc[0]["year"]  # type: ignore
                month = group_with_info.iloc[0]["month"]  # type: ignore
                day = group_with_info.iloc[0]["day"]  # type: ignore
                game_id = group_with_info.iloc[0]["GAME_ID"]  # type: ignore
            if self.find == "player":
                player_id = group_with_info.iloc[0]["RESP_PIT_ID"]  # type: ignore
                team = pd.NA
            elif self.find == "team":
                player_id = pd.NA
                team = group_with_info.iloc[0]["FLD_TEAM_ID"]  # type: ignore
            self.create_player_row(player_id, team, year, month, day, game_id)  # type: ignore
            player_row_idx = len(self.stats_l) - 1
            self.stats_l[player_row_idx]["start_year"] = group["year"].min()  # type: ignore
            self.stats_l[player_row_idx]["end_year"] = group["year"].max()  # type: ignore
            for stat in self.basic_stat_columns:
                if stat == "R" and self.find == "player":
                    self.stats_l[player_row_idx][stat] = group[group["BAT_DEST_ID"] >= 4].shape[0] + run1_group[run1_group["RUN1_DEST_ID"] >= 4].shape[0] + run2_group[run2_group["RUN2_DEST_ID"] >= 4].shape[0] + run3_group[run3_group["RUN3_DEST_ID"] >= 4].shape[0]  # type: ignore
                    continue
                elif stat == "UER" and self.find == "player":
                    self.stats_l[player_row_idx][stat] = group[group["BAT_DEST_ID"].isin((5, 7))].shape[0] + run1_group[run1_group["RUN1_DEST_ID"].isin((5, 7))].shape[0] + run2_group[run2_group["RUN2_DEST_ID"].isin((5, 7))].shape[0] + run3_group[run3_group["RUN3_DEST_ID"].isin((5, 7))].shape[0]  # type: ignore
                    continue
                elif stat == "ER" and self.find == "player":
                    self.stats_l[player_row_idx][stat] = group[group["BAT_DEST_ID"].isin((4, 6))].shape[0] + run1_group[run1_group["RUN1_DEST_ID"].isin((4, 6))].shape[0] + run2_group[run2_group["RUN2_DEST_ID"].isin((4, 6))].shape[0] + run3_group[run3_group["RUN3_DEST_ID"].isin((4, 6))].shape[0]  # type: ignore
                    continue
                elif stat == "UER" and self.find == "team":
                    # This includes runs unearned for the team
                    self.stats_l[player_row_idx][stat] = group["UER"].sum() + group["T_UER"].sum()  # type: ignore
                    continue
                elif stat == "ER" and self.find == "team":
                    # This includes runs earned for the team (earned runs - team unearned runs)
                    self.stats_l[player_row_idx][stat] = group["ER"].sum() - group["T_UER"].sum()  # type: ignore
                    continue
                if stat == "G":
                    # The number of games in this sample is the number of unique GAME_IDs
                    self.stats_l[player_row_idx][stat] = group["GAME_ID"].nunique()  # type: ignore
                    continue
                if stat == "GS":
                    self.stats_l[player_row_idx][stat] = group[group["RESP_PIT_START_FL"] == True]["GAME_ID"].nunique()
                    continue
                if stat == "IP":
                    self.stats_l[player_row_idx][stat] = group["EVENT_OUTS_CT"].sum() / 3  # type: ignore
                    continue
                if stat == "TBF":
                    self.stats_l[player_row_idx][stat] = group["PA"].sum()  # type: ignore
                    continue
                self.stats_l[player_row_idx][stat] = group[stat].sum()  # type: ignore

        self.stats = pd.DataFrame(self.stats_l, columns=self.stats.columns)  # type: ignore

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
