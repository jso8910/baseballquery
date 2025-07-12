import pandas as pd
from typing_extensions import override
import numpy as np
from .database import engine


class StatCalculator:
    def __init__(
        self,
        # events: pd.DataFrame
        linear_weights: pd.DataFrame,
        find: str = "player",
        split: str = "year",
        query_where: str = "",
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
        self.query_where = query_where

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
        # events: pd.DataFrame,
        linear_weights: pd.DataFrame,
        find: str = "player",
        split: str = "year",
        query_where: str = "",
    ):
        """
        Args:
            events (pd.DataFrame): A Pandas DataFrame that contains the events data.
            linear_weights (pd.DataFrame): A DataFrame that contains the linear weights for each event. Make sure that you have the linear weights for any year you're including in the events. If not, there will be an error.
            find (str): The split of the data. It can be "player" or "team".
            split (str): The split of the data. It can be "year", "month", "career", "day", or "game".
        """
        # super().__init__(events, linear_weights, find, split)
        super().__init__(linear_weights, find, split, query_where=query_where)
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
        # A list which contains the columns that are being grouped (based on split and find)
        to_group_by: list[str] = []
        if self.find == "player":
            to_group_by.append("events.RESP_BAT_ID")
        elif self.find == "team":
            to_group_by.append("events.BAT_TEAM_ID")

        if self.split == "year":
            to_group_by.append("events.year")
        elif self.split == "month":
            to_group_by.append("events.year")
            to_group_by.append("events.month")
        elif self.split == "day":
            to_group_by.append("events.year")
            to_group_by.append("events.month")
            to_group_by.append("events.day")
        elif self.split == "game":
            to_group_by.append("events.GAME_ID")

        if self.split == "year":
            query_select = """
            MIN(year) as year,
            NULL as month,
            NULL as day,
            NULL as game_id,
            """
        elif self.split == "month":
            query_select = """
            MIN(year) as year,
            MIN(month) as month,
            NULL as day,
            NULL as game_id,
            """
        elif self.split == "career":
            query_select = """
            NULL as year,
            NULL as month,
            NULL as day,
            NULL as game_id,
            """
        elif self.split == "day":
            query_select = """
            MIN(year) as year,
            MIN(month) as month,
            MIN(day) as day,
            NULL as game_id,
            """
        elif self.split == "game":
            query_select = """
            MIN(year) as year,
            MIN(month) as month,
            MIN(day) as day,
            MIN(GAME_ID) as game_id,
            """
        else:
            raise ValueError(f"split must be 'year', 'month', 'career', 'day', or 'game', not '{self.split}'")
        if self.find == "player":
            query_select = """
            MIN(events.RESP_BAT_ID) as player_id,
            NULL as team,
            """ + query_select
        elif self.find == "team":
            query_select = """
            NULL as player_id,
            MIN(events.BAT_TEAM_ID) as team,
            """ + query_select
        else:
            raise ValueError(f"find must be 'player' or 'team', not '{self.find}'")
        query = f"""
        SELECT
            {query_select}
            min(year) as start_year,
            max(year) as end_year,
            COUNT(DISTINCT events.GAME_ID) AS G, 
            SUM(events.PA) AS PA,
            SUM(events.AB) AS AB,
            SUM(events.H) AS H,
            SUM(events."1B") AS "1B",
            SUM(events."2B") AS "2B",
            SUM(events."3B") AS "3B",
            SUM(events.HR) AS HR,
            SUM(events.UBB) AS UBB,
            SUM(events.IBB) AS IBB,
            SUM(events.HBP) AS HBP,
            SUM(events.SF) AS SF,
            SUM(events.SH) AS SH,
            SUM(events.K) AS K,
            SUM(events.DP) AS DP,
            SUM(events.TP) AS TP,
            {"" if self.find == "player" else "SUM(SB) AS SB,"}
            {"" if self.find == "player" else "SUM(CS) AS CS,"}
            SUM(events.ROE) AS ROE,
            SUM(events.FC) AS FC,
            SUM(events.R) AS R,
            SUM(events.RBI) AS RBI,
            SUM(events.GB) AS GB,
            SUM(events.LD) AS LD,
            SUM(events.FB) AS FB,
            SUM(events.PU) AS PU
        FROM events
        LEFT JOIN cwgame ON events.GAME_ID = cwgame.GAME_ID
        WHERE {self.query_where}
        GROUP BY {", ".join(to_group_by)}
        """
        # Just for data display purposes
        to_group_original = to_group_by.copy()
        if "events.BAT_TEAM_ID" in to_group_by:
            to_group_by.remove("events.BAT_TEAM_ID")
            to_group_by.append("team")
        if "events.RESP_BAT_ID" in to_group_by:
            to_group_original.remove("events.RESP_BAT_ID")
            to_group_by.remove("events.RESP_BAT_ID")
            to_group_by.append("player_id")
        df = pd.read_sql(query, engine, index_col=[elem.split(".")[-1] for elem in to_group_by])  # type: ignore

        # Separate query for SB and CS if find is player
        if self.find == "player":
            to_group_original.append("baserunning.RESP_BAT_ID")
            query_select = query_select.replace("events.RESP_BAT_ID", "baserunning.RESP_BAT_ID")
            query_baserunning = f"""
            SELECT
                {query_select}
                SUM(baserunning.SB_indiv) AS SB,
                SUM(baserunning.CS_indiv) AS CS
            FROM baserunning
            LEFT JOIN cwgame ON baserunning.GAME_ID = cwgame.GAME_ID
            LEFT JOIN events ON events.file_index = baserunning.file_index AND events.GAME_ID = baserunning.GAME_ID
            WHERE {self.query_where}
            GROUP BY {", ".join(to_group_original)};
            """
            df_baserunning = pd.read_sql(query_baserunning, engine, index_col=[elem.split(".")[-1] for elem in to_group_by])
            # Merge the baserunning DataFrame with the main DataFrame
            df = df.merge(df_baserunning, how="left", on=[elem.split(".")[-1] for elem in to_group_by])
        df["SB"] = df["SB"].fillna(0).astype(int)
        df["CS"] = df["CS"].fillna(0).astype(int)

        self.stats = pd.DataFrame(df, columns=self.stats.columns)

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

        lg_woba_avg = stats_with_linear_weights["lg_woba_lw"]  # type: ignore
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
        linear_weights: pd.DataFrame,
        find: str = "player",
        split: str = "year",
        query_where: str = "",
    ):
        """
        Args:
            events (pd.DataFrame): A Pandas DataFrame that contains the events data.
            linear_weights (pd.DataFrame): A DataFrame that contains the linear weights for each event. Any rows other than the first row are ignored, so average the linear weights if necessary.
            find (str): The split of the data. It can be "player" or "team".
            split (str): The split of the data. It can be "year", "month", "career", "day", or "game".
        """
        super().__init__(linear_weights, find, split, query_where)

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
        # A list which contains the columns that are being grouped (based on split and find)
        to_group_by: list[str] = []
        if self.find == "player":
            to_group_by.append("events.RESP_PIT_ID")
        elif self.find == "team":
            to_group_by.append("events.FLD_TEAM_ID")

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
        if self.split == "year":
            query_select = """
            MIN(year) as year,
            NULL as month,
            NULL as day,
            NULL as game_id,
            """
        elif self.split == "month":
            query_select = """
            MIN(year) as year,
            MIN(month) as month,
            NULL as day,
            NULL as game_id,
            """
        elif self.split == "career":
            query_select = """
            NULL as year,
            NULL as month,
            NULL as day,
            NULL as game_id,
            """
        elif self.split == "day":
            query_select = """
            MIN(year) as year,
            MIN(month) as month,
            MIN(day) as day,
            NULL as game_id,
            """
        elif self.split == "game":
            query_select = """
            MIN(year) as year,
            MIN(month) as month,
            MIN(day) as day,
            MIN(GAME_ID) as game_id,
            """
        else:
            raise ValueError(f"split must be 'year', 'month', 'career', 'day', or 'game', not '{self.split}'")
        if self.find == "player":
            query_select = """
            MIN(events.RESP_PIT_ID) as player_id,
            NULL as team,
            """ + query_select
        elif self.find == "team":
            query_select = """
            NULL as player_id,
            MIN(events.FLD_TEAM_ID) as team,
            """ + query_select
        else:
            raise ValueError(f"find must be 'player' or 'team', not '{self.find}'")

        query = f"""
        SELECT
            {query_select}
            min(year) as start_year,
            max(year) as end_year,
            COUNT(DISTINCT events.GAME_ID) AS G, 
            COUNT(DISTINCT CASE WHEN events.RESP_PIT_START_FL = 1 THEN events.GAME_ID END) AS GS,
            SUM(events.EVENT_OUTS_CT) * 1.0 / 3.0 AS IP,
            SUM(events.PA) AS TBF,
            SUM(events.AB) AS AB,
            SUM(events.H) AS H,
            {"" if self.find == "player" else "SUM(events.R) AS R,"}
            {"" if self.find == "player" else "SUM(events.ER) AS ER,"}
            {"" if self.find == "player" else "SUM(events.UER) AS UER,"}
            SUM(events."1B") AS "1B",
            SUM(events."2B") AS "2B",
            SUM(events."3B") AS "3B",
            SUM(events.HR) AS HR,
            SUM(events.UBB) AS UBB,
            SUM(events.IBB) AS IBB,
            SUM(events.HBP) AS HBP,
            SUM(events.DP) AS DP,
            SUM(events.TP) AS TP,
            SUM(events.WP) AS WP,
            SUM(events.BK) AS BK,
            SUM(events.K) AS K,
            SUM(events.P) AS P,
            SUM(events.GB) AS GB,
            SUM(events.LD) AS LD,
            SUM(events.FB) AS FB,
            SUM(events.PU) AS PU,
            SUM(events.SH) AS SH,
            SUM(events.SF) AS SF
        FROM events
        LEFT JOIN cwgame ON events.GAME_ID = cwgame.GAME_ID
        WHERE {self.query_where}
        GROUP BY {", ".join(to_group_by)}
        """
        to_group_original = to_group_by.copy()
        if "events.FLD_TEAM_ID" in to_group_by:
            to_group_by.remove("events.FLD_TEAM_ID")
            to_group_by.append("team")
        if "events.RESP_PIT_ID" in to_group_by:
            to_group_original.remove("events.RESP_PIT_ID")
            to_group_by.remove("events.RESP_PIT_ID")
            to_group_by.append("player_id")
        df = pd.read_sql(query, engine, index_col=[elem.split(".")[-1] for elem in to_group_by])  # type: ignore

        # Separate query for R, UER, ER if find is player
        if self.find == "player":
            to_group_original.append("pitching_runs.RESP_PIT_ID")
            query_select = query_select.replace("events.RESP_PIT_ID", "pitching_runs.RESP_PIT_ID")
            query_run_scoring = f"""
            SELECT
                {query_select}
                SUM(pitching_runs.R_indiv) AS R,
                SUM(pitching_runs.ER_indiv) AS ER,
                SUM(pitching_runs.UER_indiv) AS UER
            FROM pitching_runs
            LEFT JOIN cwgame ON pitching_runs.GAME_ID = cwgame.GAME_ID
            LEFT JOIN events ON events.file_index = pitching_runs.file_index AND events.GAME_ID = pitching_runs.GAME_ID
            WHERE {self.query_where}
            GROUP BY {", ".join(to_group_original)};
            """
            df_run_scoring = pd.read_sql(query_run_scoring, engine, index_col=[elem.split(".")[-1] for elem in to_group_by])
            # Merge the run scoring DataFrame with the main DataFrame
            df = df.merge(df_run_scoring, how="left", on=[elem.split(".")[-1] for elem in to_group_by])
        df["R"] = df["R"].fillna(0).astype(int)
        df["ER"] = df["ER"].fillna(0).astype(int)
        df["UER"] = df["UER"].fillna(0).astype(int)

        self.stats = pd.DataFrame(df, columns=self.stats.columns)

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
