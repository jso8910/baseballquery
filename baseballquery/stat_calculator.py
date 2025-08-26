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
        custom_column_where: str = "",
        custom_cols: dict = {},
        custom_select: str = "",
        wins_included: bool = False,
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
        self.custom_cols = custom_cols
        self.custom_column_where = custom_column_where
        self.custom_select = custom_select
        self.wins_included = wins_included

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
        custom_column_where: str = "",
        custom_cols: dict = {},
        custom_select: str = "",
        wins_included: bool = False,
    ):
        """
        Args:
            events (pd.DataFrame): A Pandas DataFrame that contains the events data.
            linear_weights (pd.DataFrame): A DataFrame that contains the linear weights for each event. Make sure that you have the linear weights for any year you're including in the events_custom. If not, there will be an error.
            find (str): The split of the data. It can be "player" or "team".
            split (str): The split of the data. It can be "year", "month", "career", "day", or "game".
        """
        super().__init__(linear_weights, find, split, query_where=query_where, custom_column_where=custom_column_where, custom_cols=custom_cols, custom_select=custom_select, wins_included=wins_included)
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
            to_group_by.append("events_custom.RESP_BAT_ID")
        elif self.find == "team":
            to_group_by.append("events_custom.BAT_TEAM_ID")

        if self.split == "year":
            to_group_by.append("events_custom.year")
        elif self.split == "month":
            to_group_by.append("events_custom.year")
            to_group_by.append("events_custom.month")
        elif self.split == "day":
            to_group_by.append("events_custom.year")
            to_group_by.append("events_custom.month")
            to_group_by.append("events_custom.day")
        elif self.split == "game":
            to_group_by.append("events_custom.GAME_ID")

        if self.split == "year":
            query_select = """
            MIN(events_custom.year) as year,
            NULL as month,
            NULL as day,
            NULL as game_id,
            """
        elif self.split == "month":
            query_select = """
            MIN(events_custom.year) as year,
            MIN(events_custom.month) as month,
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
            MIN(events_custom.year) as year,
            MIN(events_custom.month) as month,
            MIN(events_custom.day) as day,
            NULL as game_id,
            """
        elif self.split == "game":
            query_select = """
            MIN(events_custom.year) as year,
            MIN(events_custom.month) as month,
            MIN(events_custom.day) as day,
            MIN(events_custom.GAME_ID) as game_id,
            """
        else:
            raise ValueError(f"split must be 'year', 'month', 'career', 'day', or 'game', not '{self.split}'")
        if self.find == "player":
            if self.split == "game":
                query_select = """
                MIN(events_custom.RESP_BAT_ID) as player_id,
                MIN(events_custom.BAT_TEAM_ID) as team,
                """ + query_select
            else:
                query_select = """
                MIN(events_custom.RESP_BAT_ID) as player_id,
                CASE WHEN COUNT(DISTINCT events_custom.BAT_TEAM_ID) = 1 THEN MIN(events_custom.BAT_TEAM_ID) ELSE COUNT(DISTINCT events_custom.BAT_TEAM_ID) || " Teams" END as team,
                """ + query_select
        elif self.find == "team":
            query_select = """
            NULL as player_id,
            MIN(events_custom.BAT_TEAM_ID) as team,
            """ + query_select
        else:
            raise ValueError(f"find must be 'player' or 'team', not '{self.find}'")
        query = f"""
        WITH {", ".join([f"\"{alias}_cte\" AS ({col})" for alias, col in self.custom_cols.items()])}{", " if self.custom_cols else ""} events_custom AS (
            SELECT events.*{", " + self.custom_select if self.custom_select else ""}
            FROM events
            LEFT JOIN cwgame ON events.GAME_ID = cwgame.GAME_ID
            {" ".join([f"LEFT JOIN \"{alias}_cte\" ON \"{alias}_cte\".GAME_ID = events.GAME_ID" for alias, _ in self.custom_cols.items()])}
            WHERE {self.query_where}{" AND " + self.custom_column_where if self.custom_column_where else ""}  -- Filter by query_where and custom_column_where
        )
        SELECT
            {query_select}
            min(events_custom.year) as start_year,
            max(events_custom.year) as end_year,
            {"count(DISTINCT events_custom.win)" if self.wins_included else "NULL"} as win,
            {"count(DISTINCT events_custom.loss)" if self.wins_included else "NULL"} as loss,
            COUNT(DISTINCT events_custom.GAME_ID) AS G, 
            SUM(events_custom.PA) AS PA,
            SUM(events_custom.AB) AS AB,
            SUM(events_custom.H) AS H,
            SUM(events_custom."1B") AS "1B",
            SUM(events_custom."2B") AS "2B",
            SUM(events_custom."3B") AS "3B",
            SUM(events_custom.HR) AS HR,
            SUM(events_custom.UBB) AS UBB,
            SUM(events_custom.IBB) AS IBB,
            SUM(events_custom.HBP) AS HBP,
            SUM(events_custom.SF) AS SF,
            SUM(events_custom.SH) AS SH,
            SUM(events_custom.K) AS K,
            SUM(events_custom.DP) AS DP,
            SUM(events_custom.TP) AS TP,
            {"" if self.find == "player" else "SUM(SB) AS SB,"}
            {"" if self.find == "player" else "SUM(CS) AS CS,"}
            SUM(events_custom.ROE) AS ROE,
            SUM(events_custom.FC) AS FC,
            SUM(events_custom.R) AS R,
            SUM(events_custom.RBI) AS RBI,
            SUM(events_custom.GB) AS GB,
            SUM(events_custom.LD) AS LD,
            SUM(events_custom.FB) AS FB,
            SUM(events_custom.PU) AS PU
        FROM events_custom
        LEFT JOIN cwgame ON events_custom.GAME_ID = cwgame.GAME_ID
        GROUP BY {", ".join(to_group_by)}
        """
        # Just for data display purposes
        to_group_original = to_group_by.copy()
        if "events_custom.BAT_TEAM_ID" in to_group_by:
            to_group_by.remove("events_custom.BAT_TEAM_ID")
            to_group_by.append("team")
        if "events_custom.RESP_BAT_ID" in to_group_by:
            to_group_original.remove("events_custom.RESP_BAT_ID")
            to_group_by.remove("events_custom.RESP_BAT_ID")
            to_group_by.append("player_id")

        if self.split == "game":
            to_group_by.remove("events_custom.GAME_ID")
            to_group_by.append("events_custom.game_id")
        df = pd.read_sql(query, engine, index_col=[elem.split(".")[-1] for elem in to_group_by])  # type: ignore

        # Separate query for SB and CS if find is player
        if self.find == "player":
            # The baserunning table's RESP_BAT_ID is NOT the same as events_custom.RESP_BAT_ID (baserunner is not the same as the real batter)
            # I probably shouldn't have used RESP_BAT_ID in this table, but it comes from when I did this a different way with Pandas
            # So, these two lines are both VERY important
            to_group_original.append("baserunning.RESP_BAT_ID")
            query_select = query_select.replace("events_custom.RESP_BAT_ID", "baserunning.RESP_BAT_ID")
            query_baserunning = f"""
            WITH {", ".join([f"\"{alias}_cte\" AS ({col})" for alias, col in self.custom_cols.items()])}{", " if self.custom_cols else ""} events_custom AS (
                SELECT events.*{", " + self.custom_select if self.custom_select else ""}
                FROM events
                LEFT JOIN cwgame ON events.GAME_ID = cwgame.GAME_ID
                {" ".join([f"LEFT JOIN \"{alias}_cte\" ON \"{alias}_cte\".GAME_ID = events.GAME_ID" for alias, _ in self.custom_cols.items()])}
                WHERE {self.query_where}{" AND " + self.custom_column_where if self.custom_column_where else ""}  -- Filter by query_where and custom_column_where
            )
            SELECT
                {query_select}
                SUM(baserunning.SB_indiv) AS SB,
                SUM(baserunning.CS_indiv) AS CS
            FROM baserunning
            LEFT JOIN cwgame ON baserunning.GAME_ID = cwgame.GAME_ID
            LEFT JOIN events_custom ON events_custom.file_index = baserunning.file_index AND events_custom.GAME_ID = baserunning.GAME_ID
            WHERE {self.query_where.replace("events", "events_custom")}
            GROUP BY {", ".join(to_group_original)};
            """
            df_baserunning = pd.read_sql(query_baserunning, engine, index_col=[elem.split(".")[-1] for elem in to_group_by])
            # Merge the baserunning DataFrame with the main DataFrame
            df = df.merge(df_baserunning, how="left", on=["year", "player_id", "team", "month", "day", "game_id"])

        df["SB"] = df["SB"].fillna(0).infer_objects()
        df["CS"] = df["CS"].fillna(0).infer_objects()

        self.stats = df.sort_values(by=[elem.split(".")[-1] for elem in to_group_by])

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
        custom_column_where: str = "",
        custom_cols: dict = {},
        custom_select: str = "",
        wins_included: bool = False,
    ):
        """
        Args:
            events (pd.DataFrame): A Pandas DataFrame that contains the events data.
            linear_weights (pd.DataFrame): A DataFrame that contains the linear weights for each event. Any rows other than the first row are ignored, so average the linear weights if necessary.
            find (str): The split of the data. It can be "player" or "team".
            split (str): The split of the data. It can be "year", "month", "career", "day", or "game".
        """
        super().__init__(linear_weights, find, split, query_where, custom_column_where=custom_column_where, custom_cols=custom_cols, custom_select=custom_select, wins_included=wins_included)

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
            "GB%",
            "LD%",
            "FB%",
            "PU%",
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
            to_group_by.append("events_custom.RESP_PIT_ID")
        elif self.find == "team":
            to_group_by.append("events_custom.FLD_TEAM_ID")

        if self.split == "year":
            to_group_by.append("events_custom.year")
        elif self.split == "month":
            to_group_by.append("events_custom.year")
            to_group_by.append("events_custom.month")
        elif self.split == "day":
            to_group_by.append("events_custom.year")
            to_group_by.append("events_custom.month")
            to_group_by.append("events_custom.day")
        elif self.split == "game":
            to_group_by.append("events_custom.GAME_ID")

        # Create a row for each player grouping
        if self.split == "year":
            query_select = """
            MIN(events_custom.year) as year,
            NULL as month,
            NULL as day,
            NULL as game_id,
            """
        elif self.split == "month":
            query_select = """
            MIN(events_custom.year) as year,
            MIN(events_custom.month) as month,
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
            MIN(events_custom.year) as year,
            MIN(events_custom.month) as month,
            MIN(events_custom.day) as day,
            NULL as game_id,
            """
        elif self.split == "game":
            query_select = """
            MIN(events_custom.year) as year,
            MIN(events_custom.month) as month,
            MIN(events_custom.day) as day,
            MIN(events_custom.GAME_ID) as game_id,
            """
        else:
            raise ValueError(f"split must be 'year', 'month', 'career', 'day', or 'game', not '{self.split}'")
        if self.find == "player":
            if self.split == "game":
                query_select = """
                MIN(events_custom.RESP_PIT_ID) as player_id,
                MIN(events_custom.FLD_TEAM_ID) as team,
                """ + query_select
            else:
                query_select = """
                MIN(events_custom.RESP_PIT_ID) as player_id,
                CASE WHEN COUNT(DISTINCT events_custom.FLD_TEAM_ID) = 1 THEN MIN(events_custom.FLD_TEAM_ID) ELSE COUNT(DISTINCT events_custom.FLD_TEAM_ID) || " Teams" END as team,
                """ + query_select
        elif self.find == "team":
            query_select = """
            NULL as player_id,
            MIN(events_custom.FLD_TEAM_ID) as team,
            """ + query_select
        else:
            raise ValueError(f"find must be 'player' or 'team', not '{self.find}'")

        query = f"""
        WITH {", ".join([f"\"{alias}_cte\" AS ({col})" for alias, col in self.custom_cols.items()])}{", " if self.custom_cols else ""} events_custom AS (
            SELECT events.*{", " + self.custom_select if self.custom_select else ""}
            FROM events
            LEFT JOIN cwgame ON events.GAME_ID = cwgame.GAME_ID
            {" ".join([f"LEFT JOIN \"{alias}_cte\" ON \"{alias}_cte\".GAME_ID = events.GAME_ID" for alias, _ in self.custom_cols.items()])}
            WHERE {self.query_where}{" AND " + self.custom_column_where if self.custom_column_where else ""}  -- Filter by query_where and custom_column_where
        )
        SELECT
            {query_select}
            min(year) as start_year,
            max(year) as end_year,
            {"count(DISTINCT events_custom.win)" if self.wins_included else "NULL"} as win,
            {"count(DISTINCT events_custom.loss)" if self.wins_included else "NULL"} as loss,
            COUNT(DISTINCT events_custom.GAME_ID) AS G, 
            COUNT(DISTINCT CASE WHEN events_custom.RESP_PIT_START_FL = 1 THEN events_custom.GAME_ID END) AS GS,
            SUM(events_custom.EVENT_OUTS_CT) * 1.0 / 3.0 AS IP,
            SUM(events_custom.PA) AS TBF,
            SUM(events_custom.AB) AS AB,
            SUM(events_custom.H) AS H,
            {"" if self.find == "player" else "SUM(events_custom.R) AS R,"}
            {"" if self.find == "player" else "SUM(events_custom.ER) AS ER,"}
            {"" if self.find == "player" else "SUM(events_custom.UER) AS UER,"}
            SUM(events_custom."1B") AS "1B",
            SUM(events_custom."2B") AS "2B",
            SUM(events_custom."3B") AS "3B",
            SUM(events_custom.HR) AS HR,
            SUM(events_custom.UBB) AS UBB,
            SUM(events_custom.IBB) AS IBB,
            SUM(events_custom.HBP) AS HBP,
            SUM(events_custom.DP) AS DP,
            SUM(events_custom.TP) AS TP,
            SUM(events_custom.WP) AS WP,
            SUM(events_custom.BK) AS BK,
            SUM(events_custom.K) AS K,
            SUM(events_custom.P) AS P,
            SUM(events_custom.GB) AS GB,
            SUM(events_custom.LD) AS LD,
            SUM(events_custom.FB) AS FB,
            SUM(events_custom.PU) AS PU,
            SUM(events_custom.SH) AS SH,
            SUM(events_custom.SF) AS SF
        FROM events_custom
        LEFT JOIN cwgame ON events_custom.GAME_ID = cwgame.GAME_ID
        GROUP BY {", ".join(to_group_by)}
        """
        to_group_original = to_group_by.copy()
        if "events_custom.FLD_TEAM_ID" in to_group_by:
            to_group_by.remove("events_custom.FLD_TEAM_ID")
            to_group_by.append("team")
        if "events_custom.RESP_PIT_ID" in to_group_by:
            to_group_original.remove("events_custom.RESP_PIT_ID")
            to_group_by.remove("events_custom.RESP_PIT_ID")
            to_group_by.append("player_id")
        if self.split == "game":
            to_group_by.remove("events_custom.GAME_ID")
            to_group_by.append("events_custom.game_id")
        for idx, item in enumerate(to_group_by):
            if item.startswith("events_custom."):
                to_group_by[idx] = item.split(".")[-1]
        df = pd.read_sql(query, engine, index_col=[elem.split(".")[-1] for elem in to_group_by])  # type: ignore

        # Separate query for R, UER, ER if find is player
        if self.find == "player":
            # The pitching runs table's RESP_PIT_ID is NOT the same as events_custom.RESP_PIT_ID (baserunner is not the same as the real batter)
            # I probably shouldn't have used RESP_PIT_ID in this table, but it comes from when I did this a different way with Pandas
            to_group_original.append("pitching_runs.RESP_PIT_ID")
            query_select = query_select.replace("events_custom.RESP_PIT_ID", "pitching_runs.RESP_PIT_ID")
            query_run_scoring = f"""
            WITH {", ".join([f"\"{alias}_cte\" AS ({col})" for alias, col in self.custom_cols.items()])}{", " if self.custom_cols else ""} events_custom AS (
                SELECT events.*{", " + self.custom_select if self.custom_select else ""}
                FROM events
                LEFT JOIN cwgame ON events.GAME_ID = cwgame.GAME_ID
                {" ".join([f"LEFT JOIN \"{alias}_cte\" ON \"{alias}_cte\".GAME_ID = events.GAME_ID" for alias, _ in self.custom_cols.items()])}
                WHERE {self.query_where}{" AND " + self.custom_column_where if self.custom_column_where else ""}  -- Filter by query_where and custom_column_where
            )
            SELECT
                {query_select}
                SUM(pitching_runs.R_indiv) AS R,
                SUM(pitching_runs.ER_indiv) AS ER,
                SUM(pitching_runs.UER_indiv) AS UER
            FROM pitching_runs
            LEFT JOIN cwgame ON pitching_runs.GAME_ID = cwgame.GAME_ID
            LEFT JOIN events_custom ON events_custom.file_index = pitching_runs.file_index AND events_custom.GAME_ID = pitching_runs.GAME_ID
            WHERE {self.query_where.replace("events", "events_custom")}
            GROUP BY {", ".join(to_group_original)};
            """
            df_run_scoring = pd.read_sql(query_run_scoring, engine, index_col=[elem.split(".")[-1] for elem in to_group_by])
            # Merge the run scoring DataFrame with the main DataFrame
            df = df.merge(df_run_scoring, how="left", on=["year", "player_id", "team", "month", "day", "game_id"])
        df["R"] = df["R"].fillna(0).infer_objects()
        df["ER"] = df["ER"].fillna(0).infer_objects()
        df["UER"] = df["UER"].fillna(0).infer_objects()

        self.stats = df.sort_values(by=[elem.split(".")[-1] for elem in to_group_by])

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