import subprocess
from pathlib import Path
from tqdm import tqdm
import os
import pandas as pd
from collections import defaultdict
from .chadwick_cols import chadwick_dtypes, cwgame_dtypes
from .database import engine
import sqlalchemy
from sqlalchemy import text

def convert_files_to_csv():
    data_dir = Path("~/.baseballquery").expanduser()
    download_dir: Path = data_dir / "downloads"
    if not download_dir.exists():
        raise FileNotFoundError("Retrosheet files not downloaded")

    outdir = data_dir / "chadwick"
    outdir.mkdir(parents=True, exist_ok=True)
    os.chdir(download_dir)

    for file in tqdm(list(download_dir.iterdir()), desc="Converting retrosheet to Chadwick", position=1, leave=False):
        if not file.name[-4:] in (".EVN", ".EVA"):
            continue

        # NOTE: This is a very specific bug I need to fix
        if file.name == "1978CLE.EVA":
            with file.open("r") as f:
                filedata = f.read()
            filedata = filedata.replace("play,5,0,lynnf001,*BCSS,,K", "play,5,0,lynnf001,12,*BCSS,K")
            with file.open("w") as f:
                f.write(filedata)

        # Process event-level info with cwevent
        with open(outdir / f"{file.name}.csv", "w") as f:
            try:
                _ = subprocess.run(
                    [
                        "cwevent",
                        "-q",
                        "-f",
                        "0-2,4-7,8-9,12-13,16-17,26-28,32-34,36-45,47,58-61,66-77",
                        "-x",
                        "0-2,12-14,16,20,33,38-39,44-45,50,55",
                        f"-y",
                        f"{file.stem[:4]}",
                        f"-n",
                        f"{file}",
                    ],
                    stdout=f,
                    check=True
                )
            except subprocess.CalledProcessError as e:
                print(f"Error converting {file.name}. Is Chadwick correctly installed? Deleting all Chadwick files to avoid issues...")
                for file in outdir.iterdir():
                    file.unlink()
                outdir.rmdir()
                raise e

        # Process individual game info with cwgame
        with open(outdir / f"cwgame-{file.name}.csv", "w") as f:
            try:
                _ = subprocess.run(
                    [
                        "cwgame",
                        "-q",
                        "-f",
                        "0,3,4,6,9,18,26-35,42-44",
                        f"-y",
                        f"{file.stem[:4]}",
                        f"-n",
                        f"{file}",
                    ],
                    stdout=f,
                    stderr=subprocess.DEVNULL,  # Sometimes warnings about integer values are put here... don't need to see
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                print(f"Error converting {file.name}. Is Chadwick correctly installed? Deleting all Chadwick files to avoid issues...")
                for file in outdir.iterdir():
                    file.unlink()
                outdir.rmdir()
    os.chdir(data_dir)

    years: dict[int, pd.DataFrame] = defaultdict(pd.DataFrame)
    years_cwgame: dict[int, pd.DataFrame] = defaultdict(pd.DataFrame)
    for file in tqdm(list(outdir.iterdir()), desc="Converting Chadwick CSVs to Feather", position=1, leave=False):
        if file.name.startswith("cwgame-"):
            df: pd.DataFrame = pd.read_csv(file, true_values=["t", "T"], false_values=["f", "F"])  # type: ignore
            df = df.rename({"INN_CT": "FINAL_INN_CT", "HOME_SCORE_CT": "FINAL_HOME_SCORE_CT", "AWAY_SCORE_CT": "FINAL_AWAY_SCORE_CT"}, axis=1)
            df.astype(cwgame_dtypes)
            year = int(file.name[7:11])
            years_cwgame[year] = pd.concat([years_cwgame[year], df])
            continue
        df: pd.DataFrame = pd.read_csv(file, true_values=["t", "T"], false_values=["f", "F"])  # type: ignore
        df["MLB_STATSAPI_APPROX"] = False
        df["mlbam_id"] = None
        df.astype(chadwick_dtypes)
        year = int(file.name[:4])
        years[year] = pd.concat([years[year], df])  # type: ignore

    for year, df in tqdm(years.items(), desc="Saving to SQL", position=1, leave=False):
        proc_df = process_df(df)
        if not sqlalchemy.inspect(engine).has_table("events"):
            query = text(pd.io.sql.get_schema(df, 'events'))  # type: ignore
            with engine.begin() as conn:
                conn.execute(query)
        # Save the processed DataFrame to SQL
        events_table = sqlalchemy.Table("events", sqlalchemy.MetaData(), autoload_with=engine)
        insert_events = events_table.insert()
        with engine.begin() as conn:
            conn.execute(insert_events, proc_df.to_dict(orient="records"))  # type: ignore
        # proc_df.to_sql("events", engine, if_exists="append", index=False, method="multi", chunksize=100)
        sb_cs, runs = proc_sb_cs_runs(proc_df)
        if not sqlalchemy.inspect(engine).has_table("baserunning"):
            query = text(pd.io.sql.get_schema(sb_cs, 'baserunning'))    # type: ignore
            with engine.begin() as conn:
                conn.execute(query)
        if not sqlalchemy.inspect(engine).has_table("pitching_runs"):
            query = text(pd.io.sql.get_schema(runs, 'pitching_runs'))   # type: ignore
            with engine.begin() as conn:
                conn.execute(query)
        if not sqlalchemy.inspect(engine).has_table("cwgame"):
            query = text(pd.io.sql.get_schema(years_cwgame[year], 'cwgame'))    # type: ignore
            with engine.begin() as conn:
                conn.execute(query)
        # Save the baserunning and runs DataFrames to SQL
        sb_cs_table = sqlalchemy.Table("baserunning", sqlalchemy.MetaData(), autoload_with=engine)
        insert_baserunning = sb_cs_table.insert()
        with engine.begin() as conn:
            conn.execute(insert_baserunning, sb_cs.to_dict(orient="records"))   # type: ignore
        runs_table = sqlalchemy.Table("pitching_runs", sqlalchemy.MetaData(), autoload_with=engine)
        insert_runs = runs_table.insert()
        with engine.begin() as conn:
            conn.execute(insert_runs, runs.to_dict(orient="records"))   # type: ignore
        # Save the cwgame DataFrame to SQL
        cwgame_table = sqlalchemy.Table("cwgame", sqlalchemy.MetaData(), autoload_with=engine)
        insert_cwgame = cwgame_table.insert()
        with engine.begin() as conn:
            conn.execute(insert_cwgame, years_cwgame[year].to_dict(orient="records"))   # type: ignore

    # Delete Chadwick CSVs
    for child in outdir.iterdir():
        child.unlink()
    outdir.rmdir()

    # Delete Retrosheet files
    for child in download_dir.iterdir():
        child.unlink()
    download_dir.rmdir()

def get_counts_from_pitch_sequence(pitch_sequence):
    """
    Parses a Retrosheet pitch sequence and returns a list of ball-strike counts.
    """
    if pd.isna(pitch_sequence) or pitch_sequence == "":
        return [False] * 12
    # Strikes: A C K L M O Q S T
    # Fouls: F R
    # Balls: B I P V
    balls = 0
    strikes = 0
    counts = [
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
    for pitch in [c for c in pitch_sequence if c not in "+*.123>HNXY"]:
        if pitch in ['B', 'I', 'P', 'V']:  # Balls
            balls += 1
        elif pitch in ["A", "C", "K", "L", "M", "O", "Q", "S", "T"]:  # Strikes
            strikes += 1
        elif pitch in ["F", "R"]:  # Fouls are strikes if less than 2 strikes
            if strikes < 2:
                strikes += 1
        elif pitch == "U":  # Unknown pitch - ignore rest of sequence
            break
        if balls == 4 or strikes == 3:  # End of at-bat (walk or strikeout)
            break
        counts[balls * 3 + strikes] = True  # Set the count for the current balls-strikes combination
        # Reset for next at-bat if X encountered, or handle other scenarios as needed
    return counts

def process_df(df: pd.DataFrame, statsapi_approx=False) -> pd.DataFrame:
    baserunning_outcomes_not_pa: list[int] = [4, 5, 6, 7, 8, 9, 10, 11, 12]
    fields: dict[int, str] = {
        3: "K",
        # 4: "SB",
        # 6: "CS",
        # 8: "PK",
        # 9: "WP",
        # 10: "PB",
        11: "BK",
        14: "UBB",
        15: "IBB",
        16: "HBP",
        # 17: "INT",
        # 18: "E",
        19: "FC",
        20: "1B",
        21: "2B",
        22: "3B",
        23: "HR",
    }
    df["PA"] = (~df["EVENT_CD"].isin(baserunning_outcomes_not_pa + [13])).astype(int)  # type: ignore
    df["AB"] = df["AB_FL"].astype(int)  # type: ignore
    df["SH"] = df["SH_FL"].astype(int)  # type: ignore
    df["SF"] = df["SF_FL"].astype(int)  # type: ignore
    df["R"] = df["EVENT_RUNS_CT"].astype(int)  # type: ignore
    df["RBI"] = df["RBI_CT"].astype(int)  # type: ignore
    df["SB"] = df["RUN1_SB_FL"].astype(int) + df["RUN2_SB_FL"].astype(int) + df["RUN3_SB_FL"].astype(int)  # type: ignore
    df["CS"] = df["RUN1_CS_FL"].astype(int) + df["RUN2_CS_FL"].astype(int) + df["RUN3_CS_FL"].astype(int)  # type: ignore
    for field, name in fields.items():
        df[name] = df["EVENT_CD"].eq(field).astype(int)  # type: ignore
    df["H"] = df["EVENT_CD"].isin([20, 21, 22, 23]).astype(int)  # type: ignore
    df["DP"] = df["DP_FL"].astype(int)  # type: ignore
    df["TP"] = df["TP_FL"].astype(int)  # type: ignore
    df["ROE"] = (df["BAT_SAFE_ERR_FL"] & df["EVENT_CD"].eq(18)).astype(int)  # type: ignore
    df["WP"] = df["WP_FL"].astype(int)  # type: ignore
    df["P"] = (df["PA_BALL_CT"] + df["PA_STRIKE_CT"] - df["PA_OTHER_BALL_CT"] - df["PA_OTHER_STRIKE_CT"]) * (
        df["PA"] | df["PA_TRUNC_FL"]
    )
    df["GB"] = df["BATTEDBALL_CD"].eq("G").astype(int)  # type: ignore
    df["FB"] = df["BATTEDBALL_CD"].eq("F").astype(int)  # type: ignore
    df["LD"] = df["BATTEDBALL_CD"].eq("L").astype(int)  # type: ignore
    df["PU"] = df["BATTEDBALL_CD"].eq("P").astype(int)  # type: ignore
    df["ER"] = (
        df["BAT_DEST_ID"].isin([4, 6]).astype(int)  # type: ignore
        + df["RUN1_DEST_ID"].isin([4, 6]).astype(int)  # type: ignore
        + df["RUN2_DEST_ID"].isin([4, 6]).astype(int)  # type: ignore
        + df["RUN3_DEST_ID"].isin([4, 6]).astype(int)  # type: ignore
    )
    df["T_UER"] = (
        df["BAT_DEST_ID"].eq(6).astype(int)  # type: ignore
        + df["RUN1_DEST_ID"].eq(6).astype(int)  # type: ignore
        + df["RUN2_DEST_ID"].eq(6).astype(int)  # type: ignore
        + df["RUN3_DEST_ID"].eq(6).astype(int)  # type: ignore
    )

    df["UER"] = (
        df["BAT_DEST_ID"].isin([5, 7]).astype(int)  # type: ignore
        + df["RUN1_DEST_ID"].isin([5, 7]).astype(int)  # type: ignore
        + df["RUN2_DEST_ID"].isin([5, 7]).astype(int)  # type: ignore
        + df["RUN3_DEST_ID"].isin([5, 7]).astype(int)  # type: ignore
    )

    df["MLB_STATSAPI_APPROX"] = statsapi_approx

    df["year"] = df["GAME_ID"].str.slice(3, 7).astype(int)  # type: ignore
    df["month"] = df["GAME_ID"].str.slice(7, 9).astype(int)  # type: ignore
    df["day"] = df["GAME_ID"].str.slice(9, 11).astype(int)  # type: ignore

    df["file_index"] = df.index
    df = df.reset_index(drop=True)
    if not df["MLB_STATSAPI_APPROX"].any():
        # Process all counts which occurred during the PA
        if df["year"].iloc[0] < 1988:
            # For years before 1988, there is no pitch sequence data (or very spotty, so not worth including)
            df["counts"] = [[False] * 12] * len(df)
        else:
            df["PITCH_SEQ_TX"] = df["PITCH_SEQ_TX"].fillna("")
            df["counts"] = df.apply(lambda row: get_counts_from_pitch_sequence(row["PITCH_SEQ_TX"]), axis=1)  # type: ignore

        df["0-0"] = df["counts"].apply(lambda x: x[0])
        df["0-1"] = df["counts"].apply(lambda x: x[1])
        df["0-2"] = df["counts"].apply(lambda x: x[2])
        df["1-0"] = df["counts"].apply(lambda x: x[3])
        df["1-1"] = df["counts"].apply(lambda x: x[4])
        df["1-2"] = df["counts"].apply(lambda x: x[5])
        df["2-0"] = df["counts"].apply(lambda x: x[6])
        df["2-1"] = df["counts"].apply(lambda x: x[7])
        df["2-2"] = df["counts"].apply(lambda x: x[8])
        df["3-0"] = df["counts"].apply(lambda x: x[9])
        df["3-1"] = df["counts"].apply(lambda x: x[10])
        df["3-2"] = df["counts"].apply(lambda x: x[11])
        df = df.drop(columns=["counts", "PITCH_SEQ_TX"])  # type: ignore
    # df = df.reset_index(drop=True)
    return df

def proc_sb_cs_runs(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Process stolen bases, caught stealing, and earned runs and return two separate dataframes (one with baserunning and one with earned runs).
    """
    stolen_first = df[df["RUN1_SB_FL"] != 0].copy()
    stolen_first["RESP_BAT_ID"] = stolen_first["BASE1_RUN_ID"]
    stolen_second = df[df["RUN2_SB_FL"] != 0].copy()
    stolen_second["RESP_BAT_ID"] = stolen_second["BASE2_RUN_ID"]
    stolen_third = df[df["RUN3_SB_FL"] != 0].copy()
    stolen_third["RESP_BAT_ID"] = stolen_third["BASE3_RUN_ID"]
    # Remove all other values (stats, etc) from the stolen bases events
    stolen_first = stolen_first[["file_index", "RESP_BAT_ID", "GAME_ID"]]
    stolen_second = stolen_second[["file_index", "RESP_BAT_ID", "GAME_ID"]]
    stolen_third = stolen_third[["file_index", "RESP_BAT_ID", "GAME_ID"]]

    # Set SB to 1 for the stolen bases events
    stolen_first["SB_indiv"] = 1
    stolen_second["SB_indiv"] = 1
    stolen_third["SB_indiv"] = 1

    # Do the same for CS
    caught_first = df[df["RUN1_CS_FL"] != 0].copy()
    caught_first["RESP_BAT_ID"] = caught_first["BASE1_RUN_ID"]
    caught_second = df[df["RUN2_CS_FL"] != 0].copy()
    caught_second["RESP_BAT_ID"] = caught_second["BASE2_RUN_ID"]
    caught_third = df[df["RUN3_CS_FL"] != 0].copy()
    caught_third["RESP_BAT_ID"] = caught_third["BASE3_RUN_ID"]
    # Remove all other values (stats, etc) from the CS events
    caught_first = caught_first[["file_index", "RESP_BAT_ID", "GAME_ID"]]
    caught_second = caught_second[["file_index", "RESP_BAT_ID", "GAME_ID"]]
    caught_third = caught_third[["file_index", "RESP_BAT_ID", "GAME_ID"]]
    # Set CS to 1 for the stolen bases events
    caught_first["CS_indiv"] = 1
    caught_second["CS_indiv"] = 1
    caught_third["CS_indiv"] = 1


    # Process run allowed events
    run_score_0 = df[df["BAT_DEST_ID"] >= 4].copy()
    run_score_1 = df[df["RUN1_DEST_ID"] >= 4].copy()
    run_score_2 = df[df["RUN2_DEST_ID"] >= 4].copy()
    run_score_3 = df[df["RUN3_DEST_ID"] >= 4].copy()
    # Set the RESP_PIT_ID to the pitcher that is responsible for the run
    run_score_1["RESP_PIT_ID"] = run_score_1["RUN1_RESP_PIT_ID"]
    run_score_2["RESP_PIT_ID"] = run_score_2["RUN2_RESP_PIT_ID"]
    run_score_3["RESP_PIT_ID"] = run_score_3["RUN3_RESP_PIT_ID"]
    # Remove all other values (stats, etc) from the run scoring events
    run_score_0 = run_score_0[["file_index", "RESP_PIT_ID", "GAME_ID", "BAT_DEST_ID", "RUN1_DEST_ID", "RUN2_DEST_ID", "RUN3_DEST_ID"]]
    run_score_1 = run_score_1[["file_index", "RESP_PIT_ID", "GAME_ID", "BAT_DEST_ID", "RUN1_DEST_ID", "RUN2_DEST_ID", "RUN3_DEST_ID"]]
    run_score_2 = run_score_2[["file_index", "RESP_PIT_ID", "GAME_ID", "BAT_DEST_ID", "RUN1_DEST_ID", "RUN2_DEST_ID", "RUN3_DEST_ID"]]
    run_score_3 = run_score_3[["file_index", "RESP_PIT_ID", "GAME_ID", "BAT_DEST_ID", "RUN1_DEST_ID", "RUN2_DEST_ID", "RUN3_DEST_ID"]]
    # Set R, ER, and UER to 1 for the run scoring events
    run_score_0["R_indiv"] = 1
    run_score_1["R_indiv"] = 1
    run_score_2["R_indiv"] = 1
    run_score_3["R_indiv"] = 1
    run_score_0.loc[run_score_0["BAT_DEST_ID"].isin((4, 6)), "ER_indiv"] = 1
    run_score_0.loc[run_score_0["BAT_DEST_ID"].isin((5, 7)), "UER_indiv"] = 1
    run_score_1.loc[run_score_1["RUN1_DEST_ID"].isin((4, 6)), "ER_indiv"] = 1
    run_score_1.loc[run_score_1["RUN1_DEST_ID"].isin((5, 7)), "UER_indiv"] = 1
    run_score_2.loc[run_score_2["RUN2_DEST_ID"].isin((4, 6)), "ER_indiv"] = 1
    run_score_2.loc[run_score_2["RUN2_DEST_ID"].isin((5, 7)), "UER_indiv"] = 1
    run_score_3.loc[run_score_3["RUN3_DEST_ID"].isin((4, 6)), "ER_indiv"] = 1
    run_score_3.loc[run_score_3["RUN3_DEST_ID"].isin((5, 7)), "UER_indiv"] = 1

    return (pd.concat([stolen_first, stolen_second, stolen_third, caught_first, caught_second, caught_third])), pd.concat([run_score_0, run_score_1, run_score_2, run_score_3])