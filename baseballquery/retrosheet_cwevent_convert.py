import subprocess
from pathlib import Path
from tqdm import tqdm
import os
import pandas as pd
from collections import defaultdict

chadwick_dtypes = {
    "GAME_ID": "object",
    "AWAY_TEAM_ID": "object",
    "INN_CT": "int64",
    "OUTS_CT": "int64",
    "BALLS_CT": "int64",
    "STRIKES_CT": "int64",
    "AWAY_SCORE_CT": "int64",
    "HOME_SCORE_CT": "int64",
    "BAT_ID": "object",
    "BAT_HAND_CD": "object",
    "RESP_BAT_ID": "object",
    "RESP_BAT_HAND_CD": "object",
    "PIT_ID": "object",
    "PIT_HAND_CD": "object",
    "RESP_PIT_ID": "object",
    "RESP_PIT_HAND_CD": "object",
    "BASE1_RUN_ID": "object",
    "BASE2_RUN_ID": "object",
    "BASE3_RUN_ID": "object",
    "LEADOFF_FL": "bool",
    "PH_FL": "bool",
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
    "BUNT_FL": "bool",
    "FOUL_FL": "bool",
    "BATTEDBALL_LOC_TX": "object",
    "BAT_DEST_ID": "int64",
    "RUN1_DEST_ID": "int64",
    "RUN2_DEST_ID": "int64",
    "RUN3_DEST_ID": "int64",
    "BAT_PLAY_TX": "object",
    "RUN1_PLAY_TX": "object",
    "RUN2_PLAY_TX": "object",
    "RUN3_PLAY_TX": "object",
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
    "GAME_NEW_FL": "bool",
    "GAME_END_FL": "bool",
    "HOME_TEAM_ID": "object",
    "BAT_TEAM_ID": "object",
    "FLD_TEAM_ID": "object",
    "INN_RUNS_CT": "int64",
    "PA_TRUNC_FL": "bool",
    "START_BASES_CD": "int64",
    "END_BASES_CD": "int64",
    "PIT_START_FL": "bool",
    "RESP_PIT_START_FL": "bool",
    "PA_BALL_CT": "int64",
    "PA_CALLED_BALL_CT": "int64",
    "PA_INTENT_BALL_CT": "int64",
    "PA_PITCHOUT_BALL_CT": "int64",
    "PA_HITBATTER_BALL_CT": "int64",
    "PA_OTHER_BALL_CT": "int64",
    "PA_STRIKE_CT": "int64",
    "PA_CALLED_STRIKE_CT": "int64",
    "PA_SWINGMISS_STRIKE_CT": "int64",
    "PA_FOUL_STRIKE_CT": "int64",
    "PA_INPLAY_STRIKE_CT": "int64",
    "PA_OTHER_STRIKE_CT": "int64",
    "EVENT_RUNS_CT": "int64",
    "BAT_FATE_ID": "int64",
    "BAT_SAFE_ERR_FL": "bool",
    "FATE_RUNS_CT": "int64",
}


def convert_files_to_csv():
    cwd = Path(__file__).parent
    download_dir: Path = cwd / "downloads"
    if not download_dir.exists():
        raise FileNotFoundError("Retrosheet files not downloaded")

    outdir = cwd / "chadwick"
    outdir.mkdir(parents=True, exist_ok=True)
    os.chdir(download_dir)

    for file in tqdm(list(download_dir.iterdir()), desc="Converting retrosheet to Chadwick"):
        if not file.name[-4:] in (".EVN", ".EVA"):
            continue
        with open(outdir / f"{file.name}.csv", "w") as f:
            _ = subprocess.run(
                [
                    "cwevent",
                    "-q",
                    "-f", "0-2,4-6,8-17,26-28,30-45,47-50,58-79",
                    "-x", "0-2,5,8,12-14,15-16,19-20,33-45,50,55",
                    f"-y", f"{file.stem[:4]}",
                    f"-n", f"{file}",
                ],
                stdout=f,
            )
    os.chdir(cwd)

    years: dict[str, pd.DataFrame] = defaultdict(pd.DataFrame)
    for file in tqdm(list(outdir.iterdir()), desc="Converting Chadwick CSVs to HDF5"):
        df: pd.DataFrame = pd.read_csv(file, true_values=["t", "T"], false_values=["f", "F"], dtype=chadwick_dtypes)  # type: ignore
        year: str = file.name[:4]
        years[year] = pd.concat([years[year], df]) # type: ignore

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
    for year, df in tqdm(years.items(), desc="Saving HDF5 file"):
        df["PA"] = (~df["EVENT_CD"].isin(baserunning_outcomes_not_pa + [13])).astype(int)   # type: ignore
        df["AB"] = df["AB_FL"].astype(int)  # type: ignore
        df["SH"] = df["SH_FL"].astype(int)  # type: ignore
        df["SF"] = df["SF_FL"].astype(int)  # type: ignore
        df["R"] = df["EVENT_RUNS_CT"].astype(int)   # type: ignore
        df["RBI"] = df["RBI_CT"].astype(int)    # type: ignore
        df["SB"] = df["RUN1_SB_FL"].astype(int) + df["RUN2_SB_FL"].astype(int) + df["RUN3_SB_FL"].astype(int)   # type: ignore
        df["CS"] = df["RUN1_CS_FL"].astype(int) + df["RUN2_CS_FL"].astype(int) + df["RUN3_CS_FL"].astype(int)   # type: ignore
        for field, name in fields.items():
            df[name] = df["EVENT_CD"].eq(field).astype(int) # type: ignore
        df["H"] = df["EVENT_CD"].isin([20, 21, 22, 23]).astype(int) # type: ignore
        df["DP"] = df["DP_FL"].astype(int)  # type: ignore
        df["TP"] = df["TP_FL"].astype(int)  # type: ignore
        df["ROE"] = (df["BAT_SAFE_ERR_FL"] & df["EVENT_CD"].eq(18)).astype(int) # type: ignore
        df["WP"] = df["WP_FL"].astype(int)  # type: ignore
        df["P"] = (df["PA_BALL_CT"] + df["PA_STRIKE_CT"] - df["PA_OTHER_BALL_CT"] - df["PA_OTHER_STRIKE_CT"]) * (df["PA"] | df["R"])
        df["GB"] = df["BATTEDBALL_CD"].eq("G").astype(int)  # type: ignore
        df["FB"] = df["BATTEDBALL_CD"].eq("F").astype(int)  # type: ignore
        df["LD"] = df["BATTEDBALL_CD"].eq("L").astype(int)  # type: ignore
        df["PU"] = df["BATTEDBALL_CD"].eq("P").astype(int)  # type: ignore
        df["ER"] = (
            df["BAT_DEST_ID"].isin([4, 6]).astype(int) +   # type: ignore
            df["RUN1_DEST_ID"].isin([4, 6]).astype(int) +  # type: ignore
            df["RUN2_DEST_ID"].isin([4, 6]).astype(int) +  # type: ignore
            df["RUN3_DEST_ID"].isin([4, 6]).astype(int)    # type: ignore
        )
        df["T_UER"] = (
            df["BAT_DEST_ID"].eq(6).astype(int) +   # type: ignore
            df["RUN1_DEST_ID"].eq(6).astype(int) +  # type: ignore
            df["RUN2_DEST_ID"].eq(6).astype(int) +  # type: ignore
            df["RUN3_DEST_ID"].eq(6).astype(int)    # type: ignore
        )

        df["UER"] = (
            df["BAT_DEST_ID"].isin([5, 7]).astype(int) +   # type: ignore
            df["RUN1_DEST_ID"].isin([5, 7]).astype(int) +  # type: ignore
            df["RUN2_DEST_ID"].isin([5, 7]).astype(int) +  # type: ignore
            df["RUN3_DEST_ID"].isin([5, 7]).astype(int)    # type: ignore
        )

        df.to_hdf(cwd / "chadwick.hdf5", key=f"year_{year}", format="table")  # type: ignore

    print("Cleaning up...")
    print("Deleting Chadwick CSVs...")
    for child in outdir.iterdir():
        child.unlink()
    outdir.rmdir()

    print("Deleting retrosheet files...")
    for child in download_dir.iterdir():
        child.unlink()
    download_dir.rmdir()
