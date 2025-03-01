import subprocess
from pathlib import Path
from tqdm import tqdm
import os
import pandas as pd  # type: ignore
from collections import defaultdict
from .chadwick_cols import chadwick_dtypes, cwgame_dtypes

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

        # Process event-level info with cwevent
        with open(outdir / f"{file.name}.csv", "w") as f:
            try:
                _ = subprocess.run(
                    [
                        "cwevent",
                        "-q",
                        "-f",
                        "0-2,4-6,8-9,12-13,16-17,26-28,32-34,36-45,47,58-61,66-77",
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
                        "0,3,4,6,9,18,26-32,42-44",
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

    for year, df in tqdm(years.items(), desc="Saving Feather file", position=1, leave=False):
        process_df(df).to_feather(data_dir / f"{year}.feather")
        years_cwgame[year].to_feather(data_dir / f"cwgame-{year}.feather")

    # Delete Chadwick CSVs
    for child in outdir.iterdir():
        child.unlink()
    outdir.rmdir()

    # Delete Retrosheet files
    for child in download_dir.iterdir():
        child.unlink()
    download_dir.rmdir()


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
    return df
