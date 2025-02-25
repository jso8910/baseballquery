import pandas as pd
from pathlib import Path


def get_year_events(year: int) -> pd.DataFrame:
    try:
        data_dir = Path("~/.baseballquery").expanduser()
        return pd.read_feather(data_dir / f"{year}.feather")  # type: ignore
    except FileNotFoundError:
        raise KeyError(f"Year {year} not found in data. Have you run baseballquery.update_data() to download the data?")


def get_year_path(year: int) -> Path:
    data_dir = Path("~/.baseballquery").expanduser()
    return data_dir / f"{year}.feather"


def get_years() -> list[int]:
    files = Path("~/.baseballquery").expanduser().glob("*.feather")
    if files is None:
        return []
    return [int(file.stem) for file in files]


def get_linear_weights() -> pd.DataFrame:
    data_dir = Path("~/.baseballquery").expanduser()
    if not (data_dir / "linear_weights.csv").exists():
        raise FileNotFoundError(
            "Linear weights not found. Have you run baseballquery.update_data() to download the data?"
        )
    return pd.read_csv(data_dir / "linear_weights.csv")  # type: ignore
