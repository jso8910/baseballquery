import pandas as pd
from pathlib import Path

def get_year_events(year: int) -> pd.DataFrame:
    return pd.merge(get_year_events_raw(year), get_year_game_info(year), on="GAME_ID")

def get_year_events_raw(year: int) -> pd.DataFrame:
    try:
        return pd.read_feather(get_year_path(year))  # type: ignore
    except FileNotFoundError:
        raise KeyError(f"Year {year} not found in data. Have you run baseballquery.update_data() to download the data?")

def get_year_game_info(year: int) -> pd.DataFrame:
    try:
        return pd.read_feather(get_year_cwgame_path(year))  # type: ignore
    except FileNotFoundError:
        raise KeyError(f"Game info data for year {year} not found in data. Have you run baseballquery.update_data() to download the data? Keep in mind that cwgame data is new and you must run baseballquery.update_data(redownload=True) to download it.")


def get_year_path(year: int) -> Path:
    data_dir = Path("~/.baseballquery").expanduser()
    return data_dir / f"{year}.feather"

def get_year_cwgame_path(year: int) -> Path:
    data_dir = Path("~/.baseballquery").expanduser()
    return data_dir / f"cwgame-{year}.feather"

def get_years() -> list[int]:
    files = Path("~/.baseballquery").expanduser().glob("[0-9][0-9][0-9][0-9].feather")
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
