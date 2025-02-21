import pandas as pd
from pathlib import Path
import h5py

def get_year_events(year: int) -> pd.DataFrame:
    try:
        cwd = Path(__file__).parent
        return pd.read_hdf(cwd / "chadwick.hdf5", key=f"year_{year}")   # type: ignore
    except KeyError:
        raise KeyError(f"Year {year} not found in data. Have you run baseballquery.update_data() to download the data?")

def get_years() -> list[int]:
    cwd = Path(__file__).parent
    with h5py.File(cwd / "chadwick.hdf5") as f:
        return [int(year[-4:]) for year in list(f.keys())]   # type: ignore

def get_linear_weights() -> pd.DataFrame:
    cwd = Path(__file__).parent
    return pd.read_csv(cwd / "linear_weights.csv")   # type: ignore