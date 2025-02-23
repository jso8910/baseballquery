from pathlib import Path
import h5py
from tqdm import tqdm
import pandas as pd
from datetime import datetime
from .parse_season import ParseSeason
from .utils import get_linear_weights
from . import download
from . import retrosheet_cwevent_convert
from . import linear_weights


def update_data():
    print("Updating data...")
    current_directory = Path(__file__).parent

    # First and last year of retrosheet data
    START_YEAR = 1912
    END_YEAR = 2024
    years = [year for year in range(START_YEAR, END_YEAR + 1)]

    if (current_directory / "chadwick.hdf5").exists():
        with h5py.File(current_directory / "chadwick.hdf5") as f:
            years_h5 = list(f.keys())  # type: ignore
    else:
        years_h5 = []
    years_updated = [year for year in years if f"year_{year}" not in years_h5]

    # Check that the last year is retrosheet, not StatsAPI approximated
    years_in_df = [year for year in years if f"year_{year}" in years_h5]
    if years_in_df:
        last_year = years_in_df[-1]
        df = pd.read_hdf(current_directory / "chadwick.hdf5", key=f"year_{last_year}")
        if df["MLB_STATSAPI_APPROX"].any():
            print("Deleting and redownloading StatsAPI approximated year")
            with h5py.File(current_directory / "chadwick.hdf5", "a") as f:
                del f[f"year_{last_year}"]
            years_updated.append(last_year)


    if years_updated:
        print("Downloading and processing data for missing years")
        for year in tqdm(years_updated, desc="Years", position=0, leave=True):
            download.download_year(year)
            retrosheet_cwevent_convert.convert_files_to_csv()
            years_h5.append(f"year_{year}")

    if (current_directory / "linear_weights.csv").exists():
        lin_weights = get_linear_weights()
        years_missing_weights = [year for year in years if year not in lin_weights["year"].values]
    else:
        years_missing_weights = years
    if years_missing_weights:
        print(f"Generating linear weights...")
        linear_weights.calc_weights(years_list=years_missing_weights)

    # Check the schedule for the current year
    if datetime.now().year > END_YEAR:
        print("Downloading data for current year (approximated; view README.md on Github for more information)")
        df = ParseSeason(datetime.now().year).parse()
        if df is None:
            return
        df = retrosheet_cwevent_convert.process_df(df, statsapi_approx = True)
        df.to_hdf(current_directory / "chadwick.hdf5", key=f"year_{datetime.now().year}", format="table")
        linear_weights.calc_weights(years_list=[datetime.now().year])

