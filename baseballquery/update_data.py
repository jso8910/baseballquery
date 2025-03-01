from pathlib import Path
from tqdm import tqdm
from datetime import datetime
from .parse_season import ParseSeason
from . import utils
from . import download
from . import retrosheet_cwevent_convert
from . import linear_weights


def set_first_data_year(year):
    data_dir = Path("~/.baseballquery").expanduser()
    data_dir.mkdir(exist_ok=True)
    with open(data_dir / "min_year.txt", "w") as f:
        f.write(str(year))

def delete_data():
    data_dir = Path("~/.baseballquery").expanduser()
    min_year = 1912
    if data_dir.exists():
        if (data_dir / "min_year.txt").exists():
            with open(data_dir / "min_year.txt", "r") as f:
                min_year = int(f.read())
        for file in data_dir.iterdir():
            file.unlink()
    set_first_data_year(min_year)

def update_data(redownload=False):
    if redownload:
        print("Redownloading all data...")
        delete_data()
        
    print("Updating data...")
    data_dir = Path("~/.baseballquery").expanduser()
    if not data_dir.exists():
        data_dir.mkdir()

    # First and last year of retrosheet data
    if not (data_dir / "min_year.txt").exists():
        set_first_data_year(1912)

    with open(data_dir / "min_year.txt", "r") as f:
        min_year = int(f.read())

    START_YEAR = min_year
    END_YEAR = 2024
    years = [year for year in range(START_YEAR, END_YEAR + 1)]

    years_feather = utils.get_years()
    years_updated = [year for year in years if year not in years_feather]

    # Check that the last year is retrosheet, not StatsAPI approximated
    years_in_df = [year for year in years if year in years_feather]
    if years_in_df:
        last_year = years_in_df[-1]
        df = utils.get_year_events(last_year)
        if df["MLB_STATSAPI_APPROX"].any():
            print("Deleting and redownloading StatsAPI approximated year")
            utils.get_year_path(last_year).unlink()
            utils.get_year_cwgame_path(last_year).unlink()
            years_updated.append(last_year)

    if years_updated:
        print("Downloading and processing data for missing years")
        for year in tqdm(years_updated, desc="Years", position=0, leave=True):
            download.download_year(year)
            retrosheet_cwevent_convert.convert_files_to_csv()
            years_feather.append(year)

    try:
        lin_weights = utils.get_linear_weights()
        years_missing_weights = [year for year in years if year not in lin_weights["year"].values]
    except FileNotFoundError:
        years_missing_weights = years

    if years_missing_weights:
        print(f"Generating linear weights...")
        linear_weights.calc_weights(years_list=years_missing_weights)

    # Check the schedule for the current year
    if datetime.now().year > END_YEAR:
        print("Downloading data for current year (approximated; view README.md on Github for more information)")
        year = datetime.now().year
        df = ParseSeason(year).parse()
        if df is None:
            return
        df_proc = retrosheet_cwevent_convert.process_df(df[0], statsapi_approx=True)
        df_proc.to_feather(utils.get_year_path(year))
        df[1].to_feather(utils.get_year_cwgame_path(year))
        linear_weights.calc_weights(years_list=[year])
