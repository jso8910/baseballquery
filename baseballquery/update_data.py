from pathlib import Path
from tqdm import tqdm
from datetime import datetime
from .parse_season import ParseSeason
from . import utils
from . import download
from . import retrosheet_cwevent_convert
from . import linear_weights
from .database import engine
from .retrosheet_cwevent_convert import proc_sb_cs_runs
from .migrations import create_tables
import sqlalchemy
import pandas as pd
from sqlalchemy import text


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

    create_tables()

    # Delete all files in ~/.baseballquery/downloads and ~/.baseballquery/chadwick
    downloads_dir = Path("~/.baseballquery/downloads").expanduser()
    chadwick_dir = Path("~/.baseballquery/chadwick").expanduser()
    if downloads_dir.exists():
        for file in downloads_dir.iterdir():
            file.unlink()
    if chadwick_dir.exists():
        for file in chadwick_dir.iterdir():
            file.unlink()
        
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
            # Delete data from SQL database in events, baserunning, pitching_runs, cwgame, and linear_weights
            utils.delete_year_events(last_year)
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
        linear_weights.calc_linear_weights_from_db(years_list=years_missing_weights)

    # Check the schedule for the current year
    if datetime.now().year > END_YEAR:
        print("Downloading data for current year (approximated; view README.md on Github for more information)")
        year = datetime.now().year
        df = ParseSeason(year).parse()
        if df is None:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT DISTINCT year FROM events"))
                years = [row[0] for row in result.fetchall()]
            with open(data_dir / "years.txt", "w") as f:
                f.write("\n".join(map(str, sorted(years))))
            return
        df_proc = retrosheet_cwevent_convert.process_df(df[0], statsapi_approx=True)

        # Delete existing data for the current year
        utils.delete_year_events(year)

        if not sqlalchemy.inspect(engine).has_table("events"):
            query = text(pd.io.sql.get_schema(df, 'events'))  # type: ignore
            with engine.begin() as conn:
                conn.execute(query)
        # Save the processed DataFrame to SQL
        events_table = sqlalchemy.Table("events", sqlalchemy.MetaData(), autoload_with=engine)
        insert_events = events_table.insert()
        with engine.begin() as conn:
            conn.execute(insert_events, df_proc.to_dict(orient="records"))  # type: ignore
        sb_cs, runs = proc_sb_cs_runs(df_proc)
        if not sqlalchemy.inspect(engine).has_table("baserunning"):
            query = text(pd.io.sql.get_schema(sb_cs, 'baserunning'))    # type: ignore
            with engine.begin() as conn:
                conn.execute(query)
        if not sqlalchemy.inspect(engine).has_table("pitching_runs"):
            query = text(pd.io.sql.get_schema(runs, 'pitching_runs'))   # type: ignore
            with engine.begin() as conn:
                conn.execute(query)
        if not sqlalchemy.inspect(engine).has_table("cwgame"):
            query = text(pd.io.sql.get_schema(df[1], 'cwgame'))    # type: ignore
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
            conn.execute(insert_cwgame, df[1].to_dict(orient="records"))   # type: ignore
        linear_weights.calc_linear_weights_from_db(years_list=[year])

    # Update years.txt file
    with engine.connect() as conn:
        result = conn.execute(text("SELECT DISTINCT year FROM events"))
        years = [row[0] for row in result.fetchall()]
    with open(data_dir / "years.txt", "w") as f:
        f.write("\n".join(map(str, sorted(years))))