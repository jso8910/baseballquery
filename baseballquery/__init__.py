from pathlib import Path
from . import download
from . import retrosheet_cwevent_convert
from . import linear_weights
from . import stat_calculator  # type: ignore
from .stat_splits import StatSplits, BattingStatSplits, PitchingStatSplits  # type: ignore
from .parse_game import ParseGame
import h5py  # type: ignore


current_directory = Path(__file__).parent

START_YEAR = 1912
END_YEAR = 2024
years = [year for year in range(START_YEAR, END_YEAR + 1)]

# if not (current_directory / "chadwick.hdf5").exists():
#     print("Chadwick HDF5 event files not generated")
#     if not (current_directory / "downloads").exists():
#         print("Retrosheet files not downloaded. Downloading...")
#         # download.download_year(END_YEAR)
#     print("Generating Chadwick event files...")
#     retrosheet_cwevent_convert.convert_files_to_csv()

# if not (current_directory / "linear_weights.csv").exists():
#     print("Linear weights not generated. Generating...")
#     linear_weights.calc_all_weights()

with h5py.File(current_directory / "chadwick.hdf5") as f:
    years_h5 = list(f.keys())  # type: ignore
years_updated = []
for year in years:
    if f"year_{year}" not in years_h5:
        years_updated.append(year)
        print(f"Downloading Retrosheet files for {year}...")
        download.download_year(year)
        print(f"Generating Chadwick event files for {year}...")
        retrosheet_cwevent_convert.convert_files_to_csv()

if years_updated:
    print(f"Generating linear weights...")
    linear_weights.calc_weights(years_list=years_updated)

__version__ = "0.0.3"
