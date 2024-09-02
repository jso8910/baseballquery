from pathlib import Path
from . import download
from . import retrosheet_cwevent_convert
from . import linear_weights
from . import stat_calculator
from .stat_splits import StatSplits, BattingStatSplits, PitchingStatSplits


current_directory = Path(__file__).parent

if not (current_directory / "chadwick.hdf5").exists():
    print("Chadwick HDF5 event files not generated")
    if not (current_directory / "downloads").exists():
        print("Retrosheet files not downloaded. Downloading...")
        download.download_games()
    print("Generating Chadwick event files...")
    retrosheet_cwevent_convert.convert_files_to_csv()

if not (current_directory / "linear_weights.csv").exists():
    print("Linear weights not generated. Generating...")
    linear_weights.calc_all_weights()

__version__ = "0.0.2"
