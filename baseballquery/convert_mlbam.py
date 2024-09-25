from pathlib import Path
import pandas as pd

class ConvertMLBAM:
    def __init__(self):
        cwd = Path()
        player_csv_dir = cwd / "register" / "data"
        self.player_csvs = list(player_csv_dir.glob("people-*.csv"))
        self.player_lookup = pd.concat([pd.read_csv(csv, low_memory=False) for csv in self.player_csvs])  # type: ignore
        self.player_lookup = self.player_lookup[self.player_lookup["key_mlbam"].notna()]    # type: ignore
        self.player_lookup["key_mlbam"] = self.player_lookup["key_mlbam"].astype(int)  # type: ignore
        self.player_lookup = self.player_lookup.set_index("key_mlbam")  # type: ignore
        self.player_lookup = self.player_lookup[["key_retro"]] # type: ignore

    def mlbam_to_retro(self, key: int) -> str:
        return self.player_lookup.loc[key, "key_retro"]    # type: ignore


