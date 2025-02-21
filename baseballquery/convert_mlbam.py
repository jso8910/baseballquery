from pathlib import Path
import pandas as pd
import unicodedata


def strip_accents(s):
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


class ConvertMLBAM:
    def __init__(self):
        cwd = Path(__file__).parent
        player_csv_dir = cwd / "register" / "data"
        self.player_csvs = list(player_csv_dir.glob("people-*.csv"))
        self.player_lookup = pd.concat([pd.read_csv(csv, low_memory=False) for csv in self.player_csvs])  # type: ignore
        self.player_lookup = self.player_lookup[self.player_lookup["key_mlbam"].notna()]  # type: ignore
        self.player_lookup["key_mlbam"] = self.player_lookup["key_mlbam"].astype(int)  # type: ignore
        self.player_lookup = self.player_lookup.set_index("key_mlbam")  # type: ignore
        self.player_lookup = self.player_lookup[["key_retro", "name_last", "name_first"]]  # type: ignore

    def mlbam_to_retro(self, key: int) -> str:
        pid = self.player_lookup.loc[key, "key_retro"]
        if pd.isna(pid):
            # Player hasn't player in any retrosheet files yet, so reconstruct their likely retrosheet id
            last_name = self.player_lookup.loc[key, "name_last"]
            last_name_retro = "".join(filter(str.isalpha, strip_accents(last_name))).ljust(4, "-")[:4].lower()  # type: ignore
            first_name_retro = strip_accents(self.player_lookup.loc[key, "name_first"]).ljust(1, "-")[0].lower()
            retro_num = 1
            retro_id = f"{last_name_retro}{first_name_retro}{retro_num:03d}"
            while not self.player_lookup[self.player_lookup["key_retro"] == retro_id].empty:
                retro_num += 1
                retro_id = f"{last_name_retro}{first_name_retro}{retro_num:03d}"
            # x indicates a name is approximated because they haven't been in MLB in a previous retrosheet release
            return retro_id + "x"
        return pid  # type: ignore
