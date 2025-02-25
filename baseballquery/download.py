"""
Download and extract all the retrosheet zips including CurrentNames.csv

https://www.retrosheet.org/Nickname.htm
https://www.retrosheet.org/BIOFILE.TXT
https://www.retrosheet.org/game.htm#Regular%20Season%20Games
"""

from io import BytesIO
from pathlib import Path
import zipfile
import tqdm
import requests


def download_games():
    data_dir = Path("~/.baseballquery").expanduser()
    download_dir = data_dir / "downloads"
    download_dir.mkdir(parents=True, exist_ok=True)
    decade_zips = [
        "https://www.retrosheet.org/events/1910seve.zip",
        "https://www.retrosheet.org/events/1920seve.zip",
        "https://www.retrosheet.org/events/1930seve.zip",
        "https://www.retrosheet.org/events/1940seve.zip",
        "https://www.retrosheet.org/events/1950seve.zip",
        "https://www.retrosheet.org/events/1960seve.zip",
        "https://www.retrosheet.org/events/1970seve.zip",
        "https://www.retrosheet.org/events/1980seve.zip",
        "https://www.retrosheet.org/events/1990seve.zip",
        "https://www.retrosheet.org/events/2000seve.zip",
        "https://www.retrosheet.org/events/2010seve.zip",
        "https://www.retrosheet.org/events/2020seve.zip",
    ]
    for url in tqdm.tqdm(decade_zips, desc=" Retrosheet files downloading", position=1, leave=False):
        request = requests.get(url)
        zip = zipfile.ZipFile(BytesIO(request.content))
        zip.extractall(download_dir)


def download_year(year: int):
    data_dir = Path("~/.baseballquery").expanduser()
    download_dir = data_dir / "downloads"
    download_dir.mkdir(parents=True, exist_ok=True)
    url = f"https://www.retrosheet.org/events/{year}eve.zip"
    request = requests.get(url)
    zip = zipfile.ZipFile(BytesIO(request.content))
    zip.extractall(download_dir)
