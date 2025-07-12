from pathlib import Path
import sqlalchemy

data_dir = Path("~/.baseballquery").expanduser()
if not data_dir.exists():
    data_dir.mkdir()
# Create a SQLite database in the data directory
db_path = data_dir / "baseballquery.db"
engine = sqlalchemy.create_engine(f"sqlite:///{db_path}")