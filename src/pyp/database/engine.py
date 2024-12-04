from pathlib import Path

from sqlalchemy import create_engine

db_file = Path(__file__).parent / "pyp.sqlite"

engine = create_engine(f"sqlite:///{db_file.as_posix()}", echo=False)
