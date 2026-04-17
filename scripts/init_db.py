"""
Run SQL schema files against the bungewatch database.
Used for local setup without Docker (Docker auto-runs these via initdb.d).
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pipeline.config import settings
from pipeline.db import get_engine
from sqlalchemy import text

SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "sql")
FILES = ["001_schema.sql", "002_indexes.sql", "003_functions.sql"]


def run():
    engine = get_engine()
    with engine.connect() as conn:
        for fname in FILES:
            path = os.path.join(SQL_DIR, fname)
            print(f"Running {fname}...")
            with open(path) as f:
                sql = f.read()
            conn.execute(text(sql))
            conn.commit()
            print(f"  {fname} OK")
    print("Database initialised.")


if __name__ == "__main__":
    run()
