"""
Load seeds/foreign_laws.csv into the foreign_laws table.
Idempotent: skips rows where (jurisdiction, law_name) already exists.
"""
import csv
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pipeline.db import get_engine
from sqlalchemy import text

SEED_PATH = os.path.join(os.path.dirname(__file__), "..", "seeds", "foreign_laws.csv")


def run():
    engine = get_engine()
    inserted = 0
    skipped = 0

    with open(SEED_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        with engine.connect() as conn:
            for row in reader:
                keywords = [k.strip() for k in row["keywords"].split(",") if k.strip()]
                result = conn.execute(
                    text("""
                        INSERT INTO foreign_laws
                            (jurisdiction, law_name, law_year, summary, full_text_url, keywords)
                        VALUES
                            (:jurisdiction, :law_name, :law_year, :summary, :full_text_url, :keywords)
                        ON CONFLICT DO NOTHING
                        RETURNING id
                    """),
                    {
                        "jurisdiction": row["jurisdiction"],
                        "law_name": row["law_name"],
                        "law_year": int(row["law_year"]) if row["law_year"] else None,
                        "summary": row["summary"],
                        "full_text_url": row["full_text_url"],
                        "keywords": keywords,
                    },
                )
                if result.rowcount:
                    inserted += 1
                    print(f"  Inserted: {row['jurisdiction']} — {row['law_name']}")
                else:
                    skipped += 1
            conn.commit()

    print(f"\nSeed complete. Inserted: {inserted}, Skipped (already exist): {skipped}")


if __name__ == "__main__":
    run()
