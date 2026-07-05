"""Executes a .sql DDL file against a Spark session, one statement at a time.

Used to stand up the curated/BDH/ADL schemas on a fresh environment (local
dev box or a real cluster) before any Airflow DAG runs. Substitutes
${DATALAKE_ROOT} the same way the runtime jobs resolve config paths.

    python scripts/run_sql_file.py sql/curated/01_schema_scada_upstream.sql
    python scripts/run_sql_file.py sql/bdh/*.sql   (shell-expanded, runs each in order)
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from aramco_etl.common.spark_session import get_spark_session  # noqa: E402


def run_sql_file(spark, path: str) -> None:
    with open(path, "r", encoding="utf-8") as fh:
        text = fh.read()
    text = os.path.expandvars(text)
    for statement in text.split(";"):
        # Only skip genuinely empty chunks -- a chunk can legitimately start
        # with a "-- comment" line and still contain real SQL after it;
        # Spark's own SQL parser strips comments, so just hand it the chunk.
        statement = statement.strip()
        if statement:
            spark.sql(statement)
    print(f"OK: {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one or more .sql DDL files against Spark/Delta")
    parser.add_argument("paths", nargs="+", help="Path(s) to .sql file(s), applied in order given")
    args = parser.parse_args()

    spark = get_spark_session("aramco_run_sql_file")
    for path in args.paths:
        run_sql_file(spark, path)


if __name__ == "__main__":
    main()
