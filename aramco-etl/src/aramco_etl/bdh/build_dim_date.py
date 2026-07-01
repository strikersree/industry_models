"""BDH dimension build -- dim_date.

Static calendar dimension. Idempotent full-refresh: safe to re-run for any
date range without affecting other dimensions/facts.

    spark-submit src/aramco_etl/bdh/build_dim_date.py \\
        --start-date 2020-01-01 --end-date 2035-12-31
"""
from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from aramco_etl.common.logging_utils import get_logger, job_run
from aramco_etl.common.spark_session import get_spark_session

logger = get_logger("bdh.build_dim_date")

# Fixed Gregorian Saudi public holidays are religious/lunar (Hijri) and shift
# year to year in practice; this framework flags weekends (Fri/Sat, the KSA
# weekend) and leaves holiday enrichment to a maintained reference table.
KSA_WEEKEND_DAYS = {"Friday", "Saturday"}


def build_dim_date(start_date: str, end_date: str, run_id: str) -> None:
    spark = get_spark_session("bdh_build_dim_date")
    with job_run(logger, "bdh_build_dim_date", run_id):
        df = spark.sql(
            f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) AS calendar_date"
        )
        dim = (
            df.withColumn("date_key", F.date_format("calendar_date", "yyyyMMdd").cast("int"))
            .withColumn("day_of_week", F.date_format("calendar_date", "EEEE"))
            .withColumn("day_of_month", F.dayofmonth("calendar_date"))
            .withColumn("month_number", F.month("calendar_date"))
            .withColumn("month_name", F.date_format("calendar_date", "MMMM"))
            .withColumn("quarter_number", F.quarter("calendar_date"))
            .withColumn("fiscal_year", F.year("calendar_date"))
            .withColumn("is_weekend", F.col("day_of_week").isin(*KSA_WEEKEND_DAYS))
            .withColumn("is_holiday_ksa", F.lit(False))
        )
        dim.write.format("delta").mode("overwrite").saveAsTable("bdh.dim_date")
        logger.info("dim_date refreshed: %d rows", dim.count())


def main() -> None:
    parser = argparse.ArgumentParser(description="Build BDH dim_date")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    build_dim_date(args.start_date, args.end_date, args.run_id)


if __name__ == "__main__":
    main()
