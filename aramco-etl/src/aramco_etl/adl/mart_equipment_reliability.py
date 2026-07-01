"""ADL mart build -- mart_equipment_reliability (grain: asset x month).

Recomputes month-to-date MTBF / MTTR / availability every run so the
current month's partition always reflects all downtime booked so far;
completed months are simply no longer touched once the run date moves on.

    spark-submit src/aramco_etl/adl/mart_equipment_reliability.py \\
        --business-date 2026-07-01 --run-id <airflow_run_id>
"""
from __future__ import annotations

import argparse
from datetime import datetime

from pyspark.sql import functions as F

from aramco_etl.common.delta_io import write_overwrite_partition
from aramco_etl.common.logging_utils import get_logger, job_run
from aramco_etl.common.spark_session import get_spark_session

logger = get_logger("adl.mart_equipment_reliability")

TARGET_TABLE = "adl.mart_equipment_reliability"


def build_mart_equipment_reliability(business_date: str, run_id: str) -> None:
    spark = get_spark_session("adl_mart_equipment_reliability")
    with job_run(logger, "adl_mart_equipment_reliability", run_id):
        as_of = datetime.strptime(business_date, "%Y-%m-%d")
        year_month = as_of.strftime("%Y-%m")
        month_start_key = int(f"{as_of.year:04d}{as_of.month:02d}01")
        month_end_key = int(as_of.strftime("%Y%m%d"))
        elapsed_days = as_of.day

        downtime = spark.table("bdh.fact_equipment_downtime").filter(
            F.col("date_key").between(month_start_key, month_end_key)
        )
        assets = spark.table("bdh.dim_asset").filter(F.col("current_row_flag") == "Y")

        agg = (
            downtime.groupBy("asset_sk")
            .agg(
                F.count("*").cast("int").alias("failure_count"),
                F.sum("downtime_hours").alias("total_downtime_hours"),
            )
            .join(assets, on="asset_sk", how="inner")
        )

        period_hours = elapsed_days * 24.0
        mart_df = (
            agg.withColumn("total_downtime_hours", F.coalesce(F.col("total_downtime_hours"), F.lit(0.0)))
            .withColumn(
                "mtbf_hours",
                F.when(F.col("failure_count") > 0, (period_hours - F.col("total_downtime_hours")) / F.col("failure_count")),
            )
            .withColumn(
                "mttr_hours",
                F.when(F.col("failure_count") > 0, F.col("total_downtime_hours") / F.col("failure_count")),
            )
            .withColumn(
                "availability_pct",
                F.greatest(F.lit(0.0), (F.lit(period_hours) - F.col("total_downtime_hours")) / F.lit(period_hours) * 100.0),
            )
            .withColumn("year_month", F.lit(year_month))
            .withColumn("_adl_load_ts", F.current_timestamp())
            .select(
                "year_month", "asset_id", "facility_id", "failure_count", "total_downtime_hours",
                "mtbf_hours", "mttr_hours", "availability_pct", "_adl_load_ts",
            )
        )

        write_overwrite_partition(mart_df, TARGET_TABLE, partition_by=["year_month"])
        logger.info(
            "mart_equipment_reliability written: %d rows for year_month=%s", mart_df.count(), year_month
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Build ADL mart_equipment_reliability")
    parser.add_argument("--business-date", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    build_mart_equipment_reliability(args.business_date, args.run_id)


if __name__ == "__main__":
    main()
