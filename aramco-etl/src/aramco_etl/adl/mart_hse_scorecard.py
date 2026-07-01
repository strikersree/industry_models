"""ADL mart build -- mart_hse_scorecard (grain: facility x month).

TRIR/LTIR use the standard OSHA-style incidence-rate formula (rate per
200,000 hours worked, i.e. 100 employees working 40 hrs/week for 50 weeks).

NOTE: no payroll/timekeeping source is modelled in this framework, so
`labor_hours_worked` is approximated from booked maintenance labor hours per
facility (bdh.fact_maintenance_workorder via dim_asset.facility_id). Wire in
a real HR/timekeeping feed and swap this proxy out before using TRIR/LTIR
for regulatory reporting.

    spark-submit src/aramco_etl/adl/mart_hse_scorecard.py \\
        --business-date 2026-07-01 --run-id <airflow_run_id>
"""
from __future__ import annotations

import argparse
from datetime import datetime

from pyspark.sql import functions as F

from aramco_etl.common.delta_io import write_overwrite_partition
from aramco_etl.common.logging_utils import get_logger, job_run
from aramco_etl.common.spark_session import get_spark_session

logger = get_logger("adl.mart_hse_scorecard")

TARGET_TABLE = "adl.mart_hse_scorecard"
OSHA_RATE_BASE = 200000.0


def build_mart_hse_scorecard(business_date: str, run_id: str) -> None:
    spark = get_spark_session("adl_mart_hse_scorecard")
    with job_run(logger, "adl_mart_hse_scorecard", run_id):
        as_of = datetime.strptime(business_date, "%Y-%m-%d")
        year_month = as_of.strftime("%Y-%m")
        month_start_key = int(f"{as_of.year:04d}{as_of.month:02d}01")
        month_end_key = int(as_of.strftime("%Y%m%d"))

        incidents = spark.table("bdh.fact_hse_incident").filter(
            F.col("date_key").between(month_start_key, month_end_key)
        )
        incident_agg = incidents.groupBy("facility_id").agg(
            F.count("*").cast("int").alias("total_incidents"),
            F.sum(F.when(F.col("incident_type") == "LTI", 1).otherwise(0)).cast("int").alias("lost_time_incidents"),
            F.sum("lost_workdays").cast("int").alias("total_lost_workdays"),
        )

        assets = spark.table("bdh.dim_asset").filter(F.col("current_row_flag") == "Y").select(
            "asset_sk", "facility_id"
        )
        labor_hours = (
            spark.table("bdh.fact_maintenance_workorder")
            .filter(F.col("date_key").between(month_start_key, month_end_key))
            .join(assets, on="asset_sk", how="inner")
            .groupBy("facility_id")
            .agg(F.sum("labor_hours").alias("labor_hours_worked"))
        )

        mart_df = (
            incident_agg.join(labor_hours, on="facility_id", how="left")
            .fillna({"labor_hours_worked": 0.0, "total_lost_workdays": 0})
            .withColumn(
                "trir",
                F.when(
                    F.col("labor_hours_worked") > 0,
                    F.col("total_incidents") * OSHA_RATE_BASE / F.col("labor_hours_worked"),
                ).otherwise(F.lit(None).cast("double")),
            )
            .withColumn(
                "ltir",
                F.when(
                    F.col("labor_hours_worked") > 0,
                    F.col("lost_time_incidents") * OSHA_RATE_BASE / F.col("labor_hours_worked"),
                ).otherwise(F.lit(None).cast("double")),
            )
            .withColumn("year_month", F.lit(year_month))
            .withColumn("_adl_load_ts", F.current_timestamp())
            .select(
                "year_month", "facility_id", "total_incidents", "lost_time_incidents",
                "total_lost_workdays", "labor_hours_worked", "trir", "ltir", "_adl_load_ts",
            )
        )

        write_overwrite_partition(mart_df, TARGET_TABLE, partition_by=["year_month"])
        logger.info("mart_hse_scorecard written: %d rows for year_month=%s", mart_df.count(), year_month)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build ADL mart_hse_scorecard")
    parser.add_argument("--business-date", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    build_mart_hse_scorecard(args.business_date, args.run_id)


if __name__ == "__main__":
    main()
