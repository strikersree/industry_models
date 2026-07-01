"""ADL mart build -- mart_pipeline_integrity (grain: pipeline segment x month).

    spark-submit src/aramco_etl/adl/mart_pipeline_integrity.py \\
        --business-date 2026-07-01 --run-id <airflow_run_id>
"""
from __future__ import annotations

import argparse
from datetime import datetime

from pyspark.sql import functions as F

from aramco_etl.common.delta_io import write_overwrite_partition
from aramco_etl.common.logging_utils import get_logger, job_run
from aramco_etl.common.spark_session import get_spark_session

logger = get_logger("adl.mart_pipeline_integrity")

TARGET_TABLE = "adl.mart_pipeline_integrity"


def build_mart_pipeline_integrity(business_date: str, run_id: str) -> None:
    spark = get_spark_session("adl_mart_pipeline_integrity")
    with job_run(logger, "adl_mart_pipeline_integrity", run_id):
        as_of = datetime.strptime(business_date, "%Y-%m-%d")
        year_month = as_of.strftime("%Y-%m")
        month_start_key = int(f"{as_of.year:04d}{as_of.month:02d}01")
        month_end_key = int(as_of.strftime("%Y%m%d"))

        flow = spark.table("bdh.fact_pipeline_flow").filter(
            F.col("date_key").between(month_start_key, month_end_key)
        )

        mart_df = (
            flow.groupBy("pipeline_segment_id")
            .agg(
                F.avg("avg_flow_rate_bpd").alias("avg_flow_rate_bpd"),
                F.sum("leak_alarm_count").cast("int").alias("total_alarms"),
                F.sum("critical_alarm_count").cast("int").alias("critical_alarms"),
                F.sum(F.col("avg_flow_rate_bpd")).alias("_period_volume_bbl"),
            )
            .withColumn(
                "alarm_rate_per_1000bbl",
                F.when(
                    F.col("_period_volume_bbl") > 0,
                    F.col("total_alarms") / (F.col("_period_volume_bbl") / 1000.0),
                ),
            )
            .drop("_period_volume_bbl")
            .withColumn("year_month", F.lit(year_month))
            .withColumn("_adl_load_ts", F.current_timestamp())
            .select(
                "year_month", "pipeline_segment_id", "avg_flow_rate_bpd", "total_alarms",
                "critical_alarms", "alarm_rate_per_1000bbl", "_adl_load_ts",
            )
        )

        write_overwrite_partition(mart_df, TARGET_TABLE, partition_by=["year_month"])
        logger.info(
            "mart_pipeline_integrity written: %d rows for year_month=%s", mart_df.count(), year_month
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Build ADL mart_pipeline_integrity")
    parser.add_argument("--business-date", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    build_mart_pipeline_integrity(args.business_date, args.run_id)


if __name__ == "__main__":
    main()
