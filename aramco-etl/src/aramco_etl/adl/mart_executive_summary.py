"""ADL mart build -- mart_executive_summary (grain: month, company-wide).

Rolls up production, reliability, HSE, financial, and pipeline-integrity
marts into a single company-wide monthly KPI row for the executive
dashboard. Must run after all other ADL mart builders for the same
business date (see dags/aramco_adl_mart_dag.py).

    spark-submit src/aramco_etl/adl/mart_executive_summary.py \\
        --business-date 2026-07-01 --run-id <airflow_run_id>
"""
from __future__ import annotations

import argparse
from datetime import datetime

from pyspark.sql import functions as F

from aramco_etl.common.delta_io import write_overwrite_partition
from aramco_etl.common.logging_utils import get_logger, job_run
from aramco_etl.common.spark_session import get_spark_session

logger = get_logger("adl.mart_executive_summary")

TARGET_TABLE = "adl.mart_executive_summary"


def build_mart_executive_summary(business_date: str, run_id: str) -> None:
    spark = get_spark_session("adl_mart_executive_summary")
    with job_run(logger, "adl_mart_executive_summary", run_id):
        as_of = datetime.strptime(business_date, "%Y-%m-%d")
        year_month = as_of.strftime("%Y-%m")

        production = spark.table("adl.mart_production_kpi").filter(
            F.col("date_key").between(int(f"{as_of.year:04d}{as_of.month:02d}01"), int(as_of.strftime("%Y%m%d")))
        ).agg(
            F.sum("total_oil_bbl").alias("total_oil_bbl"),
            F.avg("production_efficiency_pct").alias("production_efficiency_pct"),
        ).collect()[0]

        reliability = spark.table("adl.mart_equipment_reliability").filter(
            F.col("year_month") == year_month
        ).agg(F.avg("availability_pct").alias("avg_asset_availability_pct")).collect()[0]

        hse = spark.table("adl.mart_hse_scorecard").filter(F.col("year_month") == year_month).agg(
            F.avg("trir").alias("trir"), F.avg("ltir").alias("ltir")
        ).collect()[0]

        financial = spark.table("adl.mart_financial_cost").filter(F.col("year_month") == year_month).agg(
            F.sum("actual_cost_usd").alias("total_cost_usd"),
            F.avg("cost_per_bbl_usd").alias("avg_cost_per_bbl_usd"),
        ).collect()[0]

        pipeline = spark.table("adl.mart_pipeline_integrity").filter(F.col("year_month") == year_month).agg(
            F.sum("critical_alarms").alias("critical_pipeline_alarms")
        ).collect()[0]

        summary_df = spark.createDataFrame(
            [(
                year_month,
                production["total_oil_bbl"],
                production["production_efficiency_pct"],
                reliability["avg_asset_availability_pct"],
                hse["trir"],
                hse["ltir"],
                financial["total_cost_usd"],
                financial["avg_cost_per_bbl_usd"],
                pipeline["critical_pipeline_alarms"],
            )],
            schema=(
                "year_month string, total_oil_bbl double, production_efficiency_pct double, "
                "avg_asset_availability_pct double, trir double, ltir double, "
                "total_cost_usd decimal(18,2), avg_cost_per_bbl_usd double, critical_pipeline_alarms int"
            ),
        ).withColumn("_adl_load_ts", F.current_timestamp())

        write_overwrite_partition(summary_df, TARGET_TABLE, partition_by=["year_month"])
        logger.info("mart_executive_summary written for year_month=%s", year_month)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build ADL mart_executive_summary")
    parser.add_argument("--business-date", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    build_mart_executive_summary(args.business_date, args.run_id)


if __name__ == "__main__":
    main()
