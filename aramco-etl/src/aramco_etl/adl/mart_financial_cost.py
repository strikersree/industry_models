"""ADL mart build -- mart_financial_cost (grain: cost center x month).

`cost_per_bbl_usd` is only meaningful for upstream cost centers and is
computed against total company-wide upstream production for the same
month (bdh.fact_production_daily via adl.mart_production_kpi), since
production isn't booked to individual cost centers in this model.

    spark-submit src/aramco_etl/adl/mart_financial_cost.py \\
        --business-date 2026-07-01 --run-id <airflow_run_id>
"""
from __future__ import annotations

import argparse
from datetime import datetime

from pyspark.sql import functions as F

from aramco_etl.common.delta_io import write_overwrite_partition
from aramco_etl.common.logging_utils import get_logger, job_run
from aramco_etl.common.spark_session import get_spark_session

logger = get_logger("adl.mart_financial_cost")

TARGET_TABLE = "adl.mart_financial_cost"


def build_mart_financial_cost(business_date: str, run_id: str) -> None:
    spark = get_spark_session("adl_mart_financial_cost")
    with job_run(logger, "adl_mart_financial_cost", run_id):
        as_of = datetime.strptime(business_date, "%Y-%m-%d")
        year_month = as_of.strftime("%Y-%m")
        month_start_key = int(f"{as_of.year:04d}{as_of.month:02d}01")
        month_end_key = int(as_of.strftime("%Y%m%d"))

        actuals = spark.table("bdh.fact_financial_actuals").filter(
            F.col("date_key").between(month_start_key, month_end_key)
        )
        cost_centers = spark.table("bdh.dim_cost_center")

        cost_agg = (
            actuals.join(cost_centers, on="cost_center_sk", how="inner")
            .groupBy("cost_center_code", "business_domain")
            .agg(
                F.sum("actual_amount_usd").cast("decimal(18,2)").alias("actual_cost_usd"),
                F.sum("budget_amount_usd").cast("decimal(18,2)").alias("budget_cost_usd"),
            )
        )

        total_production = (
            spark.table("adl.mart_production_kpi")
            .filter(F.col("date_key").between(month_start_key, month_end_key))
            .agg(F.sum("total_oil_bbl").alias("production_bbl"))
            .collect()[0]["production_bbl"]
        ) or 0.0

        mart_df = (
            cost_agg.withColumn(
                "variance_pct",
                F.when(
                    F.col("budget_cost_usd") != 0,
                    (F.col("actual_cost_usd") - F.col("budget_cost_usd")) / F.col("budget_cost_usd") * 100.0,
                ),
            )
            .withColumn("production_bbl", F.lit(total_production))
            .withColumn(
                "cost_per_bbl_usd",
                F.when(
                    (F.col("business_domain") == "upstream") & (F.lit(total_production) > 0),
                    F.col("actual_cost_usd") / F.lit(total_production),
                ),
            )
            .withColumn("year_month", F.lit(year_month))
            .withColumn("_adl_load_ts", F.current_timestamp())
            .select(
                "year_month", "cost_center_code", "business_domain", "actual_cost_usd", "budget_cost_usd",
                "variance_pct", "production_bbl", "cost_per_bbl_usd", "_adl_load_ts",
            )
        )

        write_overwrite_partition(mart_df, TARGET_TABLE, partition_by=["year_month"])
        logger.info("mart_financial_cost written: %d rows for year_month=%s", mart_df.count(), year_month)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build ADL mart_financial_cost")
    parser.add_argument("--business-date", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    build_mart_financial_cost(args.business_date, args.run_id)


if __name__ == "__main__":
    main()
