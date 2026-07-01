"""ADL mart build -- mart_production_kpi (grain: field x day).

Aggregates BDH fact_production_daily (well grain) up to field grain and
derives the production-efficiency / uptime KPIs used by the upstream
operations dashboard.

    spark-submit src/aramco_etl/adl/mart_production_kpi.py \\
        --business-date 2026-07-01 --run-id <airflow_run_id>
"""
from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from aramco_etl.common.delta_io import write_overwrite_partition
from aramco_etl.common.logging_utils import get_logger, job_run
from aramco_etl.common.spark_session import get_spark_session

logger = get_logger("adl.mart_production_kpi")

TARGET_TABLE = "adl.mart_production_kpi"


def build_mart_production_kpi(business_date: str, run_id: str) -> None:
    spark = get_spark_session("adl_mart_production_kpi")
    with job_run(logger, "adl_mart_production_kpi", run_id):
        date_key = int(business_date.replace("-", ""))
        fact = spark.table("bdh.fact_production_daily").filter(F.col("date_key") == date_key)
        wells = spark.table("bdh.dim_well_field").filter(F.col("current_row_flag") == "Y")

        joined = fact.join(wells, on="well_field_sk", how="inner")

        mart_df = (
            joined.groupBy("date_key", "field_id")
            .agg(
                F.sum("oil_volume_bbl").alias("total_oil_bbl"),
                F.sum("gas_volume_mscf").alias("total_gas_mscf"),
                F.sum("water_volume_bbl").alias("total_water_bbl"),
                F.sum("planned_volume_bbl").alias("planned_volume_bbl"),
                F.avg(F.col("uptime_hours") / F.lit(24.0)).alias("uptime_pct"),
            )
            .withColumn(
                "production_efficiency_pct",
                F.least(
                    F.lit(100.0),
                    F.when(
                        F.col("planned_volume_bbl") > 0,
                        F.col("total_oil_bbl") / F.col("planned_volume_bbl") * 100.0,
                    ).otherwise(F.lit(100.0)),
                ),
            )
            .withColumn("uptime_pct", F.col("uptime_pct") * 100.0)
            .withColumn("_adl_load_ts", F.current_timestamp())
            .select(
                "date_key", "field_id", "total_oil_bbl", "total_gas_mscf", "total_water_bbl",
                "planned_volume_bbl", "production_efficiency_pct", "uptime_pct", "_adl_load_ts",
            )
        )

        write_overwrite_partition(mart_df, TARGET_TABLE, partition_by=["date_key"])
        logger.info("mart_production_kpi written: %d rows for business_date=%s", mart_df.count(), business_date)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build ADL mart_production_kpi")
    parser.add_argument("--business-date", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    build_mart_production_kpi(args.business_date, args.run_id)


if __name__ == "__main__":
    main()
