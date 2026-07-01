"""BDH fact build -- fact_production_daily (grain: well x day).

Aggregates curated_scada_upstream well telemetry to a daily grain, resolves
dimension keys, DQ-gates the batch, and writes the conformed fact.

    spark-submit src/aramco_etl/bdh/build_fact_production_daily.py \\
        --business-date 2026-07-01 --run-id <airflow_run_id>
"""
from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from aramco_etl.bdh.dq_gate import run_dq_gate
from aramco_etl.common.config import load_dq_thresholds
from aramco_etl.common.data_quality import DQRule
from aramco_etl.common.delta_io import write_overwrite_partition
from aramco_etl.common.logging_utils import get_logger, job_run
from aramco_etl.common.spark_session import get_spark_session

logger = get_logger("bdh.build_fact_production_daily")

TARGET_TABLE = "bdh.fact_production_daily"


def _reading_dq_rules() -> list[DQRule]:
    return [
        DQRule("PROD-01", "well_id must not be null", "REJECT", F.col("well_id").isNotNull()),
        DQRule("PROD-02", "oil_rate_bopd must be non-negative", "REJECT", F.col("oil_rate_bopd") >= 0),
        DQRule(
            "PROD-03", "tubing_pressure_psi must be within a physically plausible range",
            "WARN", F.col("tubing_pressure_psi").between(0, 15000),
        ),
    ]


def build_fact_production_daily(business_date: str, run_id: str) -> None:
    spark = get_spark_session("bdh_build_fact_production_daily")
    thresholds = load_dq_thresholds()

    with job_run(logger, "bdh_build_fact_production_daily", run_id):
        readings = spark.table("curated_scada_upstream.raw_well_production_reading").filter(
            F.col("_business_date") == business_date
        )

        valid_readings = run_dq_gate(
            spark=spark,
            df=readings,
            rules=_reading_dq_rules(),
            target_table=TARGET_TABLE,
            run_id=run_id,
            business_date=business_date,
            thresholds=thresholds,
        )

        daily = valid_readings.groupBy("well_id", "field_id").agg(
            F.avg("oil_rate_bopd").alias("oil_volume_bbl"),
            F.avg("gas_rate_mscfd").alias("gas_volume_mscf"),
            F.avg("water_rate_bwpd").alias("water_volume_bbl"),
            F.avg("tubing_pressure_psi").alias("avg_tubing_pressure_psi"),
            F.countDistinct(
                F.when(F.col("well_status_code") == "PRODUCING", F.hour("reading_ts"))
            ).cast("double").alias("uptime_hours"),
        ).withColumn("downtime_hours", F.lit(24.0) - F.col("uptime_hours"))

        current_wells = spark.table("bdh.dim_well_field").filter(F.col("current_row_flag") == "Y")
        daily_with_keys = daily.join(current_wells, on=["well_id", "field_id"], how="inner").withColumn(
            "date_key", F.date_format(F.lit(business_date), "yyyyMMdd").cast("int")
        )

        # No nominations/plan source is modelled yet; use the well's own trailing
        # 7-day average as a proxy "planned" volume for the OEE-style efficiency KPI.
        if spark.catalog.tableExists(TARGET_TABLE) and not spark.table(TARGET_TABLE).isEmpty():
            history = spark.table(TARGET_TABLE).filter(
                F.col("date_key") >= F.date_format(F.date_sub(F.lit(business_date), 7), "yyyyMMdd").cast("int")
            )
            trailing_avg = history.groupBy("well_field_sk").agg(
                F.avg("oil_volume_bbl").alias("planned_volume_bbl")
            )
            daily_with_keys = daily_with_keys.join(trailing_avg, on="well_field_sk", how="left")
        else:
            daily_with_keys = daily_with_keys.withColumn("planned_volume_bbl", F.lit(None).cast("double"))

        fact_df = (
            daily_with_keys.withColumn(
                "planned_volume_bbl", F.coalesce(F.col("planned_volume_bbl"), F.col("oil_volume_bbl"))
            )
            .withColumn("_bdh_load_ts", F.current_timestamp())
            .withColumn("_dq_pass_rate", F.lit(None).cast("double"))
            .select(
                "date_key", "well_field_sk", "oil_volume_bbl", "gas_volume_mscf", "water_volume_bbl",
                "avg_tubing_pressure_psi", "planned_volume_bbl", "uptime_hours", "downtime_hours",
                "_bdh_load_ts", "_dq_pass_rate",
            )
        )

        write_overwrite_partition(fact_df, TARGET_TABLE, partition_by=["date_key"])
        logger.info("fact_production_daily written: %d rows for business_date=%s", fact_df.count(), business_date)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build BDH fact_production_daily")
    parser.add_argument("--business-date", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    build_fact_production_daily(args.business_date, args.run_id)


if __name__ == "__main__":
    main()
