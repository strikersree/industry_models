"""BDH fact build -- fact_maintenance_workorder (grain: work order).

    spark-submit src/aramco_etl/bdh/build_fact_maintenance_workorder.py \\
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

logger = get_logger("bdh.build_fact_maintenance_workorder")

TARGET_TABLE = "bdh.fact_maintenance_workorder"


def _work_order_dq_rules() -> list[DQRule]:
    return [
        DQRule("WO-01", "work_order_id must not be null", "REJECT", F.col("work_order_id").isNotNull()),
        DQRule("WO-02", "asset_id must not be null", "REJECT", F.col("asset_id").isNotNull()),
        DQRule("WO-03", "labor_hours must be non-negative", "WARN", F.col("labor_hours") >= 0),
    ]


def build_fact_maintenance_workorder(business_date: str, run_id: str) -> None:
    spark = get_spark_session("bdh_build_fact_maintenance_workorder")
    thresholds = load_dq_thresholds()

    with job_run(logger, "bdh_build_fact_maintenance_workorder", run_id):
        work_orders = spark.table("curated_maximo_eam.raw_work_order").filter(
            F.col("_business_date") == business_date
        )

        valid_wo = run_dq_gate(
            spark=spark, df=work_orders, rules=_work_order_dq_rules(), target_table=TARGET_TABLE,
            run_id=run_id, business_date=business_date, thresholds=thresholds,
        )

        current_assets = spark.table("bdh.dim_asset").filter(F.col("current_row_flag") == "Y")
        fact_df = (
            valid_wo.join(current_assets, on="asset_id", how="inner")
            .withColumn(
                "duration_hours",
                (F.unix_timestamp("actual_finish_ts") - F.unix_timestamp("actual_start_ts")) / 3600.0,
            )
            .withColumn(
                "schedule_variance_hours",
                (F.unix_timestamp("actual_start_ts") - F.unix_timestamp("scheduled_start_ts")) / 3600.0,
            )
            .withColumn("date_key", F.date_format(F.lit(business_date), "yyyyMMdd").cast("int"))
            .withColumn("_bdh_load_ts", F.current_timestamp())
            .withColumn("_dq_pass_rate", F.lit(None).cast("double"))
            .select(
                "date_key", "asset_sk", "work_order_id", "work_type", "priority", "status",
                "labor_hours", "material_cost_usd", "duration_hours", "schedule_variance_hours",
                "_bdh_load_ts", "_dq_pass_rate",
            )
        )

        write_overwrite_partition(fact_df, TARGET_TABLE, partition_by=["date_key"])
        logger.info(
            "fact_maintenance_workorder written: %d rows for business_date=%s", fact_df.count(), business_date
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Build BDH fact_maintenance_workorder")
    parser.add_argument("--business-date", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    build_fact_maintenance_workorder(args.business_date, args.run_id)


if __name__ == "__main__":
    main()
