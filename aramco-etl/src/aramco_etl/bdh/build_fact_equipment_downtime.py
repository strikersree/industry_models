"""BDH fact build -- fact_equipment_downtime (grain: asset x downtime event).

    spark-submit src/aramco_etl/bdh/build_fact_equipment_downtime.py \\
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

logger = get_logger("bdh.build_fact_equipment_downtime")

TARGET_TABLE = "bdh.fact_equipment_downtime"


def _downtime_dq_rules() -> list[DQRule]:
    return [
        DQRule("DOWN-01", "asset_id must not be null", "REJECT", F.col("asset_id").isNotNull()),
        DQRule(
            "DOWN-02", "downtime_end_ts must not precede downtime_start_ts",
            "REJECT", F.col("downtime_end_ts") >= F.col("downtime_start_ts"),
        ),
    ]


def build_fact_equipment_downtime(business_date: str, run_id: str) -> None:
    spark = get_spark_session("bdh_build_fact_equipment_downtime")
    thresholds = load_dq_thresholds()

    with job_run(logger, "bdh_build_fact_equipment_downtime", run_id):
        events = spark.table("curated_maximo_eam.raw_downtime_event").filter(
            F.col("_business_date") == business_date
        )

        valid_events = run_dq_gate(
            spark=spark, df=events, rules=_downtime_dq_rules(), target_table=TARGET_TABLE,
            run_id=run_id, business_date=business_date, thresholds=thresholds,
        )

        current_assets = spark.table("bdh.dim_asset").filter(F.col("current_row_flag") == "Y")
        fact_df = (
            valid_events.join(current_assets, on="asset_id", how="inner")
            .withColumn(
                "downtime_hours",
                (F.unix_timestamp("downtime_end_ts") - F.unix_timestamp("downtime_start_ts")) / 3600.0,
            )
            .withColumn("date_key", F.date_format(F.lit(business_date), "yyyyMMdd").cast("int"))
            .withColumn("_bdh_load_ts", F.current_timestamp())
            .withColumn("_dq_pass_rate", F.lit(None).cast("double"))
            .select(
                "date_key", "asset_sk", "downtime_start_ts", "downtime_end_ts", "downtime_hours",
                "downtime_reason_code", "linked_work_order_id", "_bdh_load_ts", "_dq_pass_rate",
            )
        )

        write_overwrite_partition(fact_df, TARGET_TABLE, partition_by=["date_key"])
        logger.info(
            "fact_equipment_downtime written: %d rows for business_date=%s", fact_df.count(), business_date
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Build BDH fact_equipment_downtime")
    parser.add_argument("--business-date", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    build_fact_equipment_downtime(args.business_date, args.run_id)


if __name__ == "__main__":
    main()
