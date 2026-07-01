"""BDH fact build -- fact_pipeline_flow (grain: pipeline segment x day).

    spark-submit src/aramco_etl/bdh/build_fact_pipeline_flow.py \\
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

logger = get_logger("bdh.build_fact_pipeline_flow")

TARGET_TABLE = "bdh.fact_pipeline_flow"


def _flow_dq_rules() -> list[DQRule]:
    return [
        DQRule(
            "PIPE-01", "pipeline_segment_id must not be null", "REJECT",
            F.col("pipeline_segment_id").isNotNull(),
        ),
        DQRule("PIPE-02", "flow_rate_bpd must be non-negative", "REJECT", F.col("flow_rate_bpd") >= 0),
    ]


def build_fact_pipeline_flow(business_date: str, run_id: str) -> None:
    spark = get_spark_session("bdh_build_fact_pipeline_flow")
    thresholds = load_dq_thresholds()

    with job_run(logger, "bdh_build_fact_pipeline_flow", run_id):
        readings = spark.table("curated_pipeline_scada.raw_pipeline_flow_reading").filter(
            F.col("_business_date") == business_date
        )
        valid_readings = run_dq_gate(
            spark=spark, df=readings, rules=_flow_dq_rules(), target_table=TARGET_TABLE,
            run_id=run_id, business_date=business_date, thresholds=thresholds,
        )

        alarms = spark.table("curated_pipeline_scada.raw_leak_detection_alarm").filter(
            F.col("_business_date") == business_date
        )
        alarm_counts = alarms.groupBy("pipeline_segment_id").agg(
            F.count("*").cast("int").alias("leak_alarm_count"),
            F.sum(F.when(F.col("severity") == "CRITICAL", 1).otherwise(0)).cast("int").alias("critical_alarm_count"),
        )

        daily_flow = valid_readings.groupBy("pipeline_segment_id").agg(
            F.avg("flow_rate_bpd").alias("avg_flow_rate_bpd"),
            F.avg("line_pressure_psi").alias("avg_line_pressure_psi"),
        )

        fact_df = (
            daily_flow.join(alarm_counts, on="pipeline_segment_id", how="left")
            .fillna({"leak_alarm_count": 0, "critical_alarm_count": 0})
            .withColumn("date_key", F.date_format(F.lit(business_date), "yyyyMMdd").cast("int"))
            .withColumn("_bdh_load_ts", F.current_timestamp())
            .withColumn("_dq_pass_rate", F.lit(None).cast("double"))
            .select(
                "date_key", "pipeline_segment_id", "avg_flow_rate_bpd", "avg_line_pressure_psi",
                "leak_alarm_count", "critical_alarm_count", "_bdh_load_ts", "_dq_pass_rate",
            )
        )

        write_overwrite_partition(fact_df, TARGET_TABLE, partition_by=["date_key"])
        logger.info("fact_pipeline_flow written: %d rows for business_date=%s", fact_df.count(), business_date)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build BDH fact_pipeline_flow")
    parser.add_argument("--business-date", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    build_fact_pipeline_flow(args.business_date, args.run_id)


if __name__ == "__main__":
    main()
