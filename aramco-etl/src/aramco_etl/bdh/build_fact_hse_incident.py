"""BDH fact build -- fact_hse_incident (grain: incident).

    spark-submit src/aramco_etl/bdh/build_fact_hse_incident.py \\
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

logger = get_logger("bdh.build_fact_hse_incident")

TARGET_TABLE = "bdh.fact_hse_incident"


def _incident_dq_rules() -> list[DQRule]:
    return [
        DQRule("HSE-01", "incident_id must not be null", "REJECT", F.col("incident_id").isNotNull()),
        DQRule("HSE-02", "facility_id must not be null", "REJECT", F.col("facility_id").isNotNull()),
        DQRule("HSE-03", "lost_workdays must be non-negative", "WARN", F.col("lost_workdays") >= 0),
    ]


def build_fact_hse_incident(business_date: str, run_id: str) -> None:
    spark = get_spark_session("bdh_build_fact_hse_incident")
    thresholds = load_dq_thresholds()

    with job_run(logger, "bdh_build_fact_hse_incident", run_id):
        incidents = spark.table("curated_hse.raw_incident_report").filter(
            F.col("_business_date") == business_date
        )

        valid_incidents = run_dq_gate(
            spark=spark, df=incidents, rules=_incident_dq_rules(), target_table=TARGET_TABLE,
            run_id=run_id, business_date=business_date, thresholds=thresholds,
        )

        parties = spark.table("bdh.dim_employee_vendor")
        fact_df = (
            valid_incidents.join(
                parties, valid_incidents["involved_employee_id"] == parties["party_id"], "left"
            )
            .withColumn("date_key", F.date_format(F.lit(business_date), "yyyyMMdd").cast("int"))
            .withColumn("_bdh_load_ts", F.current_timestamp())
            .withColumn("_dq_pass_rate", F.lit(None).cast("double"))
            .select(
                "date_key", "facility_id", "incident_id", "incident_type", "severity_rank",
                "lost_workdays", "party_sk", "_bdh_load_ts", "_dq_pass_rate",
            )
        )

        write_overwrite_partition(fact_df, TARGET_TABLE, partition_by=["date_key"])
        logger.info("fact_hse_incident written: %d rows for business_date=%s", fact_df.count(), business_date)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build BDH fact_hse_incident")
    parser.add_argument("--business-date", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    build_fact_hse_incident(args.business_date, args.run_id)


if __name__ == "__main__":
    main()
