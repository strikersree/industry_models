"""BDH dimension build -- dim_employee_vendor (Type-1, overwrite in place).

Conforms parties referenced across Maximo work orders (crews) and HSE
incident reports (involved employees) into one party dimension, joined by
fact_maintenance_workorder and fact_hse_incident.

    spark-submit src/aramco_etl/bdh/build_dim_employee_vendor.py \\
        --business-date 2026-07-01 --run-id <airflow_run_id>
"""
from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from aramco_etl.common.logging_utils import get_logger, job_run
from aramco_etl.common.spark_session import get_spark_session
from aramco_etl.common.type1_dim import upsert_type1_dimension

logger = get_logger("bdh.build_dim_employee_vendor")


def build_dim_employee_vendor(business_date: str, run_id: str) -> None:
    spark = get_spark_session("bdh_build_dim_employee_vendor")
    with job_run(logger, "bdh_build_dim_employee_vendor", run_id):
        crews = (
            spark.table("curated_maximo_eam.raw_work_order")
            .filter(F.col("_business_date") == business_date)
            .select(F.col("assigned_crew_id").alias("party_id"))
            .filter(F.col("party_id").isNotNull())
            .distinct()
            .withColumn("party_type", F.lit("CREW"))
        )
        employees = (
            spark.table("curated_hse.raw_incident_report")
            .filter(F.col("_business_date") == business_date)
            .select(F.col("involved_employee_id").alias("party_id"))
            .filter(F.col("party_id").isNotNull())
            .distinct()
            .withColumn("party_type", F.lit("EMPLOYEE"))
        )

        source = (
            crews.unionByName(employees)
            .dropDuplicates(["party_id"])
            .withColumn("party_name", F.col("party_id"))
            .withColumn("department_code", F.lit(None).cast("string"))
            .withColumn("last_updated_ts", F.current_timestamp())
        )

        upsert_type1_dimension(
            spark=spark,
            source_df=source,
            target_table="bdh.dim_employee_vendor",
            surrogate_key_col="party_sk",
            business_key_cols=["party_id"],
            update_columns=["party_type", "party_name", "department_code", "last_updated_ts"],
        )
        logger.info("dim_employee_vendor upsert complete for business_date=%s", business_date)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build BDH dim_employee_vendor")
    parser.add_argument("--business-date", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    build_dim_employee_vendor(args.business_date, args.run_id)


if __name__ == "__main__":
    main()
