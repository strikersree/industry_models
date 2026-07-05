"""BDH dimension build -- dim_well_field (SCD2).

Conforms the distinct well/field identifiers observed in upstream SCADA
telemetry into a stable reference dimension joined by the production fact.

    spark-submit src/aramco_etl/bdh/build_dim_well_field.py \\
        --business-date 2026-07-01 --run-id <airflow_run_id>
"""
from __future__ import annotations

import argparse

from pyspark.sql import Window, functions as F

from aramco_etl.common.logging_utils import get_logger, job_run
from aramco_etl.common.scd2 import apply_scd2, with_change_hash
from aramco_etl.common.spark_session import get_spark_session

logger = get_logger("bdh.build_dim_well_field")

TRACKED_COLUMNS = ["business_domain"]


def build_dim_well_field(business_date: str, run_id: str) -> None:
    spark = get_spark_session("bdh_build_dim_well_field")
    with job_run(logger, "bdh_build_dim_well_field", run_id):
        readings = spark.table("curated_scada_upstream.raw_well_production_reading").filter(
            F.col("_business_date") == business_date
        )
        source = (
            readings.select("well_id", "field_id")
            .distinct()
            .withColumn("business_domain", F.lit("upstream"))
        )

        if not spark.catalog.tableExists("bdh.dim_well_field") or spark.table("bdh.dim_well_field").isEmpty():
            seeded = (
                with_change_hash(source, TRACKED_COLUMNS)
                .withColumn("well_field_sk", F.row_number().over(Window.orderBy("well_id")).cast("bigint"))
                .withColumn("row_eff_date", F.to_date(F.lit(business_date)))
                .withColumn("row_exp_date", F.to_date(F.lit("9999-12-31")))
                .withColumn("current_row_flag", F.lit("Y"))
            )
            seeded.write.format("delta").mode("append").saveAsTable("bdh.dim_well_field")
            logger.info("dim_well_field seeded: %d rows", seeded.count())
            return

        apply_scd2(
            spark=spark,
            source_df=source,
            target_table="bdh.dim_well_field",
            surrogate_key_col="well_field_sk",
            business_key_cols=["well_id", "field_id"],
            tracked_columns=TRACKED_COLUMNS,
            as_of_date=business_date,
        )
        logger.info("dim_well_field SCD2 merge complete for business_date=%s", business_date)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build BDH dim_well_field")
    parser.add_argument("--business-date", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    build_dim_well_field(args.business_date, args.run_id)


if __name__ == "__main__":
    main()
