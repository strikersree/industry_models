"""BDH dimension build -- dim_asset (SCD2).

Conforms curated_maximo_eam.raw_asset_master into the versioned asset
dimension used by every reliability/maintenance fact.

    spark-submit src/aramco_etl/bdh/build_dim_asset.py \\
        --business-date 2026-07-01 --run-id <airflow_run_id>
"""
from __future__ import annotations

import argparse

from pyspark.sql import Window, functions as F

from aramco_etl.common.logging_utils import get_logger, job_run
from aramco_etl.common.scd2 import apply_scd2, with_change_hash
from aramco_etl.common.spark_session import get_spark_session

logger = get_logger("bdh.build_dim_asset")

TRACKED_COLUMNS = [
    "asset_description", "asset_type", "facility_id", "location_code",
    "manufacturer", "criticality_rank", "parent_asset_id", "record_status",
]


def build_dim_asset(business_date: str, run_id: str) -> None:
    spark = get_spark_session("bdh_build_dim_asset")
    with job_run(logger, "bdh_build_dim_asset", run_id):
        raw = spark.table("curated_maximo_eam.raw_asset_master").filter(
            F.col("_business_date") == business_date
        )
        # Multiple loads may land for the same asset_id in one batch; keep the
        # most recently modified record per asset for this business date.
        dedup_window = Window.partitionBy("asset_id").orderBy(F.col("last_modified_ts").desc())
        source = (
            raw.withColumn("_rn", F.row_number().over(dedup_window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
            .select(
                "asset_id", "asset_description", "asset_type", "facility_id",
                "location_code", "manufacturer", "install_date", "criticality_rank",
                "parent_asset_id", "record_status",
            )
        )

        if not spark.catalog.tableExists("bdh.dim_asset") or spark.table("bdh.dim_asset").isEmpty():
            seeded = (
                with_change_hash(source, TRACKED_COLUMNS)
                .withColumn("asset_sk", F.row_number().over(Window.orderBy("asset_id")).cast("bigint"))
                .withColumn("row_eff_date", F.to_date(F.lit(business_date)))
                .withColumn("row_exp_date", F.to_date(F.lit("9999-12-31")))
                .withColumn("current_row_flag", F.lit("Y"))
            )
            seeded.write.format("delta").mode("append").saveAsTable("bdh.dim_asset")
            logger.info("dim_asset seeded: %d rows", seeded.count())
            return

        apply_scd2(
            spark=spark,
            source_df=source,
            target_table="bdh.dim_asset",
            surrogate_key_col="asset_sk",
            business_key_cols=["asset_id"],
            tracked_columns=TRACKED_COLUMNS,
            as_of_date=business_date,
        )
        logger.info("dim_asset SCD2 merge complete for business_date=%s", business_date)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build BDH dim_asset")
    parser.add_argument("--business-date", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    build_dim_asset(args.business_date, args.run_id)


if __name__ == "__main__":
    main()
