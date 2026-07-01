"""BDH dimension build -- dim_product (Type-1, overwrite in place).

Conforms curated_refinery_lims.raw_product_quality_test product codes into a
stable reference dimension joined by fact_refinery_yield.

    spark-submit src/aramco_etl/bdh/build_dim_product.py \\
        --business-date 2026-07-01 --run-id <airflow_run_id>
"""
from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from aramco_etl.common.logging_utils import get_logger, job_run
from aramco_etl.common.spark_session import get_spark_session
from aramco_etl.common.type1_dim import upsert_type1_dimension

logger = get_logger("bdh.build_dim_product")

# Refined-product code prefix -> product family.
PRODUCT_FAMILY_MAP = {
    "GASOLINE": "LIGHT_DISTILLATE",
    "DIESEL": "MIDDLE_DISTILLATE",
    "JET": "MIDDLE_DISTILLATE",
}


def _infer_product_family():
    return F.coalesce(
        *[
            F.when(F.col("product_code").startswith(prefix), F.lit(family))
            for prefix, family in PRODUCT_FAMILY_MAP.items()
        ],
        F.lit("OTHER"),
    )


def build_dim_product(business_date: str, run_id: str) -> None:
    spark = get_spark_session("bdh_build_dim_product")
    with job_run(logger, "bdh_build_dim_product", run_id):
        raw = spark.table("curated_refinery_lims.raw_product_quality_test").filter(
            F.col("_business_date") == business_date
        )
        source = (
            raw.select("product_code")
            .distinct()
            .withColumn("product_name", F.col("product_code"))
            .withColumn("product_family", _infer_product_family())
            .withColumn("last_updated_ts", F.current_timestamp())
        )

        upsert_type1_dimension(
            spark=spark,
            source_df=source,
            target_table="bdh.dim_product",
            surrogate_key_col="product_sk",
            business_key_cols=["product_code"],
            update_columns=["product_name", "product_family", "last_updated_ts"],
        )
        logger.info("dim_product upsert complete for business_date=%s", business_date)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build BDH dim_product")
    parser.add_argument("--business-date", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    build_dim_product(args.business_date, args.run_id)


if __name__ == "__main__":
    main()
