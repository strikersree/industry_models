"""BDH fact build -- fact_refinery_yield (grain: refinery x product x day).

    spark-submit src/aramco_etl/bdh/build_fact_refinery_yield.py \\
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

logger = get_logger("bdh.build_fact_refinery_yield")

TARGET_TABLE = "bdh.fact_refinery_yield"


def _assay_dq_rules() -> list[DQRule]:
    return [DQRule("YLD-01", "refinery_id must not be null", "REJECT", F.col("refinery_id").isNotNull())]


def _quality_dq_rules() -> list[DQRule]:
    return [
        DQRule("YLD-01", "refinery_id must not be null", "REJECT", F.col("refinery_id").isNotNull()),
        DQRule("YLD-02", "product_code must not be null", "REJECT", F.col("product_code").isNotNull()),
    ]


def build_fact_refinery_yield(business_date: str, run_id: str) -> None:
    spark = get_spark_session("bdh_build_fact_refinery_yield")
    thresholds = load_dq_thresholds()

    with job_run(logger, "bdh_build_fact_refinery_yield", run_id):
        assays = spark.table("curated_refinery_lims.raw_crude_assay").filter(
            F.col("_business_date") == business_date
        )
        valid_assays = run_dq_gate(
            spark=spark, df=assays, rules=_assay_dq_rules(),
            target_table=TARGET_TABLE, run_id=run_id, business_date=business_date, thresholds=thresholds,
        )
        assay_agg = valid_assays.groupBy("refinery_id").agg(
            F.avg("api_gravity").alias("avg_api_gravity"),
            F.avg("sulfur_content_pct").alias("avg_sulfur_pct"),
        )

        quality_tests = spark.table("curated_refinery_lims.raw_product_quality_test").filter(
            F.col("_business_date") == business_date
        )
        valid_tests = run_dq_gate(
            spark=spark, df=quality_tests, rules=_quality_dq_rules(), target_table=TARGET_TABLE,
            run_id=run_id, business_date=business_date, thresholds=thresholds,
        )
        current_products = spark.table("bdh.dim_product")
        test_agg = (
            valid_tests.join(current_products, on="product_code", how="inner")
            .groupBy("refinery_id", "product_sk")
            .agg(
                F.count("*").cast("int").alias("test_count"),
                F.sum(F.when(F.col("pass_fail_flag") == "PASS", 1).otherwise(0)).cast("int").alias("pass_count"),
                F.sum(F.when(F.col("pass_fail_flag") == "FAIL", 1).otherwise(0)).cast("int").alias("fail_count"),
            )
        )

        fact_df = (
            test_agg.join(assay_agg, on="refinery_id", how="left")
            .withColumn("date_key", F.date_format(F.lit(business_date), "yyyyMMdd").cast("int"))
            .withColumn("_bdh_load_ts", F.current_timestamp())
            .withColumn("_dq_pass_rate", F.lit(None).cast("double"))
            .select(
                "date_key", "refinery_id", "product_sk", "avg_api_gravity", "avg_sulfur_pct",
                "test_count", "pass_count", "fail_count", "_bdh_load_ts", "_dq_pass_rate",
            )
        )

        write_overwrite_partition(fact_df, TARGET_TABLE, partition_by=["date_key"])
        logger.info("fact_refinery_yield written: %d rows for business_date=%s", fact_df.count(), business_date)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build BDH fact_refinery_yield")
    parser.add_argument("--business-date", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    build_fact_refinery_yield(args.business_date, args.run_id)


if __name__ == "__main__":
    main()
