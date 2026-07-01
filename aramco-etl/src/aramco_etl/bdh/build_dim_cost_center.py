"""BDH dimension build -- dim_cost_center (Type-1, overwrite in place).

Conforms curated_sap_erp.raw_cost_center_actuals into a stable reference
dimension joined by fact_financial_actuals / mart_financial_cost.

    spark-submit src/aramco_etl/bdh/build_dim_cost_center.py \\
        --business-date 2026-07-01 --run-id <airflow_run_id>
"""
from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from aramco_etl.common.logging_utils import get_logger, job_run
from aramco_etl.common.spark_session import get_spark_session
from aramco_etl.common.type1_dim import upsert_type1_dimension

logger = get_logger("bdh.build_dim_cost_center")

# SAP cost-center code prefix -> business domain, used for cost-per-domain KPIs.
COST_CENTER_DOMAIN_MAP = {
    "UP": "upstream",
    "MS": "midstream",
    "DS": "downstream",
}


def _infer_business_domain():
    return F.coalesce(
        *[
            F.when(F.col("cost_center_code").startswith(prefix), F.lit(domain))
            for prefix, domain in COST_CENTER_DOMAIN_MAP.items()
        ],
        F.lit("other"),
    )


def build_dim_cost_center(business_date: str, run_id: str) -> None:
    spark = get_spark_session("bdh_build_dim_cost_center")
    with job_run(logger, "bdh_build_dim_cost_center", run_id):
        raw = spark.table("curated_sap_erp.raw_cost_center_actuals").filter(
            F.col("_business_date") == business_date
        )
        source = (
            raw.select("cost_center_code")
            .distinct()
            .withColumn("cost_center_name", F.col("cost_center_code"))
            .withColumn("business_domain", _infer_business_domain())
            .withColumn("last_updated_ts", F.current_timestamp())
        )

        upsert_type1_dimension(
            spark=spark,
            source_df=source,
            target_table="bdh.dim_cost_center",
            surrogate_key_col="cost_center_sk",
            business_key_cols=["cost_center_code"],
            update_columns=["cost_center_name", "business_domain", "last_updated_ts"],
        )
        logger.info("dim_cost_center upsert complete for business_date=%s", business_date)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build BDH dim_cost_center")
    parser.add_argument("--business-date", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    build_dim_cost_center(args.business_date, args.run_id)


if __name__ == "__main__":
    main()
