"""BDH fact build -- fact_financial_actuals (grain: cost center x cost element x month).

    spark-submit src/aramco_etl/bdh/build_fact_financial_actuals.py \\
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

logger = get_logger("bdh.build_fact_financial_actuals")

TARGET_TABLE = "bdh.fact_financial_actuals"


def _financial_dq_rules() -> list[DQRule]:
    return [
        DQRule("FIN-01", "cost_center_code must not be null", "REJECT", F.col("cost_center_code").isNotNull()),
        DQRule("FIN-02", "fiscal_year must not be null", "REJECT", F.col("fiscal_year").isNotNull()),
    ]


def build_fact_financial_actuals(business_date: str, run_id: str) -> None:
    spark = get_spark_session("bdh_build_fact_financial_actuals")
    thresholds = load_dq_thresholds()

    with job_run(logger, "bdh_build_fact_financial_actuals", run_id):
        actuals = spark.table("curated_sap_erp.raw_cost_center_actuals").filter(
            F.col("_business_date") == business_date
        )

        valid_actuals = run_dq_gate(
            spark=spark, df=actuals, rules=_financial_dq_rules(), target_table=TARGET_TABLE,
            run_id=run_id, business_date=business_date, thresholds=thresholds,
        )

        current_cost_centers = spark.table("bdh.dim_cost_center")
        fact_df = (
            valid_actuals.join(current_cost_centers, on="cost_center_code", how="inner")
            .withColumn(
                "date_key",
                F.date_format(
                    F.to_date(
                        F.concat_ws("-", F.col("fiscal_year"), F.col("fiscal_period"), F.lit("01")),
                        "yyyy-M-dd",
                    ),
                    "yyyyMMdd",
                ).cast("int"),
            )
            .withColumn("_bdh_load_ts", F.current_timestamp())
            .withColumn("_dq_pass_rate", F.lit(None).cast("double"))
            .select(
                "date_key", "cost_center_sk", "cost_element_code", "actual_amount_usd",
                "budget_amount_usd", "_bdh_load_ts", "_dq_pass_rate",
            )
        )

        write_overwrite_partition(fact_df, TARGET_TABLE, partition_by=["date_key"])
        logger.info(
            "fact_financial_actuals written: %d rows for business_date=%s", fact_df.count(), business_date
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Build BDH fact_financial_actuals")
    parser.add_argument("--business-date", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    build_fact_financial_actuals(args.business_date, args.run_id)


if __name__ == "__main__":
    main()
