"""Shared DQ-gate helper used by every BDH fact builder.

Wraps common.data_quality.evaluate_rules + enforce_gate with the
bookkeeping every fact build needs: writing rejects to bdh.err_reject_log,
writing a row to bdh.dq_audit_log, and pulling the pass-rate threshold for
this target table out of config/dq_thresholds.yaml.
"""
from __future__ import annotations

from typing import List

from pyspark.sql import DataFrame, SparkSession, functions as F

from aramco_etl.common.data_quality import DQRule, enforce_gate, evaluate_rules
from aramco_etl.common.logging_utils import get_logger

logger = get_logger("bdh.dq_gate")


def _threshold_for(target_table: str, thresholds: dict) -> float:
    table_name = target_table.split(".")[-1]
    gates = thresholds["gates"]["bdh"]
    return gates.get(table_name, gates["default"])


def run_dq_gate(
    spark: SparkSession,
    df: DataFrame,
    rules: List[DQRule],
    target_table: str,
    run_id: str,
    business_date: str,
    thresholds: dict,
) -> DataFrame:
    """Evaluate `rules` against `df`, persist audit/reject rows, enforce the
    gate, and return the valid rows to load into `target_table`."""
    result = evaluate_rules(spark, df, rules)
    threshold_pct = _threshold_for(target_table, thresholds)
    gate_result = "PASS" if result.pass_rate_pct >= threshold_pct else "FAIL"

    if result.rejected_rows > 0:
        (
            result.reject_log_df.withColumn("target_table", F.lit(target_table))
            .withColumn("rejected_ts", F.current_timestamp())
            .withColumn("ingestion_run_id", F.lit(run_id))
            .select(
                "target_table", "rule_id", "severity", "rejected_row_json",
                "rejection_reason", "rejected_ts", "ingestion_run_id",
            )
            .write.format("delta").mode("append").saveAsTable("bdh.err_reject_log")
        )

    audit_row = spark.createDataFrame(
        [(
            target_table, run_id, business_date, result.total_rows, result.passed_rows,
            result.rejected_rows, result.warned_rows, result.pass_rate_pct, threshold_pct, gate_result,
        )],
        schema=(
            "target_table string, ingestion_run_id string, business_date string, total_rows long, "
            "passed_rows long, rejected_rows long, warned_rows long, dq_pass_rate_pct double, "
            "threshold_pct double, gate_result string"
        ),
    ).withColumn("business_date", F.to_date("business_date")).withColumn(
        "evaluated_ts", F.current_timestamp()
    )
    audit_row.write.format("delta").mode("append").saveAsTable("bdh.dq_audit_log")

    logger.info(
        "DQ gate for %s: pass_rate=%.2f%% threshold=%.2f%% result=%s",
        target_table, result.pass_rate_pct, threshold_pct, gate_result,
    )
    enforce_gate(result.pass_rate_pct, threshold_pct, target_table)
    return result.valid_df
