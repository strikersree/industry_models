"""Data-quality rule engine enforced at the CURATED -> BDH gate (and
optionally BDH -> ADL). Mirrors the rule-severity model documented in
config/dq_thresholds.yaml:

    REJECT       -> row is excluded from the target table and written to
                    bdh.err_reject_log
    WARN         -> row proceeds, violation is logged for follow-up
    AUTO_CORRECT -> value is corrected in place by the rule's `corrector`

A "rule" is a small dict: {id, description, severity, predicate, corrector}
where `predicate` is a boolean pyspark Column expression that evaluates to
True for a VALID row (i.e. the rule passes).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, List, Optional

from pyspark.sql import Column, DataFrame, SparkSession, functions as F


@dataclass(frozen=True)
class DQRule:
    rule_id: str
    description: str
    severity: str  # REJECT | WARN | AUTO_CORRECT
    predicate: Column
    corrector: Optional[Callable[[DataFrame], DataFrame]] = None


@dataclass
class DQResult:
    valid_df: DataFrame
    reject_log_df: DataFrame
    total_rows: int
    passed_rows: int
    rejected_rows: int
    warned_rows: int
    pass_rate_pct: float


def evaluate_rules(spark: SparkSession, df: DataFrame, rules: List[DQRule]) -> DQResult:
    total_rows = df.count()
    working_df = df
    reject_dfs = []
    rejected_rows = 0
    warned_rows = 0

    for rule in rules:
        failing = working_df.filter(~rule.predicate)
        fail_count = failing.count()

        if fail_count == 0:
            continue

        if rule.severity == "REJECT":
            reject_dfs.append(
                failing.select(F.to_json(F.struct(*working_df.columns)).alias("rejected_row_json"))
                .withColumn("rule_id", F.lit(rule.rule_id))
                .withColumn("severity", F.lit(rule.severity))
                .withColumn("rejection_reason", F.lit(rule.description))
            )
            working_df = working_df.filter(rule.predicate)
            rejected_rows += fail_count
        elif rule.severity == "WARN":
            warned_rows += fail_count
        elif rule.severity == "AUTO_CORRECT" and rule.corrector is not None:
            working_df = rule.corrector(working_df)
        else:
            raise ValueError(f"Unsupported rule severity '{rule.severity}' for rule {rule.rule_id}")

    passed_rows = working_df.count()
    pass_rate_pct = (passed_rows / total_rows * 100.0) if total_rows > 0 else 100.0

    if reject_dfs:
        reject_log_df = reject_dfs[0]
        for extra in reject_dfs[1:]:
            reject_log_df = reject_log_df.unionByName(extra)
    else:
        reject_log_df = spark.createDataFrame(
            [], schema="rejected_row_json string, rule_id string, severity string, rejection_reason string"
        )

    return DQResult(
        valid_df=working_df,
        reject_log_df=reject_log_df,
        total_rows=total_rows,
        passed_rows=passed_rows,
        rejected_rows=rejected_rows,
        warned_rows=warned_rows,
        pass_rate_pct=pass_rate_pct,
    )


def enforce_gate(pass_rate_pct: float, threshold_pct: float, target_table: str) -> None:
    """Raise if the computed pass rate is below the configured threshold.

    Airflow tasks call this after `evaluate_rules` so a failing DQ gate fails
    the task (and the DAG's alerting takes over) instead of silently loading
    a bad batch.
    """
    if pass_rate_pct < threshold_pct:
        raise RuntimeError(
            f"DQ gate FAILED for '{target_table}': pass_rate={pass_rate_pct:.2f}% "
            f"< threshold={threshold_pct:.2f}%"
        )
