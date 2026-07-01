from pyspark.sql import functions as F

from aramco_etl.common.data_quality import DQRule, enforce_gate, evaluate_rules


def test_reject_rule_removes_failing_rows(spark):
    df = spark.createDataFrame(
        [("W1", 100.0), ("W2", -5.0), (None, 40.0)], schema="well_id string, oil_rate_bopd double"
    )
    rules = [
        DQRule("R1", "well_id not null", "REJECT", F.col("well_id").isNotNull()),
        DQRule("R2", "oil_rate non-negative", "REJECT", F.col("oil_rate_bopd") >= 0),
    ]

    result = evaluate_rules(spark, df, rules)

    assert result.total_rows == 3
    assert result.passed_rows == 1
    assert result.rejected_rows == 2
    assert result.valid_df.collect()[0]["well_id"] == "W1"
    assert result.reject_log_df.count() == 2


def test_warn_rule_keeps_row_but_counts_violation(spark):
    df = spark.createDataFrame([("W1", 20000.0)], schema="well_id string, tubing_pressure_psi double")
    rules = [DQRule("R3", "pressure plausible", "WARN", F.col("tubing_pressure_psi").between(0, 15000))]

    result = evaluate_rules(spark, df, rules)

    assert result.passed_rows == 1
    assert result.rejected_rows == 0
    assert result.warned_rows == 1


def test_enforce_gate_raises_below_threshold():
    try:
        enforce_gate(pass_rate_pct=90.0, threshold_pct=97.0, target_table="bdh.fact_production_daily")
    except RuntimeError as exc:
        assert "FAILED" in str(exc)
    else:
        raise AssertionError("enforce_gate should have raised for a sub-threshold pass rate")


def test_enforce_gate_passes_at_or_above_threshold():
    enforce_gate(pass_rate_pct=97.0, threshold_pct=97.0, target_table="bdh.fact_production_daily")
