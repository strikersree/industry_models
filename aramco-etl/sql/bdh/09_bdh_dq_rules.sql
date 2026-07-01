-- ============================================================================
-- BDH (Silver) Layer -- Data Quality Scorecard
-- The actual rule *evaluation* runs in PySpark (src/aramco_etl/common/data_quality.py)
-- against config/dq_thresholds.yaml. This view exposes the resulting audit
-- trail in SQL for BI / ops dashboards and for the Airflow DQ-gate task to
-- query the latest pass rate per target table.
-- ============================================================================

USE bdh;

CREATE TABLE IF NOT EXISTS dq_audit_log (
    target_table         STRING,
    ingestion_run_id     STRING,
    business_date        DATE,
    total_rows           BIGINT,
    passed_rows          BIGINT,
    rejected_rows        BIGINT,
    warned_rows          BIGINT,
    dq_pass_rate_pct     DOUBLE,
    threshold_pct        DOUBLE,
    gate_result          STRING     COMMENT 'PASS / FAIL',
    evaluated_ts         TIMESTAMP
)
USING DELTA
PARTITIONED BY (target_table)
COMMENT 'One row per DQ-gate evaluation, per target table per run';

CREATE OR REPLACE VIEW vw_dq_latest_scorecard AS
SELECT
    target_table,
    business_date,
    total_rows,
    passed_rows,
    rejected_rows,
    dq_pass_rate_pct,
    threshold_pct,
    gate_result,
    evaluated_ts
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY target_table
            ORDER BY evaluated_ts DESC
        ) AS rn
    FROM dq_audit_log
)
WHERE rn = 1;
