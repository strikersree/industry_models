-- ============================================================================
-- ADL (Gold) Layer -- Executive & Operational Views
-- ============================================================================

USE adl;

CREATE TABLE IF NOT EXISTS mart_executive_summary (
    year_month              STRING,
    total_oil_bbl           DOUBLE,
    production_efficiency_pct DOUBLE,
    avg_asset_availability_pct DOUBLE,
    trir                     DOUBLE,
    ltir                     DOUBLE,
    total_cost_usd           DECIMAL(18,2),
    avg_cost_per_bbl_usd      DOUBLE,
    critical_pipeline_alarms  INT,
    _adl_load_ts              TIMESTAMP
)
USING DELTA
PARTITIONED BY (year_month)
COMMENT 'Company-wide monthly executive KPI roll-up across all marts';

CREATE OR REPLACE VIEW vw_exec_dashboard_latest AS
SELECT *
FROM mart_executive_summary
WHERE year_month = (SELECT MAX(year_month) FROM mart_executive_summary);

CREATE OR REPLACE VIEW vw_hse_alert_facilities AS
SELECT
    facility_id,
    year_month,
    trir,
    ltir,
    total_incidents
FROM mart_hse_scorecard
WHERE trir > 1.0  -- above industry-typical upstream oil & gas benchmark
ORDER BY trir DESC;

CREATE OR REPLACE VIEW vw_reliability_watchlist AS
SELECT
    asset_id,
    facility_id,
    year_month,
    mtbf_hours,
    mttr_hours,
    availability_pct
FROM mart_equipment_reliability
WHERE availability_pct < 95.0
ORDER BY availability_pct ASC;
