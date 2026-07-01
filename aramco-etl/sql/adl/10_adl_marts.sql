-- ============================================================================
-- ADL (Gold) Layer -- Analytics Data Layer
-- Pre-aggregated, BI-ready KPI marts. Built from BDH facts/dimensions by
-- src/aramco_etl/adl/mart_*.py. Consumed by Power BI / Tableau / self-service SQL.
-- ============================================================================

CREATE DATABASE IF NOT EXISTS adl
COMMENT 'ADL (Gold) -- curated KPI marts for BI / reporting consumption'
LOCATION '${DATALAKE_ROOT}/adl';

USE adl;

-- ---------------------------------------------------------------------------
-- MART_PRODUCTION_KPI -- grain: field x day
-- OEE-style production KPIs for upstream operations dashboards.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart_production_kpi (
    date_key             INT,
    field_id             STRING,
    total_oil_bbl        DOUBLE,
    total_gas_mscf       DOUBLE,
    total_water_bbl      DOUBLE,
    planned_volume_bbl   DOUBLE,
    production_efficiency_pct DOUBLE COMMENT 'actual/planned volume, capped at 100',
    uptime_pct           DOUBLE      COMMENT 'uptime_hours / 24 across wells in the field',
    _adl_load_ts         TIMESTAMP
)
USING DELTA
PARTITIONED BY (date_key)
COMMENT 'Daily field-level production efficiency & uptime KPIs';

-- ---------------------------------------------------------------------------
-- MART_EQUIPMENT_RELIABILITY -- grain: asset x month
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart_equipment_reliability (
    year_month           STRING      COMMENT 'yyyy-MM',
    asset_id             STRING,
    facility_id          STRING,
    failure_count        INT,
    total_downtime_hours DOUBLE,
    mtbf_hours           DOUBLE      COMMENT 'Mean Time Between Failures',
    mttr_hours           DOUBLE      COMMENT 'Mean Time To Repair',
    availability_pct     DOUBLE,
    _adl_load_ts         TIMESTAMP
)
USING DELTA
PARTITIONED BY (year_month)
COMMENT 'Monthly asset reliability KPIs (MTBF / MTTR / availability)';

-- ---------------------------------------------------------------------------
-- MART_HSE_SCORECARD -- grain: facility x month
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart_hse_scorecard (
    year_month           STRING,
    facility_id          STRING,
    total_incidents      INT,
    lost_time_incidents  INT,
    total_lost_workdays  INT,
    labor_hours_worked   DOUBLE,
    trir                 DOUBLE      COMMENT 'Total Recordable Incident Rate = incidents * 200000 / hours worked',
    ltir                 DOUBLE      COMMENT 'Lost Time Incident Rate = LTIs * 200000 / hours worked',
    _adl_load_ts         TIMESTAMP
)
USING DELTA
PARTITIONED BY (year_month)
COMMENT 'Monthly HSE safety-performance scorecard (TRIR / LTIR, OSHA-style formula)';

-- ---------------------------------------------------------------------------
-- MART_FINANCIAL_COST -- grain: cost center x month
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart_financial_cost (
    year_month           STRING,
    cost_center_code     STRING,
    business_domain      STRING,
    actual_cost_usd      DECIMAL(18,2),
    budget_cost_usd      DECIMAL(18,2),
    variance_pct         DOUBLE,
    production_bbl       DOUBLE,
    cost_per_bbl_usd     DOUBLE      COMMENT 'actual_cost_usd / production_bbl for upstream cost centers',
    _adl_load_ts         TIMESTAMP
)
USING DELTA
PARTITIONED BY (year_month)
COMMENT 'Monthly cost-center financial performance, incl. unit cost per barrel';

-- ---------------------------------------------------------------------------
-- MART_PIPELINE_INTEGRITY -- grain: pipeline segment x month
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart_pipeline_integrity (
    year_month           STRING,
    pipeline_segment_id  STRING,
    avg_flow_rate_bpd    DOUBLE,
    total_alarms         INT,
    critical_alarms      INT,
    alarm_rate_per_1000bbl DOUBLE,
    _adl_load_ts         TIMESTAMP
)
USING DELTA
PARTITIONED BY (year_month)
COMMENT 'Monthly midstream pipeline integrity/leak-alarm KPIs';
