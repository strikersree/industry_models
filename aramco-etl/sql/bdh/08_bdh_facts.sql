-- ============================================================================
-- BDH (Silver) Layer -- Business Data Hub
-- Conformed fact tables. All facts are DQ-gated on write: rows failing a
-- REJECT-severity rule are diverted to bdh.err_reject_log rather than loaded.
-- Built/merged by src/aramco_etl/bdh/build_fact_*.py
-- ============================================================================

USE bdh;

-- ---------------------------------------------------------------------------
-- FACT_PRODUCTION_DAILY -- grain: well x day
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_production_daily (
    date_key             INT,
    well_field_sk        BIGINT,
    oil_volume_bbl        DOUBLE     COMMENT 'Daily oil volume, barrels',
    gas_volume_mscf       DOUBLE,
    water_volume_bbl      DOUBLE,
    avg_tubing_pressure_psi DOUBLE,
    planned_volume_bbl    DOUBLE     COMMENT 'Nominated/planned volume for OEE calc',
    uptime_hours          DOUBLE,
    downtime_hours        DOUBLE,
    _bdh_load_ts          TIMESTAMP,
    _dq_pass_rate         DOUBLE
)
USING DELTA
PARTITIONED BY (date_key)
COMMENT 'Conformed daily well production fact';

-- ---------------------------------------------------------------------------
-- FACT_EQUIPMENT_DOWNTIME -- grain: asset x downtime event
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_equipment_downtime (
    date_key             INT,
    asset_sk             BIGINT,
    downtime_start_ts    TIMESTAMP,
    downtime_end_ts      TIMESTAMP,
    downtime_hours       DOUBLE,
    downtime_reason_code STRING,
    linked_work_order_id STRING,
    _bdh_load_ts         TIMESTAMP,
    _dq_pass_rate        DOUBLE
)
USING DELTA
PARTITIONED BY (date_key)
COMMENT 'Conformed equipment downtime fact, source for MTBF/MTTR marts';

-- ---------------------------------------------------------------------------
-- FACT_MAINTENANCE_WORKORDER -- grain: work order
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_maintenance_workorder (
    date_key             INT,
    asset_sk             BIGINT,
    work_order_id        STRING,
    work_type            STRING,
    priority             STRING,
    status               STRING,
    labor_hours          DOUBLE,
    material_cost_usd    DOUBLE,
    duration_hours       DOUBLE     COMMENT 'actual_finish_ts - actual_start_ts',
    schedule_variance_hours DOUBLE  COMMENT 'actual_start_ts - scheduled_start_ts',
    _bdh_load_ts         TIMESTAMP,
    _dq_pass_rate        DOUBLE
)
USING DELTA
PARTITIONED BY (date_key)
COMMENT 'Conformed maintenance work-order fact';

-- ---------------------------------------------------------------------------
-- FACT_PIPELINE_FLOW -- grain: pipeline segment x day
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_pipeline_flow (
    date_key             INT,
    pipeline_segment_id  STRING,
    avg_flow_rate_bpd    DOUBLE,
    avg_line_pressure_psi DOUBLE,
    leak_alarm_count     INT,
    critical_alarm_count INT,
    _bdh_load_ts         TIMESTAMP,
    _dq_pass_rate        DOUBLE
)
USING DELTA
PARTITIONED BY (date_key)
COMMENT 'Conformed midstream pipeline flow/integrity fact';

-- ---------------------------------------------------------------------------
-- FACT_REFINERY_YIELD -- grain: refinery x product x day
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_refinery_yield (
    date_key             INT,
    refinery_id          STRING,
    product_sk           BIGINT,
    avg_api_gravity      DOUBLE,
    avg_sulfur_pct       DOUBLE,
    test_count           INT,
    pass_count           INT,
    fail_count           INT,
    _bdh_load_ts         TIMESTAMP,
    _dq_pass_rate        DOUBLE
)
USING DELTA
PARTITIONED BY (date_key)
COMMENT 'Conformed downstream refinery yield/quality fact';

-- ---------------------------------------------------------------------------
-- FACT_HSE_INCIDENT -- grain: incident
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_hse_incident (
    date_key             INT,
    facility_id          STRING,
    incident_id          STRING,
    incident_type        STRING,
    severity_rank        STRING,
    lost_workdays        INT,
    party_sk             BIGINT,
    _bdh_load_ts         TIMESTAMP,
    _dq_pass_rate        DOUBLE
)
USING DELTA
PARTITIONED BY (date_key)
COMMENT 'Conformed HSE incident fact, source for TRIR/LTIR marts';

-- ---------------------------------------------------------------------------
-- FACT_FINANCIAL_ACTUALS -- grain: cost center x cost element x month
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_financial_actuals (
    date_key             INT      COMMENT 'First day of fiscal period',
    cost_center_sk       BIGINT,
    cost_element_code    STRING,
    actual_amount_usd    DECIMAL(18,2),
    budget_amount_usd    DECIMAL(18,2),
    _bdh_load_ts         TIMESTAMP,
    _dq_pass_rate        DOUBLE
)
USING DELTA
PARTITIONED BY (date_key)
COMMENT 'Conformed financial actuals-vs-budget fact';

-- ---------------------------------------------------------------------------
-- Reject sink for rows failing REJECT-severity DQ rules anywhere in BDH
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS err_reject_log (
    target_table         STRING,
    rule_id              STRING,
    severity             STRING,
    rejected_row_json    STRING,
    rejection_reason     STRING,
    rejected_ts          TIMESTAMP,
    ingestion_run_id     STRING
)
USING DELTA
PARTITIONED BY (target_table)
COMMENT 'Rows rejected by BDH data-quality gate, retained for triage/reprocessing';
