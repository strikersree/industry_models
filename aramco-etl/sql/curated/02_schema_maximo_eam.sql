-- ============================================================================
-- CURATED (Bronze) Layer -- Source: IBM Maximo (Enterprise Asset Management)
-- Schema: curated_maximo_eam
-- ============================================================================

CREATE DATABASE IF NOT EXISTS curated_maximo_eam
COMMENT 'Curated (Bronze) landing zone for Maximo EAM asset & maintenance data'
LOCATION '${DATALAKE_ROOT}/curated/maximo_eam';

USE curated_maximo_eam;

CREATE TABLE IF NOT EXISTS raw_asset_master (
    asset_id            STRING      COMMENT 'Maximo ASSETNUM',
    asset_description   STRING,
    asset_type          STRING      COMMENT 'e.g. PUMP, COMPRESSOR, VESSEL, PIPELINE_SEGMENT',
    facility_id         STRING,
    location_code       STRING,
    manufacturer        STRING,
    install_date        DATE,
    criticality_rank    STRING      COMMENT 'Maximo criticality: A / B / C',
    parent_asset_id     STRING,
    record_status       STRING      COMMENT 'ACTIVE / DECOMMISSIONED / STANDBY',
    last_modified_ts    TIMESTAMP,
    _ingest_ts          TIMESTAMP,
    _source_file        STRING,
    _business_date      DATE,
    _ingestion_run_id   STRING
)
USING DELTA
PARTITIONED BY (_business_date)
COMMENT 'Raw asset master extract, full snapshot per load (source of BDH SCD2 asset dimension)';

CREATE TABLE IF NOT EXISTS raw_work_order (
    work_order_id        STRING,
    asset_id             STRING,
    work_type            STRING     COMMENT 'PM (Preventive) / CM (Corrective) / EM (Emergency)',
    priority             STRING,
    status               STRING     COMMENT 'WAPPR, APPR, INPRG, COMP, CLOSE, CAN',
    problem_code         STRING,
    reported_ts          TIMESTAMP,
    scheduled_start_ts   TIMESTAMP,
    actual_start_ts      TIMESTAMP,
    actual_finish_ts     TIMESTAMP,
    labor_hours          DOUBLE,
    material_cost_usd    DOUBLE,
    assigned_crew_id     STRING,
    _ingest_ts           TIMESTAMP,
    _source_file         STRING,
    _business_date       DATE,
    _ingestion_run_id    STRING
)
USING DELTA
PARTITIONED BY (_business_date)
COMMENT 'Raw work order transactions (preventive/corrective/emergency maintenance)';

CREATE TABLE IF NOT EXISTS raw_downtime_event (
    asset_id             STRING,
    downtime_start_ts    TIMESTAMP,
    downtime_end_ts      TIMESTAMP,
    downtime_reason_code STRING     COMMENT 'e.g. MECH_FAILURE, PLANNED_TURNAROUND, POWER_LOSS',
    linked_work_order_id STRING,
    _ingest_ts           TIMESTAMP,
    _source_file         STRING,
    _business_date       DATE,
    _ingestion_run_id    STRING
)
USING DELTA
PARTITIONED BY (_business_date)
COMMENT 'Raw equipment downtime event log, source for MTBF/MTTR reliability facts';
