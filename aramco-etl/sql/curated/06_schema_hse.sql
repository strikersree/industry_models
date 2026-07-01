-- ============================================================================
-- CURATED (Bronze) Layer -- Source: HSE (Health, Safety & Environment)
-- Schema: curated_hse
-- ============================================================================

CREATE DATABASE IF NOT EXISTS curated_hse
COMMENT 'Curated (Bronze) landing zone for Health, Safety & Environment records'
LOCATION '${DATALAKE_ROOT}/curated/hse';

USE curated_hse;

CREATE TABLE IF NOT EXISTS raw_incident_report (
    incident_id          STRING,
    facility_id          STRING,
    incident_ts          TIMESTAMP,
    incident_type        STRING     COMMENT 'e.g. LTI, MTI, FAI, NEAR_MISS, ENVIRONMENTAL_SPILL',
    severity_rank        STRING     COMMENT 'e.g. MINOR, SERIOUS, MAJOR, CATASTROPHIC',
    lost_workdays        INT,
    involved_employee_id STRING,
    involved_contractor  STRING,
    description          STRING,
    _ingest_ts           TIMESTAMP,
    _source_file         STRING,
    _business_date       DATE,
    _ingestion_run_id    STRING
)
USING DELTA
PARTITIONED BY (_business_date)
COMMENT 'Raw HSE incident report feed';

CREATE TABLE IF NOT EXISTS raw_safety_observation (
    observation_id       STRING,
    facility_id          STRING,
    observation_ts       TIMESTAMP,
    observation_category STRING     COMMENT 'e.g. UNSAFE_ACT, UNSAFE_CONDITION, POSITIVE_OBSERVATION',
    observer_employee_id STRING,
    corrective_action    STRING,
    closed_flag          BOOLEAN,
    _ingest_ts           TIMESTAMP,
    _source_file         STRING,
    _business_date       DATE,
    _ingestion_run_id    STRING
)
USING DELTA
PARTITIONED BY (_business_date)
COMMENT 'Raw behaviour-based safety observation feed';
