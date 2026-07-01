-- ============================================================================
-- CURATED (Bronze) Layer -- Source: SCADA / OSIsoft PI (Upstream)
-- Schema: curated_scada_upstream
--
-- Raw, source-shaped well and field telemetry. No business transformation is
-- applied here -- typing and light structural normalisation only. Every table
-- carries the standard curated audit columns so the BDH layer can trace every
-- conformed row back to its exact source batch.
-- ============================================================================

CREATE DATABASE IF NOT EXISTS curated_scada_upstream
COMMENT 'Curated (Bronze) landing zone for upstream SCADA / OSIsoft PI telemetry'
LOCATION '${DATALAKE_ROOT}/curated/scada_upstream';

USE curated_scada_upstream;

-- Standard curated audit columns (repeated on every curated table by convention):
--   _ingest_ts        TIMESTAMP  -- when the ingestion job wrote the row
--   _source_file      STRING     -- landing file/object the row came from
--   _business_date    DATE       -- logical business date of the batch
--   _ingestion_run_id STRING     -- Airflow run_id / job correlation id

CREATE TABLE IF NOT EXISTS raw_well_production_reading (
    well_id             STRING      COMMENT 'Source well identifier (SCADA tag root)',
    field_id            STRING,
    reading_ts          TIMESTAMP   COMMENT 'Instrument timestamp (UTC)',
    oil_rate_bopd       DOUBLE      COMMENT 'Oil rate, barrels of oil per day',
    gas_rate_mscfd      DOUBLE      COMMENT 'Gas rate, thousand standard cubic feet per day',
    water_rate_bwpd     DOUBLE      COMMENT 'Water rate, barrels of water per day',
    tubing_pressure_psi DOUBLE,
    casing_pressure_psi DOUBLE,
    choke_size_64th     INT,
    well_status_code    STRING      COMMENT 'Raw SCADA status enum (source-defined)',
    _ingest_ts          TIMESTAMP,
    _source_file        STRING,
    _business_date      DATE,
    _ingestion_run_id   STRING
)
USING DELTA
PARTITIONED BY (_business_date)
COMMENT 'Raw well production telemetry, one row per well per reading interval';

CREATE TABLE IF NOT EXISTS raw_field_header_pressure (
    field_id            STRING,
    header_id           STRING,
    reading_ts          TIMESTAMP,
    header_pressure_psi DOUBLE,
    header_temp_f       DOUBLE,
    _ingest_ts          TIMESTAMP,
    _source_file        STRING,
    _business_date      DATE,
    _ingestion_run_id   STRING
)
USING DELTA
PARTITIONED BY (_business_date)
COMMENT 'Raw gathering-header pressure/temperature readings per field';

CREATE TABLE IF NOT EXISTS raw_wellhead_events (
    well_id             STRING,
    event_ts            TIMESTAMP,
    event_type          STRING      COMMENT 'e.g. SHUT_IN, START_UP, CHOKE_CHANGE, ALARM',
    event_detail        STRING,
    raw_payload         STRING      COMMENT 'Full original JSON payload retained for replay',
    _ingest_ts          TIMESTAMP,
    _source_file        STRING,
    _business_date      DATE,
    _ingestion_run_id   STRING
)
USING DELTA
PARTITIONED BY (_business_date)
COMMENT 'Raw wellhead state-change / alarm event stream';
