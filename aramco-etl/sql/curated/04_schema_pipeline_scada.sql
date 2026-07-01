-- ============================================================================
-- CURATED (Bronze) Layer -- Source: Pipeline SCADA (Midstream)
-- Schema: curated_pipeline_scada
-- ============================================================================

CREATE DATABASE IF NOT EXISTS curated_pipeline_scada
COMMENT 'Curated (Bronze) landing zone for midstream pipeline SCADA telemetry'
LOCATION '${DATALAKE_ROOT}/curated/pipeline_scada';

USE curated_pipeline_scada;

CREATE TABLE IF NOT EXISTS raw_pipeline_flow_reading (
    pipeline_segment_id  STRING,
    reading_ts           TIMESTAMP,
    flow_rate_bpd        DOUBLE      COMMENT 'Barrels per day through the segment',
    line_pressure_psi    DOUBLE,
    line_temp_f          DOUBLE,
    pump_station_id      STRING,
    _ingest_ts           TIMESTAMP,
    _source_file         STRING,
    _business_date       DATE,
    _ingestion_run_id    STRING
)
USING DELTA
PARTITIONED BY (_business_date)
COMMENT 'Raw pipeline segment flow/pressure/temperature telemetry';

CREATE TABLE IF NOT EXISTS raw_leak_detection_alarm (
    pipeline_segment_id  STRING,
    alarm_ts             TIMESTAMP,
    alarm_type           STRING     COMMENT 'e.g. PRESSURE_DROP, MASS_BALANCE_DEVIATION, ACOUSTIC',
    severity             STRING     COMMENT 'LOW / MEDIUM / HIGH / CRITICAL',
    acknowledged_ts      TIMESTAMP,
    acknowledged_by      STRING,
    _ingest_ts           TIMESTAMP,
    _source_file         STRING,
    _business_date       DATE,
    _ingestion_run_id    STRING
)
USING DELTA
PARTITIONED BY (_business_date)
COMMENT 'Raw leak-detection system alarm stream';
