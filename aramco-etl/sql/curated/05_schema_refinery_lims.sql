-- ============================================================================
-- CURATED (Bronze) Layer -- Source: Refinery LIMS (Downstream)
-- Schema: curated_refinery_lims
-- ============================================================================

CREATE DATABASE IF NOT EXISTS curated_refinery_lims
COMMENT 'Curated (Bronze) landing zone for downstream refinery LIMS lab data'
LOCATION '${DATALAKE_ROOT}/curated/refinery_lims';

USE curated_refinery_lims;

CREATE TABLE IF NOT EXISTS raw_crude_assay (
    refinery_id          STRING,
    sample_id            STRING,
    sample_ts            TIMESTAMP,
    crude_grade          STRING     COMMENT 'e.g. ARABIAN_LIGHT, ARABIAN_HEAVY, ARABIAN_EXTRA_LIGHT',
    api_gravity          DOUBLE,
    sulfur_content_pct   DOUBLE,
    water_content_pct    DOUBLE,
    _ingest_ts           TIMESTAMP,
    _source_file         STRING,
    _business_date       DATE,
    _ingestion_run_id    STRING
)
USING DELTA
PARTITIONED BY (_business_date)
COMMENT 'Raw crude oil assay lab results, per sample';

CREATE TABLE IF NOT EXISTS raw_product_quality_test (
    refinery_id          STRING,
    product_code         STRING     COMMENT 'e.g. GASOLINE_91, DIESEL_ULSD, JET_A1',
    batch_id             STRING,
    test_ts              TIMESTAMP,
    test_parameter       STRING     COMMENT 'e.g. OCTANE_NUMBER, CETANE_NUMBER, FLASH_POINT',
    test_result          DOUBLE,
    spec_min             DOUBLE,
    spec_max             DOUBLE,
    pass_fail_flag       STRING,
    _ingest_ts           TIMESTAMP,
    _source_file         STRING,
    _business_date       DATE,
    _ingestion_run_id    STRING
)
USING DELTA
PARTITIONED BY (_business_date)
COMMENT 'Raw finished-product quality test results against spec';
